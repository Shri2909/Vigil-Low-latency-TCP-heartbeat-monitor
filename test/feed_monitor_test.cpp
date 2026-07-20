// Integration tests: real loopback sockets, a real FeedSimulator (or, where a
// specific failure mode needs more control than FeedSimulator currently
// offers -- fault injection is Phase 9 -- a minimal raw TestPeerListener
// borrowed from connection_test.cpp's pattern), and FeedMonitor::run() driven
// on a background thread the way a real process would drive it.
//
// Thread-safety note (see feed_monitor.h): only stop() is safe to call from
// another thread while run() is active. Every other interaction below either
// happens before run() starts, after stop()+join, through the tick-callback
// mechanism (which executes on the reactor thread and only hands data to the
// test thread through a mutex-protected StatsBox), or through real socket I/O
// on the *other* end of a connection (not a C++-level race).

#include "config.h"
#include "feed_monitor.h"
#include "feed_simulator.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// Mutex-protected, test-only: the one deliberate exception to "no locks" in
// this project, since it exists purely to let a test thread observe snapshots
// produced on the reactor thread. Production code (main.cpp, Phase 8) uses
// the same tick callback synchronously on the reactor thread and needs no
// lock at all.
class StatsBox {
public:
    void set(AggregateStats s) {
        std::lock_guard<std::mutex> lock(mu_);
        latest_ = std::move(s);
        has_value_ = true;
    }
    std::optional<AggregateStats> get() const {
        std::lock_guard<std::mutex> lock(mu_);
        if (!has_value_) return std::nullopt;
        return latest_;
    }

private:
    mutable std::mutex mu_;
    AggregateStats latest_;
    bool has_value_ = false;
};

template <typename Pred>
bool wait_until(Pred pred, int timeout_ms = 5000, int poll_interval_ms = 10) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    do {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
    } while (std::chrono::steady_clock::now() < deadline);
    return pred();
}

std::optional<ConnectionState> state_of(const AggregateStats& stats, uint32_t feed_id) {
    for (const auto& f : stats.feeds) {
        if (f.feed_id == feed_id) return f.state;
    }
    return std::nullopt;
}

uint64_t reconnects_of(const AggregateStats& stats, uint32_t feed_id) {
    for (const auto& f : stats.feeds) {
        if (f.feed_id == feed_id) return f.stats.reconnects;
    }
    return 0;
}

uint64_t acked_of(const AggregateStats& stats, uint32_t feed_id) {
    for (const auto& f : stats.feeds) {
        if (f.feed_id == feed_id) return f.stats.heartbeats_acked;
    }
    return 0;
}

// Runs a FeedMonitor's run() on a background thread and stops+joins it on
// destruction. Every test below constructs the monitor, calls add_feed() and
// set_tick_callback() (both pre-run(), so no race), then wraps it in this.
class MonitorBackgroundRun {
public:
    explicit MonitorBackgroundRun(FeedMonitor& monitor)
        : monitor_(monitor), thread_([&monitor] { monitor.run(); }) {}
    ~MonitorBackgroundRun() {
        monitor_.stop();
        if (thread_.joinable()) thread_.join();
    }
    MonitorBackgroundRun(const MonitorBackgroundRun&) = delete;
    MonitorBackgroundRun& operator=(const MonitorBackgroundRun&) = delete;

private:
    FeedMonitor& monitor_;
    std::thread thread_;
};

class SimulatorBackgroundRun {
public:
    explicit SimulatorBackgroundRun(FeedSimulator& sim) : sim_(sim), thread_([&sim] { sim.run(); }) {}
    ~SimulatorBackgroundRun() {
        sim_.stop();
        if (thread_.joinable()) thread_.join();
    }
    SimulatorBackgroundRun(const SimulatorBackgroundRun&) = delete;
    SimulatorBackgroundRun& operator=(const SimulatorBackgroundRun&) = delete;

private:
    FeedSimulator& sim_;
    std::thread thread_;
};

// Redirects the process's stderr (fd 2) to a temp file for the lifetime of
// this object, so a test can verify FeedMonitor::emit_failure_alert's real
// log line actually fires -- it writes straight to std::cerr with no
// callback hook, so this is the only way to observe it without adding
// production API surface just for testability. Construct it around exactly
// the window under test (after any setup logging that isn't part of what's
// being asserted); call finish() once that window is over (typically right
// after the background reactor thread has been stopped+joined) to restore
// the real stderr and read back what was captured.
class StderrCapture {
public:
    StderrCapture() {
        char path_template[] = "/tmp/feed_monitor_test_stderr_XXXXXX";
        capture_fd_ = ::mkstemp(path_template);
        path_ = path_template;
        std::fflush(stderr);
        saved_stderr_fd_ = ::dup(fileno(stderr));
        ::dup2(capture_fd_, fileno(stderr));
    }
    ~StderrCapture() {
        if (saved_stderr_fd_ >= 0) {
            finish();
        }
    }
    StderrCapture(const StderrCapture&) = delete;
    StderrCapture& operator=(const StderrCapture&) = delete;

    // Restores the real stderr and returns everything captured while
    // redirected. Safe to call at most once; the destructor is a no-op if
    // this already ran.
    std::string finish() {
        std::fflush(stderr);
        ::dup2(saved_stderr_fd_, fileno(stderr));
        ::close(saved_stderr_fd_);
        saved_stderr_fd_ = -1;

        std::ifstream in(path_);
        std::ostringstream ss;
        ss << in.rdbuf();

        ::close(capture_fd_);
        ::unlink(path_.c_str());
        return ss.str();
    }

private:
    int capture_fd_ = -1;
    int saved_stderr_fd_ = -1;
    std::string path_;
};

Config fast_test_config() {
    Config config;
    config.heartbeat_interval = std::chrono::milliseconds(30);
    config.heartbeat_timeout = std::chrono::milliseconds(100);
    config.max_missed_heartbeats = 2;
    config.reconnect_base_delay = std::chrono::milliseconds(50);
    config.reconnect_max_delay = std::chrono::milliseconds(500);
    config.tick_interval = std::chrono::milliseconds(10);
    return config;
}

// Returns a TCP port on 127.0.0.1 with a guaranteed-closed listener -- bind,
// discover the assigned port, then close it. Connecting here is guaranteed to
// get a prompt ECONNREFUSED on loopback, no fault-injection machinery needed.
uint16_t closed_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &len);
    const uint16_t port = ntohs(bound.sin_port);
    ::close(fd);
    return port;
}

// A minimal raw listening peer that can accept multiple sequential
// connections (unlike connection_test.cpp's single-peer version) -- needed
// here to observe a *second* real TCP connection arriving after a forced
// disconnect, which is the concrete, external proof that reconnect actually
// re-connects rather than just flipping an internal enum.
class MultiAcceptListener {
public:
    MultiAcceptListener() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 4);
        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &len);
        port_ = ntohs(bound.sin_port);
    }
    ~MultiAcceptListener() {
        for (int fd : accepted_) ::close(fd);
        ::close(listen_fd_);
    }
    MultiAcceptListener(const MultiAcceptListener&) = delete;
    MultiAcceptListener& operator=(const MultiAcceptListener&) = delete;

    uint16_t port() const { return port_; }

    int accept_one() {
        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd >= 0) accepted_.push_back(fd);
        return fd;
    }

private:
    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::vector<int> accepted_;
};

void send_wire(int fd, const HeartbeatMessage& msg) {
    HeartbeatWireBuffer wire;
    encode_heartbeat(msg, wire);
    size_t total = 0;
    while (total < wire.size()) {
        const ssize_t n = ::send(fd, wire.data() + total, wire.size() - total, 0);
        REQUIRE(n > 0);
        total += static_cast<size_t>(n);
    }
}

HeartbeatMessage recv_wire(int fd) {
    HeartbeatWireBuffer wire{};
    size_t total = 0;
    while (total < wire.size()) {
        const ssize_t n = ::recv(fd, wire.data() + total, wire.size() - total, 0);
        REQUIRE(n > 0);
        total += static_cast<size_t>(n);
    }
    HeartbeatMessage msg{};
    REQUIRE(decode_heartbeat(wire, &msg));
    return msg;
}

void manual_handshake(int fd, uint32_t feed_id) {
    const HeartbeatMessage hello = recv_wire(fd);
    REQUIRE(static_cast<MessageType>(hello.type) == MessageType::kConnectHello);
    REQUIRE(hello.feed_id == feed_id);
    HeartbeatMessage ack{};
    ack.type = static_cast<uint8_t>(MessageType::kConnectAck);
    ack.feed_id = feed_id;
    send_wire(fd, ack);
}

}  // namespace

TEST_CASE("add_feed reaches HEALTHY within heartbeat_timeout") {
    FeedSimulator sim(0, /*feed_id=*/1, Config{});
    const uint16_t sim_port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", sim_port}));

    MonitorBackgroundRun monitor_bg(monitor);

    const bool became_healthy = wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    });
    CHECK(became_healthy);
}

TEST_CASE("remove_feed's swap-and-pop does not corrupt the remaining feeds' event routing") {
    FeedSimulator sim_a(0, 1, Config{});
    FeedSimulator sim_b(0, 2, Config{});
    FeedSimulator sim_c(0, 3, Config{});
    const uint16_t port_a = sim_a.port(), port_b = sim_b.port(), port_c = sim_c.port();
    SimulatorBackgroundRun bg_a(sim_a), bg_b(sim_b), bg_c(sim_c);

    FeedMonitor monitor(fast_test_config());
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "A", "127.0.0.1", port_a}));
    REQUIRE(monitor.add_feed(FeedEndpoint{2, "B", "127.0.0.1", port_b}));
    REQUIRE(monitor.add_feed(FeedEndpoint{3, "C", "127.0.0.1", port_c}));

    // Remove the middle slot before run() starts -- still single-threaded
    // here, so this is safe and isolates the swap-and-pop bookkeeping itself.
    // Slot C (the last one) moves into slot B's old position; if index maps
    // aren't repointed correctly, feed 3's future epoll events would either
    // vanish or misroute to whatever now occupies its old index.
    monitor.remove_feed(2);

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    MonitorBackgroundRun monitor_bg(monitor);

    const bool both_healthy = wait_until([&box] {
        auto s = box.get();
        return s && s->feeds.size() == 2 && state_of(*s, 1) == ConnectionState::kHealthy &&
               state_of(*s, 3) == ConnectionState::kHealthy;
    });
    CHECK(both_healthy);
}

TEST_CASE("remove_slot_at does not resurrect a stale index_of_fd_ entry for a moved kFailed slot") {
    // Regression test for a real bug: on_connection_failed erases the dead
    // fd from index_of_fd_ without closing it (the fd stays open, just
    // unmapped, until the slot's next successful rebind()). If that kFailed
    // slot later gets moved by remove_slot_at's swap-and-pop -- because a
    // *different*, earlier-indexed slot is removed -- the old code
    // unconditionally repointed index_of_fd_[connection.fd()], and
    // connection.fd() is still >= 0 for a kFailed connection even though it
    // must NOT be registered there. That silently resurrected the exact
    // entry on_connection_failed had just erased, leaking it (permanently,
    // if the slot later gives up retrying).
    FeedSimulator sim_b(0, /*feed_id=*/2, Config{});
    const uint16_t port_b = sim_b.port();
    SimulatorBackgroundRun sim_bg(sim_b);

    Config config = fast_test_config();
    // Long enough that feed A is still sitting in backoff (not yet
    // reconnected) by the time this test checks it -- the bug only manifests
    // while it's kFailed, before rebind() ever runs again.
    config.reconnect_base_delay = std::chrono::seconds(10);
    config.reconnect_max_delay = std::chrono::seconds(10);
    FeedMonitor monitor(config);

    // B added first (index 0, stays healthy); A added second (index 1 =
    // last, fails against a closed port and stays in backoff). This
    // ordering is what makes the *failed* slot the one swap-and-pop moves
    // when B is removed below.
    REQUIRE(monitor.add_feed(FeedEndpoint{2, "B", "127.0.0.1", port_b}));
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "A", "127.0.0.1", closed_port()}));

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "remove-b") monitor.remove_feed(2);
    });

    {
        MonitorBackgroundRun monitor_bg(monitor);

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && state_of(*s, 2) == ConnectionState::kHealthy &&
                   state_of(*s, 1) == ConnectionState::kFailed;
        }));

        const std::string cmd = "remove-b\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && s->feeds.size() == 1;
        }));
    }
    // safe now: MonitorBackgroundRun's destructor already stopped+joined the
    // reactor thread, so there is no concurrent access to index_of_fd_.
    CHECK(monitor.debug_fd_index_count() == 0);  // A is the only slot left, and it's kFailed

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("a dead connection is detected, and reconnect re-establishes a real new connection") {
    MultiAcceptListener peer;
    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    MonitorBackgroundRun monitor_bg(monitor);

    const int peer_fd1 = peer.accept_one();
    REQUIRE(peer_fd1 >= 0);
    manual_handshake(peer_fd1, 1);
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    ::close(peer_fd1);  // simulate the exchange vanishing

    // Blocks until FeedMonitor's background thread actually dials back in --
    // concrete, external proof that reconnect happened, not just an internal
    // state flag flipping.
    const int peer_fd2 = peer.accept_one();
    REQUIRE(peer_fd2 >= 0);
    manual_handshake(peer_fd2, 1);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    const auto* snap = [&]() -> const FeedSnapshot* {
        for (auto& f : final_snapshot->feeds)
            if (f.feed_id == 1) return &f;
        return nullptr;
    }();
    REQUIRE(snap != nullptr);
    CHECK(snap->stats.reconnects >= 1u);
}

TEST_CASE("reconnect_delay_ns doubles with each attempt, capped, within +/-25% jitter") {
    Config config;
    config.reconnect_base_delay = std::chrono::milliseconds(1000);
    config.reconnect_max_delay = std::chrono::milliseconds(30000);
    FeedMonitor monitor(config);

    const int64_t base_ns = 1'000'000'000LL;
    const int64_t max_ns = 30'000'000'000LL;
    // attempt 0 -> 1x base, attempt 1 -> 2x, attempt 2 -> 4x, ... capped at max_ns.
    for (int attempt = 0; attempt <= 6; ++attempt) {
        const int64_t uncapped = base_ns * (int64_t{1} << attempt);
        const int64_t expected = std::min(uncapped, max_ns);
        // Sample repeatedly since jitter randomizes each call -- every
        // sample must still land within the documented +/-25% band.
        for (int i = 0; i < 20; ++i) {
            const int64_t delay = monitor.debug_reconnect_delay_ns(attempt);
            CHECK(delay >= static_cast<int64_t>(static_cast<double>(expected) * 0.75));
            CHECK(delay <= static_cast<int64_t>(static_cast<double>(expected) * 1.25));
        }
    }
}

TEST_CASE("reconnect_delay_ns jitter actually varies across calls, not a fixed value") {
    // Proof the +/-25% jitter is real randomization, not a no-op -- if this
    // ever regressed to a constant, every failing feed configured with the
    // same base delay would retry in lockstep, exactly the thundering-herd
    // failure mode jitter exists to prevent.
    Config config;
    config.reconnect_base_delay = std::chrono::milliseconds(1000);
    config.reconnect_max_delay = std::chrono::milliseconds(30000);
    FeedMonitor monitor(config);

    std::set<int64_t> distinct_values;
    for (int i = 0; i < 30; ++i) {
        distinct_values.insert(monitor.debug_reconnect_delay_ns(0));
    }
    CHECK(distinct_values.size() > 1u);
}

TEST_CASE("repeated failed reconnect attempts against an offline peer are spaced out, not tight-looped") {
    // Wall-clock proof (not just the pure-function unit test above) that the
    // monitor actually waits between real reconnect attempts instead of
    // hammering the peer -- this is what "the monitor will not reconnect in
    // a tight loop" actually means end to end.
    MultiAcceptListener peer;

    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::milliseconds(80);
    config.reconnect_max_delay = std::chrono::milliseconds(2000);
    config.max_reconnect_attempts = -1;
    FeedMonitor monitor(config);
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    MonitorBackgroundRun monitor_bg(monitor);

    // Each accepted connection is closed immediately without ever
    // completing the handshake, forcing another failure+backoff+reconnect
    // cycle -- four real, observed connection attempts in total.
    std::vector<std::chrono::steady_clock::time_point> accept_times;
    for (int i = 0; i < 4; ++i) {
        const int fd = peer.accept_one();
        REQUIRE(fd >= 0);
        accept_times.push_back(std::chrono::steady_clock::now());
        ::close(fd);
    }

    // The very first retry must already be meaningfully delayed, not
    // near-instant: base_delay=80ms with -25% jitter floors at 60ms; allow
    // generous scheduling slack down to 30ms so this isn't flaky.
    const auto gap1 = accept_times[1] - accept_times[0];
    CHECK(gap1 >= std::chrono::milliseconds(30));

    // Later gaps should be roughly growing (doubling, within jitter and
    // scheduling noise) -- proof this is exponential backoff, not a fixed
    // small retry interval repeated forever.
    const auto gap2 = accept_times[2] - accept_times[1];
    const auto gap3 = accept_times[3] - accept_times[2];
    CHECK(gap2 > gap1 / 2);
    CHECK(gap3 > gap2 / 2);
}

TEST_CASE("dropped PONGs drive HEALTHY -> DEGRADED -> FAILED, emit a real alert, and the feed reconnects") {
    // This is the complete "silent feed failure" story the whole project
    // exists to detect, exercised through the real fault-injection path
    // (FeedSimulator's drop_probability), not a hand-crafted message: PING
    // sent, no PONG, timeout detected, HEALTHY -> DEGRADED, miss limit
    // reached, DEGRADED -> FAILED, a real alert line logged, and (since
    // drop_probability only affects PONG replies -- the handshake is
    // unaffected, see FeedSimulator::on_ping_intercept) a reconnect actually
    // succeeds and the feed comes back HEALTHY before dropping into the same
    // cycle again.
    FaultConfig faults;
    faults.drop_probability = 1.0;
    FeedSimulator sim(0, 1, Config{}, faults);
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    Config config = fast_test_config();
    config.max_reconnect_attempts = -1;  // must actually retry, not give up after the first failure
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "NASDAQ", "127.0.0.1", port}));

    StderrCapture stderr_capture;
    {
        MonitorBackgroundRun monitor_bg(monitor);

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && state_of(*s, 1) == ConnectionState::kHealthy;
        }));

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && state_of(*s, 1) == ConnectionState::kDegraded;
        }));

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && state_of(*s, 1) == ConnectionState::kFailed;
        }));

        // Reconnect was scheduled and actually happened: a fresh handshake
        // (unaffected by drop_probability) brings it back to HEALTHY.
        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && state_of(*s, 1) == ConnectionState::kHealthy;
            },
            /*timeout_ms=*/5000));
    }
    const std::string log = stderr_capture.finish();
    CHECK(log.find("ALERT") != std::string::npos);
    CHECK(log.find("feed 1") != std::string::npos);
    CHECK(log.find("failed") != std::string::npos);

    auto snap = box.get();
    REQUIRE(snap.has_value());
    const auto* f = [&]() -> const FeedSnapshot* {
        for (auto& feed : snap->feeds)
            if (feed.feed_id == 1) return &feed;
        return nullptr;
    }();
    REQUIRE(f != nullptr);
    CHECK(f->stats.reconnects >= 1u);
}

TEST_CASE("after a socket-error kill, the feed reconnects with a fresh socket, resumes heartbeats, and keeps its cumulative stats") {
    // Covers both "reconnection" (socket error detected, not just EOF; old
    // connection torn down; backoff; new socket; handshake repeated;
    // HEALTHY again) and "recovery state" (heartbeats genuinely resume, not
    // just the state flag flipping once; the reconnect counter increases;
    // historical stats survive rather than resetting to 0).
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    sim.set_command_input(pipe_fds[0], [&sim](const std::string& line) {
        if (line == "kill") sim.kill_random_connection();
    });
    SimulatorBackgroundRun sim_bg(sim);

    Config config = fast_test_config();
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    // Let a couple of real heartbeats go by first so there's real "before"
    // history to prove survives the reconnect.
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        if (!s) return false;
        for (auto& f : s->feeds)
            if (f.feed_id == 1) return f.stats.heartbeats_acked >= 2;
        return false;
    }));
    const uint64_t acked_before_kill = [&] {
        auto s = box.get();
        for (auto& f : s->feeds)
            if (f.feed_id == 1) return f.stats.heartbeats_acked;
        return uint64_t{0};
    }();

    const std::string cmd = "kill\n";
    REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

    // kill_random_connection uses SO_LINGER{1,0}, sending a raw RST -- this
    // drives FeedMonitor down the socket-error (ECONNRESET) path, distinct
    // from the existing "a dead connection is detected" test, which only
    // exercises plain EOF via a graceful close().
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kFailed;
    }));

    REQUIRE(wait_until(
        [&box] {
            auto s = box.get();
            return s && state_of(*s, 1) == ConnectionState::kHealthy;
        },
        /*timeout_ms=*/5000));

    // Heartbeats actually resume -- not just the state enum flipping once --
    // wait for the acked count to climb past its pre-kill value.
    REQUIRE(wait_until(
        [&box, acked_before_kill] {
            auto s = box.get();
            if (!s) return false;
            for (auto& f : s->feeds)
                if (f.feed_id == 1) return f.stats.heartbeats_acked > acked_before_kill;
            return false;
        },
        /*timeout_ms=*/3000));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    const auto* snap = [&]() -> const FeedSnapshot* {
        for (auto& f : final_snapshot->feeds)
            if (f.feed_id == 1) return &f;
        return nullptr;
    }();
    REQUIRE(snap != nullptr);
    CHECK(snap->stats.reconnects >= 1u);                       // reconnect counter increased
    CHECK(snap->stats.heartbeats_acked > acked_before_kill);   // historical stats survived, kept growing

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("dropped PONGs that resume before the miss limit cause DEGRADED -> HEALTHY, not a reconnect") {
    // Proves the monitor doesn't overreact to a transient blip: a temporary
    // run of dropped replies should recover in place once replies resume,
    // not force a full disconnect/reconnect cycle.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    FaultConfig faults;
    sim.set_command_input(pipe_fds[0], [&sim, &faults](const std::string& line) {
        if (line == "drop-on") {
            faults.drop_probability = 1.0;
            sim.set_fault_config(faults);
        } else if (line == "drop-off") {
            faults.drop_probability = 0.0;
            sim.set_fault_config(faults);
        }
    });
    SimulatorBackgroundRun sim_bg(sim);

    Config config = fast_test_config();
    config.max_missed_heartbeats = 4;  // headroom to recover before hitting FAILED
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    const std::string on_cmd = "drop-on\n";
    REQUIRE(::write(pipe_fds[1], on_cmd.data(), on_cmd.size()) == static_cast<ssize_t>(on_cmd.size()));

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kDegraded;
    }));

    const std::string off_cmd = "drop-off\n";
    REQUIRE(::write(pipe_fds[1], off_cmd.data(), off_cmd.size()) == static_cast<ssize_t>(off_cmd.size()));

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    auto snap = box.get();
    REQUIRE(snap.has_value());
    const auto* f = [&]() -> const FeedSnapshot* {
        for (auto& feed : snap->feeds)
            if (feed.feed_id == 1) return &feed;
        return nullptr;
    }();
    REQUIRE(f != nullptr);
    CHECK(f->stats.reconnects == 0u);  // recovered in place -- never actually disconnected

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("simulator latency above heartbeat_timeout is treated as unhealthy, not masked by eventually-arriving replies") {
    // The FeedSimulator-level counterpart to connection_test.cpp's stale-PONG
    // unit test: real extra_latency fault injection, not a hand-crafted
    // message. Every reply genuinely arrives -- nothing is dropped -- but
    // always well after heartbeat_timeout, so it must never count as proof
    // of current health.
    FaultConfig faults;
    faults.extra_latency = std::chrono::milliseconds(500);  // well above fast_test_config's 100ms timeout
    FeedSimulator sim(0, 1, Config{}, faults);
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    Config config = fast_test_config();
    config.max_reconnect_attempts = 0;  // give up after the first failure -- keeps this test's timeline simple
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    // Handshake isn't delayed by extra_latency (only PING replies are), so
    // this still reaches HEALTHY first.
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    REQUIRE(wait_until(
        [&box] {
            auto s = box.get();
            return s && state_of(*s, 1) == ConnectionState::kFailed;
        },
        /*timeout_ms=*/3000));
}

TEST_CASE("a healthy feed under normal conditions never transitions away from HEALTHY") {
    // The complement to every fault-injection test above: proof the monitor
    // doesn't cry wolf during ordinary operation, with no fault injection at
    // all.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    std::atomic<bool> saw_non_healthy{false};
    monitor.set_tick_callback([&box, &saw_non_healthy](const AggregateStats& s) {
        box.set(s);
        const auto st = state_of(s, 1);
        if (st && *st == ConnectionState::kDegraded) {
            saw_non_healthy.store(true, std::memory_order_relaxed);
        }
        if (st && *st == ConnectionState::kFailed) {
            saw_non_healthy.store(true, std::memory_order_relaxed);
        }
    });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    // Stay healthy across many real heartbeat cycles -- fast_test_config's
    // 30ms interval means this covers dozens of real ping/pong round trips.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    CHECK(!saw_non_healthy.load(std::memory_order_relaxed));
    CHECK(state_of(*box.get(), 1) == ConnectionState::kHealthy);
}

TEST_CASE("snapshot_stats reflects a mix of healthy and permanently-failed feeds") {
    FeedSimulator sim(0, 1, Config{});
    const uint16_t good_port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);
    const uint16_t bad_port = closed_port();

    Config config = fast_test_config();
    config.max_reconnect_attempts = 0;  // give up after the first failure -- keeps this test deterministic
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "GOOD", "127.0.0.1", good_port}));
    REQUIRE(monitor.add_feed(FeedEndpoint{2, "BAD", "127.0.0.1", bad_port}));

    MonitorBackgroundRun monitor_bg(monitor);

    const bool settled = wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy &&
               state_of(*s, 2) == ConnectionState::kFailed;
    });
    REQUIRE(settled);

    auto snapshot = box.get();
    REQUIRE(snapshot.has_value());
    CHECK(snapshot->healthy_count == 1u);
    CHECK(snapshot->failed_count == 1u);
}

TEST_CASE("a command arriving in the same batch as command-fd EOF is still processed") {
    // Regression test for a real bug: on_command_fd_readable() used to
    // return immediately on EOF without ever running the line-parsing loop
    // over whatever had just been appended to command_line_buffer_ in that
    // same read() call. That's exactly what happens with piped/redirected
    // input (as opposed to an interactive TTY): the last command and EOF
    // routinely arrive together in one read(), and the command was silently
    // dropped. Reproduced here by writing one command and closing the pipe's
    // write end immediately, rather than leaving it open the way an
    // interactive terminal would.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t sim_port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", sim_port}));

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "disconnect 1") monitor.force_disconnect(1);
    });

    MonitorBackgroundRun monitor_bg(monitor);
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    const std::string cmd = "disconnect 1\n";
    REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));
    ::close(pipe_fds[1]);  // EOF arrives bundled with the command above, not as a separate later event

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kFailed;
    }));

    ::close(pipe_fds[0]);
}

TEST_CASE("force_reconnect gives a slot a full fresh retry budget, not one more attempt then give-up") {
    // Regression test for a real bug: force_reconnect() cleared give_up and
    // armed an immediate retry, but left reconnect_attempts at whatever value
    // had already tripped give_up -- so a fresh manual reconnect that also
    // failed would immediately re-trigger give_up on schedule_reconnect's
    // very next call, silently defeating the "give it a full fresh chance"
    // intent behind calling force_reconnect at all.
    //
    // With max_reconnect_attempts=1 against a permanently-closed port: the
    // natural sequence is initial-connect-fails -> one scheduled retry
    // (reconnects becomes 1) -> that fails too -> give_up (reconnects stays
    // at 1 forever). Calling force_reconnect() once here should let it climb
    // past 1 -- with the bug, it settles at exactly 2 (one more attempt, then
    // gives up again); with the fix, it keeps retrying past that.
    Config config = fast_test_config();
    config.max_reconnect_attempts = 1;
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "DEAD", "127.0.0.1", closed_port()}));

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "reconnect-1") monitor.force_reconnect(1);
    });

    MonitorBackgroundRun monitor_bg(monitor);

    // Let it naturally exhaust its budget and give up (reconnects settles at 1).
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && reconnects_of(*s, 1) >= 1 && state_of(*s, 1) == ConnectionState::kFailed;
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));  // confirm it's truly settled, not mid-retry
    REQUIRE(reconnects_of(*box.get(), 1) == 1u);

    const std::string cmd = "reconnect-1\n";
    REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

    // With the fix, the fresh attempt (reconnects -> 2) fails, gets its own
    // full retry budget again, retries once more (reconnects -> 3), then
    // finally gives up. Without the fix, it stops at 2.
    REQUIRE(wait_until(
        [&box] {
            auto s = box.get();
            return s && reconnects_of(*s, 1) >= 3;
        },
        2000));

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("an operator-forced reconnect while a feed is mid-backoff does not double-connect or corrupt slot state") {
    // A real automatic failure (drop_probability=1 cycle, not
    // force_disconnect) arms a real backoff delay; the operator fires
    // "reconnect" via the command pipe *while* that delay is still pending,
    // well before the automatic retry would have fired on its own --
    // exactly the scenario nobody had proven correct before this pass.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int sim_pipe_fds[2];
    REQUIRE(::pipe(sim_pipe_fds) == 0);
    const int sim_flags = ::fcntl(sim_pipe_fds[0], F_GETFL, 0);
    ::fcntl(sim_pipe_fds[0], F_SETFL, sim_flags | O_NONBLOCK);
    FaultConfig faults;
    sim.set_command_input(sim_pipe_fds[0], [&sim, &faults](const std::string& line) {
        if (line == "drop-on") {
            faults.drop_probability = 1.0;
            sim.set_fault_config(faults);
        } else if (line == "drop-off") {
            faults.drop_probability = 0.0;
            sim.set_fault_config(faults);
        }
    });
    SimulatorBackgroundRun sim_bg(sim);

    // Long, fixed backoff -- a comfortable window to inject the forced
    // reconnect while the automatic retry is still pending, and long enough
    // afterward to prove the *original* automatic retry never separately
    // fires on top of the forced one.
    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::milliseconds(2000);
    config.reconnect_max_delay = std::chrono::milliseconds(2000);
    config.max_reconnect_attempts = -1;
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    int mon_pipe_fds[2];
    REQUIRE(::pipe(mon_pipe_fds) == 0);
    const int mon_flags = ::fcntl(mon_pipe_fds[0], F_GETFL, 0);
    ::fcntl(mon_pipe_fds[0], F_SETFL, mon_flags | O_NONBLOCK);
    monitor.set_command_input(mon_pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "reconnect-1") monitor.force_reconnect(1);
    });

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    const std::string drop_on = "drop-on\n";
    REQUIRE(::write(sim_pipe_fds[1], drop_on.data(), drop_on.size()) == static_cast<ssize_t>(drop_on.size()));

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kFailed;
    }));

    // Restore replies so the forced reconnect can actually succeed, then
    // fire it well within the 2s automatic-retry window.
    const std::string drop_off = "drop-off\n";
    REQUIRE(::write(sim_pipe_fds[1], drop_off.data(), drop_off.size()) == static_cast<ssize_t>(drop_off.size()));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const std::string reconnect_cmd = "reconnect-1\n";
    REQUIRE(::write(mon_pipe_fds[1], reconnect_cmd.data(), reconnect_cmd.size()) ==
            static_cast<ssize_t>(reconnect_cmd.size()));

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    // Let the *original* 2s automatic-retry window fully elapse -- if that
    // stale timer had somehow fired independently of the forced reconnect,
    // it would show up as a second disturbance to this now-settled feed.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    CHECK(state_of(*final_snapshot, 1) == ConnectionState::kHealthy);
    CHECK(reconnects_of(*final_snapshot, 1) == 1u);  // exactly one -- the forced one, not a duplicate

    ::close(sim_pipe_fds[1]);
    ::close(sim_pipe_fds[0]);
    ::close(mon_pipe_fds[1]);
    ::close(mon_pipe_fds[0]);
}

TEST_CASE("shrinking heartbeat_timeout mid-flight does not spuriously degrade an already-healthy connection") {
    // Regression test: set_heartbeat_timeout used to just overwrite
    // config_.heartbeat_timeout, leaving every connection's
    // last_liveness_ns_ untouched -- so shrinking the timeout retroactively
    // re-judged however much wait time had already accumulated against the
    // new, tighter budget. A connection that was perfectly healthy a moment
    // ago (comfortably within the OLD, more generous timeout) would
    // immediately register as missed purely because of the config change
    // itself, not because anything about the peer changed.
    //
    // heartbeat_interval is deliberately large (10s) so no automatic ping
    // fires during this test's short window -- last_liveness_ns_ stays
    // fixed at the handshake-completion timestamp and genuinely goes stale
    // as real time passes, the same way the standalone diagnostic that
    // found this bug worked.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    Config config;
    config.heartbeat_interval = std::chrono::seconds(10);
    config.heartbeat_timeout = std::chrono::seconds(30);  // generous old budget
    config.tick_interval = std::chrono::milliseconds(20);
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "shrink-timeout") {
            monitor.set_heartbeat_timeout(std::chrono::milliseconds(150));
        }
    });

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    // 300ms of real elapsed time with no ping activity (heartbeat_interval
    // is 10s) -- fine under the old 30s budget, but already well past what
    // the *new* 150ms budget will be.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    REQUIRE(state_of(*box.get(), 1) == ConnectionState::kHealthy);

    const std::string cmd = "shrink-timeout\n";
    REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

    // Checked quickly, well inside the *new* 150ms budget from the moment
    // of the shrink -- with the bug, the very next tick's check_timeout
    // call would already see 300ms+ elapsed >= the new 150ms and
    // immediately register a miss; with the fix, the reset baseline means
    // it's only been a few ms since "now" as far as check_timeout is
    // concerned.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    CHECK(state_of(*box.get(), 1) == ConnectionState::kHealthy);

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("heartbeat_interval smaller than tick_interval is throttled to the tick rate, not violated or burst-caught-up") {
    // Ping-sending is tick-driven -- on_timer_tick's single
    // `if (now_ns >= slot.next_ping_due_ns)` check runs once per tick, not
    // a loop that "catches up" to a configured sub-tick rate. Configuring
    // heartbeat_interval below tick_interval was never tested or documented
    // as a real, locked-in behavior before this pass. This proves what
    // actually happens: the effective ping rate is silently throttled to
    // roughly once per tick, nothing bursts, nothing crashes, and the
    // connection stays healthy throughout the mismatch.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    Config config;
    config.heartbeat_interval = std::chrono::milliseconds(5);  // far below tick_interval
    config.heartbeat_timeout = std::chrono::milliseconds(500);
    config.tick_interval = std::chrono::milliseconds(50);
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    const uint64_t acked_start = acked_of(*box.get(), 1);

    // Real wall-clock window: at the (literal, impossible-to-honor)
    // configured 5ms rate this would be on the order of 100 pings; at the
    // actual tick-throttled rate (~50ms per tick) it should be roughly 10.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const uint64_t acked_delta = acked_of(*box.get(), 1) - acked_start;
    CHECK(acked_delta > 0u);   // still made real progress
    CHECK(acked_delta < 40u);  // but nowhere near the literal 5ms rate -- genuinely throttled, not caught up in a burst

    CHECK(state_of(*box.get(), 1) == ConnectionState::kHealthy);  // never destabilized by the mismatch
}

TEST_CASE("stop() from another thread causes run() to return promptly") {
    FeedMonitor monitor(fast_test_config());
    std::thread runner([&monitor] { monitor.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    monitor.stop();
    runner.join();
    CHECK(true);  // reaching this line at all is the assertion
}

TEST_CASE("50 concurrent feeds all reach HEALTHY, exercising the epoll batch path") {
    constexpr int kFeedCount = 50;
    std::vector<std::unique_ptr<FeedSimulator>> sims;
    std::vector<std::unique_ptr<SimulatorBackgroundRun>> sim_runs;
    sims.reserve(kFeedCount);
    sim_runs.reserve(kFeedCount);

    FeedMonitor monitor(fast_test_config());
    for (int i = 0; i < kFeedCount; ++i) {
        const uint32_t feed_id = static_cast<uint32_t>(i + 1);
        sims.push_back(std::make_unique<FeedSimulator>(0, feed_id, Config{}));
        const uint16_t port = sims.back()->port();
        sim_runs.push_back(std::make_unique<SimulatorBackgroundRun>(*sims.back()));
        REQUIRE(monitor.add_feed(FeedEndpoint{feed_id, "FEED" + std::to_string(feed_id), "127.0.0.1", port}));
    }

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    MonitorBackgroundRun monitor_bg(monitor);

    const bool all_healthy = wait_until(
        [&box] {
            auto s = box.get();
            return s && s->healthy_count == static_cast<size_t>(kFeedCount);
        },
        /*timeout_ms=*/10000);
    CHECK(all_healthy);
}

TEST_CASE("20 feeds failing simultaneously all reach FAILED, alert individually, and recover on force_reconnect") {
    // The biggest gap the Phase-14 audit found: every fault-injection test
    // before this one used a single feed. Nothing proved alerting stays
    // accurate or that nothing gets stuck/misrouted when many feeds fail at
    // once. One FeedSimulator per feed_id is required here, not a shared
    // one -- FeedSimulator's accepted clients all get its own single fixed
    // feed_id_ (see accept_new_connections), and handle_connect_hello
    // rejects any CONNECT_HELLO whose feed_id doesn't match it, so 20
    // distinct FeedMonitor-side feed_ids structurally cannot share one
    // simulator instance.
    constexpr int kFeedCount = 20;
    std::vector<std::unique_ptr<FeedSimulator>> sims;
    std::vector<std::unique_ptr<SimulatorBackgroundRun>> sim_runs;
    std::vector<int> pipe_write_fds;
    std::vector<int> pipe_read_fds;
    std::vector<std::shared_ptr<FaultConfig>> faults;
    sims.reserve(kFeedCount);
    sim_runs.reserve(kFeedCount);
    faults.reserve(kFeedCount);

    // Deterministic: each feed gives up after exactly one failure, so
    // there's no natural-backoff re-fail race that could log a second ALERT
    // for the same feed before this test manages to capture stderr.
    Config config = fast_test_config();
    config.max_reconnect_attempts = 0;
    FeedMonitor monitor(config);

    for (int i = 0; i < kFeedCount; ++i) {
        const uint32_t feed_id = static_cast<uint32_t>(i + 1);
        sims.push_back(std::make_unique<FeedSimulator>(0, feed_id, Config{}));
        FeedSimulator& sim = *sims.back();
        const uint16_t port = sim.port();

        int pipe_fds[2];
        REQUIRE(::pipe(pipe_fds) == 0);
        const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
        ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);

        faults.push_back(std::make_shared<FaultConfig>());
        std::shared_ptr<FaultConfig> fc = faults.back();
        sim.set_command_input(pipe_fds[0], [&sim, fc](const std::string& line) {
            if (line == "drop-on") {
                fc->drop_probability = 1.0;
                sim.set_fault_config(*fc);
            }
        });

        sim_runs.push_back(std::make_unique<SimulatorBackgroundRun>(sim));
        pipe_write_fds.push_back(pipe_fds[1]);
        pipe_read_fds.push_back(pipe_fds[0]);
        REQUIRE(monitor.add_feed(FeedEndpoint{feed_id, "FEED" + std::to_string(feed_id), "127.0.0.1", port}));
    }

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });

    // force_reconnect() is reactor-thread-only (like every FeedMonitor
    // method except stop()) -- must be routed through the command-pipe
    // mechanism, same pattern as the existing single-feed force_reconnect
    // test, not called directly from this (main) thread while
    // MonitorBackgroundRun is running. Registered before the reactor starts.
    int monitor_pipe_fds[2];
    REQUIRE(::pipe(monitor_pipe_fds) == 0);
    const int monitor_pipe_flags = ::fcntl(monitor_pipe_fds[0], F_GETFL, 0);
    ::fcntl(monitor_pipe_fds[0], F_SETFL, monitor_pipe_flags | O_NONBLOCK);
    monitor.set_command_input(monitor_pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "reconnect-all") {
            for (uint32_t feed_id = 1; feed_id <= 20; ++feed_id) {
                monitor.force_reconnect(feed_id);
            }
        }
    });

    StderrCapture stderr_capture;
    {
        MonitorBackgroundRun monitor_bg(monitor);

        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->healthy_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));

        // Fail all 20 feeds via real fault injection -- a tight loop of
        // command-pipe writes, one per simulator's own reactor thread.
        for (int fd : pipe_write_fds) {
            const std::string cmd = "drop-on\n";
            REQUIRE(::write(fd, cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));
        }

        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->failed_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));

        // give_up=true after max_reconnect_attempts=0, so natural backoff
        // won't fire on its own -- explicitly un-give-up every feed via the
        // command pipe (see above), not a direct cross-thread call.
        const std::string reconnect_cmd = "reconnect-all\n";
        REQUIRE(::write(monitor_pipe_fds[1], reconnect_cmd.data(), reconnect_cmd.size()) ==
                static_cast<ssize_t>(reconnect_cmd.size()));

        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->healthy_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));
    }
    const std::string log = stderr_capture.finish();

    // Exactly one ALERT per feed -- no duplicates, none missing.
    size_t alert_count = 0;
    size_t pos = 0;
    while ((pos = log.find("ALERT", pos)) != std::string::npos) {
        ++alert_count;
        pos += 5;
    }
    CHECK(alert_count == static_cast<size_t>(kFeedCount));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    CHECK(final_snapshot->feeds.size() == static_cast<size_t>(kFeedCount));
    for (const auto& f : final_snapshot->feeds) {
        CHECK(f.stats.reconnects >= 1u);
    }

    // Stop every simulator's reactor thread *before* closing its command
    // pipe -- sim_runs lives past monitor_bg's scope above, so without this
    // a simulator thread could still be reading its command_fd_ (the other
    // end of a pipe_write_fds entry) at the exact moment this closes it.
    sim_runs.clear();
    for (int fd : pipe_write_fds) ::close(fd);
    for (int fd : pipe_read_fds) ::close(fd);
    ::close(monitor_pipe_fds[0]);
    ::close(monitor_pipe_fds[1]);
}

TEST_CASE("force_disconnect across 20 feeds at once leaves slots_/index_of_fd_ internally consistent and every feed reconnects") {
    // Complements the fault-injection version above with a synchronous,
    // timing-independent stress of the bookkeeping itself: force_disconnect
    // fails every slot within one command-callback invocation (no real
    // timeout wait), specifically to catch corruption in slots_/
    // index_of_fd_/index_of_feed_id_ under concurrent failure that a
    // fault-injection-paced test might not reliably trigger.
    constexpr int kFeedCount = 20;
    std::vector<std::unique_ptr<FeedSimulator>> sims;
    std::vector<std::unique_ptr<SimulatorBackgroundRun>> sim_runs;
    sims.reserve(kFeedCount);
    sim_runs.reserve(kFeedCount);

    FeedMonitor monitor(fast_test_config());  // unlimited reconnect attempts (default)
    for (int i = 0; i < kFeedCount; ++i) {
        const uint32_t feed_id = static_cast<uint32_t>(i + 1);
        sims.push_back(std::make_unique<FeedSimulator>(0, feed_id, Config{}));
        const uint16_t port = sims.back()->port();
        sim_runs.push_back(std::make_unique<SimulatorBackgroundRun>(*sims.back()));
        REQUIRE(monitor.add_feed(FeedEndpoint{feed_id, "FEED" + std::to_string(feed_id), "127.0.0.1", port}));
    }

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "fail-all") {
            for (uint32_t feed_id = 1; feed_id <= 20; ++feed_id) {
                monitor.force_disconnect(feed_id);
            }
        }
    });

    {
        MonitorBackgroundRun monitor_bg(monitor);

        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->healthy_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));

        const std::string cmd = "fail-all\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

        // Must wait for the failure to actually land before waiting for
        // recovery -- otherwise, if "fail-all" hasn't been processed yet,
        // this second wait could trivially succeed immediately against the
        // *original* still-healthy state, never actually observing the
        // fail-and-recover cycle at all.
        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->failed_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));

        REQUIRE(wait_until(
            [&box] {
                auto s = box.get();
                return s && s->healthy_count == static_cast<size_t>(kFeedCount);
            },
            /*timeout_ms=*/10000));
    }

    // Bookkeeping integrity: exactly one index_of_fd_ entry per live slot,
    // reusing the F1 regression test's exact assertion style. Safe to read
    // now -- MonitorBackgroundRun's destructor already stopped the reactor.
    CHECK(monitor.debug_fd_index_count() == static_cast<size_t>(kFeedCount));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    CHECK(final_snapshot->feeds.size() == static_cast<size_t>(kFeedCount));
    for (const auto& f : final_snapshot->feeds) {
        CHECK(f.stats.reconnects >= 1u);
    }

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("an unterminated command line beyond the length cap is discarded, not accumulated forever") {
    // Regression test: command_line_buffer_ used to have no length cap at
    // all, so a command source that never emits '\n' would grow it without
    // bound. Sends one byte past the cap with no trailing newline, then a
    // real well-formed command in the same write: if the cap is enforced,
    // the oversized prefix is discarded and "ping" arrives on its own; if
    // not, "ping" arrives concatenated onto the ~64KB of 'x's that preceded
    // it, and this test's REQUIRE on the received line fails.
    FeedMonitor monitor(fast_test_config());

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);

    std::mutex mu;
    std::vector<std::string> received;
    monitor.set_command_input(pipe_fds[0], [&](const std::string& line) {
        std::lock_guard<std::mutex> lock(mu);
        received.push_back(line);
    });

    MonitorBackgroundRun monitor_bg(monitor);

    // Sent as two separate writes, not one buffer with "ping\n" appended
    // directly after the 'x's: on_command_fd_readable reads in fixed
    // 1024-byte chunks, so the boundary between the oversized prefix and a
    // trailing command could otherwise land inside the very chunk that
    // crosses the cap, discarding the command along with the garbage that
    // preceded it -- a false failure of this test, not a real one. Splitting
    // into two writes with a pause between them guarantees the reactor
    // drains and discards the first payload in its own read cycle before
    // "ping\n" ever arrives.
    std::string oversized(FeedMonitor::kMaxCommandLineBufferBytes + 1, 'x');
    size_t written = 0;
    while (written < oversized.size()) {
        const ssize_t n = ::write(pipe_fds[1], oversized.data() + written, oversized.size() - written);
        REQUIRE(n > 0);
        written += static_cast<size_t>(n);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const std::string cmd = "ping\n";
    REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

    REQUIRE(wait_until([&] {
        std::lock_guard<std::mutex> lock(mu);
        return !received.empty();
    }));

    {
        std::lock_guard<std::mutex> lock(mu);
        REQUIRE(received.size() == 1u);
        CHECK(received[0] == "ping");
    }

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("debug_epoll_wait_count and debug_events_processed_count reflect real reactor activity") {
    // These counters back throughput_bench's epoll_wait-calls/sec and
    // events-processed/sec metrics (PROJECT_PLAN.md section 12 promised
    // this instrumentation; it was never actually built until now). No
    // revert-discipline needed here -- this is new observability, not a bug
    // fix -- just a basic correctness check that the counters move.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    CHECK(monitor.debug_epoll_wait_count() == 0u);
    CHECK(monitor.debug_events_processed_count() == 0u);

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && state_of(*s, 1) == ConnectionState::kHealthy;
    }));

    // Handshake alone guarantees at least a few epoll_wait wakeups (connect
    // completing, HELLO/ACK, the periodic tick's own timerfd firing), and
    // every real wakeup returns at least one event.
    CHECK(monitor.debug_epoll_wait_count() > 0u);
    CHECK(monitor.debug_events_processed_count() >= monitor.debug_epoll_wait_count());
}
