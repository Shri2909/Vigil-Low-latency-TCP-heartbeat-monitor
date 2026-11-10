// Integration tests: real loopback sockets, a real FeedSimulator (or a raw
// MultiAcceptListener where finer control is needed), and FeedMonitor::run()
// on a background thread. Only stop() is thread-safe; everything else runs
// before run(), after stop()+join, on the reactor thread via the tick callback
// (into a mutex-protected StatsBox), or through socket I/O on the peer's end.

#include "config.h"
#include "feed_monitor.h"
#include "feed_simulator.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <functional>
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
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// Test-only: hands snapshots from the reactor thread to the test thread.
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

bool wait_for_state(const StatsBox& box, uint32_t feed_id, ConnectionState state, int timeout_ms = 5000) {
    return wait_until(
        [&box, feed_id, state] {
            auto s = box.get();
            return s && state_of(*s, feed_id) == state;
        },
        timeout_ms);
}

// Waits until the given AggregateStats counter equals `expected`.
bool wait_for_count(const StatsBox& box, size_t AggregateStats::*counter, int expected, int timeout_ms = 10000) {
    return wait_until(
        [&box, counter, expected] {
            auto s = box.get();
            return s && (*s).*counter == static_cast<size_t>(expected);
        },
        timeout_ms);
}

void make_nonblocking_pipe(int fds[2]) {
    REQUIRE(::pipe(fds) == 0);
    REQUIRE(::fcntl(fds[0], F_SETFL, ::fcntl(fds[0], F_GETFL, 0) | O_NONBLOCK) == 0);
}

void write_all(int fd, const std::string& data) {
    size_t written = 0;
    while (written < data.size()) {
        const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        REQUIRE(n > 0);
        written += static_cast<size_t>(n);
    }
}

// Runs monitor.run() on a background thread; stops and joins on destruction.
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

// Redirects fd 2 to a temp file so tests can assert on the monitor's stderr
// logging; finish() restores stderr and returns what was captured.
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

    // Restores stderr and returns the captured text; the destructor is a no-op afterwards.
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

// A loopback port with no listener (connect gets a prompt ECONNREFUSED).
uint16_t closed_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &len) == 0);
    const uint16_t port = ntohs(bound.sin_port);
    ::close(fd);
    return port;
}

// Raw loopback listener that accepts multiple sequential connections, so tests
// can observe real (re)connects.
class MultiAcceptListener {
public:
    // small_rcvbuf: shrink the receive buffer so a peer that never reads closes its window quickly.
    explicit MultiAcceptListener(bool small_rcvbuf = false) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(listen_fd_ >= 0);
        if (small_rcvbuf) {
            const int bytes = 2048;
            REQUIRE(::setsockopt(listen_fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) == 0);
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        REQUIRE(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(::listen(listen_fd_, 4) == 0);
        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        REQUIRE(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &len) == 0);
        port_ = ntohs(bound.sin_port);
    }
    ~MultiAcceptListener() {
        for (int fd : accepted_) ::close(fd);
        ::close(listen_fd_);
    }
    MultiAcceptListener(const MultiAcceptListener&) = delete;
    MultiAcceptListener& operator=(const MultiAcceptListener&) = delete;

    uint16_t port() const { return port_; }

    // Returns the accepted fd, or -1 on timeout (bounded so a regression fails instead of hanging).
    int accept_one(int timeout_ms = 5000) {
        pollfd pfd{listen_fd_, POLLIN, 0};
        if (::poll(&pfd, 1, timeout_ms) <= 0) return -1;
        const int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) return -1;
        const timeval tv{5, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        accepted_.push_back(fd);
        return fd;
    }

    // Closes an accepted fd and forgets it; reset=true sends an RST instead of a FIN.
    void close_peer(int fd, bool reset = false) {
        if (reset) {
            const linger rst{1, 0};
            ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &rst, sizeof(rst));
        }
        ::close(fd);
        accepted_.erase(std::remove(accepted_.begin(), accepted_.end(), fd), accepted_.end());
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
        // n <= 0 on EOF, error or the SO_RCVTIMEO set by accept_one(): never blocks forever.
        const ssize_t n = ::recv(fd, wire.data() + total, wire.size() - total, 0);
        REQUIRE(n > 0);
        total += static_cast<size_t>(n);
    }
    HeartbeatMessage msg{};
    REQUIRE(decode_heartbeat(wire, &msg));
    return msg;
}

// True once the peer sees FIN or RST (discarding pending PINGs); false on SO_RCVTIMEO.
bool peer_sees_close(int fd) {
    char buf[256];
    while (true) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n == 0) return true;
        if (n < 0) return errno == ECONNRESET;
    }
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

    const bool became_healthy = wait_for_state(box, 1, ConnectionState::kHealthy);
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

    // Slot C moves into B's position; stale index maps would drop or misroute its events.
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
    // Regression: moving a kFailed slot via swap-and-pop must not re-insert its
    // (already erased) fd into index_of_fd_.
    FeedSimulator sim_b(0, /*feed_id=*/2, Config{});
    const uint16_t port_b = sim_b.port();
    SimulatorBackgroundRun sim_bg(sim_b);

    Config config = fast_test_config();
    // Long backoff keeps feed A kFailed for the whole test.
    config.reconnect_base_delay = std::chrono::seconds(10);
    config.reconnect_max_delay = std::chrono::seconds(10);
    FeedMonitor monitor(config);

    // B at index 0 (healthy), A last (failed): removing B makes swap-and-pop move the failed slot.
    REQUIRE(monitor.add_feed(FeedEndpoint{2, "B", "127.0.0.1", port_b}));
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "A", "127.0.0.1", closed_port()}));

    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
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

        write_all(pipe_fds[1], "remove-b\n");

        REQUIRE(wait_until([&box] {
            auto s = box.get();
            return s && s->feeds.size() == 1;
        }));
    }
    // Safe: the reactor thread is stopped and joined.
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
    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    peer.close_peer(peer_fd1);  // simulate the exchange vanishing

    // Accepting again is external proof that a real reconnect happened.
    const int peer_fd2 = peer.accept_one();
    REQUIRE(peer_fd2 >= 0);
    manual_handshake(peer_fd2, 1);

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    CHECK(reconnects_of(*final_snapshot, 1) >= 1u);
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
    // Jitter is random, so sample repeatedly.
        for (int i = 0; i < 20; ++i) {
            const int64_t delay = monitor.debug_reconnect_delay_ns(attempt);
            CHECK(delay >= static_cast<int64_t>(static_cast<double>(expected) * 0.75));
            CHECK(delay <= static_cast<int64_t>(static_cast<double>(expected) * 1.25));
            CHECK(delay <= max_ns);  // jitter must never push past the configured ceiling
        }
    }
    // A huge attempt number (overflow guard) is also held to the ceiling.
    for (int i = 0; i < 20; ++i) {
        CHECK(monitor.debug_reconnect_delay_ns(1000) <= max_ns);
    }
}

TEST_CASE("reconnect_delay_ns jitter actually varies across calls, not a fixed value") {
    // Jitter must be real randomization, or failing feeds would retry in lockstep.
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
    // End-to-end check that the monitor waits between real reconnect attempts.
    MultiAcceptListener peer;

    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::milliseconds(80);
    config.reconnect_max_delay = std::chrono::milliseconds(2000);
    config.max_reconnect_attempts = -1;
    FeedMonitor monitor(config);
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    MonitorBackgroundRun monitor_bg(monitor);

    // Each connection is closed before the handshake, forcing another backoff cycle (four attempts).
    std::vector<std::chrono::steady_clock::time_point> accept_times;
    for (int i = 0; i < 4; ++i) {
        const int fd = peer.accept_one();
        REQUIRE(fd >= 0);
        accept_times.push_back(std::chrono::steady_clock::now());
        peer.close_peer(fd);
    }

    // base 80ms with -25% jitter floors at 60ms; allow slack down to 30ms.
    const auto gap1 = accept_times[1] - accept_times[0];
    CHECK(gap1 >= std::chrono::milliseconds(30));

    // The third gap is nominally 4x the first, so it must clear 1.5x despite jitter and tick noise.
    const auto gap2 = accept_times[2] - accept_times[1];
    const auto gap3 = accept_times[3] - accept_times[2];
    CHECK(gap3 > gap1 * 3 / 2);
    CHECK(gap3 > gap2);
}

TEST_CASE("dropped PONGs drive HEALTHY -> DEGRADED -> FAILED, emit a real alert, and the feed reconnects") {
    // Full silent-failure path via real fault injection: no PONG -> DEGRADED ->
    // FAILED -> alert -> reconnect (the handshake is unaffected by
    // drop_probability) -> HEALTHY again.
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

        REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

        REQUIRE(wait_for_state(box, 1, ConnectionState::kDegraded));

        REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed));

        // A fresh handshake brings it back to HEALTHY.
        REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));
    }
    const std::string log = stderr_capture.finish();
    CHECK(log.find("ALERT") != std::string::npos);
    CHECK(log.find("feed 1") != std::string::npos);
    CHECK(log.find("failed") != std::string::npos);

    auto snap = box.get();
    REQUIRE(snap.has_value());
    CHECK(reconnects_of(*snap, 1) >= 1u);
}

TEST_CASE("after a socket-error kill, the feed reconnects with a fresh socket, resumes heartbeats, and keeps its cumulative stats") {
    // An RST (socket-error path, unlike a graceful close) triggers reconnect;
    // heartbeats resume and cumulative stats survive.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
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

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // Accumulate some history to prove it survives the reconnect.
    REQUIRE(wait_until([&box] {
        auto s = box.get();
        return s && acked_of(*s, 1) >= 2;
    }));
    const uint64_t acked_before_kill = acked_of(*box.get(), 1);

    write_all(pipe_fds[1], "kill\n");

    // kill_random_connection sends an RST (ECONNRESET path, not plain EOF).
    REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed));

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // Heartbeats must actually resume.
    REQUIRE(wait_until(
        [&box, acked_before_kill] {
            auto s = box.get();
            return s && acked_of(*s, 1) > acked_before_kill;
        },
        /*timeout_ms=*/3000));

    auto final_snapshot = box.get();
    REQUIRE(final_snapshot.has_value());
    CHECK(reconnects_of(*final_snapshot, 1) >= 1u);
    CHECK(acked_of(*final_snapshot, 1) > acked_before_kill);  // historical stats survived and kept growing

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("dropped PONGs that resume before the miss limit cause DEGRADED -> HEALTHY, not a reconnect") {
    // A transient run of dropped replies recovers in place, with no reconnect.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
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

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    write_all(pipe_fds[1], "drop-on\n");

    REQUIRE(wait_for_state(box, 1, ConnectionState::kDegraded));

    write_all(pipe_fds[1], "drop-off\n");

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    auto snap = box.get();
    REQUIRE(snap.has_value());
    CHECK(reconnects_of(*snap, 1) == 0u);  // recovered in place

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("simulator latency above heartbeat_timeout is treated as unhealthy, not masked by eventually-arriving replies") {
    // Replies arrive but always after heartbeat_timeout, so they never prove health.
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

    // The handshake is not delayed, so HEALTHY is reached first.
    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed, 3000));
}

TEST_CASE("a healthy feed under normal conditions never transitions away from HEALTHY") {
    // No faults injected: the monitor must not cry wolf.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    std::atomic<bool> saw_non_healthy{false};
    monitor.set_tick_callback([&box, &saw_non_healthy](const AggregateStats& s) {
        box.set(s);
        const auto st = state_of(s, 1);
        if (st && (*st == ConnectionState::kDegraded || *st == ConnectionState::kFailed)) {
            saw_non_healthy.store(true, std::memory_order_relaxed);
        }
    });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // Dozens of ping/pong round trips at 30ms.
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

TEST_CASE("FeedMonitor: a command arriving in the same batch as command-fd EOF is still processed") {
    // Regression: with piped input the last command and EOF arrive in one read();
    // the command must still be dispatched.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t sim_port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    FeedMonitor monitor(fast_test_config());
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", sim_port}));

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "disconnect 1") monitor.force_disconnect(1);
    });

    MonitorBackgroundRun monitor_bg(monitor);
    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    write_all(pipe_fds[1], "disconnect 1\n");
    ::close(pipe_fds[1]);  // EOF arrives bundled with the command above, not as a separate later event

    REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed));

    ::close(pipe_fds[0]);
}

TEST_CASE("force_reconnect gives a slot a full fresh retry budget, not one more attempt then give-up") {
    // Regression: force_reconnect() must reset reconnect_attempts. With
    // max_reconnect_attempts=1 against a closed port, reconnects settles at 1;
    // after force_reconnect() it must climb past 2 (the bug stopped at 2).
    Config config = fast_test_config();
    config.max_reconnect_attempts = 1;
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "DEAD", "127.0.0.1", closed_port()}));

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
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

    write_all(pipe_fds[1], "reconnect-1\n");

    // Fresh budget: attempt (2) fails, one more retry (3), then give up.
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
    // Fire "reconnect" while the automatic backoff retry is still pending; the
    // stale timer must not fire a second connect.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int sim_pipe_fds[2];
    make_nonblocking_pipe(sim_pipe_fds);
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

    // Fixed 2s backoff: a wide window to inject the forced reconnect.
    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::milliseconds(2000);
    config.reconnect_max_delay = std::chrono::milliseconds(2000);
    config.max_reconnect_attempts = -1;
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    int mon_pipe_fds[2];
    make_nonblocking_pipe(mon_pipe_fds);
    monitor.set_command_input(mon_pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "reconnect-1") monitor.force_reconnect(1);
    });

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    write_all(sim_pipe_fds[1], "drop-on\n");

    REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed));

    // Restore replies so the forced reconnect can succeed, then fire it inside the 2s window.
    write_all(sim_pipe_fds[1], "drop-off\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    write_all(mon_pipe_fds[1], "reconnect-1\n");

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // Let the original 2s retry window elapse; a stale timer would disturb the feed.
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
    // Regression: shrinking the timeout must reset liveness baselines, or a
    // healthy connection is retroactively judged against the tighter budget.
    // The 10s heartbeat_interval keeps pings from refreshing liveness.
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
    make_nonblocking_pipe(pipe_fds);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "shrink-timeout") {
            monitor.set_heartbeat_timeout(std::chrono::milliseconds(150));
        }
    });

    MonitorBackgroundRun monitor_bg(monitor);

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // 300ms idle: fine under the old 30s budget, past the new 150ms one.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    REQUIRE(state_of(*box.get(), 1) == ConnectionState::kHealthy);

    write_all(pipe_fds[1], "shrink-timeout\n");

    // Checked inside the new 150ms budget: without the baseline reset it would already register a miss.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    CHECK(state_of(*box.get(), 1) == ConnectionState::kHealthy);

    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("heartbeat_interval smaller than tick_interval is throttled to the tick rate, not violated or burst-caught-up") {
    // Pings are tick-driven (one check per tick, no catch-up), so a sub-tick
    // interval is throttled to the tick rate without bursting or destabilizing.
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

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    const uint64_t acked_start = acked_of(*box.get(), 1);

    // ~100 pings at the literal 5ms rate, ~10 at the tick-throttled rate.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const uint64_t acked_delta = acked_of(*box.get(), 1) - acked_start;
    CHECK(acked_delta > 0u);   // still made real progress
    CHECK(acked_delta < 40u);  // but nowhere near the literal 5ms rate -- genuinely throttled, not caught up in a burst

    CHECK(state_of(*box.get(), 1) == ConnectionState::kHealthy);  // never destabilized by the mismatch
}

// Runs monitor.run() on a thread and reports whether it returned within timeout_ms.
static bool run_returns_within(FeedMonitor& monitor, int timeout_ms, const std::function<void()>& after_start) {
    std::atomic<bool> returned{false};
    std::thread runner([&] {
        monitor.run();
        returned.store(true);
    });
    after_start();
    const bool ok = wait_until([&returned] { return returned.load(); }, timeout_ms);
    monitor.stop();
    runner.join();
    return ok;
}

TEST_CASE("stop() from another thread causes run() to return promptly") {
    FeedMonitor monitor(fast_test_config());
    // stop() is sticky, so it does not matter whether it lands before run() enters its loop.
    CHECK(run_returns_within(monitor, 2000, [&monitor] { monitor.stop(); }));
}

TEST_CASE("stop() issued before run() is not lost: run() returns promptly") {
    // Regression: run() must not reset stop_requested_ on entry.
    FeedMonitor monitor(fast_test_config());
    monitor.stop();
    CHECK(run_returns_within(monitor, 2000, [] {}));
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

    const bool all_healthy = wait_for_count(box, &AggregateStats::healthy_count, kFeedCount);
    CHECK(all_healthy);
}

TEST_CASE("20 feeds failing simultaneously all reach FAILED, alert individually, and recover on force_reconnect") {
    // One FeedSimulator per feed_id: a simulator rejects CONNECT_HELLOs whose
    // feed_id differs from its own.
    constexpr int kFeedCount = 20;
    std::vector<std::unique_ptr<FeedSimulator>> sims;
    std::vector<std::unique_ptr<SimulatorBackgroundRun>> sim_runs;
    std::vector<int> pipe_write_fds;
    std::vector<int> pipe_read_fds;
    std::vector<std::shared_ptr<FaultConfig>> faults;
    sims.reserve(kFeedCount);
    sim_runs.reserve(kFeedCount);
    faults.reserve(kFeedCount);

    // Each feed gives up after one failure, so no second ALERT can race the stderr capture.
    Config config = fast_test_config();
    config.max_reconnect_attempts = 0;
    FeedMonitor monitor(config);

    for (int i = 0; i < kFeedCount; ++i) {
        const uint32_t feed_id = static_cast<uint32_t>(i + 1);
        sims.push_back(std::make_unique<FeedSimulator>(0, feed_id, Config{}));
        FeedSimulator& sim = *sims.back();
        const uint16_t port = sim.port();

        int pipe_fds[2];
        make_nonblocking_pipe(pipe_fds);

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

    // force_reconnect() is reactor-thread-only, so route it through a command pipe.
    int monitor_pipe_fds[2];
    make_nonblocking_pipe(monitor_pipe_fds);
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

        REQUIRE(wait_for_count(box, &AggregateStats::healthy_count, kFeedCount));

        // Fail all feeds via fault injection (one command pipe per simulator).
        for (int fd : pipe_write_fds) {
            write_all(fd, "drop-on\n");
        }

        REQUIRE(wait_for_count(box, &AggregateStats::failed_count, kFeedCount));

        // give_up is set, so backoff will not fire; un-give-up every feed via the pipe.
        write_all(monitor_pipe_fds[1], "reconnect-all\n");

        REQUIRE(wait_for_count(box, &AggregateStats::healthy_count, kFeedCount));
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

    // Stop simulators before closing their command pipes.
    sim_runs.clear();
    for (int fd : pipe_write_fds) ::close(fd);
    for (int fd : pipe_read_fds) ::close(fd);
    ::close(monitor_pipe_fds[0]);
    ::close(monitor_pipe_fds[1]);
}

TEST_CASE("force_disconnect across 20 feeds at once leaves slots_/index_of_fd_ internally consistent and every feed reconnects") {
    // Timing-independent stress of the bookkeeping: force_disconnect fails every
    // slot within one command callback.
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
    make_nonblocking_pipe(pipe_fds);
    monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "fail-all") {
            for (uint32_t feed_id = 1; feed_id <= 20; ++feed_id) {
                monitor.force_disconnect(feed_id);
            }
        }
    });

    {
        MonitorBackgroundRun monitor_bg(monitor);

        REQUIRE(wait_for_count(box, &AggregateStats::healthy_count, kFeedCount));

        write_all(pipe_fds[1], "fail-all\n");

        // Wait for the failure first, or the recovery wait could pass against the original state.
        REQUIRE(wait_for_count(box, &AggregateStats::failed_count, kFeedCount));

        REQUIRE(wait_for_count(box, &AggregateStats::healthy_count, kFeedCount));
    }

    // One index_of_fd_ entry per live slot (reactor already stopped).
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

namespace {

// A command pipe feeding a FeedMonitor; received lines are collected under a mutex.
struct CommandPipeHarness {
    int fds[2] = {-1, -1};
    std::mutex mu;
    std::vector<std::string> received;

    explicit CommandPipeHarness(FeedMonitor& monitor) {
        make_nonblocking_pipe(fds);
        REQUIRE(monitor.set_command_input(fds[0], [this](const std::string& line) {
            std::lock_guard<std::mutex> lock(mu);
            received.push_back(line);
        }));
    }
    ~CommandPipeHarness() {
        ::close(fds[0]);
        ::close(fds[1]);
    }
    size_t count() {
        std::lock_guard<std::mutex> lock(mu);
        return received.size();
    }
    void write_all(const std::string& data) { ::write_all(fds[1], data); }
};

}  // namespace

TEST_CASE("an unterminated command line beyond the length cap is discarded, along with its tail") {
    // One byte past the cap with no newline, then its tail, then a real command:
    // the over-long line (head and tail) is dropped and only "ping" is dispatched.
    // lines are dispatched per chunk and the cap applies to the partial line.
    FeedMonitor monitor(fast_test_config());
    CommandPipeHarness cmds(monitor);
    MonitorBackgroundRun monitor_bg(monitor);

    cmds.write_all(std::string(FeedMonitor::kMaxCommandLineBufferBytes + 1, 'x') + "tail\nping\n");

    REQUIRE(wait_until([&] { return cmds.count() >= 1; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // let any (wrong) extra line show up
    std::lock_guard<std::mutex> lock(cmds.mu);
    REQUIRE(cmds.received.size() == 1u);
    CHECK(cmds.received[0] == "ping");
}

TEST_CASE("many complete command lines totalling far more than the cap are all delivered") {
    // Regression: the cap bounds only the unterminated remainder, not a whole burst.
    FeedMonitor monitor(fast_test_config());
    CommandPipeHarness cmds(monitor);
    MonitorBackgroundRun monitor_bg(monitor);

    constexpr size_t kLines = 20000;  // ~140KB at 7 bytes each
    std::string burst;
    for (size_t i = 0; i < kLines; ++i) burst += "line-" + std::to_string(i % 10) + "\n";
    cmds.write_all(burst);

    REQUIRE(wait_until([&] { return cmds.count() == kLines; }));
    std::lock_guard<std::mutex> lock(cmds.mu);
    CHECK(cmds.received.front() == "line-0");
    CHECK(cmds.received.back() == "line-9");
}

TEST_CASE("a fast command producer cannot starve the reactor's timer ticks") {
    // The per-wakeup read budget must let ticks keep firing during the flood;
    // the tick count is sampled when the last line is processed.
    Config config = fast_test_config();
    config.tick_interval = std::chrono::milliseconds(1);
    FeedMonitor monitor(config);

    std::atomic<uint64_t> ticks{0};
    monitor.set_tick_callback([&ticks](const AggregateStats&) { ticks.fetch_add(1); });

    constexpr size_t kLines = 300000;
    std::atomic<size_t> lines_seen{0};
    std::atomic<uint64_t> ticks_at_last_line{0};
    int fds[2];
    make_nonblocking_pipe(fds);
    REQUIRE(monitor.set_command_input(fds[0], [&](const std::string&) {
        if (lines_seen.fetch_add(1) + 1 == kLines) ticks_at_last_line.store(ticks.load());
    }));

    {
        MonitorBackgroundRun monitor_bg(monitor);
        const std::string chunk = [] {
            std::string c;
            for (int i = 0; i < 400; ++i) c += "flood-flood\n";
            return c;
        }();
        for (size_t sent = 0; sent < kLines; sent += 400) {
            write_all(fds[1], chunk);
        }
        REQUIRE(wait_until([&] { return lines_seen.load() == kLines; }, 10000));
    }
    CHECK(ticks_at_last_line.load() >= 3u);

    ::close(fds[0]);
    ::close(fds[1]);
}

TEST_CASE("set_command_input reports epoll registration failure") {
    FeedMonitor monitor(fast_test_config());
    CHECK(!monitor.set_command_input(-1, [](const std::string&) {}));
}

TEST_CASE("reconnect backoff counts consecutive failures: it resets once a connection has recovered") {
    // Regression: reconnect_attempts must reset once a connection has recovered
    // (max_reconnect_attempts counts consecutive failures). With a budget of 2:
    // two failures, a stable connection, then a drop must still be retried.
    MultiAcceptListener peer;

    Config config = fast_test_config();
    config.max_reconnect_attempts = 2;
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    MonitorBackgroundRun monitor_bg(monitor);

    // Two connections that die before the handshake (budget now exhausted)...
    for (int i = 0; i < 2; ++i) {
        const int fd = peer.accept_one();
        REQUIRE(fd >= 0);
        peer.close_peer(fd);
    }

    // ...then one that handshakes and stays healthy past the stability window (100ms), answering pings.
    const int good_fd = peer.accept_one();
    REQUIRE(good_fd >= 0);
    manual_handshake(good_fd, 1);
    const auto healthy_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(400);
    while (std::chrono::steady_clock::now() < healthy_until) {
        const HeartbeatMessage ping = recv_wire(good_fd);
        REQUIRE(static_cast<MessageType>(ping.type) == MessageType::kPing);
        HeartbeatMessage pong = ping;
        pong.type = static_cast<uint8_t>(MessageType::kPong);
        send_wire(good_fd, pong);
    }
    REQUIRE(state_of(*box.get(), 1) == ConnectionState::kHealthy);

    peer.close_peer(good_fd);  // fail again

    const int retry_fd = peer.accept_one(3000);
    CHECK(retry_fd >= 0);  // -1 means the monitor gave up despite the intervening recovery
}

TEST_CASE("a connection that fails while sending a heartbeat inside the tick is reaped and reconnected exactly once") {
    // A send_ping() failure inside the tick must be reaped by the tick itself.
    // The reactor is held in the tick callback while the peer resets, so the next
    // epoll_wait returns [timer, connection error]; the stale connection event
    // must then be ignored (one ALERT, one reconnect).
    // megabytes, so filling it takes minutes at one ping per tick.)
    MultiAcceptListener peer;

    Config config = fast_test_config();
    config.heartbeat_interval = std::chrono::milliseconds(10);
    config.heartbeat_timeout = std::chrono::seconds(600);  // the timeout path must not be what reaps it
    config.tick_interval = std::chrono::milliseconds(50);
    FeedMonitor monitor(config);

    StatsBox box;
    std::atomic<bool> stall_requested{false};
    std::atomic<bool> in_stall{false};
    monitor.set_tick_callback([&](const AggregateStats& s) {
        box.set(s);
        if (stall_requested.exchange(false)) {
            in_stall.store(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    StderrCapture stderr_capture;
    {
        MonitorBackgroundRun monitor_bg(monitor);

        const int fd1 = peer.accept_one();
        REQUIRE(fd1 >= 0);
        manual_handshake(fd1, 1);
        REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

        stall_requested.store(true);
        REQUIRE(wait_until([&in_stall] { return in_stall.load(); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(75));  // > one tick period: the timer is ready first
        peer.close_peer(fd1, /*reset=*/true);

        const int fd2 = peer.accept_one(5000);
        CHECK(fd2 >= 0);  // the slot was rescheduled and dialed back in
    }
    const std::string log = stderr_capture.finish();
    size_t alerts = 0;
    for (size_t pos = 0; (pos = log.find("ALERT", pos)) != std::string::npos; pos += 5) ++alerts;
    CHECK(alerts == 1u);
}

TEST_CASE("a failed connection is closed promptly: the peer sees EOF during backoff") {
    // Regression: a failed connection's socket must be closed immediately, so
    // the peer sees EOF during backoff (10s, so no rebind() in this test).
    MultiAcceptListener peer;

    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::seconds(10);
    config.reconnect_max_delay = std::chrono::seconds(10);
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", peer.port()}));

    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
    REQUIRE(monitor.set_command_input(pipe_fds[0], [&monitor](const std::string& line) {
        if (line == "disconnect 1") monitor.force_disconnect(1);
    }));

    {
        MonitorBackgroundRun monitor_bg(monitor);

        const int fd = peer.accept_one();
        REQUIRE(fd >= 0);
        manual_handshake(fd, 1);
        REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

        write_all(pipe_fds[1], "disconnect 1\n");

        CHECK(peer_sees_close(fd));
        REQUIRE(wait_for_state(box, 1, ConnectionState::kFailed));
    }
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("set_heartbeat_interval/timeout reject non-positive values and leave the monitor working") {
    FeedMonitor monitor(fast_test_config());
    CHECK(!monitor.set_heartbeat_interval(std::chrono::milliseconds(0)));
    CHECK(!monitor.set_heartbeat_interval(std::chrono::milliseconds(-5)));
    CHECK(!monitor.set_heartbeat_timeout(std::chrono::milliseconds(0)));
    CHECK(!monitor.set_heartbeat_timeout(std::chrono::milliseconds(-1)));
    CHECK(monitor.set_heartbeat_interval(std::chrono::milliseconds(20)));
    CHECK(monitor.set_heartbeat_timeout(std::chrono::milliseconds(200)));
}

TEST_CASE("shortening heartbeat_interval takes effect immediately, not after the old interval elapses") {
    // With a 60s interval the next ping is a minute away; shrinking to 20ms must
    // pull it forward.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    SimulatorBackgroundRun sim_bg(sim);

    Config config = fast_test_config();
    config.heartbeat_interval = std::chrono::seconds(60);
    config.heartbeat_timeout = std::chrono::seconds(120);
    FeedMonitor monitor(config);
    StatsBox box;
    monitor.set_tick_callback([&box](const AggregateStats& s) { box.set(s); });
    REQUIRE(monitor.add_feed(FeedEndpoint{1, "TEST", "127.0.0.1", port}));

    std::atomic<int> setter_result{-1};
    int pipe_fds[2];
    make_nonblocking_pipe(pipe_fds);
    REQUIRE(monitor.set_command_input(pipe_fds[0], [&](const std::string& line) {
        if (line == "bad") setter_result.store(monitor.set_heartbeat_interval(std::chrono::milliseconds(0)) ? 1 : 0);
        if (line == "fast") monitor.set_heartbeat_interval(std::chrono::milliseconds(20));
    }));

    {
        MonitorBackgroundRun monitor_bg(monitor);
        REQUIRE(wait_until([&box] { auto s = box.get(); return s && acked_of(*s, 1) >= 1; }));
        const uint64_t acked_before = acked_of(*box.get(), 1);

        write_all(pipe_fds[1], "bad\n");
        REQUIRE(wait_until([&] { return setter_result.load() != -1; }));
        CHECK(setter_result.load() == 0);  // rejected: the 60s interval is still in force
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK(acked_of(*box.get(), 1) == acked_before);

        write_all(pipe_fds[1], "fast\n");
        CHECK(wait_until([&box, acked_before] { auto s = box.get(); return s && acked_of(*s, 1) >= acked_before + 3; },
                         3000));
    }
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("debug_epoll_wait_count and debug_events_processed_count reflect real reactor activity") {
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

    REQUIRE(wait_for_state(box, 1, ConnectionState::kHealthy));

    // The handshake alone causes several wakeups, each returning at least one event.
    CHECK(monitor.debug_epoll_wait_count() > 0u);
    CHECK(monitor.debug_events_processed_count() >= monitor.debug_epoll_wait_count());
}

TEST_CASE("an alert callback receives the failure message instead of stderr") {
    Config config = fast_test_config();
    config.reconnect_base_delay = std::chrono::seconds(10);
    config.reconnect_max_delay = std::chrono::seconds(10);
    FeedMonitor monitor(config);

    std::mutex alert_mu;
    std::vector<std::string> alerts;
    monitor.set_alert_callback([&](const std::string& message) {
        std::lock_guard<std::mutex> lock(alert_mu);
        alerts.push_back(message);
    });
    REQUIRE(monitor.add_feed(FeedEndpoint{7, "DEAD", "127.0.0.1", closed_port()}));

    MonitorBackgroundRun monitor_bg(monitor);
    REQUIRE(wait_until([&] {
        std::lock_guard<std::mutex> lock(alert_mu);
        return !alerts.empty();
    }));

    std::lock_guard<std::mutex> lock(alert_mu);
    CHECK(alerts.front().find("feed 7") != std::string::npos);
    CHECK(alerts.front().find("DEAD") != std::string::npos);
    CHECK(alerts.front().find("failed") != std::string::npos);
}
