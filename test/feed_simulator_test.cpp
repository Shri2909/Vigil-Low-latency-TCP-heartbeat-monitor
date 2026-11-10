// FeedSimulator-specific coverage: multi-client handling, ephemeral port, stop(),
// fault injection, command input, resource limits. Handshake/ping-pong are
// covered via feed_monitor_test.cpp. run() is driven from a background thread;
// tests stop()+join() before reading client_count() (not synchronized).

#include "config.h"
#include "feed_simulator.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

// Bounds every blocking socket call so a regression fails a REQUIRE instead of hanging.
constexpr int kIoTimeoutSec = 5;

void set_io_timeouts(int fd) {
    timeval tv{kIoTimeoutSec, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

int connect_blocking(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    set_io_timeouts(fd);  // SO_SNDTIMEO also bounds connect()
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    REQUIRE(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    return fd;
}

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

bool wait_readable(int fd, int timeout_ms) {
    pollfd pfd{fd, POLLIN, 0};
    const int rc = ::poll(&pfd, 1, timeout_ms);
    return rc > 0 && (pfd.revents & POLLIN) != 0;
}

// set_command_input needs a non-blocking fd; pipe(2) has no O_NONBLOCK option.
void set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    REQUIRE(flags >= 0);
    REQUIRE(::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0);
}

// True if the peer has closed (EOF) or reset the connection within timeout_ms.
bool peer_closed_within(int fd, int timeout_ms) {
    if (!wait_readable(fd, timeout_ms)) return false;
    uint8_t buf[64];
    const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    return n == 0 || (n < 0 && errno == ECONNRESET);
}

// Runs sim.run() on a thread; true if it returns within timeout_ms (else stops it and joins).
bool run_returns_within(FeedSimulator& sim, int timeout_ms) {
    std::atomic<bool> returned{false};
    std::thread runner([&] {
        sim.run();
        returned.store(true);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!returned.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const bool ok = returned.load();
    if (!ok) sim.stop();
    runner.join();
    return ok;
}

void handshake(int fd, uint32_t feed_id) {
    HeartbeatMessage hello{};
    hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
    hello.feed_id = feed_id;
    send_wire(fd, hello);
    const HeartbeatMessage ack = recv_wire(fd);
    REQUIRE(static_cast<MessageType>(ack.type) == MessageType::kConnectAck);
}

// Runs `sim` on a background thread; stop()s and joins on destruction.
class BackgroundRun {
public:
    explicit BackgroundRun(FeedSimulator& sim) : sim_(sim), thread_([&sim] { sim.run(); }) {}
    ~BackgroundRun() {
        sim_.stop();
        if (thread_.joinable()) thread_.join();
    }
    BackgroundRun(const BackgroundRun&) = delete;
    BackgroundRun& operator=(const BackgroundRun&) = delete;

private:
    FeedSimulator& sim_;
    std::thread thread_;
};

}  // namespace

TEST_CASE("constructing with port 0 binds an OS-assigned ephemeral port") {
    FeedSimulator sim(0, 1, Config{});
    CHECK(sim.port() != 0);
}

TEST_CASE("a single client completes the handshake and gets tracked") {
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    {
        BackgroundRun bg(sim);
        const int fd = connect_blocking(port);
        handshake(fd, 1);
        // Give the reactor time to observe the close before stop() can race it.
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    CHECK(sim.client_count() == 0);  // the client closed its side before we got here
}

TEST_CASE("multiple concurrent clients are all tracked independently") {
    FeedSimulator sim(0, /*feed_id=*/9, Config{});
    const uint16_t port = sim.port();
    int fd_a, fd_b, fd_c;
    {
        BackgroundRun bg(sim);
        fd_a = connect_blocking(port);
        fd_b = connect_blocking(port);
        fd_c = connect_blocking(port);
        handshake(fd_a, 9);
        handshake(fd_b, 9);
        handshake(fd_c, 9);

        HeartbeatMessage ping{};
        ping.type = static_cast<uint8_t>(MessageType::kPing);
        ping.feed_id = 9;
        ping.sequence = 1;
        ping.timestamp_ns = 42;
        send_wire(fd_b, ping);
        const HeartbeatMessage pong = recv_wire(fd_b);
        CHECK(static_cast<MessageType>(pong.type) == MessageType::kPong);
        CHECK(pong.sequence == 1u);
        CHECK(pong.timestamp_ns == 42);
    }
    CHECK(sim.client_count() == 3);  // all three sockets are still open post-join
    ::close(fd_a);
    ::close(fd_b);
    ::close(fd_c);
}

TEST_CASE("a client that disconnects is removed from tracking") {
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    {
        BackgroundRun bg(sim);
        const int fd = connect_blocking(port);
        handshake(fd, 1);
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    CHECK(sim.client_count() == 0);
}

TEST_CASE("a client stuck mid-handshake is reaped after heartbeat_timeout") {
    Config config;
    config.heartbeat_timeout = std::chrono::milliseconds(150);
    config.tick_interval = std::chrono::milliseconds(20);
    FeedSimulator sim(0, 1, config);
    const uint16_t port = sim.port();
    int fd;
    {
        BackgroundRun bg(sim);
        fd = connect_blocking(port);
        std::this_thread::sleep_for(std::chrono::milliseconds(400));  // well past heartbeat_timeout
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
}

TEST_CASE("a burst of 300 stuck-mid-handshake clients are all eventually reaped without stalling or leaking") {
    constexpr int kClientCount = 300;
    Config config;
    config.heartbeat_timeout = std::chrono::milliseconds(200);
    config.tick_interval = std::chrono::milliseconds(20);
    FeedSimulator sim(0, 1, config);
    const uint16_t port = sim.port();

    std::vector<int> fds;
    fds.reserve(kClientCount);
    {
        BackgroundRun bg(sim);

        for (int i = 0; i < kClientCount; ++i) {
            fds.push_back(connect_blocking(port));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(800));  // well past heartbeat_timeout for all of them
    }
    CHECK(sim.client_count() == 0);

    for (int fd : fds) ::close(fd);
}

TEST_CASE("a healthy client that stops sending PING is reaped after heartbeat_timeout") {
    Config config;
    config.heartbeat_timeout = std::chrono::milliseconds(150);
    config.tick_interval = std::chrono::milliseconds(20);
    FeedSimulator sim(0, 1, config);
    const uint16_t port = sim.port();
    int fd;
    {
        BackgroundRun bg(sim);
        fd = connect_blocking(port);
        handshake(fd, 1);
        // then goes silent -- never sends another PING
        std::this_thread::sleep_for(std::chrono::milliseconds(400));  // well past heartbeat_timeout
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
}

TEST_CASE("FeedSimulator: stop() from another thread causes run() to return promptly") {
    FeedSimulator sim(0, 1, Config{});
    std::atomic<bool> returned{false};
    std::thread runner([&] {
        sim.run();
        returned.store(true);
    });
    sim.stop();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!returned.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const bool ok = returned.load();
    if (!ok) sim.stop();  // let the thread finish so join() below can't hang
    runner.join();
    CHECK(ok);
}

TEST_CASE("a stop() issued before run() is not lost") {
    FeedSimulator sim(0, 1, Config{});
    sim.stop();
    CHECK(run_returns_within(sim, 5000));
}

TEST_CASE("run() sends DISCONNECT to connected clients on graceful shutdown") {
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);
    }  // stop()+join: run() has returned and said goodbye
    const HeartbeatMessage msg = recv_wire(fd);
    CHECK(static_cast<MessageType>(msg.type) == MessageType::kDisconnect);
    ::close(fd);
}

TEST_CASE("drop_probability=1.0 causes every PONG reply to be silently dropped") {
    FaultConfig faults;
    faults.drop_probability = 1.0;
    FeedSimulator sim(0, 1, Config{}, faults);
    const uint16_t port = sim.port();
    BackgroundRun bg(sim);

    const int fd = connect_blocking(port);
    handshake(fd, 1);

    HeartbeatMessage ping{};
    ping.type = static_cast<uint8_t>(MessageType::kPing);
    ping.feed_id = 1;
    ping.sequence = 0;
    ping.timestamp_ns = 1'000;
    send_wire(fd, ping);

    // Dropped, not just slow: an immediate reply would land in well under 1ms.
    CHECK(!wait_readable(fd, 200));
    ::close(fd);
}

TEST_CASE("extra_latency delays the PONG reply but it still eventually arrives") {
    FaultConfig faults;
    faults.extra_latency = std::chrono::milliseconds(150);
    FeedSimulator sim(0, 1, Config{}, faults);
    const uint16_t port = sim.port();
    BackgroundRun bg(sim);

    const int fd = connect_blocking(port);
    handshake(fd, 1);

    HeartbeatMessage ping{};
    ping.type = static_cast<uint8_t>(MessageType::kPing);
    ping.feed_id = 1;
    ping.sequence = 5;
    ping.timestamp_ns = 2'000;
    send_wire(fd, ping);

    // Not yet at 30ms (well under the 150ms configured delay)...
    CHECK(!wait_readable(fd, 30));
    // ...but it does show up well within a generous upper bound.
    REQUIRE(wait_readable(fd, 1000));
    const HeartbeatMessage pong = recv_wire(fd);
    CHECK(static_cast<MessageType>(pong.type) == MessageType::kPong);
    CHECK(pong.sequence == 5u);
    CHECK(pong.timestamp_ns == 2'000);  // still echoed unchanged, just late
    ::close(fd);
}

TEST_CASE("kill_random_connection removes a tracked client via the command-input path") {
    // Routed through set_command_input so kill runs on the reactor thread, like the stdin command.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    sim.set_command_input(pipe_fds[0], [&sim](const std::string& line) {
        if (line == "kill") sim.kill_random_connection();
    });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        const std::string cmd = "kill\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

        // Expect an abrupt RST (ECONNRESET), not a graceful EOF.
        REQUIRE(wait_readable(fd, 1000));
        uint8_t buf[4];
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        CHECK(n < 0);
        if (n < 0) {
            CHECK(errno == ECONNRESET);
        }
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("set-drop-rate command updates fault config at runtime") {
    FeedSimulator sim(0, 1, Config{});  // starts with drop_probability=0
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    FaultConfig faults;
    sim.set_command_input(pipe_fds[0], [&sim, &faults](const std::string& line) {
        if (line == "set-drop-rate 1") {
            faults.drop_probability = 1.0;
            sim.set_fault_config(faults);
        }
    });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        const std::string cmd = "set-drop-rate 1\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        HeartbeatMessage ping{};
        ping.type = static_cast<uint8_t>(MessageType::kPing);
        ping.feed_id = 1;
        ping.sequence = 0;
        ping.timestamp_ns = 1;
        send_wire(fd, ping);

        CHECK(!wait_readable(fd, 200));  // now dropped, per the command just sent
    }
    ::close(fd);
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("FeedSimulator: a command arriving in the same batch as command-fd EOF is still processed") {
    // Regression: EOF arriving in the same read() as the last line must still run it.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    sim.set_command_input(pipe_fds[0], [&sim](const std::string& line) {
        if (line == "kill") sim.kill_random_connection();
    });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        const std::string cmd = "kill\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));
        ::close(pipe_fds[1]);  // EOF bundled with the command, not a separate later event

        REQUIRE(wait_readable(fd, 1000));
        uint8_t buf[4];
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        CHECK(n < 0);
        if (n < 0) {
            CHECK(errno == ECONNRESET);
        }
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
    ::close(pipe_fds[0]);
}

TEST_CASE("an unterminated command line beyond the length cap is discarded, not accumulated forever") {
    // Oversized line (one byte past the cap, no newline) is dropped whole, tail included;
    // the following "kill" line must still run.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    sim.set_command_input(pipe_fds[0], [&sim](const std::string& line) {
        if (line == "kill") sim.kill_random_connection();
    });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        std::string oversized(FeedSimulator::kMaxCommandLineBufferBytes + 1, 'x');
        size_t written = 0;
        while (written < oversized.size()) {
            const ssize_t n = ::write(pipe_fds[1], oversized.data() + written, oversized.size() - written);
            REQUIRE(n > 0);
            written += static_cast<size_t>(n);
        }
        const std::string cmd = "\nkill\n";
        REQUIRE(::write(pipe_fds[1], cmd.data(), cmd.size()) == static_cast<ssize_t>(cmd.size()));

        REQUIRE(wait_readable(fd, 2000));
        uint8_t buf[4];
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        CHECK(n < 0);
        if (n < 0) {
            CHECK(errno == ECONNRESET);
        }
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("many short commands in one burst are all executed even when the total exceeds the line cap") {
    // Regression: the cap bounds one line, not a whole burst. 20000 x "noop\n" = 100KB
    // also needs several bounded read batches (edge-triggered re-arm).
    constexpr int kLines = 20000;
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    std::atomic<int> noops{0};
    sim.set_command_input(pipe_fds[0], [&](const std::string& line) {
        if (line == "noop") noops.fetch_add(1);
        if (line == "kill") sim.kill_random_connection();
    });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        std::string burst;
        for (int i = 0; i < kLines; ++i) burst += "noop\n";
        burst += "kill\n";
        size_t written = 0;
        while (written < burst.size()) {
            const ssize_t n = ::write(pipe_fds[1], burst.data() + written, burst.size() - written);
            REQUIRE(n > 0);
            written += static_cast<size_t>(n);
        }
        // "kill" is last: the reset proves everything before it ran.
        CHECK(peer_closed_within(fd, 5000));
    }
    CHECK(noops.load() == kLines);
    ::close(fd);
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("a hard read error on the command fd deregisters it instead of spinning") {
    // Regression: a hard read error must deregister the fd, not busy-loop (checked via CPU time).
    Config config;
    config.edge_triggered = false;
    FeedSimulator sim(0, 1, config);

    // Connected loopback pair, reset from one side so the other read() fails with ECONNRESET.
    const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(lfd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(lfd, 1) == 0);
    socklen_t alen = sizeof(addr);
    REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr), &alen) == 0);
    const int cmd_peer = connect_blocking(ntohs(addr.sin_port));
    const int cmd_fd = ::accept(lfd, nullptr, nullptr);
    REQUIRE(cmd_fd >= 0);
    set_nonblocking(cmd_fd);
    sim.set_command_input(cmd_fd, [](const std::string&) {});

    const linger sl{1, 0};
    ::setsockopt(cmd_peer, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    ::close(cmd_peer);  // RST

    {
        BackgroundRun bg(sim);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // let it hit the error
        const std::clock_t before = std::clock();
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        const double cpu_ms = 1000.0 * static_cast<double>(std::clock() - before) / CLOCKS_PER_SEC;
        CHECK(cpu_ms < 200.0);  // a spinning reactor burns ~400ms here

        const int fd = connect_blocking(sim.port());
        handshake(fd, 1);
        ::close(fd);
    }
    ::close(cmd_fd);
    ::close(lfd);
}

TEST_CASE("set_command_input throws when the fd cannot be registered with epoll") {
    FeedSimulator sim(0, 1, Config{});
    const int fd = ::open("/dev/null", O_RDONLY);  // epoll refuses it (EPERM)
    REQUIRE(fd >= 0);
    bool threw = false;
    try {
        sim.set_command_input(fd, [](const std::string&) {});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    ::close(fd);
}

TEST_CASE("clients beyond max_clients are closed immediately; earlier ones are unaffected") {
    SimulatorOptions opts;
    opts.max_clients = 2;
    FeedSimulator sim(0, 1, Config{}, FaultConfig{}, opts);
    const uint16_t port = sim.port();
    BackgroundRun bg(sim);

    const int a = connect_blocking(port);
    const int b = connect_blocking(port);
    handshake(a, 1);  // proves both a and (accept order) b were accepted before c
    handshake(b, 1);
    const int c = connect_blocking(port);
    CHECK(peer_closed_within(c, 3000));

    ::close(a);
    int d = -1;
    bool admitted = false;
    for (int attempt = 0; attempt < 50 && !admitted; ++attempt) {
        d = connect_blocking(port);
        HeartbeatMessage hello{};
        hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
        hello.feed_id = 1;
        send_wire(d, hello);
        if (wait_readable(d, 200)) {
            uint8_t buf[64];
            const ssize_t n = ::recv(d, buf, sizeof(buf), 0);
            admitted = n > 0;
        }
        ::close(d);
        if (!admitted) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    CHECK(admitted);
    ::close(b);
    ::close(c);
}

TEST_CASE("an invalid bind address is rejected with an exception") {
    SimulatorOptions opts;
    opts.bind_address = "not-an-ip";
    bool threw = false;
    try {
        FeedSimulator sim(0, 1, Config{}, FaultConfig{}, opts);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

TEST_CASE("an explicit 0.0.0.0 bind is accepted and reachable") {
    SimulatorOptions opts;
    opts.bind_address = "0.0.0.0";
    FeedSimulator sim(0, 1, Config{}, FaultConfig{}, opts);
    BackgroundRun bg(sim);
    const int fd = connect_blocking(sim.port());
    handshake(fd, 1);
    ::close(fd);
}

TEST_CASE("pending deferred replies are capped under a huge extra_latency") {
    FaultConfig faults;
    faults.extra_latency = std::chrono::hours(1);
    Config config;
    config.heartbeat_timeout = std::chrono::milliseconds(60'000);  // keep the pinging client alive
    FeedSimulator sim(0, 1, config, faults);
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    std::atomic<size_t> observed{0};
    sim.set_command_input(pipe_fds[0], [&](const std::string&) { observed.store(sim.pending_reply_count()); });

    const int fd = connect_blocking(port);
    {
        BackgroundRun bg(sim);
        handshake(fd, 1);

        const size_t kPings = FeedSimulator::kMaxPendingReplies + 100;
        for (size_t i = 0; i < kPings; ++i) {
            HeartbeatMessage ping{};
            ping.type = static_cast<uint8_t>(MessageType::kPing);
            ping.feed_id = 1;
            ping.sequence = i;
            ping.timestamp_ns = 1;
            send_wire(fd, ping);
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        size_t last = 0;
        int stable = 0;
        while (stable < 3 && std::chrono::steady_clock::now() < deadline) {
            REQUIRE(::write(pipe_fds[1], "p\n", 2) == 2);
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            const size_t now = observed.load();
            stable = (now == last && now > 0) ? stable + 1 : 0;
            last = now;
        }
        CHECK(stable >= 3);
        CHECK(last == FeedSimulator::kMaxPendingReplies);
    }
    ::close(fd);
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

TEST_CASE("accept4 EMFILE does not stall the listener: the queued connection is shed and service continues") {
    // The spare fd lets accept4 shed the queued connection on EMFILE (else it would stick in
    // the backlog under edge-triggered epoll).
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();
    BackgroundRun bg(sim);

    rlimit original{};
    REQUIRE(::getrlimit(RLIMIT_NOFILE, &original) == 0);
    struct RestoreLimit {
        rlimit saved;
        std::vector<int> hogs;
        ~RestoreLimit() {
            ::setrlimit(RLIMIT_NOFILE, &saved);
            for (int h : hogs) ::close(h);
        }
    } guard{original, {}};

    rlimit squeezed = original;
    squeezed.rlim_cur = std::min<rlim_t>(original.rlim_cur, 256);
    REQUIRE(::setrlimit(RLIMIT_NOFILE, &squeezed) == 0);

    // Hog all descriptors, then free exactly one for the client socket (none left for accept4).
    while (true) {
        const int h = ::dup(STDIN_FILENO);
        if (h < 0) break;
        guard.hogs.push_back(h);
    }
    REQUIRE(!guard.hogs.empty());
    ::close(guard.hogs.back());
    guard.hogs.pop_back();

    const int victim = connect_blocking(port);  // connect() completes in the kernel backlog
    CHECK(peer_closed_within(victim, 3000));    // shed by the spare-fd path
    ::close(victim);

    for (int h : guard.hogs) ::close(h);
    guard.hogs.clear();
    ::setrlimit(RLIMIT_NOFILE, &original);

    const int fd = connect_blocking(port);
    handshake(fd, 1);
    ::close(fd);
}

TEST_CASE("fd-number reuse churn: stale events never reach a recycled client") {
    // Regression: stale events in one batch must not reach a recycled fd number. Not forceable
    // deterministically, so hammer it: kill clients while new ones reuse the numbers.
    FeedSimulator sim(0, 1, Config{});
    const uint16_t port = sim.port();

    int pipe_fds[2];
    REQUIRE(::pipe(pipe_fds) == 0);
    set_nonblocking(pipe_fds[0]);
    std::atomic<int> kills_done{0};
    sim.set_command_input(pipe_fds[0], [&](const std::string& line) {
        if (line == "kill") {
            sim.kill_random_connection();
            kills_done.fetch_add(1);
        }
    });

    constexpr int kRounds = 100;
    {
        BackgroundRun bg(sim);
        std::vector<int> open_fds;
        for (int round = 0; round < kRounds; ++round) {
            for (int i = 0; i < 4; ++i) open_fds.push_back(connect_blocking(port));
            REQUIRE(::write(pipe_fds[1], "kill\nkill\n", 10) == 10);
            for (int fd : open_fds) {
                ::close(fd);
            }
            open_fds.clear();
        }
        // Let every queued "kill" run first so none lands on the fresh client below.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (kills_done.load() < 2 * kRounds && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        REQUIRE(kills_done.load() == 2 * kRounds);

        const int fd = connect_blocking(port);
        handshake(fd, 1);
        HeartbeatMessage ping{};
        ping.type = static_cast<uint8_t>(MessageType::kPing);
        ping.feed_id = 1;
        ping.sequence = 7;
        ping.timestamp_ns = 99;
        send_wire(fd, ping);
        const HeartbeatMessage pong = recv_wire(fd);
        CHECK(static_cast<MessageType>(pong.type) == MessageType::kPong);
        CHECK(pong.sequence == 7u);
        ::close(fd);
    }
    ::close(pipe_fds[1]);
    ::close(pipe_fds[0]);
}

namespace {

// Runs the real feed_simulator binary (stdin /dev/null, stderr merged); returns its exit
// status, or -1 on timeout (5s, so a regression that starts a server fails instead of hanging).
int run_simulator_binary(const std::string& args, std::string* output) {
    // feed_simulator sits next to run_tests in bin/.
    char self[4096];
    const ssize_t len = ::readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (len <= 0) return -1;
    std::string dir(self, static_cast<size_t>(len));
    dir.erase(dir.rfind('/'));
    const std::string cmd = "timeout 5 \"" + dir + "/feed_simulator\" " + args + " </dev/null 2>&1";
    FILE* pipe = ::popen(cmd.c_str(), "r");
    if (pipe == nullptr) return -1;
    char buf[512];
    output->clear();
    while (std::fgets(buf, sizeof(buf), pipe) != nullptr) output->append(buf);
    const int status = ::pclose(pipe);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

}  // namespace

TEST_CASE("feed_simulator CLI rejects out-of-range and malformed values with exit status 2") {
    const char* kBad[] = {
        "--port 70000",
        "--port -1",
        "--port 1 --feed-id 4294967296",
        "--port 1 --feed-id -1",
        "--port 1 --drop-rate 1.5",
        "--port 1 --drop-rate -0.1",
        "--port 1 --drop-rate nan",
        "--port 1 --extra-latency-ms -5",
        "--port 1 --extra-latency-ms 99999999999",
        "--port 1 --jitter maybe",
        "--port 1 --tcp-nodelay 2",
        "--port 1 --bind not-an-ip",
        "--port 1 --max-clients 0",
        "--port 1 --heartbeat-timeout-ms 0",
        "--port 1 --tick-interval-ms -1",
        "--port abc",
        "--feed-id 1",  // --port missing
    };
    for (const char* args : kBad) {
        std::string out;
        const int rc = run_simulator_binary(args, &out);
        if (rc != 2) std::fprintf(stderr, "unexpected exit %d for args: %s\n", rc, args);
        CHECK(rc == 2);
        CHECK(out.find("error:") != std::string::npos);
    }
}

TEST_CASE("feed_simulator --help documents the new flags and prints a single percent sign") {
    std::string out;
    CHECK(run_simulator_binary("--help", &out) == 0);
    CHECK(out.find("--bind") != std::string::npos);
    CHECK(out.find("--max-clients") != std::string::npos);
    CHECK(out.find("--heartbeat-timeout-ms") != std::string::npos);
    CHECK(out.find("--tick-interval-ms") != std::string::npos);
    CHECK(out.find("%%") == std::string::npos);
    CHECK(out.find("+/-50%") != std::string::npos);
}
