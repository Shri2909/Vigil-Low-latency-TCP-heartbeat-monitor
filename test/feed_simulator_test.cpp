// FeedSimulator-specific coverage only. The bulk of its behavior (handshake,
// ping/pong correctness) is exercised end-to-end by feed_monitor_test.cpp,
// which uses it as FeedMonitor's test double -- duplicating that here would
// just be redundant. What's specific to FeedSimulator itself: multi-client
// handling, ephemeral port binding, and stop()-from-another-thread.
//
// Note on thread safety in these tests: FeedSimulator's clients_ map has no
// internal synchronization (single-threaded reactor by design -- see
// PROJECT_PLAN.md finding #17). run() is driven from a background
// std::thread here purely as test scaffolding (to let the test's main thread
// act as a client), so every test takes care to call stop() and join()
// *before* touching client_count() -- reading it while run() is still active
// on another thread would itself be the kind of data race the single-thread
// design is supposed to avoid needing a mutex for.

#include "config.h"
#include "feed_simulator.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <chrono>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

int connect_blocking(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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

// set_command_input requires a non-blocking fd (its read loop drains to
// EAGAIN); a pipe's default is blocking, and pipe(2) gives no way to request
// O_NONBLOCK at creation time the way socket()/accept4() do, so this must be
// applied explicitly after the fact.
void set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool wait_readable(int fd, int timeout_ms) {
    pollfd pfd{fd, POLLIN, 0};
    const int rc = ::poll(&pfd, 1, timeout_ms);
    return rc > 0 && (pfd.revents & POLLIN) != 0;
}

void handshake(int fd, uint32_t feed_id) {
    HeartbeatMessage hello{};
    hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
    hello.feed_id = feed_id;
    send_wire(fd, hello);
    const HeartbeatMessage ack = recv_wire(fd);
    REQUIRE(static_cast<MessageType>(ack.type) == MessageType::kConnectAck);
}

// Runs `sim` on a background thread and joins it on destruction (after
// calling stop()), so every test gets a clean, race-free shutdown without
// repeating the same three lines everywhere.
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
        // handshake()'s blocking recv() proves the *accept* was already
        // processed by the reactor thread -- but that says nothing about
        // the close() below, which is a separate, independent event racing
        // against BackgroundRun's destructor calling stop() at the end of
        // this scope. There is no ordering guarantee between "the reactor
        // thread's epoll_wait observes the FIN from this close()" and "the
        // reactor thread's epoll_wait observes stop()'s eventfd write" --
        // under a fast build the former usually (not always) wins by
        // chance; under ASan/UBSan's much slower instrumented execution it
        // didn't, and this test flaked. Same fix as the near-identical test
        // below: give the reactor a bounded window to observe the close
        // before stop() can possibly race it.
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // safe to read now: BackgroundRun's destructor already stopped and joined
    // the reactor thread, so there is no concurrent access to clients_.
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
        // fd_a and fd_c stay open (untouched) to prove the simulator handles
        // three simultaneous clients, not just whichever one is talked to.
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
        // give the reactor a moment to observe the close via recv()==0
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    CHECK(sim.client_count() == 0);
}

TEST_CASE("a client stuck mid-handshake is reaped after heartbeat_timeout") {
    // Regression test for F13: Role::kResponder connections used to never
    // time out at all, so a client that connects and never sends
    // CONNECT_HELLO would occupy a Connection+fd on FeedSimulator forever --
    // a real, unbounded resource-exhaustion vector for a hung test client.
    Config config;
    config.heartbeat_timeout = std::chrono::milliseconds(150);
    config.tick_interval = std::chrono::milliseconds(20);
    FeedSimulator sim(0, 1, config);
    const uint16_t port = sim.port();
    int fd;
    {
        BackgroundRun bg(sim);
        fd = connect_blocking(port);
        // deliberately never send CONNECT_HELLO -- just go silent
        std::this_thread::sleep_for(std::chrono::milliseconds(400));  // well past heartbeat_timeout
    }
    CHECK(sim.client_count() == 0);
    ::close(fd);
}

TEST_CASE("a burst of 300 stuck-mid-handshake clients are all eventually reaped without stalling or leaking") {
    // Stress test for F13's timeout-reaping under an actual connection-flood
    // / handshake-storm burst, not just one client at a time. Nothing limits
    // how many simultaneous un-handshaked connections FeedSimulator will
    // track -- bounded eventually by RLIMIT_NOFILE and F13's reaping -- but
    // this was never tested under many connect-then-go-silent clients
    // hitting at once, which is exactly the shape a real flood (accidental
    // or otherwise) would take.
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
        // deliberately never send CONNECT_HELLO from any of them.

        std::this_thread::sleep_for(std::chrono::milliseconds(800));  // well past heartbeat_timeout for all of them
    }
    // Safe to read now -- BackgroundRun's destructor already stopped+joined
    // the reactor thread (same rule as every other client_count() read in
    // this file).
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

TEST_CASE("stop() from another thread causes run() to return promptly") {
    FeedSimulator sim(0, 1, Config{});
    std::thread runner([&sim] { sim.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    sim.stop();

    // join() with a manual timeout via a detached watchdog would be overkill
    // here -- if stop() doesn't work, this test hangs and the test binary's
    // own runtime is the timeout. That's an acceptable failure mode for a
    // regression test of exactly this property.
    runner.join();
    CHECK(true);  // reaching this line at all is the assertion
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

    // With drop_probability=1.0, no PONG should arrive within a generous
    // window -- a real network could always be slow, so this can't prove a
    // negative *forever*, but it distinguishes "dropped" from "immediate
    // reply" with plenty of margin (immediate replies land in well under 1ms
    // on loopback, as the RTT numbers elsewhere in this test suite show).
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
    // kill_random_connection() mutates clients_, so -- like every other
    // FeedSimulator method except stop() -- it must run on the reactor
    // thread. Route it through set_command_input, exactly as
    // main_simulator.cpp's stdin "kill" command does, rather than calling it
    // directly from this test thread while run() is active elsewhere.
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

        // The killed connection should observe an abrupt reset (RST, via
        // SO_LINGER), not a graceful close -- recv() returns -1/ECONNRESET
        // rather than 0/EOF. Poll first so this doesn't race the kill command
        // actually being processed on the reactor thread.
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
        // give the reactor a moment to process the command before the ping below
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

TEST_CASE("a command arriving in the same batch as command-fd EOF is still processed") {
    // Regression test for a real bug: on_command_fd_readable() used to
    // return immediately on EOF without ever running the line-parsing loop
    // over whatever had just been appended to command_line_buffer_ in that
    // same read() call -- exactly what piped/redirected input does (unlike
    // an interactive TTY, where EOF essentially never arrives bundled with
    // the last real line in one read()). Reproduced here by closing the
    // pipe's write end immediately after writing "kill", rather than at the
    // end of the test the way the sibling tests above do.
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
    // Regression test: command_line_buffer_ used to have no length cap at
    // all, so a command source that never emits '\n' would grow it without
    // bound. Sends one byte past the cap with no trailing newline, then a
    // real "kill" command in the same write: if the cap is enforced, the
    // oversized prefix is discarded and "kill" arrives on its own, killing
    // the connection; if not, "kill" arrives concatenated onto the ~64KB of
    // 'x's that preceded it, fails the `line == "kill"` comparison, and the
    // connection is never touched -- observed here via the client socket's
    // own liveness, the same way "kill_random_connection removes a tracked
    // client" above observes it.
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

        // Sent as two separate writes, not one buffer with "kill\n" appended
        // directly after the 'x's: on_command_fd_readable reads in fixed
        // 1024-byte chunks, so the boundary between the oversized prefix and
        // a trailing command could otherwise land inside the very chunk that
        // crosses the cap, discarding the command along with the garbage
        // that preceded it -- a false failure of this test, not a real one.
        // Splitting into two writes with a pause between them guarantees the
        // reactor drains and discards the first payload in its own read
        // cycle before "kill\n" ever arrives.
        std::string oversized(FeedSimulator::kMaxCommandLineBufferBytes + 1, 'x');
        size_t written = 0;
        while (written < oversized.size()) {
            const ssize_t n = ::write(pipe_fds[1], oversized.data() + written, oversized.size() - written);
            REQUIRE(n > 0);
            written += static_cast<size_t>(n);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        const std::string cmd = "kill\n";
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
