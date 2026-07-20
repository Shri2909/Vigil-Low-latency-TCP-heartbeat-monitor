#pragma once

// Mock exchange: an epoll reactor around a listening socket, mirroring
// FeedMonitor's structure but using Connection in Role::kResponder mode for
// each accepted client. Added per PROJECT_PLAN.md audit finding #1 -- without
// this, Connection/FeedMonitor have nothing to connect to. Includes fault
// injection (drop/latency/jitter/kill) so failure-detection and reconnect
// logic in FeedMonitor are actually exercisable and testable, per section 6.
//
// Storage note: unlike FeedMonitor's connections_ (a flat std::vector, see
// PROJECT_PLAN.md section 5 and 15), clients_ here is an
// unordered_map<int, Connection>. That's a deliberate *different* choice for
// a deliberately different access pattern: FeedMonitor runs a periodic full
// scan over every connection every tick (send_ping + check_timeout), which is
// exactly the cache-locality-sensitive case the flat vector was for.
// FeedSimulator has no such scan -- Role::kResponder's check_timeout is a
// no-op and it never sends pings -- its only access pattern is "an epoll
// event carries an fd, look up the Connection for it," which is precisely
// what a hash map is for. Applying the flat-vector trick here would just
// reintroduce a fd-to-index map to get back point lookups, for no benefit.

#include "config.h"
#include "connection.h"
#include "unique_fd.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

struct FaultConfig {
    double drop_probability = 0.0;               // [0,1]; probability a PONG reply is silently not sent
    std::chrono::milliseconds extra_latency{0};   // artificial delay before an (otherwise undropped) reply
    bool jitter = false;                          // randomize extra_latency in [0.5x, 1.5x] when > 0
};

class FeedSimulator {
public:
    // port == 0 binds an OS-assigned ephemeral port (see port()) -- used by
    // tests; a real deployment passes a fixed port. Throws std::runtime_error
    // if socket setup fails (system-boundary failure, not a content one).
    FeedSimulator(uint16_t port, uint32_t feed_id, Config config, FaultConfig faults = {});

    FeedSimulator(const FeedSimulator&) = delete;
    FeedSimulator& operator=(const FeedSimulator&) = delete;

    uint16_t port() const noexcept { return bound_port_; }
    size_t client_count() const noexcept { return clients_.size(); }

    void run();   // blocks until stop() is called
    void stop();  // safe to call from another thread or a signal handler

    // Safe to call from the reactor thread only (e.g. from a command-input
    // callback, or before run() starts) -- same rule as FeedMonitor's
    // non-stop() methods; faults_/clients_ have no internal synchronization.
    void set_fault_config(FaultConfig faults);

    // Picks one connected client at random and closes it with SO_LINGER{1,0}
    // set first, so the close sends a raw RST instead of a graceful FIN --
    // more realistic for "the exchange abruptly crashed" than a clean
    // shutdown, and it exercises the monitor's ECONNRESET path specifically,
    // not just the "peer closed" EOF path FeedSimulator's normal disconnects
    // already cover. No-op if there are no clients.
    void kill_random_connection();

    // Registers fd (e.g. STDIN_FILENO) into this same epoll set for simple
    // operator commands ("kill", "set-drop-rate 0.5", "quit"); mirrors
    // FeedMonitor::set_command_input for the same reasons (see feed_monitor.h).
    //
    // PRECONDITION: fd must already be non-blocking (O_NONBLOCK) -- see the
    // detailed reasoning on FeedMonitor::set_command_input; the failure mode
    // (the reactor thread freezes on every client, not just this fd) is
    // identical here. A pipe's default is blocking and pipe(2) has no
    // equivalent of SOCK_NONBLOCK, so a test feeding one via a pipe must
    // fcntl() it explicitly, same as main_simulator.cpp does for stdin.
    using CommandLineCallback = std::function<void(const std::string& line)>;
    void set_command_input(int fd, CommandLineCallback on_line);

    // How large command_line_buffer_ is allowed to grow while waiting for a
    // '\n' before its contents are discarded (see the warning logged in
    // on_command_fd_readable). Public so tests can exercise the boundary
    // without duplicating the constant.
    static constexpr size_t kMaxCommandLineBufferBytes = 64 * 1024;

private:
    struct PendingReply {
        int64_t fire_at_ns;
        int fd;
        uint64_t sequence;
        int64_t timestamp_ns;
    };

    void accept_new_connections(int64_t now_ns);
    void handle_client_event(int fd, uint32_t events, int64_t now_ns);
    void remove_client(int fd);
    void sync_epoll_interest(int fd, Connection& conn);
    void on_timer_tick();
    void on_command_fd_readable();
    // Called synchronously from within a client Connection's handle_ping via
    // set_ping_interceptor; returns false to suppress the automatic reply
    // (either dropping it for good, or scheduling a deferred send_pong()).
    bool on_ping_intercept(int fd, uint64_t sequence, int64_t timestamp_ns);

    UniqueFd listen_fd_;
    UniqueFd epoll_fd_;
    UniqueFd wake_fd_;
    UniqueFd timer_fd_;
    uint16_t bound_port_ = 0;
    uint32_t feed_id_;
    Config config_;
    FaultConfig faults_;
    std::mt19937_64 rng_{std::random_device{}()};
    std::unordered_map<int, Connection> clients_;
    std::vector<PendingReply> pending_replies_;
    int command_fd_ = -1;
    std::string command_line_buffer_;
    CommandLineCallback on_command_line_;
    std::atomic<bool> stop_requested_{false};
    std::vector<epoll_event> event_batch_;
};
