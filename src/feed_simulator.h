#pragma once

// Mock exchange: an epoll reactor around a listening socket that serves each
// client with a Connection in Role::kResponder, plus fault injection
// (drop/latency/jitter/kill) to exercise FeedMonitor's failure handling.
// clients_ is a hash map (point lookup by event fd), unlike FeedMonitor's flat
// vector which is scanned in full every tick.

#include "config.h"
#include "connection.h"
#include "unique_fd.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>

struct FaultConfig {
    double drop_probability = 0.0;               // [0,1]; probability a PONG reply is silently not sent
    std::chrono::milliseconds extra_latency{0};   // artificial delay before an (otherwise undropped) reply
    bool jitter = false;                          // randomize extra_latency in [0.5x, 1.5x] when > 0
};

// Listener knobs. Defaults are safe: loopback only and a bounded client count.
struct SimulatorOptions {
    std::string bind_address = "127.0.0.1";  // dotted-quad IPv4; "0.0.0.0" to listen on every interface
    size_t max_clients = 1024;               // accepts beyond this are closed immediately
};

class FeedSimulator {
public:
    // port == 0 binds an OS-assigned ephemeral port (see port()). Throws
    // std::runtime_error if socket setup fails or bind_address is not IPv4.
    FeedSimulator(uint16_t port, uint32_t feed_id, Config config, FaultConfig faults = {},
                  const SimulatorOptions& options = {});

    FeedSimulator(const FeedSimulator&) = delete;
    FeedSimulator& operator=(const FeedSimulator&) = delete;

    uint16_t port() const noexcept { return bound_port_; }
    size_t client_count() const noexcept { return clients_.size(); }
    // Deferred (extra_latency) replies currently queued. Reactor thread only.
    size_t pending_reply_count() const noexcept { return pending_replies_.size(); }

    // Blocks until stop(); a stop() issued before run() is honored. On return,
    // best-effort sends DISCONNECT to every client.
    void run();
    void stop();  // safe to call from another thread or a signal handler

    // Reactor thread only (e.g. a command callback, or before run()).
    void set_fault_config(FaultConfig faults);

    // Closes a random client with SO_LINGER{1,0} so it sees a RST (abrupt crash,
    // ECONNRESET path) rather than a FIN. No-op if there are no clients.
    void kill_random_connection();

    // Watches fd (e.g. STDIN_FILENO) for line commands. PRECONDITION: fd must
    // already be O_NONBLOCK, or a blocking read stalls the whole reactor.
    using CommandLineCallback = std::function<void(const std::string& line)>;
    void set_command_input(int fd, CommandLineCallback on_line);

    // Max bytes buffered while waiting for '\n' before the line is discarded.
    static constexpr size_t kMaxCommandLineBufferBytes = size_t{64} * 1024;

    // Cap on queued deferred replies; excess is dropped (looks like injected loss).
    static constexpr size_t kMaxPendingReplies = 4096;

private:
    struct PendingReply {
        int64_t fire_at_ns;
        int fd;
        uint64_t sequence;
        int64_t timestamp_ns;
    };

    void accept_new_connections();
    void handle_client_event(int fd, uint32_t events, int64_t now_ns);
    void remove_client(int fd);
    void sync_epoll_interest(int fd, Connection& conn);
    void on_timer_tick();
    void on_command_fd_readable();
    void process_command_buffer();
    void close_command_input();
    void disarm_listener();
    void send_disconnect_to_all();
    // Connection ping interceptor: returns false to suppress the automatic
    // reply (dropped, or deferred via pending_replies_).
    bool on_ping_intercept(int fd, uint64_t sequence, int64_t timestamp_ns);

    UniqueFd listen_fd_;
    UniqueFd epoll_fd_;
    UniqueFd wake_fd_;
    UniqueFd timer_fd_;
    // Reserved /dev/null fd, released to shed a connection on EMFILE/ENFILE.
    UniqueFd spare_fd_;
    bool listener_disarmed_ = false;  // set by disarm_listener(), cleared by the next tick
    uint16_t bound_port_ = 0;
    uint32_t feed_id_;
    Config config_;
    FaultConfig faults_;
    std::mt19937_64 rng_{std::random_device{}()};
    std::unordered_map<int, Connection> clients_;
    std::vector<PendingReply> pending_replies_;
    int command_fd_ = -1;
    std::string command_line_buffer_;
    bool discarding_oversized_line_ = false;  // dropping the tail of an over-long line up to its '\n'
    size_t max_clients_;
    CommandLineCallback on_command_line_;
    std::atomic<bool> stop_requested_{false};
    std::vector<epoll_event> event_batch_;
    // fds closed during the current epoll batch. Later events in the batch for
    // these numbers are stale (the fd may already be reused by a new client).
    std::vector<int> removed_in_batch_;
};
