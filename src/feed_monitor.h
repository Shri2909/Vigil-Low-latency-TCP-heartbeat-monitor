#pragma once

// Single-threaded epoll reactor: keeps outbound connections to N feeds, sends
// heartbeats, detects failures, reconnects with backoff, and exposes stats.

#include "config.h"
#include "connection.h"
#include "stats.h"
#include "unique_fd.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

struct FeedSnapshot {
    uint32_t feed_id = 0;
    std::string name;
    ConnectionState state = ConnectionState::kDisconnected;
    ConnectionStats stats;
};

struct AggregateStats {
    size_t healthy_count = 0;
    size_t degraded_count = 0;
    size_t failed_count = 0;
    size_t reconnecting_count = 0;
    std::vector<FeedSnapshot> feeds;
};

class FeedMonitor {
public:
    explicit FeedMonitor(Config config);

    // Not copyable/movable: each Connection holds a `const Config*` to config_,
    // so config changes apply instantly and config_'s address must never move.
    FeedMonitor(const FeedMonitor&) = delete;
    FeedMonitor& operator=(const FeedMonitor&) = delete;
    FeedMonitor(FeedMonitor&&) = delete;
    FeedMonitor& operator=(FeedMonitor&&) = delete;

    // Returns false (adding nothing) on a duplicate feed_id or a synchronous
    // socket()/inet_pton()/epoll_ctl failure. Only IPv4 literals are supported.
    // A refused connect is not a failure here; it takes the normal
    // kConnecting -> kFailed -> reconnect path.
    bool add_feed(const FeedEndpoint& endpoint);
    void remove_feed(uint32_t feed_id);

    // Also what the CLI's "disconnect"/"reconnect" commands call.
    void force_disconnect(uint32_t feed_id);
    void force_reconnect(uint32_t feed_id);

    // Test/observability hooks. debug_fd_index_count() is reactor-thread only;
    // the two counters are relaxed atomics, readable from any thread.
    // index_of_fd_ holds one entry per slot whose connection is not kFailed.
    size_t debug_fd_index_count() const noexcept { return index_of_fd_.size(); }
    uint64_t debug_epoll_wait_count() const noexcept {
        return epoll_wait_count_.load(std::memory_order_relaxed);
    }
    uint64_t debug_events_processed_count() const noexcept {
        return events_processed_count_.load(std::memory_order_relaxed);
    }
    int64_t debug_reconnect_delay_ns(int attempt) const { return reconnect_delay_ns(attempt); }

    // Cap on one unterminated command line before it is discarded.
    static constexpr size_t kMaxCommandLineBufferBytes = size_t{64} * 1024;

    // Blocks until stop(). stop() is sticky: after it, run() returns
    // immediately and the monitor is not restartable.
    void run();
    void stop();  // safe from another thread or a signal handler

    // Reactor thread only (e.g. from the tick callback or before run()); slots_
    // is unsynchronized by design.
    AggregateStats snapshot_stats() const;

    // Return false and change nothing unless the value is > 0. Interval vs.
    // timeout ordering is deliberately not enforced. A shorter interval also
    // pulls every slot's next ping forward.
    bool set_heartbeat_interval(std::chrono::milliseconds interval);
    bool set_heartbeat_timeout(std::chrono::milliseconds timeout);

    // Called once per tick with a fresh snapshot (keeps this class independent
    // of cli_display).
    using TickCallback = std::function<void(const AggregateStats&)>;
    void set_tick_callback(TickCallback callback);

    // Sink for failure alerts (reactor thread); without one, alerts go to std::cerr.
    using AlertCallback = std::function<void(const std::string&)>;
    void set_alert_callback(AlertCallback callback);

    // Registers fd (e.g. STDIN_FILENO) in the epoll set and hands each complete
    // '\n'-terminated line to on_line, on the reactor thread. Returns false if
    // the fd could not be added.
    // PRECONDITION: fd is O_NONBLOCK. The read loop drains to EAGAIN; a blocking
    // fd would freeze the whole reactor.
    using CommandLineCallback = std::function<void(const std::string& line)>;
    bool set_command_input(int fd, CommandLineCallback on_line);

private:
    struct Slot {
        Connection connection;
        std::string name;
        int reconnect_attempts = 0;
        int64_t reconnect_ready_at_ns = -1;  // -1 = not waiting on a backoff timer
        bool give_up = false;                // max_reconnect_attempts exhausted
        int64_t next_ping_due_ns = 0;
    };

    std::optional<int> create_and_connect_socket(const std::string& host, uint16_t port);
    void arm_timer();
    void handle_connection_event(size_t index, uint32_t events, int64_t now_ns);
    void on_timer_tick();
    void on_command_fd_readable();
    void dispatch_command_line(std::string line);
    void on_connection_failed(size_t index);
    void initiate_connect(size_t index, int64_t now_ns);
    void schedule_reconnect(size_t index, int64_t now_ns);
    int64_t reconnect_delay_ns(int attempt) const;
    void sync_epoll_interest(size_t index);
    void remove_slot_at(size_t index);  // swap-and-pop; repoints both index maps for the moved slot
    void emit_failure_alert(const Slot& slot) const;

    UniqueFd epoll_fd_;
    UniqueFd timer_fd_;
    UniqueFd wake_fd_;  // eventfd written by stop() to interrupt epoll_wait
    int command_fd_ = -1;  // not a UniqueFd: not owned (e.g. STDIN_FILENO)
    std::string command_line_buffer_;
    bool discarding_oversized_line_ = false;  // dropping input until the next '\n'
    CommandLineCallback on_command_line_;
    TickCallback on_tick_;
    AlertCallback on_alert_;

    std::vector<Slot> slots_;
    std::unordered_map<int, size_t> index_of_fd_;
    std::unordered_map<uint32_t, size_t> index_of_feed_id_;

    Config config_;
    std::atomic<bool> stop_requested_{false};
    std::vector<epoll_event> event_batch_;
    std::atomic<uint64_t> epoll_wait_count_{0};
    std::atomic<uint64_t> events_processed_count_{0};
};
