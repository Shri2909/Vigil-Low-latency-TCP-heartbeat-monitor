#pragma once

// The main deliverable: a single-threaded epoll reactor that maintains
// outbound connections to N feed endpoints, sends heartbeats, detects
// failures, reconnects with backoff, and exposes stats for a CLI to display.
// See PROJECT_PLAN.md section 5, amended per section 15 (flat-vector storage)
// and the notes below (further refinements made while implementing).

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

// FeedSnapshot/AggregateStats combine ConnectionState (connection.h) with
// ConnectionStats (stats.h) -- deliberately live here, not in stats.h, so
// stats.h stays a dependency-free leaf module. See PROJECT_PLAN.md section 7.
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

    // Not copyable or movable: every Connection this owns stores a raw
    // `const Config*` pointing at this object's own config_ member (see
    // connection.h) so that set_heartbeat_interval()/set_heartbeat_timeout()
    // take effect for every connection instantly, with no per-connection
    // propagation. That only stays valid if config_'s address never moves --
    // so neither can FeedMonitor.
    FeedMonitor(const FeedMonitor&) = delete;
    FeedMonitor& operator=(const FeedMonitor&) = delete;
    FeedMonitor(FeedMonitor&&) = delete;
    FeedMonitor& operator=(FeedMonitor&&) = delete;

    // Returns false (and adds nothing) if this feed_id is already present, or
    // if socket()/inet_pton fail synchronously (fd exhaustion, unparseable
    // IPv4 address -- DNS hostnames are out of scope, see PROJECT_PLAN.md).
    // A refused/unreachable *connect* is NOT a failure case here: the fd
    // stays valid either way and the normal kConnecting -> kFailed ->
    // reconnect path (driven by epoll) handles it uniformly. Deviates from
    // the plan's void return -- see PROJECT_PLAN.md section 15 for why the
    // caller needs to know synchronous failures apart from async ones.
    bool add_feed(const FeedEndpoint& endpoint);
    void remove_feed(uint32_t feed_id);

    // Manual test/ops hooks -- also what the CLI's "disconnect"/"reconnect"
    // commands (wired up in Phase 8) end up calling.
    void force_disconnect(uint32_t feed_id);
    void force_reconnect(uint32_t feed_id);

    // Test/observability only, same spirit as FeedSimulator::client_count():
    // index_of_fd_ should always contain exactly one entry per slot whose
    // connection is not kFailed (kFailed connections are deliberately
    // deregistered from it -- see the comment in remove_slot_at). Exposed so
    // tests can catch a stale/leaked entry directly instead of only via its
    // eventual, timing-dependent symptom (a reused fd number misrouting
    // events to the wrong Connection).
    size_t debug_fd_index_count() const noexcept { return index_of_fd_.size(); }

    // Observability counters, incremented once per epoll_wait() call / once
    // per event returned by it. Safe to read from any thread at any time --
    // unlike debug_fd_index_count() these are plain monotonic counters with
    // no cross-field consistency requirement, so an atomic relaxed load
    // needs no extra coordination with the reactor thread. Added to close a
    // gap PROJECT_PLAN.md section 12 had promised (epoll_wait calls/sec,
    // events processed/sec) but throughput_bench never actually measured.
    uint64_t debug_epoll_wait_count() const noexcept {
        return epoll_wait_count_.load(std::memory_order_relaxed);
    }
    uint64_t debug_events_processed_count() const noexcept {
        return events_processed_count_.load(std::memory_order_relaxed);
    }

    // Exposes the private backoff-delay computation directly so its
    // doubling-with-cap-and-jitter shape can be tested deterministically and
    // fast (many calls, checking bounds), instead of only observable
    // indirectly by waiting out real reconnect delays end to end.
    int64_t debug_reconnect_delay_ns(int attempt) const { return reconnect_delay_ns(attempt); }

    // The scheduled-reconnect timestamp for one feed's slot (-1 if not
    // currently waiting on a backoff timer, or if feed_id isn't tracked).
    // Same thread-safety rule as debug_fd_index_count(): only safe to call
    // once the reactor thread has been stopped, since slots_/
    // index_of_feed_id_ have no internal synchronization. Lets a test assert
    // that jitter actually desynchronizes many feeds' reconnect timing,
    // instead of only checking each one's delay falls in-bounds in
    // isolation.
    int64_t debug_reconnect_ready_at_ns(uint32_t feed_id) const {
        auto it = index_of_feed_id_.find(feed_id);
        if (it == index_of_feed_id_.end()) {
            return -1;
        }
        return slots_[it->second].reconnect_ready_at_ns;
    }

    // How large command_line_buffer_ is allowed to grow while waiting for a
    // '\n' before its contents are discarded (see the warning logged in
    // on_command_fd_readable). Public so tests can exercise the boundary
    // without duplicating the constant.
    static constexpr size_t kMaxCommandLineBufferBytes = 64 * 1024;

    void run();   // blocks until stop() is called
    void stop();  // safe to call from another thread or a signal handler

    // Safe to call from the reactor thread only (e.g. from the tick callback
    // below, or before run() starts) -- like the rest of this class's state,
    // slots_ has no internal synchronization by design (PROJECT_PLAN.md
    // finding #17). Calling this from another thread while run() is active
    // is the exact kind of data race the single-thread design exists to
    // avoid needing a mutex for.
    AggregateStats snapshot_stats() const;

    void set_heartbeat_interval(std::chrono::milliseconds interval);
    void set_heartbeat_timeout(std::chrono::milliseconds timeout);

    // Invoked once per completed tick (every config.tick_interval) with a
    // fresh stats snapshot. Lets main.cpp (Phase 8) wire up terminal
    // rendering without FeedMonitor depending on cli_display.h.
    using TickCallback = std::function<void(const AggregateStats&)>;
    void set_tick_callback(TickCallback callback);

    // Registers fd (e.g. STDIN_FILENO) into this same epoll set; complete
    // lines (newline-delimited, partial lines buffered across calls) are
    // handed to on_line. Command *syntax* ("disconnect 3") is intentionally
    // not FeedMonitor's concern -- main.cpp parses lines and calls
    // force_disconnect()/force_reconnect()/set_heartbeat_interval()/stop()
    // directly from inside on_line, which runs on the reactor thread, so
    // those calls are not a cross-thread concern.
    //
    // PRECONDITION: fd must already be non-blocking (O_NONBLOCK). The read
    // loop behind this drains fd to EAGAIN on every readable event; handed a
    // blocking fd, the *second* read call in that loop -- once the first
    // drains whatever was immediately available -- blocks waiting for more
    // input that may never come, freezing the entire single-threaded reactor
    // (every connection, not just this fd) until it does. main.cpp sets this
    // on STDIN_FILENO before calling this; a test feeding a pipe must too.
    using CommandLineCallback = std::function<void(const std::string& line)>;
    void set_command_input(int fd, CommandLineCallback on_line);

private:
    struct Slot {
        Connection connection;
        std::string name;
        int reconnect_attempts = 0;
        int64_t reconnect_ready_at_ns = -1;  // -1 = not currently waiting on a backoff timer
        bool give_up = false;                // true once max_reconnect_attempts is exhausted
        int64_t next_ping_due_ns = 0;
    };

    std::optional<int> create_and_connect_socket(const std::string& host, uint16_t port);
    void arm_timer();
    void handle_connection_event(size_t index, uint32_t events, int64_t now_ns);
    void on_timer_tick();
    void on_command_fd_readable();
    void on_connection_failed(size_t index);
    void initiate_connect(size_t index, int64_t now_ns);
    void schedule_reconnect(size_t index, int64_t now_ns);
    int64_t reconnect_delay_ns(int attempt) const;
    void sync_epoll_interest(size_t index);
    void remove_slot_at(size_t index);  // swap-and-pop; repoints index maps for the moved slot
    void emit_failure_alert(const Slot& slot) const;

    UniqueFd epoll_fd_;
    UniqueFd timer_fd_;
    UniqueFd wake_fd_;
    int command_fd_ = -1;  // not a UniqueFd: FeedMonitor doesn't own this fd (e.g. STDIN_FILENO)
    std::string command_line_buffer_;
    CommandLineCallback on_command_line_;
    TickCallback on_tick_;

    std::vector<Slot> slots_;
    std::unordered_map<int, size_t> index_of_fd_;
    std::unordered_map<uint32_t, size_t> index_of_feed_id_;

    Config config_;
    std::atomic<bool> stop_requested_{false};
    std::vector<epoll_event> event_batch_;
    std::atomic<uint64_t> epoll_wait_count_{0};
    std::atomic<uint64_t> events_processed_count_{0};
};
