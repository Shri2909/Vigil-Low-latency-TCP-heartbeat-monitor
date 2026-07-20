#pragma once

// Represents one TCP peer -- used identically by FeedMonitor (as a client
// connection to an exchange, Role::kInitiator) and FeedSimulator (as an
// accepted client, Role::kResponder). See PROJECT_PLAN.md section 4.
//
// Connection owns no epoll fd and never calls epoll_ctl itself: FeedMonitor
// and FeedSimulator own the epoll instance and dispatch events into
// on_readable/on_writable/on_hangup_or_error; Connection exposes wants_write()
// so the caller knows whether EPOLLOUT needs to be armed after each call.
//
// Every method that needs "now" takes it as a now_ns parameter supplied by the
// caller (captured once per epoll_wait wakeup), rather than calling
// clock_gettime() itself -- one syscall per reactor tick instead of one per
// connection per tick.

#include "config.h"
#include "heartbeat.h"
#include "stats.h"
#include "unique_fd.h"

#include <array>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <vector>

enum class Role : uint8_t { kInitiator, kResponder };

// Connection's own state_ only ever occupies {kConnecting, kHandshaking,
// kHealthy, kDegraded, kFailed} -- kDisconnected (no live Connection object
// exists yet for a feed) and kReconnecting (waiting out a backoff timer
// between Connection objects) are FeedMonitor-level bookkeeping states about
// the *absence* or *pending replacement* of a Connection, not states this
// class transitions through itself. Both are kept here because they're part
// of the state machine any caller reasons about (see the transition table in
// PROJECT_PLAN.md section 4).
enum class ConnectionState : uint8_t {
    kDisconnected,
    kConnecting,
    kHandshaking,
    kHealthy,
    kDegraded,
    kFailed,
    kReconnecting,
};

const char* to_string(ConnectionState state);

class Connection {
public:
    // fd must already be a non-blocking socket: for Role::kInitiator, one on
    // which a non-blocking connect() has been issued (state starts at
    // kConnecting); for Role::kResponder, one already returned by accept()
    // (state starts at kHandshaking, waiting for the peer's CONNECT_HELLO).
    Connection(int fd, Role role, uint32_t feed_id, std::string host, uint16_t port,
               const Config& config);

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) noexcept = default;
    Connection& operator=(Connection&&) noexcept = default;

    int fd() const noexcept { return fd_.get(); }
    ConnectionState state() const noexcept { return state_; }
    uint32_t feed_id() const noexcept { return feed_id_; }
    const std::string& host() const noexcept { return host_; }
    uint16_t port() const noexcept { return port_; }
    const ConnectionStats& stats() const noexcept { return stats_; }

    // True when this connection has bytes queued to write, or is mid-connect
    // awaiting the writability event that signals connect() completion --
    // callers use this after every method below to decide whether EPOLLOUT
    // needs to be (re)armed on this fd.
    bool wants_write() const noexcept;

    void on_readable(int64_t now_ns);
    void on_writable(int64_t now_ns);
    void on_hangup_or_error(int64_t now_ns, int err);

    // Role::kInitiator only; no-op otherwise or outside kHealthy/kDegraded
    // (e.g. called before the handshake completes).
    void send_ping(int64_t now_ns);

    // Advances the timeout/backoff state machine by one tick. Returns true iff
    // this call is what just transitioned the connection into kFailed, so the
    // caller emits exactly one alert rather than one per tick while it stays
    // failed. Role::kInitiator counts consecutive missed heartbeats
    // (kHealthy -> kDegraded -> kFailed, per max_missed_heartbeats);
    // Role::kResponder has no reconnect/backoff concept of its own (the
    // caller just reaps a kFailed connection), so it uses a simpler
    // single-strike check against the last PING it received.
    bool check_timeout(int64_t now_ns);

    // Re-baselines last_liveness_ns_ to now_ns without touching anything
    // else -- called by FeedMonitor::set_heartbeat_timeout() for every
    // slot when the timeout is changed at runtime. Without this, shrinking
    // heartbeat_timeout retroactively re-judges however much wait time has
    // already accumulated against the new, tighter budget: a connection
    // that was perfectly fine a moment ago (comfortably within the old,
    // more generous timeout) could immediately register as missed purely
    // because of the config change itself, not because anything about the
    // peer changed. Safe to call regardless of state -- it's a no-op for
    // kConnecting/kHandshaking, which use state_entered_ns_ instead.
    void reset_liveness_baseline(int64_t now_ns) noexcept { last_liveness_ns_ = now_ns; }

    // Closes the old fd and re-arms this Connection on new_fd, going back to
    // kConnecting (kInitiator) or kHandshaking (kResponder). Clears
    // per-attempt state (buffers, sequence numbers) but preserves cumulative
    // ConnectionStats and increments stats().reconnects. This folds together
    // the plan's "reset_for_reconnect()" with fd reassignment, since the
    // original no-argument signature had no way to hand the connection its
    // new fd.
    void rebind(int new_fd, int64_t now_ns);

    // Role::kResponder only. Builds and queues a PONG echoing sequence and
    // timestamp unchanged. Public (not just called internally from
    // handle_ping) so a deferred fault-injection reply -- see
    // set_ping_interceptor -- can send it later than the PING that triggered
    // it, without FeedSimulator reaching into Connection's private write path.
    void send_pong(uint64_t sequence, int64_t echoed_timestamp_ns);

    // Role::kResponder only. If set, called with (sequence, timestamp_ns) for
    // every received PING *before* the automatic immediate PONG reply;
    // returning false suppresses that automatic reply, leaving the
    // interceptor responsible for calling send_pong() itself later (or never,
    // to simulate a dropped reply). Unset (default) means every PING is
    // replied to immediately -- existing behavior, unchanged. This is the one
    // extension point FeedSimulator's fault injection (drop/extra-latency,
    // PROJECT_PLAN.md section 6) needs; Connection itself has no notion of
    // faults and stays exactly as reusable by FeedMonitor as before.
    using PingInterceptor = std::function<bool(uint64_t sequence, int64_t timestamp_ns)>;
    void set_ping_interceptor(PingInterceptor interceptor);

    // How large read_accum_/write_pending_ are allowed to grow before the
    // connection is failed rather than left to accumulate forever. Public so
    // tests can exercise the boundary without duplicating the constants. See
    // the comments at on_readable()'s recv loop and queue_write() for why
    // each one is bounded and what "exceeded" means for each.
    static constexpr size_t kMaxReadAccumBytes = 64 * 1024;
    static constexpr size_t kMaxWritePendingBytes = 64 * 1024;

private:
    void transition_to(ConnectionState new_state, int64_t now_ns);
    void drain_read_buffer(int64_t now_ns);
    void handle_message(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_connect_hello(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_connect_ack(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_ping(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_pong(const HeartbeatMessage& msg, int64_t now_ns);
    void queue_write(const HeartbeatWireBuffer& wire);
    void flush_write_buffer();
    void apply_socket_options();

    // Cold/error paths pulled into separate, non-inline, [[gnu::cold]]
    // functions so the hot "valid PONG keeps the connection healthy" path
    // (on_readable -> drain_read_buffer -> handle_message -> handle_pong)
    // doesn't carry their code in its instruction-cache footprint. Applies
    // the "slowpath removal" technique logged in PROJECT_PLAN.md section 15.
    [[gnu::cold, gnu::noinline]] void handle_protocol_violation(const char* reason, int64_t now_ns);
    [[gnu::cold, gnu::noinline]] void handle_peer_closed(int64_t now_ns);
    [[gnu::cold, gnu::noinline]] void handle_socket_error(int64_t now_ns, int err);

    UniqueFd fd_;
    Role role_;
    uint32_t feed_id_;
    std::string host_;
    uint16_t port_;
    const Config* config_;  // not a reference: Connection must stay move-assignable to live in std::vector<Connection>

    ConnectionState state_;
    int64_t state_entered_ns_ = 0;

    uint64_t next_sequence_ = 0;
    int consecutive_missed_ = 0;
    // Role-agnostic liveness timestamp checked by check_timeout: for
    // Role::kInitiator, the last time a PONG was received; for
    // Role::kResponder, the last time a PING was received (see
    // PROJECT_PLAN.md's F13 audit finding -- Role::kResponder connections
    // used to never time out at all, so a hung/silent client could occupy a
    // Connection+fd on FeedSimulator indefinitely).
    int64_t last_liveness_ns_ = 0;

    // Bounded set of recently-acked PING sequences, checked for membership in
    // handle_pong rather than compared against a single "highest sequence
    // seen" threshold. A simple threshold would misclassify a legitimate
    // reordered PONG as stale/duplicate: set_ping_interceptor (used by
    // FeedSimulator's extra_latency fault injection) can deliberately defer
    // an earlier PING's reply behind a later one's, so PONGs do not always
    // arrive in non-decreasing sequence order. 32 is comfortably larger than
    // any realistic number of PINGs that could be in flight at once (bounded
    // by max_missed_heartbeats and any configured extra_latency relative to
    // heartbeat_interval), so this never needs a "wrap-around too small"
    // correctness argument.
    static constexpr size_t kAckedSequenceWindow = 32;
    std::array<uint64_t, kAckedSequenceWindow> acked_sequences_{};
    size_t acked_sequences_count_ = 0;  // how many entries are valid (saturates at kAckedSequenceWindow)
    size_t acked_sequences_next_ = 0;   // ring cursor: next slot to overwrite
    bool has_acked_sequence(uint64_t sequence) const;
    void remember_acked_sequence(uint64_t sequence);

    std::vector<uint8_t> read_accum_;
    std::deque<uint8_t> write_pending_;

    ConnectionStats stats_;
    PingInterceptor ping_interceptor_;
};
