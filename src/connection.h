#pragma once

// One TCP peer, used by FeedMonitor (Role::kInitiator) and FeedSimulator (Role::kResponder).
// Owns only the socket: callers own epoll, dispatch events into on_*(), and check
// wants_write() afterwards to decide whether EPOLLOUT must be armed. All "now" values
// are passed in (one clock read per epoll wakeup, not per connection).

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

// A Connection itself only occupies kConnecting..kFailed; kDisconnected means no Connection yet.
enum class ConnectionState : uint8_t {
    kDisconnected,
    kConnecting,
    kHandshaking,
    kHealthy,
    kDegraded,
    kFailed,
};

const char* to_string(ConnectionState state);

class Connection {
public:
    // fd must be non-blocking: a connect()-in-progress socket for kInitiator (starts in
    // kConnecting), or an accept()ed socket for kResponder (starts in kHandshaking).
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

    // True if bytes are queued, or a connect() is pending: EPOLLOUT must be armed.
    bool wants_write() const noexcept;

    void on_readable(int64_t now_ns);
    void on_writable(int64_t now_ns);
    void on_hangup_or_error(int64_t now_ns, int err);

    // kInitiator only; no-op outside kHealthy/kDegraded.
    void send_ping(int64_t now_ns);

    // Advances the timeout state machine one tick; returns true only on the call that moves
    // the connection into kFailed (so callers alert once). kInitiator counts consecutive
    // missed heartbeats (kHealthy -> kDegraded -> kFailed); kResponder fails on a single
    // heartbeat_timeout without a PING.
    bool check_timeout(int64_t now_ns);

    // Restarts the liveness window so a runtime heartbeat_timeout change is not retroactively
    // applied to wait time already accumulated.
    void reset_liveness_baseline(int64_t now_ns) noexcept { last_liveness_ns_ = now_ns; }

    // Swaps in new_fd and returns to kConnecting/kHandshaking. Clears per-attempt state
    // (buffers, sequence numbers); keeps cumulative stats and counts a reconnect.
    void rebind(int new_fd, int64_t now_ns);

    // Closes the socket immediately (fd() becomes -1) without touching state or stats. Idempotent.
    void close_fd() noexcept { fd_.reset(); }

    // kResponder only. Public so a deferred (fault-injected) reply can be sent after the PING.
    void send_pong(uint64_t sequence, int64_t echoed_timestamp_ns);

    // kResponder only. Called for every PING before the automatic PONG; returning false
    // suppresses that PONG and leaves the caller to send_pong() later (or never).
    using PingInterceptor = std::function<bool(uint64_t sequence, int64_t timestamp_ns)>;
    void set_ping_interceptor(PingInterceptor interceptor);

    // Caps that fail the connection instead of growing without bound. read_accum_ is drained
    // after every recv(), so its cap only guards the unparsed remainder (an invariant check).
    static constexpr size_t kMaxReadAccumBytes = size_t{64} * 1024;
    static constexpr size_t kMaxWritePendingBytes = size_t{64} * 1024;

private:
    void transition_to(ConnectionState new_state, int64_t now_ns);
    void drain_read_buffer(int64_t now_ns);
    void handle_message(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_connect_hello(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_connect_ack(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_ping(const HeartbeatMessage& msg, int64_t now_ns);
    void handle_pong(const HeartbeatMessage& msg, int64_t now_ns);
    void queue_message(MessageType type, uint64_t sequence, int64_t timestamp_ns);
    void queue_write(const HeartbeatWireBuffer& wire);
    void flush_write_buffer();
    void apply_socket_options();

    // Cold error paths, kept out of line so the hot PONG path stays compact.
    [[gnu::cold, gnu::noinline]] void handle_protocol_violation(const char* reason, int64_t now_ns);
    [[gnu::cold, gnu::noinline]] void handle_peer_closed(int64_t now_ns);
    [[gnu::cold, gnu::noinline]] void handle_socket_error(int64_t now_ns, int err);

    UniqueFd fd_;
    Role role_;
    uint32_t feed_id_;
    std::string host_;
    uint16_t port_;
    const Config* config_;  // pointer, not reference, so Connection stays move-assignable

    ConnectionState state_;
    int64_t state_entered_ns_ = 0;

    uint64_t next_sequence_ = 0;
    int consecutive_missed_ = 0;
    // Last PONG received (kInitiator) or last PING received (kResponder); checked by check_timeout.
    int64_t last_liveness_ns_ = 0;

    // Ring of recently acked PING sequences. Membership (not a "highest seen" threshold) is
    // checked because the ping interceptor can legitimately reorder PONGs.
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
