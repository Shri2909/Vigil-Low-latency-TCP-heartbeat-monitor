#include "connection.h"
#include "time_utils.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

const char* to_string(ConnectionState state) {
    switch (state) {
        case ConnectionState::kDisconnected: return "DISCONNECTED";
        case ConnectionState::kConnecting: return "CONNECTING";
        case ConnectionState::kHandshaking: return "HANDSHAKING";
        case ConnectionState::kHealthy: return "HEALTHY";
        case ConnectionState::kDegraded: return "DEGRADED";
        case ConnectionState::kFailed: return "FAILED";
        case ConnectionState::kReconnecting: return "RECONNECTING";
    }
    return "UNKNOWN";
}

Connection::Connection(int fd, Role role, uint32_t feed_id, std::string host, uint16_t port,
                        const Config& config)
    : fd_(fd),
      role_(role),
      feed_id_(feed_id),
      host_(std::move(host)),
      port_(port),
      config_(&config),
      state_(role == Role::kInitiator ? ConnectionState::kConnecting : ConnectionState::kHandshaking) {
    const int64_t now = now_monotonic_ns();
    state_entered_ns_ = now;
    if (role_ == Role::kResponder) {
        // an accepted fd is already ESTABLISHED -- there is no connect()
        // completion event to hang socket-option setup off of, so it happens
        // right here instead of in on_writable (see the kInitiator path there).
        apply_socket_options();
    }
}

void Connection::apply_socket_options() {
    if (config_->tcp_nodelay) {
        const int flag = 1;
        // Best-effort: a failure here only costs latency, not correctness, and
        // logging every connection's setsockopt outcome would be noise -- not
        // worth spending the "loud, not silent" budget on.
        ::setsockopt(fd_.get(), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    }
}

void Connection::transition_to(ConnectionState new_state, int64_t now_ns) {
    state_ = new_state;
    state_entered_ns_ = now_ns;
}

bool Connection::wants_write() const noexcept {
    return state_ == ConnectionState::kConnecting || !write_pending_.empty();
}

void Connection::on_readable(int64_t now_ns) {
    uint8_t buf[4096];
    bool peer_closed = false;
    int socket_error = 0;  // 0 means "no error"
    while (true) {
        const ssize_t n = ::recv(fd_.get(), buf, sizeof(buf), 0);
        if (n > 0) {
            read_accum_.insert(read_accum_.end(), buf, buf + n);
            if (read_accum_.size() > kMaxReadAccumBytes) [[unlikely]] {
                // drain_read_buffer() only runs once, after this loop exits
                // -- across many recv() calls within a single on_readable(),
                // read_accum_ isn't bounded by the "steady-state leftover is
                // small" argument that holds *between* calls (a full 28-byte
                // chunk is parsed and erased immediately once drained). A
                // peer sustaining a flood faster than we can read-and-drain
                // (plausible on loopback/a fast LAN) could otherwise grow
                // this to an arbitrary size in one shot. A legitimate peer
                // following this protocol never gets remotely close to this
                // many bytes in flight at once, so treat crossing it as
                // sufficient grounds for distrust -- fail outright rather
                // than attempting to parse/resync what's already buffered.
                handle_protocol_violation("read_accum_ exceeded max size", now_ns);
                return;
            }
            continue;
        }
        if (n == 0) {
            // EOF -- do NOT act on it yet (see the drain below). Whatever
            // was just appended to read_accum_ in this same loop may be a
            // complete, legitimate final message (a graceful kDisconnect, or
            // a fault-injection deferred PONG immediately followed by the
            // peer closing) that must still be decoded before this
            // connection is torn down, or it's silently discarded.
            peer_closed = true;
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;  // drained -- mandatory exit condition under edge-triggered epoll
        }
        if (errno == EINTR) {
            continue;
        }
        socket_error = errno;  // same reasoning as EOF above: drain before acting on it
        break;
    }

    drain_read_buffer(now_ns);
    if (state_ == ConnectionState::kFailed) [[unlikely]] {
        // drain_read_buffer already failed the connection (a protocol
        // violation, or a handler reacting to what it decoded) -- don't
        // also report EOF/socket-error on top of that with a second,
        // less-specific reason.
        return;
    }
    if (peer_closed) {
        handle_peer_closed(now_ns);
    } else if (socket_error != 0) {
        handle_socket_error(now_ns, socket_error);
    }
}

void Connection::drain_read_buffer(int64_t now_ns) {
    size_t offset = 0;
    while (read_accum_.size() - offset >= kHeartbeatMessageSize) {
        HeartbeatWireBuffer wire;
        std::copy_n(read_accum_.begin() + static_cast<long>(offset), kHeartbeatMessageSize, wire.begin());
        offset += kHeartbeatMessageSize;

        HeartbeatMessage msg{};
        if (decode_heartbeat(wire, &msg)) [[likely]] {
            handle_message(msg, now_ns);
            if (state_ == ConnectionState::kFailed) [[unlikely]] {
                // a handler above (e.g. an unexpected message for the current
                // state) may have just failed the connection -- whatever's left
                // in read_accum_ is moot, stop parsing it.
                read_accum_.clear();
                return;
            }
        } else {
            handle_protocol_violation("corrupt or desynced heartbeat message", now_ns);
            read_accum_.clear();
            return;
        }
    }
    if (offset > 0) {
        read_accum_.erase(read_accum_.begin(), read_accum_.begin() + static_cast<long>(offset));
    }
}

void Connection::handle_message(const HeartbeatMessage& msg, int64_t now_ns) {
    switch (static_cast<MessageType>(msg.type)) {
        case MessageType::kConnectHello:
            handle_connect_hello(msg, now_ns);
            return;
        case MessageType::kConnectAck:
            handle_connect_ack(msg, now_ns);
            return;
        case MessageType::kPing:
            handle_ping(msg, now_ns);
            return;
        case MessageType::kPong:
            handle_pong(msg, now_ns);
            return;
        case MessageType::kDisconnect:
            handle_peer_closed(now_ns);
            return;
    }
    // decode_heartbeat already rejects any type outside MessageType's five
    // values, so this switch is exhaustive -- no default label, so -Wswitch
    // catches a forgotten case if a sixth message type is ever added.
}

void Connection::handle_connect_hello(const HeartbeatMessage& msg, int64_t now_ns) {
    if (role_ != Role::kResponder || state_ != ConnectionState::kHandshaking) [[unlikely]] {
        handle_protocol_violation("unexpected CONNECT_HELLO", now_ns);
        return;
    }
    if (msg.feed_id != feed_id_) [[unlikely]] {
        handle_protocol_violation("CONNECT_HELLO feed_id mismatch", now_ns);
        return;
    }
    HeartbeatMessage ack{};
    ack.type = static_cast<uint8_t>(MessageType::kConnectAck);
    ack.feed_id = feed_id_;
    ack.sequence = 0;
    ack.timestamp_ns = now_ns;
    HeartbeatWireBuffer wire;
    encode_heartbeat(ack, wire);
    queue_write(wire);
    // treat handshake completion as an implicit first "activity" so a freshly
    // healthy connection doesn't look stale before its first real PING.
    last_liveness_ns_ = now_ns;
    stats_.connected_since_ns = now_ns;
    transition_to(ConnectionState::kHealthy, now_ns);
}

void Connection::handle_connect_ack(const HeartbeatMessage& msg, int64_t now_ns) {
    if (role_ != Role::kInitiator || state_ != ConnectionState::kHandshaking) [[unlikely]] {
        handle_protocol_violation("unexpected CONNECT_ACK", now_ns);
        return;
    }
    if (msg.feed_id != feed_id_) [[unlikely]] {
        handle_protocol_violation("CONNECT_ACK feed_id mismatch", now_ns);
        return;
    }
    // treat handshake completion as an implicit first "pong" so a freshly
    // healthy connection doesn't look stale before its first real ping/pong.
    last_liveness_ns_ = now_ns;
    stats_.connected_since_ns = now_ns;
    transition_to(ConnectionState::kHealthy, now_ns);
}

void Connection::handle_ping(const HeartbeatMessage& msg, int64_t now_ns) {
    if (role_ != Role::kResponder || state_ != ConnectionState::kHealthy) [[unlikely]] {
        handle_protocol_violation("unexpected PING", now_ns);
        return;
    }
    last_liveness_ns_ = now_ns;  // this PING itself is proof of life, regardless of what happens to its reply below
    if (ping_interceptor_ && !ping_interceptor_(msg.sequence, msg.timestamp_ns)) {
        return;  // interceptor is handling (deferring) or intentionally dropping the reply
    }
    send_pong(msg.sequence, msg.timestamp_ns);
}

void Connection::send_pong(uint64_t sequence, int64_t echoed_timestamp_ns) {
    // Guarded the same way the automatic reply above is: by the time a
    // deferred fault-injection reply fires (see set_ping_interceptor), this
    // connection may already be gone or mid-reconnect.
    if (role_ != Role::kResponder || state_ != ConnectionState::kHealthy) {
        return;
    }
    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = feed_id_;
    pong.sequence = sequence;
    pong.timestamp_ns = echoed_timestamp_ns;  // echoed unchanged, per protocol (see heartbeat.h)
    HeartbeatWireBuffer wire;
    encode_heartbeat(pong, wire);
    queue_write(wire);
}

void Connection::set_ping_interceptor(PingInterceptor interceptor) {
    ping_interceptor_ = std::move(interceptor);
}

bool Connection::has_acked_sequence(uint64_t sequence) const {
    for (size_t i = 0; i < acked_sequences_count_; ++i) {
        if (acked_sequences_[i] == sequence) {
            return true;
        }
    }
    return false;
}

void Connection::remember_acked_sequence(uint64_t sequence) {
    acked_sequences_[acked_sequences_next_] = sequence;
    acked_sequences_next_ = (acked_sequences_next_ + 1) % kAckedSequenceWindow;
    if (acked_sequences_count_ < kAckedSequenceWindow) {
        ++acked_sequences_count_;
    }
}

void Connection::handle_pong(const HeartbeatMessage& msg, int64_t now_ns) {
    if (role_ != Role::kInitiator ||
        (state_ != ConnectionState::kHealthy && state_ != ConnectionState::kDegraded)) [[unlikely]] {
        handle_protocol_violation("unexpected PONG", now_ns);
        return;
    }
    if (has_acked_sequence(msg.sequence)) [[unlikely]] {
        return;  // duplicate pong -- ignore, don't double-count in stats
    }
    remember_acked_sequence(msg.sequence);

    const int64_t rtt_ns = now_ns - msg.timestamp_ns;
    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_->heartbeat_timeout).count();
    if (rtt_ns >= timeout_ns || rtt_ns < 0) [[unlikely]] {
        // A reply that took at least a full heartbeat_timeout to arrive
        // doesn't prove the feed is healthy *now* -- by the same timing rule
        // check_timeout uses to declare a miss, this reply is already too
        // old to trust as current-liveness evidence. A *negative* RTT (the
        // echoed timestamp_ns claims to be in the future relative to now_ns
        // -- clock skew, a buggy peer, or a forged message) is equally
        // untrustworthy: not physically meaningful either way, and left
        // unchecked it would still be accepted as a valid heartbeat,
        // polluting stats().last_rtt_ns/ewma_rtt_ns with a nonsensical
        // negative value. Still remembered as acked above (so a
        // legitimately ultra-late duplicate can't be reprocessed), but it
        // must not reset last_liveness_ns_ or consecutive_missed_, nor
        // resurrect a kDegraded connection: doing so would let one stale or
        // bogus straggler mask every real miss that happened since it was
        // originally sent.
        return;
    }

    last_liveness_ns_ = now_ns;
    consecutive_missed_ = 0;
    stats_.record_pong_received(rtt_ns);

    if (state_ == ConnectionState::kDegraded) {
        transition_to(ConnectionState::kHealthy, now_ns);
    }
}

void Connection::on_writable(int64_t now_ns) {
    if (state_ == ConnectionState::kConnecting) {
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd_.get(), SOL_SOCKET, SO_ERROR, &err, &len) < 0) [[unlikely]] {
            err = errno;
        }
        if (err != 0) [[unlikely]] {
            handle_socket_error(now_ns, err);
            return;
        }
        apply_socket_options();
        transition_to(ConnectionState::kHandshaking, now_ns);
        if (role_ == Role::kInitiator) {
            HeartbeatMessage hello{};
            hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
            hello.feed_id = feed_id_;
            hello.sequence = 0;
            hello.timestamp_ns = now_ns;
            HeartbeatWireBuffer wire;
            encode_heartbeat(hello, wire);
            queue_write(wire);
        }
        return;
    }
    flush_write_buffer();
}

void Connection::on_hangup_or_error(int64_t now_ns, int err) {
    handle_socket_error(now_ns, err);
}

void Connection::send_ping(int64_t now_ns) {
    if (role_ != Role::kInitiator ||
        (state_ != ConnectionState::kHealthy && state_ != ConnectionState::kDegraded)) [[unlikely]] {
        return;
    }
    HeartbeatMessage ping{};
    ping.type = static_cast<uint8_t>(MessageType::kPing);
    ping.feed_id = feed_id_;
    ping.sequence = next_sequence_++;
    ping.timestamp_ns = now_ns;
    HeartbeatWireBuffer wire;
    encode_heartbeat(ping, wire);
    queue_write(wire);
    stats_.record_ping_sent();
}

bool Connection::check_timeout(int64_t now_ns) {
    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_->heartbeat_timeout).count();

    switch (state_) {
        case ConnectionState::kConnecting:
        case ConnectionState::kHandshaking:
            // Applies to both roles unchanged: a responder never enters
            // kConnecting (accept() already completed the TCP handshake), but
            // it can get stuck in kHandshaking forever if the peer never
            // sends CONNECT_HELLO -- this is what reclaims that case.
            if (now_ns - state_entered_ns_ >= timeout_ns) [[unlikely]] {
                transition_to(ConnectionState::kFailed, now_ns);
                return true;
            }
            return false;
        case ConnectionState::kHealthy:
        case ConnectionState::kDegraded:
            if (role_ == Role::kInitiator) {
                if (now_ns - last_liveness_ns_ >= timeout_ns) [[unlikely]] {
                    ++consecutive_missed_;
                    // advance the window rather than resetting to now_ns, so a
                    // long stall counts as multiple misses (converging on
                    // kFailed within a few ticks) instead of being silently
                    // forgiven every tick.
                    last_liveness_ns_ += timeout_ns;
                    stats_.record_missed();
                    if (consecutive_missed_ >= config_->max_missed_heartbeats) {
                        transition_to(ConnectionState::kFailed, now_ns);
                        return true;
                    }
                    transition_to(ConnectionState::kDegraded, now_ns);
                }
                return false;
            }
            // Role::kResponder: a single strike is enough. There's no
            // reconnect/backoff concept on this side of the connection --
            // FeedSimulator just reaps a kFailed client -- so there's no
            // reason to count consecutive misses the way the initiator does;
            // one full heartbeat_timeout with no PING at all means the peer
            // has gone silent (a hung/slow-loris test client, or a monitor
            // that crashed without closing its socket).
            if (now_ns - last_liveness_ns_ >= timeout_ns) [[unlikely]] {
                transition_to(ConnectionState::kFailed, now_ns);
                return true;
            }
            return false;
        default:
            return false;
    }
}

void Connection::rebind(int new_fd, int64_t now_ns) {
    fd_.reset(new_fd);
    read_accum_.clear();
    write_pending_.clear();
    next_sequence_ = 0;
    acked_sequences_count_ = 0;
    acked_sequences_next_ = 0;
    consecutive_missed_ = 0;
    last_liveness_ns_ = now_ns;
    stats_.connected_since_ns = 0;  // not connected again until the new handshake completes
    stats_.record_reconnect();
    transition_to(role_ == Role::kInitiator ? ConnectionState::kConnecting : ConnectionState::kHandshaking,
                  now_ns);
    if (role_ == Role::kResponder) {
        apply_socket_options();
    }
}

void Connection::queue_write(const HeartbeatWireBuffer& wire) {
    const bool was_empty = write_pending_.empty();
    write_pending_.insert(write_pending_.end(), wire.begin(), wire.end());
    if (write_pending_.size() > kMaxWritePendingBytes) [[unlikely]] {
        // The peer isn't draining what we send it (wedged, or deliberately
        // not reading) -- left unchecked this grows one heartbeat's worth of
        // bytes at a time, forever. Failing the connection is safer than an
        // unbounded memory footprint, and strictly safer than discarding
        // queued bytes mid-message: a partial message getting through would
        // desync the peer's own decoder on whatever it does eventually
        // receive, which is worse than tearing down a connection that's
        // already un-servable.
        handle_protocol_violation("write_pending_ exceeded max size (peer not draining)", now_monotonic_ns());
        return;
    }
    if (was_empty) {
        flush_write_buffer();
    }
}

void Connection::flush_write_buffer() {
    while (!write_pending_.empty()) {
        uint8_t staging[512];
        const size_t chunk = std::min(sizeof(staging), write_pending_.size());
        std::copy_n(write_pending_.begin(), chunk, staging);

        const ssize_t n = ::send(fd_.get(), staging, chunk, MSG_NOSIGNAL);
        if (n > 0) {
            write_pending_.erase(write_pending_.begin(), write_pending_.begin() + n);
            if (static_cast<size_t>(n) < chunk) {
                break;  // short write -- socket send buffer is full, wait for EPOLLOUT
            }
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        handle_socket_error(now_monotonic_ns(), errno);
        return;
    }
}

void Connection::handle_socket_error(int64_t now_ns, int err) {
    std::cerr << "[connection] feed " << feed_id_ << " (" << host_ << ":" << port_ << ") fd=" << fd_.get()
              << ": socket error: " << std::strerror(err) << "\n";
    transition_to(ConnectionState::kFailed, now_ns);
}

void Connection::handle_peer_closed(int64_t now_ns) {
    std::cerr << "[connection] feed " << feed_id_ << " (" << host_ << ":" << port_ << ") fd=" << fd_.get()
              << ": peer closed connection\n";
    transition_to(ConnectionState::kFailed, now_ns);
}

void Connection::handle_protocol_violation(const char* reason, int64_t now_ns) {
    std::cerr << "[connection] feed " << feed_id_ << " (" << host_ << ":" << port_ << ") fd=" << fd_.get()
              << ": protocol violation: " << reason << "\n";
    transition_to(ConnectionState::kFailed, now_ns);
}
