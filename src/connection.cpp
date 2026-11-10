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
    state_entered_ns_ = now_monotonic_ns();
    if (role_ == Role::kResponder) {
        // An accepted fd is already established: no connect() completion in on_writable to hook.
        apply_socket_options();
    }
}

void Connection::apply_socket_options() {
    if (config_->tcp_nodelay) {
        const int flag = 1;
        // Best-effort: failure only costs latency.
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
    if (state_ == ConnectionState::kFailed) [[unlikely]] {
        return;  // already torn down; the caller will reap/rebind this connection
    }
    uint8_t buf[4096];
    bool peer_closed = false;
    int socket_error = 0;  // 0 means "no error"
    while (true) {
        const ssize_t n = ::recv(fd_.get(), buf, sizeof(buf), 0);
        if (n > 0) {
            read_accum_.insert(read_accum_.end(), buf, buf + n);
            // Parse eagerly so read_accum_ only ever holds a sub-message remainder.
            drain_read_buffer(now_ns);
            if (state_ == ConnectionState::kFailed) [[unlikely]] {
                return;  // already failed by a handler; don't also report EOF/error
            }
            if (read_accum_.size() > kMaxReadAccumBytes) [[unlikely]] {
                // Unreachable for a peer (the remainder is < one message); invariant check.
                handle_protocol_violation("read_accum_ exceeded max size", now_ns);
                return;
            }
            continue;
        }
        if (n == 0) {
            // EOF: data received in this same call was already drained above, so a final
            // message (e.g. kDisconnect or a last PONG) is processed before teardown.
            peer_closed = true;
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;  // must drain to EAGAIN under edge-triggered epoll
        }
        if (errno == EINTR) {
            continue;
        }
        socket_error = errno;
        break;
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
                // A handler failed the connection; the rest of the buffer is moot.
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
    // No default label: -Wswitch flags a new MessageType that is not handled here.
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
    queue_message(MessageType::kConnectAck, 0, now_ns);
    if (state_ == ConnectionState::kFailed) [[unlikely]] {
        return;  // the ACK write failed the connection; don't overwrite kFailed with kHealthy
    }
    // Handshake completion counts as liveness so the connection isn't stale before its first PING.
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
    // Handshake completion counts as liveness so the connection isn't stale before its first PONG.
    last_liveness_ns_ = now_ns;
    stats_.connected_since_ns = now_ns;
    transition_to(ConnectionState::kHealthy, now_ns);
}

void Connection::handle_ping(const HeartbeatMessage& msg, int64_t now_ns) {
    if (role_ != Role::kResponder || state_ != ConnectionState::kHealthy) [[unlikely]] {
        handle_protocol_violation("unexpected PING", now_ns);
        return;
    }
    if (msg.feed_id != feed_id_) [[unlikely]] {
        handle_protocol_violation("PING feed_id mismatch", now_ns);
        return;
    }
    last_liveness_ns_ = now_ns;  // the PING is proof of life regardless of what happens to its reply
    if (ping_interceptor_ && !ping_interceptor_(msg.sequence, msg.timestamp_ns)) {
        return;  // interceptor is deferring or dropping the reply
    }
    send_pong(msg.sequence, msg.timestamp_ns);
}

void Connection::send_pong(uint64_t sequence, int64_t echoed_timestamp_ns) {
    // A deferred reply may fire after the connection has failed or been rebound.
    if (role_ != Role::kResponder || state_ != ConnectionState::kHealthy) {
        return;
    }
    queue_message(MessageType::kPong, sequence, echoed_timestamp_ns);
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
    if (msg.feed_id != feed_id_) [[unlikely]] {
        handle_protocol_violation("PONG feed_id mismatch", now_ns);
        return;
    }
    if (msg.sequence >= next_sequence_) [[unlikely]] {
        // Never sent: forged or desynced peer. Checked before remembering so it can't shadow a real PING.
        handle_protocol_violation("PONG for a sequence that was never sent", now_ns);
        return;
    }
    if (has_acked_sequence(msg.sequence)) [[unlikely]] {
        return;  // duplicate: ignore
    }
    remember_acked_sequence(msg.sequence);

    // Range-check before subtracting so a forged timestamp (e.g. INT64_MIN) can't overflow.
    if (msg.timestamp_ns < 0 || msg.timestamp_ns > now_ns) [[unlikely]] {
        return;
    }
    const int64_t rtt_ns = now_ns - msg.timestamp_ns;
    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_->heartbeat_timeout).count();
    if (rtt_ns >= timeout_ns) [[unlikely]] {
        // Too old to prove current liveness: it must not reset the miss counters or revive a
        // kDegraded connection, or one stale straggler would mask every real miss since.
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
    if (state_ == ConnectionState::kFailed) [[unlikely]] {
        return;
    }
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
            queue_message(MessageType::kConnectHello, 0, now_ns);
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
    queue_message(MessageType::kPing, next_sequence_++, now_ns);
    stats_.record_ping_sent();
}

bool Connection::check_timeout(int64_t now_ns) {
    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_->heartbeat_timeout).count();

    switch (state_) {
        case ConnectionState::kConnecting:
        case ConnectionState::kHandshaking:
            // Both roles: also reaps a responder whose peer never sends CONNECT_HELLO.
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
                    // Advance (don't reset) so a long stall counts as several misses.
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
            // kResponder: one silent heartbeat_timeout is enough (the owner just reaps it).
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
    stats_.connected_since_ns = 0;
    stats_.record_reconnect();
    transition_to(role_ == Role::kInitiator ? ConnectionState::kConnecting : ConnectionState::kHandshaking,
                  now_ns);
    if (role_ == Role::kResponder) {
        apply_socket_options();
    }
}

void Connection::queue_message(MessageType type, uint64_t sequence, int64_t timestamp_ns) {
    HeartbeatMessage msg{};
    msg.type = static_cast<uint8_t>(type);
    msg.feed_id = feed_id_;
    msg.sequence = sequence;
    msg.timestamp_ns = timestamp_ns;
    HeartbeatWireBuffer wire;
    encode_heartbeat(msg, wire);
    queue_write(wire);
}

void Connection::queue_write(const HeartbeatWireBuffer& wire) {
    const bool was_empty = write_pending_.empty();
    write_pending_.insert(write_pending_.end(), wire.begin(), wire.end());
    if (write_pending_.size() > kMaxWritePendingBytes) [[unlikely]] {
        // Peer isn't draining. Fail rather than grow forever or drop queued bytes, which
        // could cut a message in half and desync the peer's decoder.
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
                break;  // short write: send buffer full, wait for EPOLLOUT
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
