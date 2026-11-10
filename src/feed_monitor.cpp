#include "feed_monitor.h"
#include "epoll_utils.h"
#include "time_utils.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

FeedMonitor::FeedMonitor(Config config) : config_(std::move(config)) {
    epoll_fd_.reset(::epoll_create1(0));
    if (!epoll_fd_.valid()) {
        throw_errno("epoll_create1() failed");
    }

    timer_fd_.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK));
    if (!timer_fd_.valid()) {
        throw_errno("timerfd_create() failed");
    }
    arm_timer();

    wake_fd_.reset(::eventfd(0, EFD_NONBLOCK));
    if (!wake_fd_.valid()) {
        throw_errno("eventfd() failed");
    }

    epoll_event timer_ev{};
    timer_ev.events = EPOLLIN;
    timer_ev.data.fd = timer_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, timer_fd_.get(), &timer_ev) < 0) {
        throw_errno("epoll_ctl(timer_fd) failed");
    }

    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = wake_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, wake_fd_.get(), &wake_ev) < 0) {
        throw_errno("epoll_ctl(wake_fd) failed");
    }

    event_batch_.resize(static_cast<size_t>(std::max(config_.epoll_max_events, 1)));
}

void FeedMonitor::arm_timer() { arm_periodic_timer(timer_fd_.get(), config_.tick_interval); }

std::optional<int> FeedMonitor::create_and_connect_socket(const std::string& host, uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        std::cerr << "[feed_monitor] socket() failed: " << std::strerror(errno) << "\n";
        return std::nullopt;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "[feed_monitor] invalid IPv4 address '" << host << "' (hostnames are out of scope)\n";
        ::close(fd);
        return std::nullopt;
    }

    // Result deliberately ignored (EINPROGRESS, or an immediate ECONNREFUSED on
    // loopback): Connection::on_writable's SO_ERROR check finds the outcome either way.
    [[maybe_unused]] const int connect_rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    return fd;
}

bool FeedMonitor::add_feed(const FeedEndpoint& endpoint) {
    if (index_of_feed_id_.count(endpoint.feed_id) != 0) {
        std::cerr << "[feed_monitor] feed_id " << endpoint.feed_id << " already added, ignoring\n";
        return false;
    }
    const auto fd_opt = create_and_connect_socket(endpoint.host, endpoint.port);
    if (!fd_opt) {
        return false;
    }
    const int fd = *fd_opt;

    const size_t index = slots_.size();
    slots_.push_back(Slot{Connection(fd, Role::kInitiator, endpoint.feed_id, endpoint.host, endpoint.port,
                                      config_),
                           endpoint.name, 0, -1, false, 0});
    index_of_fd_[fd] = index;
    index_of_feed_id_[endpoint.feed_id] = index;

    epoll_event ev{};
    ev.events = epoll_interest_flags(true, slots_[index].connection.wants_write(), config_.edge_triggered);
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::cerr << "[feed_monitor] epoll_ctl(ADD) failed for feed " << endpoint.feed_id << ": "
                   << std::strerror(errno) << "\n";
        // Undo the insertion; the Connection's destructor closes fd.
        index_of_fd_.erase(fd);
        index_of_feed_id_.erase(endpoint.feed_id);
        slots_.pop_back();
        return false;
    }
    return true;
}

void FeedMonitor::remove_feed(uint32_t feed_id) {
    auto it = index_of_feed_id_.find(feed_id);
    if (it == index_of_feed_id_.end()) {
        return;
    }
    remove_slot_at(it->second);
}

void FeedMonitor::remove_slot_at(size_t index) {
    const int fd = slots_[index].connection.fd();
    if (fd >= 0) {
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
        index_of_fd_.erase(fd);
    }
    index_of_feed_id_.erase(slots_[index].connection.feed_id());

    const size_t last = slots_.size() - 1;
    if (index != last) {
        slots_[index] = std::move(slots_[last]);
        // Repoint both maps at the moved-in slot. A kFailed slot is not in
        // index_of_fd_ (on_connection_failed erased it), so don't re-insert it.
        const int moved_fd = slots_[index].connection.fd();
        if (moved_fd >= 0 && slots_[index].connection.state() != ConnectionState::kFailed) {
            index_of_fd_[moved_fd] = index;
        }
        index_of_feed_id_[slots_[index].connection.feed_id()] = index;
    }
    slots_.pop_back();
}

void FeedMonitor::force_disconnect(uint32_t feed_id) {
    auto it = index_of_feed_id_.find(feed_id);
    if (it == index_of_feed_id_.end()) {
        return;
    }
    Slot& slot = slots_[it->second];
    if (slot.connection.state() != ConnectionState::kFailed) {
        slot.connection.on_hangup_or_error(now_monotonic_ns(), ECONNABORTED);
        on_connection_failed(it->second);
    }
}

void FeedMonitor::force_reconnect(uint32_t feed_id) {
    auto it = index_of_feed_id_.find(feed_id);
    if (it == index_of_feed_id_.end()) {
        return;
    }
    Slot& slot = slots_[it->second];
    if (slot.connection.state() == ConnectionState::kFailed) {
        slot.give_up = false;
        slot.reconnect_attempts = 0;     // fresh retry budget, or one more failure would give up again
        slot.reconnect_ready_at_ns = 0;  // due now; the next tick fires it
    }
}

void FeedMonitor::run() {
    while (!stop_requested_.load(std::memory_order_relaxed)) {
        const int n = ::epoll_wait(epoll_fd_.get(), event_batch_.data(),
                                    static_cast<int>(event_batch_.size()), -1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "[feed_monitor] epoll_wait() failed: " << std::strerror(errno) << "\n";
            break;
        }
        epoll_wait_count_.fetch_add(1, std::memory_order_relaxed);
        events_processed_count_.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);

        const int64_t now_ns = now_monotonic_ns();
        for (int i = 0; i < n; ++i) {
            const epoll_event& ev = event_batch_[static_cast<size_t>(i)];
            const int fd = ev.data.fd;

            if (fd == wake_fd_.get()) {
                uint64_t ignored;
                [[maybe_unused]] const ssize_t discard = ::read(wake_fd_.get(), &ignored, sizeof(ignored));
                continue;
            }
            if (fd == timer_fd_.get()) {
                on_timer_tick();
                continue;
            }
            if (fd == command_fd_) {
                on_command_fd_readable();
                continue;
            }
            auto it = index_of_fd_.find(fd);
            if (it == index_of_fd_.end()) [[unlikely]] {
                continue;  // stale event for an fd already removed earlier in this same batch
            }
            handle_connection_event(it->second, ev.events, now_ns);
        }
    }
}

void FeedMonitor::stop() {
    stop_requested_.store(true, std::memory_order_relaxed);
    const uint64_t one = 1;
    [[maybe_unused]] const ssize_t discard = ::write(wake_fd_.get(), &one, sizeof(one));
}

void FeedMonitor::handle_connection_event(size_t index, uint32_t events, int64_t now_ns) {
    Connection& conn = slots_[index].connection;

    if (events & (EPOLLHUP | EPOLLERR)) {
        int err = 0;
        socklen_t len = sizeof(err);
        ::getsockopt(conn.fd(), SOL_SOCKET, SO_ERROR, &err, &len);
        conn.on_hangup_or_error(now_ns, err != 0 ? err : ECONNRESET);
    } else {
        // EPOLLOUT first: it signals connect() completion, which must precede
        // reading (both bits can arrive together on loopback).
        if (events & EPOLLOUT) {
            conn.on_writable(now_ns);
        }
        if (conn.state() != ConnectionState::kFailed && (events & EPOLLIN)) {
            conn.on_readable(now_ns);
        }
    }

    if (conn.state() == ConnectionState::kFailed) {
        on_connection_failed(index);
        return;
    }
    sync_epoll_interest(index);
}

void FeedMonitor::on_connection_failed(size_t index) {
    Slot& slot = slots_[index];
    const int fd = slot.connection.fd();
    if (fd >= 0) {
        // Deregister before closing; a HUP/ERR fd left in a level-triggered set
        // would busy-loop for the whole backoff delay.
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
        index_of_fd_.erase(fd);
        slot.connection.close_fd();  // peer sees the close during backoff; rebind() installs a fresh fd
    }
    emit_failure_alert(slot);
    schedule_reconnect(index, now_monotonic_ns());
}

void FeedMonitor::emit_failure_alert(const Slot& slot) const {
    std::ostringstream msg;
    msg << "feed " << slot.connection.feed_id() << " (\"" << slot.name << "\" " << slot.connection.host()
        << ":" << slot.connection.port() << ") failed";
    if (on_alert_) {
        on_alert_(msg.str());
        return;
    }
    std::cerr << "[feed_monitor] ALERT: " << msg.str() << "\n";
}

void FeedMonitor::schedule_reconnect(size_t index, int64_t now_ns) {
    Slot& slot = slots_[index];
    if (config_.max_reconnect_attempts >= 0 && slot.reconnect_attempts >= config_.max_reconnect_attempts) {
        slot.give_up = true;
        std::cerr << "[feed_monitor] feed " << slot.connection.feed_id()
                   << " exceeded max_reconnect_attempts (" << config_.max_reconnect_attempts
                   << "), giving up\n";
        return;
    }
    slot.reconnect_ready_at_ns = now_ns + reconnect_delay_ns(slot.reconnect_attempts);
    ++slot.reconnect_attempts;
}

int64_t FeedMonitor::reconnect_delay_ns(int attempt) const {
    const int64_t base =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_.reconnect_base_delay).count();
    const int64_t max_delay =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_.reconnect_max_delay).count();

    // Exponential backoff, capped, with +/-25% jitter so failing feeds don't retry in lockstep.
    const int capped_attempt = std::min(attempt, 30);  // avoid overflow
    int64_t delay = base;
    for (int i = 0; i < capped_attempt && delay < max_delay; ++i) {
        delay *= 2;
    }
    delay = std::min(delay, max_delay);

    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_real_distribution<double> jitter(-0.25, 0.25);
    const double factor = 1.0 + jitter(rng);
    // Cap after jitter so the delay never exceeds max_delay.
    return std::min(static_cast<int64_t>(static_cast<double>(delay) * factor), max_delay);
}

void FeedMonitor::initiate_connect(size_t index, int64_t now_ns) {
    Slot& slot = slots_[index];
    slot.reconnect_ready_at_ns = -1;

    const auto fd_opt = create_and_connect_socket(slot.connection.host(), slot.connection.port());
    if (!fd_opt) {
        schedule_reconnect(index, now_ns);
        return;
    }
    const int fd = *fd_opt;
    slot.connection.rebind(fd, now_ns);
    index_of_fd_[fd] = index;

    epoll_event ev{};
    ev.events = epoll_interest_flags(true, slot.connection.wants_write(), config_.edge_triggered);
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
        const int err = errno;
        std::cerr << "[feed_monitor] epoll_ctl(ADD) failed on reconnect for feed "
                   << slot.connection.feed_id() << ": " << std::strerror(err) << "\n";
        // Never registered, so no event will arrive; fail it now and back off.
        slot.connection.on_hangup_or_error(now_ns, err);
        on_connection_failed(index);
    }
}

void FeedMonitor::sync_epoll_interest(size_t index) {
    Connection& conn = slots_[index].connection;
    if (conn.fd() < 0) {
        return;  // closed by on_connection_failed(); nothing registered to update
    }
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, conn.wants_write(), config_.edge_triggered);
    ev.data.fd = conn.fd();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, conn.fd(), &ev) < 0) {
        std::cerr << "[feed_monitor] epoll_ctl(MOD) failed for feed " << conn.feed_id() << ": "
                   << std::strerror(errno) << "\n";
    }
}

void FeedMonitor::on_timer_tick() {
    // Drain the expiration counter or epoll keeps reporting the timerfd readable.
    uint64_t expirations;
    [[maybe_unused]] const ssize_t discard = ::read(timer_fd_.get(), &expirations, sizeof(expirations));

    const int64_t now_ns = now_monotonic_ns();
    const int64_t interval_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_.heartbeat_interval).count();

    // Backoff resets only after a connection stays healthy for one heartbeat_timeout,
    // so a peer that handshakes then drops isn't retried at the base delay forever.
    const int64_t stable_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_.heartbeat_timeout).count();

    // Indices are stable here: on_connection_failed() never removes a slot.
    for (size_t i = 0; i < slots_.size(); ++i) {
        Slot& slot = slots_[i];
        const ConnectionState state = slot.connection.state();

        if (state == ConnectionState::kHealthy && slot.reconnect_attempts > 0 &&
            now_ns - slot.connection.stats().connected_since_ns >= stable_ns) {
            slot.reconnect_attempts = 0;
        }

        if (state == ConnectionState::kHealthy || state == ConnectionState::kDegraded) {
            if (now_ns >= slot.next_ping_due_ns) {
                slot.connection.send_ping(now_ns);
                slot.next_ping_due_ns = now_ns + interval_ns;
                if (slot.connection.state() == ConnectionState::kFailed) {
                    // The send itself failed; no epoll event will report it.
                    on_connection_failed(i);
                    continue;
                }
                sync_epoll_interest(i);  // send_ping may have queued bytes needing EPOLLOUT
            }
            if (slot.connection.check_timeout(now_ns)) {
                on_connection_failed(i);
            }
        } else if (state == ConnectionState::kConnecting || state == ConnectionState::kHandshaking) {
            if (slot.connection.check_timeout(now_ns)) {
                on_connection_failed(i);
            }
        } else if (state == ConnectionState::kFailed && !slot.give_up && slot.reconnect_ready_at_ns >= 0 &&
                   now_ns >= slot.reconnect_ready_at_ns) {
            initiate_connect(i, now_ns);
        }
    }

    if (on_tick_) {
        on_tick_(snapshot_stats());
    }
}

void FeedMonitor::dispatch_command_line(std::string line) {
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (on_command_line_) {
        on_command_line_(line);
    }
}

void FeedMonitor::on_command_fd_readable() {
    // Bounds reads per wakeup so a fast producer can't starve the reactor; if
    // the budget runs out, the fd is re-armed below instead of drained to EAGAIN.
    constexpr int kMaxReadsPerWakeup = 16;
    char buf[4096];
    bool eof = false;
    bool drained = false;

    for (int reads = 0; reads < kMaxReadsPerWakeup && !stop_requested_.load(std::memory_order_relaxed); ++reads) {
        const ssize_t n = ::read(command_fd_, buf, sizeof(buf));
        if (n > 0) {
            command_line_buffer_.append(buf, static_cast<size_t>(n));

            // Dispatch complete lines now; only the unterminated remainder is length-capped.
            size_t start = 0;
            size_t pos;
            while ((pos = command_line_buffer_.find('\n', start)) != std::string::npos) {
                if (discarding_oversized_line_) {
                    discarding_oversized_line_ = false;  // the over-long line finally ended
                } else if (pos - start > kMaxCommandLineBufferBytes) {
                    // Terminated, but already past the cap.
                    std::cerr << "[feed_monitor] command line exceeded " << kMaxCommandLineBufferBytes
                              << " bytes -- discarding it\n";
                } else {
                    dispatch_command_line(command_line_buffer_.substr(start, pos - start));
                }
                start = pos + 1;
            }
            command_line_buffer_.erase(0, start);

            if (discarding_oversized_line_) {
                command_line_buffer_.clear();  // still inside the line being dropped
            } else if (command_line_buffer_.size() > kMaxCommandLineBufferBytes) {
                std::cerr << "[feed_monitor] command line exceeded " << kMaxCommandLineBufferBytes
                          << " bytes without a newline -- discarding it\n";
                command_line_buffer_.clear();
                discarding_oversized_line_ = true;  // drop its tail too, don't run it as a command
            }
            continue;
        }
        if (n == 0) {
            // EOF; lines from this read were already dispatched above.
            eof = true;
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        drained = true;  // EAGAIN/EWOULDBLOCK: nothing more to read right now
        break;           // (any other error: stop reading commands, not fatal to the reactor)
    }

    if (eof) {
        // A final command without a trailing newline still counts (`echo -n quit | ...`).
        if (!command_line_buffer_.empty() && !discarding_oversized_line_) {
            dispatch_command_line(std::move(command_line_buffer_));
        }
        command_line_buffer_.clear();
        discarding_oversized_line_ = false;
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, command_fd_, nullptr);
        command_fd_ = -1;
        return;
    }

    if (!drained && command_fd_ >= 0) {
        // Stopped before EAGAIN: edge-triggered epoll won't fire again for the
        // queued data, so MOD re-arms it (re-evaluates readiness) for the next epoll_wait.
        epoll_event ev{};
        ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
        ev.data.fd = command_fd_;
        if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, command_fd_, &ev) < 0) {
            std::cerr << "[feed_monitor] epoll_ctl(MOD) failed for command input: " << std::strerror(errno) << "\n";
        }
    }
}

bool FeedMonitor::set_command_input(int fd, CommandLineCallback on_line) {
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::cerr << "[feed_monitor] epoll_ctl(ADD) failed for command input fd " << fd << ": "
                   << std::strerror(errno) << "\n";
        return false;
    }
    command_fd_ = fd;
    on_command_line_ = std::move(on_line);
    return true;
}

void FeedMonitor::set_tick_callback(TickCallback callback) { on_tick_ = std::move(callback); }

void FeedMonitor::set_alert_callback(AlertCallback callback) { on_alert_ = std::move(callback); }

bool FeedMonitor::set_heartbeat_interval(std::chrono::milliseconds interval) {
    if (interval <= std::chrono::milliseconds::zero()) {
        return false;
    }
    config_.heartbeat_interval = interval;
    // Pull already-scheduled pings forward so a shorter interval applies immediately.
    const int64_t now_ns = now_monotonic_ns();
    const int64_t latest_due_ns =
        now_ns + std::chrono::duration_cast<std::chrono::nanoseconds>(interval).count();
    for (Slot& slot : slots_) {
        slot.next_ping_due_ns = std::min(slot.next_ping_due_ns, latest_due_ns);
    }
    return true;
}

bool FeedMonitor::set_heartbeat_timeout(std::chrono::milliseconds timeout) {
    if (timeout <= std::chrono::milliseconds::zero()) {
        return false;
    }
    config_.heartbeat_timeout = timeout;
    // Fresh liveness baseline so accumulated wait isn't judged against the new timeout.
    const int64_t now_ns = now_monotonic_ns();
    for (Slot& slot : slots_) {
        slot.connection.reset_liveness_baseline(now_ns);
    }
    return true;
}

AggregateStats FeedMonitor::snapshot_stats() const {
    AggregateStats result;
    result.feeds.reserve(slots_.size());
    for (const Slot& slot : slots_) {
        FeedSnapshot snap;
        snap.feed_id = slot.connection.feed_id();
        snap.name = slot.name;
        snap.state = slot.connection.state();
        snap.stats = slot.connection.stats();

        switch (snap.state) {
            case ConnectionState::kHealthy: ++result.healthy_count; break;
            case ConnectionState::kDegraded: ++result.degraded_count; break;
            case ConnectionState::kFailed: ++result.failed_count; break;
            default: break;
        }
        if (slot.reconnect_ready_at_ns >= 0) {
            ++result.reconnecting_count;
        }
        result.feeds.push_back(std::move(snap));
    }
    return result;
}
