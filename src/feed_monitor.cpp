#include "feed_monitor.h"
#include "epoll_utils.h"
#include "time_utils.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <random>

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

    // A non-blocking connect() may succeed immediately (rc==0, rare), report
    // EINPROGRESS (the common case -- wait for EPOLLOUT), or fail immediately
    // (e.g. ECONNREFUSED on loopback to a closed port -- very much a live
    // case for this project). All three are handled uniformly: the fd stays
    // valid either way, and Connection::on_writable's getsockopt(SO_ERROR)
    // check discovers the outcome whether it happened synchronously or
    // asynchronously, so no special-casing is needed here.
    (void)::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
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
        // the moved-in slot's fd/feed_id now live at `index`, not `last` --
        // both maps must be repointed or every subsequent lookup for that
        // feed silently targets the wrong (now-erased) slot. But a kFailed
        // connection's fd is open-but-deliberately-unmapped: on_connection_failed
        // erases its index_of_fd_ entry the moment it fails, without closing the
        // fd (that only happens later, via rebind()) -- so connection.fd() for a
        // kFailed slot is still >= 0 even though it must NOT be in index_of_fd_.
        // Repointing unconditionally here would silently re-insert the exact
        // entry that erase was meant to remove, leaking it until that slot's next
        // successful reconnect (or forever, if it has given up retrying).
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
        // A manual "reconnect now" implies a full fresh retry budget, not
        // "immediately re-trigger give_up on the very next failure" -- without
        // this reset, forcing a reconnect on a slot that already exhausted
        // max_reconnect_attempts would silently give up again after just one
        // more failed attempt, defeating the operator's actual intent.
        slot.reconnect_attempts = 0;
        slot.reconnect_ready_at_ns = 0;  // ready immediately -- the next tick fires it
    }
}

void FeedMonitor::run() {
    stop_requested_.store(false, std::memory_order_relaxed);
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
        // EPOLLOUT before EPOLLIN: for a kConnecting connection, EPOLLOUT is
        // what signals connect() completion and must be processed first --
        // on_readable() would be meaningless (or a wasted recv() call) before
        // that transition happens, and both bits can legitimately be set in
        // the same event on a fast loopback connection.
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
        // Stop watching the dead fd immediately, rather than waiting for the
        // eventual reconnect to close it via rebind(): under level-triggered
        // epoll (edge_triggered=false), a HUP/ERR fd left registered would
        // keep reporting ready on every epoll_wait call until acted on --
        // effectively a busy-loop for however long the backoff delay is.
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
        index_of_fd_.erase(fd);
    }
    emit_failure_alert(slot);
    schedule_reconnect(index, now_monotonic_ns());
}

void FeedMonitor::emit_failure_alert(const Slot& slot) const {
    std::cerr << "[feed_monitor] ALERT: feed " << slot.connection.feed_id() << " (\"" << slot.name
               << "\" " << slot.connection.host() << ":" << slot.connection.port() << ") failed\n";
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

    // Exponential backoff, capped, with +/-25% jitter so many simultaneously
    // failing feeds don't all retry in lockstep (a thundering herd against
    // whatever's on the other end).
    const int capped_attempt = std::min(attempt, 30);  // avoid overflowing 2^attempt over a long-running process
    int64_t delay = base;
    for (int i = 0; i < capped_attempt && delay < max_delay; ++i) {
        delay *= 2;
    }
    delay = std::min(delay, max_delay);

    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_real_distribution<double> jitter(-0.25, 0.25);
    const double factor = 1.0 + jitter(rng);
    return static_cast<int64_t>(static_cast<double>(delay) * factor);
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
        std::cerr << "[feed_monitor] epoll_ctl(ADD) failed on reconnect for feed "
                   << slot.connection.feed_id() << ": " << std::strerror(errno) << "\n";
    }
}

void FeedMonitor::sync_epoll_interest(size_t index) {
    Connection& conn = slots_[index].connection;
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, conn.wants_write(), config_.edge_triggered);
    ev.data.fd = conn.fd();
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, conn.fd(), &ev);
}

void FeedMonitor::on_timer_tick() {
    // Drain the timerfd's expiration counter -- required even though the
    // value itself isn't used, or epoll keeps reporting it readable.
    uint64_t expirations;
    [[maybe_unused]] const ssize_t discard = ::read(timer_fd_.get(), &expirations, sizeof(expirations));

    const int64_t now_ns = now_monotonic_ns();
    const int64_t interval_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config_.heartbeat_interval).count();

    for (size_t i = 0; i < slots_.size(); ++i) {
        Slot& slot = slots_[i];
        const ConnectionState state = slot.connection.state();

        if (state == ConnectionState::kHealthy || state == ConnectionState::kDegraded) {
            if (now_ns >= slot.next_ping_due_ns) {
                slot.connection.send_ping(now_ns);
                slot.next_ping_due_ns = now_ns + interval_ns;
                sync_epoll_interest(i);  // send_ping may have queued bytes -> needs EPOLLOUT armed
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

void FeedMonitor::on_command_fd_readable() {
    char buf[1024];
    bool eof = false;
    while (true) {
        const ssize_t n = ::read(command_fd_, buf, sizeof(buf));
        if (n > 0) {
            command_line_buffer_.append(buf, static_cast<size_t>(n));
            if (command_line_buffer_.size() > kMaxCommandLineBufferBytes) {
                std::cerr << "[feed_monitor] command line exceeded " << kMaxCommandLineBufferBytes
                          << " bytes without a newline -- discarding buffered input\n";
                command_line_buffer_.clear();
            }
            continue;
        }
        if (n == 0) {
            // EOF on the command stream (e.g. stdin closed, or redirected
            // from a file/pipe that ran out). Do NOT return early here: with
            // piped/redirected input (as opposed to an interactive TTY), the
            // final batch of real commands routinely arrives in the same
            // read() loop as EOF -- returning immediately would silently
            // drop everything just appended to command_line_buffer_ above,
            // since the line-parsing loop below would never run. Break
            // instead and let that loop see it, same as any other exit path.
            eof = true;
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        break;  // any other error: stop trying to read commands, not fatal to the reactor
    }

    size_t pos;
    while ((pos = command_line_buffer_.find('\n')) != std::string::npos) {
        std::string line = command_line_buffer_.substr(0, pos);
        command_line_buffer_.erase(0, pos + 1);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (on_command_line_) {
            on_command_line_(line);
        }
    }

    if (eof) {
        // A final command with no trailing newline is still a real command
        // -- don't drop it just because the stream ended without one (e.g.
        // `echo -n quit | feed_monitor`, or a heredoc missing its last
        // newline).
        if (!command_line_buffer_.empty() && on_command_line_) {
            on_command_line_(command_line_buffer_);
        }
        command_line_buffer_.clear();
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, command_fd_, nullptr);
        command_fd_ = -1;
    }
}

void FeedMonitor::set_command_input(int fd, CommandLineCallback on_line) {
    command_fd_ = fd;
    on_command_line_ = std::move(on_line);
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
    ev.data.fd = fd;
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev);
}

void FeedMonitor::set_tick_callback(TickCallback callback) { on_tick_ = std::move(callback); }

void FeedMonitor::set_heartbeat_interval(std::chrono::milliseconds interval) {
    config_.heartbeat_interval = interval;
}

void FeedMonitor::set_heartbeat_timeout(std::chrono::milliseconds timeout) {
    config_.heartbeat_timeout = timeout;
    // Give every connection a fresh liveness baseline under the new timeout
    // instead of retroactively re-judging however much wait time has
    // already accumulated against a (possibly much smaller) new budget --
    // see Connection::reset_liveness_baseline for why. Safe to call
    // unconditionally across every slot regardless of its current state.
    const int64_t now_ns = now_monotonic_ns();
    for (Slot& slot : slots_) {
        slot.connection.reset_liveness_baseline(now_ns);
    }
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
