#include "feed_simulator.h"
#include "epoll_utils.h"
#include "time_utils.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

FeedSimulator::FeedSimulator(uint16_t port, uint32_t feed_id, Config config, FaultConfig faults,
                             const SimulatorOptions& options)
    : feed_id_(feed_id),
      config_(std::move(config)),
      faults_(faults),
      max_clients_(std::max<size_t>(options.max_clients, 1)) {
    listen_fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
    if (!listen_fd_.valid()) {
        throw_errno("socket() failed");
    }

    const int reuse = 1;
    ::setsockopt(listen_fd_.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (::inet_pton(AF_INET, options.bind_address.c_str(), &addr.sin_addr) != 1) {
        throw std::runtime_error("invalid IPv4 bind address '" + options.bind_address + "'");
    }
    addr.sin_port = htons(port);
    if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw_errno("bind() failed on port " + std::to_string(port));
    }
    if (::listen(listen_fd_.get(), SOMAXCONN) < 0) {
        throw_errno("listen() failed");
    }

    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    if (::getsockname(listen_fd_.get(), reinterpret_cast<sockaddr*>(&bound), &bound_len) < 0) {
        throw_errno("getsockname() failed");
    }
    bound_port_ = ntohs(bound.sin_port);

    spare_fd_.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));  // best-effort

    epoll_fd_.reset(::epoll_create1(0));
    if (!epoll_fd_.valid()) {
        throw_errno("epoll_create1() failed");
    }

    wake_fd_.reset(::eventfd(0, EFD_NONBLOCK));
    if (!wake_fd_.valid()) {
        throw_errno("eventfd() failed");
    }

    timer_fd_.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK));
    if (!timer_fd_.valid()) {
        throw_errno("timerfd_create() failed");
    }
    arm_periodic_timer(timer_fd_.get(), config_.tick_interval);

    epoll_event listen_ev{};
    listen_ev.events = epoll_interest_flags(/*readable=*/true, /*writable=*/false, config_.edge_triggered);
    listen_ev.data.fd = listen_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, listen_fd_.get(), &listen_ev) < 0) {
        throw_errno("epoll_ctl(listen_fd) failed");
    }

    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = wake_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, wake_fd_.get(), &wake_ev) < 0) {
        throw_errno("epoll_ctl(wake_fd) failed");
    }

    epoll_event timer_ev{};
    timer_ev.events = EPOLLIN;
    timer_ev.data.fd = timer_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, timer_fd_.get(), &timer_ev) < 0) {
        throw_errno("epoll_ctl(timer_fd) failed");
    }

    event_batch_.resize(static_cast<size_t>(std::max(config_.epoll_max_events, 1)));
}

void FeedSimulator::run() {
    // stop_requested_ is not reset here so a stop() that raced ahead of run() isn't lost.
    while (!stop_requested_.load(std::memory_order_relaxed)) {
        const int n = ::epoll_wait(epoll_fd_.get(), event_batch_.data(),
                                    static_cast<int>(event_batch_.size()), -1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "[feed_simulator] epoll_wait() failed: " << std::strerror(errno) << "\n";
            break;
        }

        const int64_t now_ns = now_monotonic_ns();
        removed_in_batch_.clear();
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
            if (fd == listen_fd_.get()) {
                accept_new_connections();
                continue;
            }
            if (fd == command_fd_) {
                on_command_fd_readable();
                continue;
            }
            // Stale-event guard: see removed_in_batch_.
            if (std::find(removed_in_batch_.begin(), removed_in_batch_.end(), fd) !=
                removed_in_batch_.end()) {
                continue;
            }
            handle_client_event(fd, ev.events, now_ns);
        }
    }
    send_disconnect_to_all();
}

void FeedSimulator::send_disconnect_to_all() {
    // Best-effort; skipped for clients with queued bytes so the message can't
    // land mid-frame.
    HeartbeatMessage msg{};
    msg.type = static_cast<uint8_t>(MessageType::kDisconnect);
    msg.feed_id = feed_id_;
    msg.timestamp_ns = now_monotonic_ns();
    HeartbeatWireBuffer wire;
    encode_heartbeat(msg, wire);
    for (auto& [fd, conn] : clients_) {
        if (conn.state() == ConnectionState::kFailed || conn.wants_write()) {
            continue;
        }
        [[maybe_unused]] const ssize_t sent = ::send(fd, wire.data(), wire.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
    }
}

void FeedSimulator::stop() {
    stop_requested_.store(true, std::memory_order_relaxed);
    const uint64_t one = 1;
    [[maybe_unused]] const ssize_t discard = ::write(wake_fd_.get(), &one, sizeof(one));
}

void FeedSimulator::disarm_listener() {
    // Leaving the listener armed after an unfixable accept4() failure would
    // spin (level-triggered) or stall (edge-triggered). Mask it; the next tick
    // re-arms it, and EPOLL_CTL_MOD re-evaluates readiness.
    epoll_event ev{};
    ev.events = 0;
    ev.data.fd = listen_fd_.get();
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, listen_fd_.get(), &ev);
    listener_disarmed_ = true;
}

void FeedSimulator::accept_new_connections() {
    bool logged = false;  // at most one stderr line per call
    while (true) {
        sockaddr_in peer_addr{};
        socklen_t addr_len = sizeof(peer_addr);
        const int client_fd = ::accept4(listen_fd_.get(), reinterpret_cast<sockaddr*>(&peer_addr),
                                         &addr_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // drained (required under edge-triggered epoll)
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;  // transient; try the next pending connection
            }
            const int err = errno;
            if (!logged) {
                std::cerr << "[feed_simulator] accept4() failed: " << std::strerror(err) << "\n";
                logged = true;
            }
            if ((err == EMFILE || err == ENFILE) && spare_fd_.valid()) {
                // Out of fds: free the spare, accept-and-close the queued
                // connection to drain the backlog, then re-reserve the spare.
                spare_fd_.reset();
                const int shed = ::accept4(listen_fd_.get(), nullptr, nullptr, SOCK_CLOEXEC);
                const int shed_errno = errno;
                if (shed >= 0) {
                    ::close(shed);
                }
                spare_fd_.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
                if (shed >= 0) {
                    continue;
                }
                if (shed_errno == EAGAIN || shed_errno == EWOULDBLOCK) {
                    break;
                }
            }
            disarm_listener();  // unrecoverable here: retry on the next tick
            break;
        }

        if (clients_.size() >= max_clients_) {
            // Unauthenticated peers must not be able to exhaust fds.
            ::close(client_fd);
            if (!logged) {
                std::cerr << "[feed_simulator] client cap (" << max_clients_ << ") reached -- refusing connections\n";
                logged = true;
            }
            continue;
        }

        char ip_buf[INET_ADDRSTRLEN] = {};
        ::inet_ntop(AF_INET, &peer_addr.sin_addr, ip_buf, sizeof(ip_buf));
        const uint16_t peer_port = ntohs(peer_addr.sin_port);

        auto [it, inserted] = clients_.try_emplace(client_fd, client_fd, Role::kResponder, feed_id_,
                                                    std::string(ip_buf), peer_port, config_);
        (void)inserted;  // fd just came from accept4(): always a fresh key

        it->second.set_ping_interceptor([this, client_fd](uint64_t seq, int64_t ts) {
            return on_ping_intercept(client_fd, seq, ts);
        });

        epoll_event ev{};
        ev.events = epoll_interest_flags(/*readable=*/true, /*writable=*/false, config_.edge_triggered);
        ev.data.fd = client_fd;
        if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            std::cerr << "[feed_simulator] epoll_ctl(ADD, client) failed: " << std::strerror(errno) << "\n";
            clients_.erase(client_fd);
        }
    }
}

void FeedSimulator::handle_client_event(int fd, uint32_t events, int64_t now_ns) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) [[unlikely]] {
        return;  // stale event for an fd removed earlier in this batch
    }
    Connection& conn = it->second;

    if (events & (EPOLLHUP | EPOLLERR)) {
        int err = 0;
        socklen_t len = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        conn.on_hangup_or_error(now_ns, err != 0 ? err : ECONNRESET);
    } else {
        if (events & EPOLLIN) {
            conn.on_readable(now_ns);
        }
        if (conn.state() != ConnectionState::kFailed && (events & EPOLLOUT)) {
            conn.on_writable(now_ns);
        }
    }

    if (conn.state() == ConnectionState::kFailed) {
        remove_client(fd);
        return;
    }
    sync_epoll_interest(fd, conn);
}

void FeedSimulator::sync_epoll_interest(int fd, Connection& conn) {
    epoll_event ev{};
    ev.events = epoll_interest_flags(/*readable=*/true, conn.wants_write(), config_.edge_triggered);
    ev.data.fd = fd;
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, fd, &ev);
}

void FeedSimulator::remove_client(int fd) {
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr);
    // Purge deferred replies first: a later accept() may reuse this fd number.
    pending_replies_.erase(
        std::remove_if(pending_replies_.begin(), pending_replies_.end(),
                        [fd](const PendingReply& p) { return p.fd == fd; }),
        pending_replies_.end());
    clients_.erase(fd);  // the Connection's UniqueFd closes fd
    removed_in_batch_.push_back(fd);
}

bool FeedSimulator::on_ping_intercept(int fd, uint64_t sequence, int64_t timestamp_ns) {
    if (faults_.drop_probability > 0.0) {
        std::uniform_real_distribution<double> coin(0.0, 1.0);
        if (coin(rng_) < faults_.drop_probability) {
            return false;  // dropped
        }
    }

    if (faults_.extra_latency.count() > 0) {
        if (pending_replies_.size() >= kMaxPendingReplies) {
            return false;  // queue full: drop rather than grow without bound
        }
        int64_t delay_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(faults_.extra_latency).count();
        if (faults_.jitter) {
            std::uniform_real_distribution<double> factor(0.5, 1.5);
            delay_ns = static_cast<int64_t>(static_cast<double>(delay_ns) * factor(rng_));
        }
        pending_replies_.push_back(PendingReply{now_monotonic_ns() + delay_ns, fd, sequence, timestamp_ns});
        return false;  // on_timer_tick sends it later
    }

    return true;  // no active faults: reply immediately
}

void FeedSimulator::on_timer_tick() {
    uint64_t expirations;
    [[maybe_unused]] const ssize_t discard = ::read(timer_fd_.get(), &expirations, sizeof(expirations));

    const int64_t now_ns = now_monotonic_ns();

    // The tick is the retry timer for a disarmed listener / lost spare fd.
    if (!spare_fd_.valid()) {
        spare_fd_.reset(::open("/dev/null", O_RDONLY | O_CLOEXEC));
    }
    if (listener_disarmed_) {
        listener_disarmed_ = false;
        epoll_event ev{};
        ev.events = epoll_interest_flags(/*readable=*/true, /*writable=*/false, config_.edge_triggered);
        ev.data.fd = listen_fd_.get();
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, listen_fd_.get(), &ev);
    }

    // Reap silent clients (stuck handshake or no PINGs). Collect first:
    // remove_client() erases from clients_ while we iterate it.
    std::vector<int> failed_fds;
    for (auto& [fd, conn] : clients_) {
        if (conn.check_timeout(now_ns)) {
            failed_fds.push_back(fd);
        }
    }
    for (const int fd : failed_fds) {
        remove_client(fd);
    }

    if (pending_replies_.empty()) {
        return;
    }

    std::vector<PendingReply> still_pending;
    still_pending.reserve(pending_replies_.size());
    std::vector<int> failed_by_send;  // remove_client() edits pending_replies_; defer past the loop

    for (const PendingReply& pending : pending_replies_) {
        if (now_ns < pending.fire_at_ns) {
            still_pending.push_back(pending);
            continue;
        }
        auto it = clients_.find(pending.fd);
        if (it != clients_.end()) {
            it->second.send_pong(pending.sequence, pending.timestamp_ns);
            if (it->second.state() == ConnectionState::kFailed) {
                // Write queue overflowed; check_timeout() skips kFailed
                // connections, so reap it here.
                failed_by_send.push_back(pending.fd);
            } else {
                sync_epoll_interest(pending.fd, it->second);  // reply may need EPOLLOUT
            }
        }
        // else: client left before its reply fired; drop it.
    }
    pending_replies_ = std::move(still_pending);
    for (const int fd : failed_by_send) {
        if (clients_.count(fd) != 0) {  // an fd can repeat
            remove_client(fd);
        }
    }
}

void FeedSimulator::kill_random_connection() {
    if (clients_.empty()) {
        return;
    }
    std::uniform_int_distribution<size_t> pick(0, clients_.size() - 1);
    auto it = clients_.begin();
    std::advance(it, static_cast<long>(pick(rng_)));
    const int fd = it->first;

    struct linger sl{1, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));

    remove_client(fd);
}

void FeedSimulator::set_fault_config(FaultConfig faults) { faults_ = faults; }

void FeedSimulator::set_command_input(int fd, CommandLineCallback on_line) {
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw_errno("epoll_ctl(command fd) failed");  // e.g. EPERM for a regular file
    }
    command_fd_ = fd;
    on_command_line_ = std::move(on_line);
}

void FeedSimulator::close_command_input() {
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, command_fd_, nullptr);
    command_fd_ = -1;  // the fd belongs to the caller; we only stop watching it
    command_line_buffer_.clear();
    discarding_oversized_line_ = false;
}

// Dispatches complete lines, then applies the length cap to the unterminated
// remainder (a cap per line, not per wakeup). An oversized line is dropped in
// full, including its tail up to the next '\n'.
void FeedSimulator::process_command_buffer() {
    size_t start = 0;
    size_t pos;
    while ((pos = command_line_buffer_.find('\n', start)) != std::string::npos) {
        if (discarding_oversized_line_) {
            discarding_oversized_line_ = false;  // this '\n' ends the oversized line
        } else {
            std::string line = command_line_buffer_.substr(start, pos - start);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (on_command_line_) {
                on_command_line_(line);
            }
        }
        start = pos + 1;
    }
    command_line_buffer_.erase(0, start);

    if (discarding_oversized_line_) {
        command_line_buffer_.clear();  // everything left is still part of the oversized line
    } else if (command_line_buffer_.size() > kMaxCommandLineBufferBytes) {
        std::cerr << "[feed_simulator] command line exceeded " << kMaxCommandLineBufferBytes
                  << " bytes without a newline -- discarding it\n";
        command_line_buffer_.clear();
        discarding_oversized_line_ = true;
    }
}

void FeedSimulator::on_command_fd_readable() {
    // Bounded reads per wakeup so this fd can't starve client I/O. If the bound
    // is hit before EAGAIN, re-arm with EPOLL_CTL_MOD (edge-triggered epoll
    // would otherwise never fire again for the queued data).
    constexpr int kMaxReadsPerWakeup = 16;
    char buf[4096];
    for (int i = 0; i < kMaxReadsPerWakeup; ++i) {
        const ssize_t n = ::read(command_fd_, buf, sizeof(buf));
        if (n > 0) {
            command_line_buffer_.append(buf, static_cast<size_t>(n));
            process_command_buffer();
            continue;
        }
        if (n == 0) {
            // EOF: only an unterminated last line remains undispatched.
            if (!discarding_oversized_line_ && !command_line_buffer_.empty() && on_command_line_) {
                on_command_line_(command_line_buffer_);
            }
            close_command_input();
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;  // drained
        }
        if (errno == EINTR) {
            continue;
        }
        std::cerr << "[feed_simulator] command input read() failed: " << std::strerror(errno)
                  << " -- no longer reading commands\n";
        close_command_input();
        return;
    }

    epoll_event ev{};
    ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
    ev.data.fd = command_fd_;
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, command_fd_, &ev);
}
