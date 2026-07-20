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
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

FeedSimulator::FeedSimulator(uint16_t port, uint32_t feed_id, Config config, FaultConfig faults)
    : feed_id_(feed_id), config_(std::move(config)), faults_(faults) {
    listen_fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
    if (!listen_fd_.valid()) {
        throw_errno("socket() failed");
    }

    const int reuse = 1;
    ::setsockopt(listen_fd_.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw_errno("bind() failed on port " + std::to_string(port));
    }
    if (::listen(listen_fd_.get(), SOMAXCONN) < 0) {
        throw_errno("listen() failed");
    }

    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    ::getsockname(listen_fd_.get(), reinterpret_cast<sockaddr*>(&bound), &bound_len);
    bound_port_ = ntohs(bound.sin_port);

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
    stop_requested_.store(false, std::memory_order_relaxed);
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
                accept_new_connections(now_ns);
                continue;
            }
            if (fd == command_fd_) {
                on_command_fd_readable();
                continue;
            }
            handle_client_event(fd, ev.events, now_ns);
        }
    }
}

void FeedSimulator::stop() {
    stop_requested_.store(true, std::memory_order_relaxed);
    const uint64_t one = 1;
    [[maybe_unused]] const ssize_t discard = ::write(wake_fd_.get(), &one, sizeof(one));
}

void FeedSimulator::accept_new_connections(int64_t now_ns) {
    (void)now_ns;  // Connection's constructor stamps its own state_entered_ns_ via now_monotonic_ns()
    while (true) {
        sockaddr_in peer_addr{};
        socklen_t addr_len = sizeof(peer_addr);
        const int client_fd = ::accept4(listen_fd_.get(), reinterpret_cast<sockaddr*>(&peer_addr),
                                         &addr_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // drained -- mandatory exit condition under edge-triggered epoll
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;  // transient; try the next pending connection
            }
            std::cerr << "[feed_simulator] accept4() failed: " << std::strerror(errno) << "\n";
            break;
        }

        char ip_buf[INET_ADDRSTRLEN] = {};
        ::inet_ntop(AF_INET, &peer_addr.sin_addr, ip_buf, sizeof(ip_buf));
        const uint16_t peer_port = ntohs(peer_addr.sin_port);

        auto [it, inserted] = clients_.try_emplace(client_fd, client_fd, Role::kResponder, feed_id_,
                                                    std::string(ip_buf), peer_port, config_);
        (void)inserted;  // fd just came from accept4() -- always a fresh key

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
        return;  // stale event for an fd already removed earlier in this same batch
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
    // Purge any fault-injection replies still pending for this fd before it's
    // closed and the fd number becomes eligible for reuse by a later accept()
    // -- otherwise a stale deferred PONG could misfire at an unrelated future
    // client that happens to get the same fd number.
    pending_replies_.erase(
        std::remove_if(pending_replies_.begin(), pending_replies_.end(),
                        [fd](const PendingReply& p) { return p.fd == fd; }),
        pending_replies_.end());
    clients_.erase(fd);  // destroys the Connection, whose UniqueFd closes fd
}

bool FeedSimulator::on_ping_intercept(int fd, uint64_t sequence, int64_t timestamp_ns) {
    if (faults_.drop_probability > 0.0) {
        std::uniform_real_distribution<double> coin(0.0, 1.0);
        if (coin(rng_) < faults_.drop_probability) {
            return false;  // dropped for good -- no deferred send scheduled
        }
    }

    if (faults_.extra_latency.count() > 0) {
        int64_t delay_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(faults_.extra_latency).count();
        if (faults_.jitter) {
            std::uniform_real_distribution<double> factor(0.5, 1.5);
            delay_ns = static_cast<int64_t>(static_cast<double>(delay_ns) * factor(rng_));
        }
        pending_replies_.push_back(PendingReply{now_monotonic_ns() + delay_ns, fd, sequence, timestamp_ns});
        return false;  // suppress the immediate reply; on_timer_tick will send it later
    }

    return true;  // no active faults -- reply immediately, as normal
}

void FeedSimulator::on_timer_tick() {
    uint64_t expirations;
    [[maybe_unused]] const ssize_t discard = ::read(timer_fd_.get(), &expirations, sizeof(expirations));

    const int64_t now_ns = now_monotonic_ns();

    // F13: reap any client that's gone silent -- stuck mid-handshake (never
    // sent CONNECT_HELLO) or healthy but no longer sending PING. Without
    // this, a hung/slow-loris test client occupies a Connection+fd forever;
    // this reuses the tick already wired up for the deferred-reply queue
    // below rather than adding a second timer. Collect failed fds into a
    // separate vector first: remove_client() erases from clients_, and
    // mutating an unordered_map while range-for iterating it is UB.
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

    for (const PendingReply& pending : pending_replies_) {
        if (now_ns < pending.fire_at_ns) {
            still_pending.push_back(pending);
            continue;
        }
        auto it = clients_.find(pending.fd);
        if (it != clients_.end()) {
            it->second.send_pong(pending.sequence, pending.timestamp_ns);
            sync_epoll_interest(pending.fd, it->second);  // the queued reply may need EPOLLOUT armed
        }
        // else: client disconnected before its deferred reply fired -- nothing to send to, just drop it.
    }
    pending_replies_ = std::move(still_pending);
}

void FeedSimulator::kill_random_connection() {
    if (clients_.empty()) {
        return;
    }
    std::uniform_int_distribution<size_t> pick(0, clients_.size() - 1);
    auto it = clients_.begin();
    std::advance(it, static_cast<long>(pick(rng_)));
    const int fd = it->first;

    // SO_LINGER{on=1, linger=0} makes the close() inside remove_client() send
    // a raw RST instead of a graceful FIN -- simulating the exchange
    // abruptly crashing rather than shutting down cleanly, so the monitor
    // exercises its ECONNRESET path (Connection::on_hangup_or_error)
    // specifically, not just the "peer closed" EOF path already covered by a
    // normal disconnect.
    struct linger sl{1, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));

    remove_client(fd);
}

void FeedSimulator::set_fault_config(FaultConfig faults) { faults_ = faults; }

void FeedSimulator::set_command_input(int fd, CommandLineCallback on_line) {
    command_fd_ = fd;
    on_command_line_ = std::move(on_line);
    epoll_event ev{};
    ev.events = epoll_interest_flags(true, false, config_.edge_triggered);
    ev.data.fd = fd;
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev);
}

void FeedSimulator::on_command_fd_readable() {
    char buf[1024];
    bool eof = false;
    while (true) {
        const ssize_t n = ::read(command_fd_, buf, sizeof(buf));
        if (n > 0) {
            command_line_buffer_.append(buf, static_cast<size_t>(n));
            if (command_line_buffer_.size() > kMaxCommandLineBufferBytes) {
                std::cerr << "[feed_simulator] command line exceeded " << kMaxCommandLineBufferBytes
                          << " bytes without a newline -- discarding buffered input\n";
                command_line_buffer_.clear();
            }
            continue;
        }
        if (n == 0) {
            // Do NOT return early here -- see the matching comment in
            // FeedMonitor::on_command_fd_readable (feed_monitor.cpp): with
            // piped/redirected input, EOF routinely arrives in the same
            // read() loop as the final real commands, and returning
            // immediately would silently drop whatever was just appended to
            // command_line_buffer_ above.
            eof = true;
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        break;
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
        if (!command_line_buffer_.empty() && on_command_line_) {
            on_command_line_(command_line_buffer_);
        }
        command_line_buffer_.clear();
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, command_fd_, nullptr);
        command_fd_ = -1;
    }
}
