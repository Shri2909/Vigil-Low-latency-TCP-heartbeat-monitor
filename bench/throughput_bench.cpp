// Epoll reactor scaling: time to bring N connections to HEALTHY, steady-state CPU
// and RSS, and the effect of epoll_max_events; optionally vs a naive
// thread-per-connection baseline.
// Run: ./bin/throughput_bench [--naive|--naive-only] [counts_csv [interval_ms [reps]]]
//
// MultiFeedEchoPeer answers any feed_id on one port, since FeedSimulator is one
// feed per instance and thousands of simulators would benchmark the scaffolding.

#include "bench_common.h"
#include "epoll_utils.h"
#include "feed_monitor.h"
#include "heartbeat.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

namespace {

class MultiFeedEchoPeer {
public:
    explicit MultiFeedEchoPeer(uint16_t port) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        const int reuse = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, SOMAXCONN);
        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &len);
        port_ = ntohs(bound.sin_port);

        epoll_fd_ = ::epoll_create1(0);
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

        wake_fd_ = ::eventfd(0, EFD_NONBLOCK);
        epoll_event wake_ev{};
        wake_ev.events = EPOLLIN;
        wake_ev.data.fd = wake_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_ev);
    }

    ~MultiFeedEchoPeer() {
        for (auto& [fd, unused] : clients_) {
            (void)unused;
            ::close(fd);
        }
        ::close(epoll_fd_);
        ::close(listen_fd_);
        ::close(wake_fd_);
    }

    uint16_t port() const { return port_; }

    void run() {
        std::vector<epoll_event> events(1024);
        while (!stop_.load(std::memory_order_relaxed)) {
            const int n = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
            if (n < 0) {
                if (errno == EINTR) continue;
                break;
            }
            for (int i = 0; i < n; ++i) {
                const int fd = events[i].data.fd;
                if (fd == wake_fd_) {
                    uint64_t ignored;
                    [[maybe_unused]] ssize_t discard = ::read(wake_fd_, &ignored, sizeof(ignored));
                    continue;
                }
                if (fd == listen_fd_) {
                    accept_loop();
                    continue;
                }
                handle_client(fd, events[i].events);
            }
        }
    }

    void stop() {
        stop_.store(true, std::memory_order_relaxed);
        const uint64_t one = 1;
        [[maybe_unused]] ssize_t discard = ::write(wake_fd_, &one, sizeof(one));
    }

private:
    void accept_loop() {
        while (true) {
            const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                break;
            }
            clients_[fd];  // creates the empty receive buffer
            epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.fd = fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
        }
    }

    void close_client(int fd) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        clients_.erase(fd);
    }

    void handle_client(int fd, uint32_t events) {
        if (events & (EPOLLHUP | EPOLLERR)) {
            close_client(fd);
            return;
        }
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        std::vector<uint8_t>& buf = it->second;

        uint8_t tmp[4096];
        while (true) {
            const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
            if (n > 0) {
                buf.insert(buf.end(), tmp, tmp + n);
                continue;
            }
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                close_client(fd);
                return;
            }
            if (n < 0 && errno == EINTR) continue;
            break;  // EAGAIN -- drained
        }

        size_t offset = 0;
        while (buf.size() - offset >= kHeartbeatMessageSize) {
            HeartbeatWireBuffer wire;
            std::copy_n(buf.begin() + static_cast<long>(offset), kHeartbeatMessageSize, wire.begin());
            offset += kHeartbeatMessageSize;

            HeartbeatMessage msg{};
            if (!decode_heartbeat(wire, &msg)) {
                close_client(fd);
                return;
            }

            HeartbeatMessage reply{};
            const auto type = static_cast<MessageType>(msg.type);
            if (type == MessageType::kConnectHello) {
                reply.type = static_cast<uint8_t>(MessageType::kConnectAck);
                reply.feed_id = msg.feed_id;
            } else if (type == MessageType::kPing) {
                reply.type = static_cast<uint8_t>(MessageType::kPong);
                reply.feed_id = msg.feed_id;
                reply.sequence = msg.sequence;
                reply.timestamp_ns = msg.timestamp_ns;
            } else {
                continue;  // only hello/ping are ever sent
            }
            HeartbeatWireBuffer out_wire;
            encode_heartbeat(reply, out_wire);
            // Best-effort send: a 28-byte reply never partial-writes at this scale.
            ::send(fd, out_wire.data(), out_wire.size(), MSG_NOSIGNAL);
        }
        if (offset > 0) {
            buf.erase(buf.begin(), buf.begin() + static_cast<long>(offset));
        }
    }

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    uint16_t port_ = 0;
    std::unordered_map<int, std::vector<uint8_t>> clients_;
    std::atomic<bool> stop_{false};
};

struct ThroughputResult {
    bool all_healthy = false;
    double setup_seconds = 0.0;
    double cpu_percent = 0.0;
    int64_t rss_bytes = 0;
    double epoll_wait_calls_per_sec = 0.0;
    double events_per_sec = 0.0;
};

// CPU-seconds used by one thread (per-thread clock, not process-wide rusage,
// which would include the echo peer). Returns 0.0 if the lookup fails.
double thread_cpu_seconds(pthread_t thread) {
    clockid_t clock_id;
    if (::pthread_getcpuclockid(thread, &clock_id) != 0) {
        return 0.0;
    }
    timespec ts{};
    if (::clock_gettime(clock_id, &ts) != 0) {
        return 0.0;
    }
    return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}

// Steady-state window, in heartbeat periods. Pings fire in near-lockstep
// bursts, so a short window can catch a varying number of bursts and swing
// CPU% and events/s by 2x; a whole number of periods bounds the error to +-1
// burst in kWindowPeriods (~10%). The settle skips the connect transient.
constexpr int kSettlePeriods = 1;
constexpr int kWindowPeriods = 10;

std::chrono::milliseconds window_for(std::chrono::milliseconds heartbeat_interval) {
    return std::max(heartbeat_interval * kWindowPeriods, std::chrono::milliseconds(2000));
}
std::chrono::milliseconds settle_for(std::chrono::milliseconds heartbeat_interval) {
    return heartbeat_interval * kSettlePeriods;
}

ThroughputResult run_throughput_trial(size_t connection_count, int epoll_max_events,
                                       std::chrono::milliseconds heartbeat_interval,
                                       std::chrono::milliseconds heartbeat_timeout) {
    MultiFeedEchoPeer peer(0);
    const uint16_t port = peer.port();
    // Spawned threads call reset_affinity_unpinned() so they never inherit an
    // RT policy or a narrowed mask (a no-op unless launched with chrt/pinning).
    std::thread peer_thread([&peer] {
        bench::reset_affinity_unpinned();
        peer.run();
    });

    Config config;
    config.heartbeat_interval = heartbeat_interval;
    config.heartbeat_timeout = heartbeat_timeout;
    config.epoll_max_events = epoll_max_events;

    FeedMonitor monitor(config);

    // Keep the callback trivial (one relaxed store) so it adds no measured cost.
    std::atomic<size_t> healthy_count{0};
    monitor.set_tick_callback(
        [&healthy_count](const AggregateStats& s) { healthy_count.store(s.healthy_count, std::memory_order_relaxed); });

    const int64_t setup_start = now_monotonic_ns();
    for (size_t i = 0; i < connection_count; ++i) {
        FeedEndpoint endpoint{static_cast<uint32_t>(i + 1), "F" + std::to_string(i + 1), "127.0.0.1", port};
        monitor.add_feed(endpoint);
    }

    std::thread monitor_thread([&monitor] {
        bench::reset_affinity_unpinned();
        monitor.run();
    });

    bool all_healthy = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        if (healthy_count.load(std::memory_order_relaxed) == connection_count) {
            all_healthy = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const int64_t setup_end = now_monotonic_ns();
    std::this_thread::sleep_for(settle_for(heartbeat_interval));  // skip the connect/handshake transient

    const double cpu_before = thread_cpu_seconds(monitor_thread.native_handle());
    const uint64_t epoll_wait_before = monitor.debug_epoll_wait_count();
    const uint64_t events_before = monitor.debug_events_processed_count();
    const int64_t window_start = now_monotonic_ns();
    std::this_thread::sleep_for(window_for(heartbeat_interval));  // steady-state measurement window
    const double cpu_after = thread_cpu_seconds(monitor_thread.native_handle());
    const uint64_t epoll_wait_after = monitor.debug_epoll_wait_count();
    const uint64_t events_after = monitor.debug_events_processed_count();
    const int64_t window_end = now_monotonic_ns();

    const double wall_seconds = static_cast<double>(window_end - window_start) / 1e9;
    const double cpu_delta = cpu_after - cpu_before;
    const double cpu_percent = wall_seconds > 0.0 ? (cpu_delta / wall_seconds) * 100.0 : 0.0;
    const int64_t rss = bench::current_rss_bytes();
    const double epoll_wait_calls_per_sec =
        wall_seconds > 0.0 ? static_cast<double>(epoll_wait_after - epoll_wait_before) / wall_seconds : 0.0;
    const double events_per_sec =
        wall_seconds > 0.0 ? static_cast<double>(events_after - events_before) / wall_seconds : 0.0;

    const size_t final_healthy = healthy_count.load(std::memory_order_relaxed);

    monitor.stop();
    monitor_thread.join();
    peer.stop();
    peer_thread.join();

    if (!all_healthy) {
        std::cerr << "[throughput_bench] warning: only " << final_healthy << "/" << connection_count
                   << " connections reached HEALTHY within the timeout\n";
    }

    ThroughputResult result;
    result.all_healthy = all_healthy && final_healthy == connection_count;
    result.setup_seconds = static_cast<double>(setup_end - setup_start) / 1e9;
    result.cpu_percent = cpu_percent;
    result.rss_bytes = rss;
    result.epoll_wait_calls_per_sec = epoll_wait_calls_per_sec;
    result.events_per_sec = events_per_sec;
    return result;
}

void print_setup_cpu_rss(const bench::Spread& setup, const bench::Spread& cpu, const bench::Spread& rss) {
    std::cout << "  setup:      min=" << setup.min << "s  median=" << setup.median << "s  max=" << setup.max << "s\n"
              << "  steady_cpu: min=" << cpu.min << "%  median=" << cpu.median << "%  max=" << cpu.max << "%\n"
              << "  rss:        min=" << (rss.min / (1024 * 1024)) << "MB  median=" << (rss.median / (1024 * 1024))
              << "MB  max=" << (rss.max / (1024 * 1024)) << "MB\n";
}

// Runs the trial `repetitions` times and prints min/median/max per metric
// (format parsed by scripts/generate_key_graphs.py). Returns false if any
// repetition did not bring every connection to HEALTHY.
bool run_and_print_repeated(size_t connection_count, int epoll_max_events,
                             std::chrono::milliseconds heartbeat_interval,
                             std::chrono::milliseconds heartbeat_timeout, size_t repetitions) {
    const auto results = bench::repeat_trials(
        [&]() { return run_throughput_trial(connection_count, epoll_max_events, heartbeat_interval, heartbeat_timeout); },
        repetitions);

    const bool all_healthy = std::all_of(results.begin(), results.end(), [](const ThroughputResult& r) { return r.all_healthy; });
    const auto setup = bench::summarize_spread(results, [](const ThroughputResult& r) { return r.setup_seconds; });
    const auto cpu = bench::summarize_spread(results, [](const ThroughputResult& r) { return r.cpu_percent; });
    const auto rss = bench::summarize_spread(results, [](const ThroughputResult& r) { return static_cast<double>(r.rss_bytes); });
    const auto waits = bench::summarize_spread(results, [](const ThroughputResult& r) { return r.epoll_wait_calls_per_sec; });
    const auto events = bench::summarize_spread(results, [](const ThroughputResult& r) { return r.events_per_sec; });

    std::cout << "N=" << connection_count << "  epoll_max_events=" << epoll_max_events
              << "  hb_interval=" << heartbeat_interval.count() << "ms"
              << "  (" << repetitions << " repetitions)" << (all_healthy ? "" : "  [INCOMPLETE on some repetition]") << "\n"
              << "  epoll_wait/s: min=" << waits.min << "  median=" << waits.median << "  max=" << waits.max << "\n"
              << "  events/s:     min=" << events.min << "  median=" << events.median << "  max=" << events.max << "\n";
    print_setup_cpu_rss(setup, cpu, rss);
    return all_healthy;
}

void raise_fd_limit() {
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) return;
    if (limit.rlim_cur < limit.rlim_max) {
        rlimit raised{limit.rlim_max, limit.rlim_max};
        ::setrlimit(RLIMIT_NOFILE, &raised);
    }
}

// --- Naive baseline: one blocking thread per connection (connect, handshake,
// then a send-PING / recv-PONG loop paced by heartbeat_interval). ---

struct NaiveWorkerState {
    std::atomic<bool> connected{false};
};

// Sleeps up to `interval` in one poll(), returning true early if stop_fd is
// readable. The shutdown eventfd is written once and never drained, so it wakes
// every sleeping worker. One wakeup per period per thread keeps the baseline fair.
bool sleep_or_stop(int stop_fd, std::chrono::milliseconds interval) {
    pollfd p{};
    p.fd = stop_fd;
    p.events = POLLIN;
    const auto deadline = std::chrono::steady_clock::now() + interval;
    while (true) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) {
            return false;
        }
        const int rc = ::poll(&p, 1, static_cast<int>(remaining.count()));
        if (rc > 0) {
            return true;
        }
        if (rc < 0 && errno != EINTR) {
            return true;  // unexpected poll failure: treat as stop rather than spin
        }
    }
}

void naive_worker(uint16_t port, std::chrono::milliseconds heartbeat_interval, std::atomic<bool>& stop_flag,
                   int stop_fd, NaiveWorkerState& state) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);  // blocking socket, no SOCK_NONBLOCK
    if (fd < 0) {
        return;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return;
    }

    // Bound blocking recv() so the worker still notices stop_flag if the peer goes quiet.
    timeval tv{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    HeartbeatMessage hello{};
    hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
    hello.feed_id = 1;
    HeartbeatWireBuffer hello_wire;
    encode_heartbeat(hello, hello_wire);
    if (::send(fd, hello_wire.data(), hello_wire.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(hello_wire.size())) {
        ::close(fd);
        return;
    }

    HeartbeatWireBuffer ack_wire;
    size_t got = 0;
    while (got < ack_wire.size()) {
        const ssize_t n = ::recv(fd, ack_wire.data() + got, ack_wire.size() - got, 0);
        if (n <= 0) {
            ::close(fd);
            return;
        }
        got += static_cast<size_t>(n);
    }
    HeartbeatMessage ack{};
    if (!decode_heartbeat(ack_wire, &ack) || static_cast<MessageType>(ack.type) != MessageType::kConnectAck) {
        ::close(fd);
        return;
    }
    state.connected.store(true, std::memory_order_relaxed);

    uint64_t seq = 0;
    while (!stop_flag.load(std::memory_order_relaxed)) {
        HeartbeatMessage ping{};
        ping.type = static_cast<uint8_t>(MessageType::kPing);
        ping.feed_id = 1;
        ping.sequence = seq++;
        ping.timestamp_ns = now_monotonic_ns();
        HeartbeatWireBuffer ping_wire;
        encode_heartbeat(ping, ping_wire);
        if (::send(fd, ping_wire.data(), ping_wire.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(ping_wire.size())) {
            break;
        }

        HeartbeatWireBuffer pong_wire;
        size_t pong_got = 0;
        bool ok = true;
        while (pong_got < pong_wire.size()) {
            const ssize_t n = ::recv(fd, pong_wire.data() + pong_got, pong_wire.size() - pong_got, 0);
            if (n <= 0) {
                ok = false;
                break;
            }
            pong_got += static_cast<size_t>(n);
        }
        if (!ok) {
            break;
        }

        if (sleep_or_stop(stop_fd, heartbeat_interval)) {
            break;
        }
    }
    ::close(fd);
}

struct NaiveThroughputResult {
    bool all_connected = false;
    double setup_seconds = 0.0;
    double cpu_percent = 0.0;
    int64_t rss_bytes = 0;
};

NaiveThroughputResult run_naive_throughput_trial(size_t connection_count, std::chrono::milliseconds heartbeat_interval) {
    MultiFeedEchoPeer peer(0);
    const uint16_t port = peer.port();
    // Peer and workers reset affinity (see run_throughput_trial).
    std::thread peer_thread([&peer] {
        bench::reset_affinity_unpinned();
        peer.run();
    });

    // unique_ptr: workers hold references into `states`, which must not move.
    std::vector<std::unique_ptr<NaiveWorkerState>> states;
    std::vector<std::thread> workers;
    states.reserve(connection_count);
    workers.reserve(connection_count);
    std::atomic<bool> stop_flag{false};
    const int stop_fd = ::eventfd(0, EFD_NONBLOCK);

    const int64_t setup_start = now_monotonic_ns();
    for (size_t i = 0; i < connection_count; ++i) {
        states.push_back(std::make_unique<NaiveWorkerState>());
        NaiveWorkerState& state = *states.back();
        workers.emplace_back([port, heartbeat_interval, &stop_flag, stop_fd, &state] {
            bench::reset_affinity_unpinned();
            naive_worker(port, heartbeat_interval, stop_flag, stop_fd, state);
        });
    }

    const auto count_connected = [&states] {
        return static_cast<size_t>(std::count_if(
            states.begin(), states.end(),
            [](const std::unique_ptr<NaiveWorkerState>& s) { return s->connected.load(std::memory_order_relaxed); }));
    };

    bool all_connected = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        if (count_connected() == connection_count) {
            all_connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const int64_t setup_end = now_monotonic_ns();
    std::this_thread::sleep_for(settle_for(heartbeat_interval));  // same settle as the epoll trial

    // Sum per-thread CPU over the N workers (excludes the echo peer).
    double cpu_before = 0.0;
    for (auto& w : workers) {
        cpu_before += thread_cpu_seconds(w.native_handle());
    }
    const int64_t window_start = now_monotonic_ns();
    std::this_thread::sleep_for(window_for(heartbeat_interval));  // same window as the epoll trial
    double cpu_after = 0.0;
    for (auto& w : workers) {
        cpu_after += thread_cpu_seconds(w.native_handle());
    }
    const int64_t window_end = now_monotonic_ns();

    const double wall_seconds = static_cast<double>(window_end - window_start) / 1e9;
    const double cpu_percent = wall_seconds > 0.0 ? ((cpu_after - cpu_before) / wall_seconds) * 100.0 : 0.0;
    const int64_t rss = bench::current_rss_bytes();

    stop_flag.store(true, std::memory_order_relaxed);
    const uint64_t one = 1;
    [[maybe_unused]] ssize_t wake_rc = ::write(stop_fd, &one, sizeof(one));
    for (auto& w : workers) {
        if (w.joinable()) {
            w.join();
        }
    }
    ::close(stop_fd);
    peer.stop();
    peer_thread.join();

    const size_t final_connected = count_connected();
    if (!all_connected) {
        std::cerr << "[throughput_bench] warning: naive baseline only got " << final_connected << "/" << connection_count
                   << " connections established within the timeout\n";
    }

    NaiveThroughputResult result;
    result.all_connected = all_connected && final_connected == connection_count;
    result.setup_seconds = static_cast<double>(setup_end - setup_start) / 1e9;
    result.cpu_percent = cpu_percent;
    result.rss_bytes = rss;
    return result;
}

bool run_and_print_naive_repeated(size_t connection_count, std::chrono::milliseconds heartbeat_interval,
                                   size_t repetitions) {
    const auto results =
        bench::repeat_trials([&]() { return run_naive_throughput_trial(connection_count, heartbeat_interval); }, repetitions);

    const bool all_connected =
        std::all_of(results.begin(), results.end(), [](const NaiveThroughputResult& r) { return r.all_connected; });
    const auto setup = bench::summarize_spread(results, [](const NaiveThroughputResult& r) { return r.setup_seconds; });
    const auto cpu = bench::summarize_spread(results, [](const NaiveThroughputResult& r) { return r.cpu_percent; });
    const auto rss = bench::summarize_spread(results, [](const NaiveThroughputResult& r) { return static_cast<double>(r.rss_bytes); });

    std::cout << "N=" << connection_count << "  (1 thread/connection)  hb_interval=" << heartbeat_interval.count()
              << "ms  (" << repetitions << " repetitions)" << (all_connected ? "" : "  [INCOMPLETE on some repetition]")
              << "\n";
    print_setup_cpu_rss(setup, cpu, rss);
    return all_connected;
}

}  // namespace

int main(int argc, char** argv) {
    raise_fd_limit();
    // No bench::try_pin_and_prioritize(): reactor, peer and naive workers must
    // run on all permitted CPUs under the normal scheduler.

    // Flags may appear anywhere; strip them so the positionals keep their meaning.
    // --naive-only exists for RSS: glibc keeps the epoll section's high-water
    // heap, which would inflate a naive reading in the same process.
    bool naive_mode = false;
    bool naive_only = false;
    std::vector<std::string> positional;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--naive") {
            naive_mode = true;
        } else if (arg == "--naive-only") {
            naive_mode = true;
            naive_only = true;
        } else {
            positional.emplace_back(argv[i]);
        }
    }

    const auto usage = [&] {
        std::cerr << "usage: " << argv[0]
                  << " [--naive|--naive-only] [counts_csv [heartbeat_interval_ms [repetitions]]]  (all positive integers)\n";
        return 2;
    };
    if (positional.size() > 3) return usage();

    std::vector<size_t> counts = {10, 100, 500, 1000};
    if (!positional.empty()) {
        counts.clear();
        const std::string& arg = positional[0];
        size_t pos = 0;
        while (pos <= arg.size()) {
            const size_t comma = arg.find(',', pos);
            const std::string token = arg.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
            size_t n = 0;
            if (!bench::parse_positive_size(token.c_str(), n)) {
                std::cerr << "invalid connection count '" << token << "'\n";
                return usage();
            }
            counts.push_back(n);
            if (comma == std::string::npos) break;
            pos = comma + 1;
        }
    }

    // Default to the shipped config (1000ms interval, 3x timeout) so CPU% is
    // what a default deployment would see.
    std::chrono::milliseconds heartbeat_interval{1000};
    bool interval_overridden = false;
    if (positional.size() > 1) {
        size_t ms = 0;
        if (!bench::parse_positive_size(positional[1].c_str(), ms) || ms > 3'600'000) return usage();
        interval_overridden = static_cast<int64_t>(ms) != heartbeat_interval.count();
        heartbeat_interval = std::chrono::milliseconds(static_cast<int64_t>(ms));
    }
    const std::chrono::milliseconds heartbeat_timeout = heartbeat_interval * 3;

    size_t repetitions = 3;
    if (positional.size() > 2 && !bench::parse_positive_size(positional[2].c_str(), repetitions)) return usage();

    std::cout << "Low-Latency TCP Heartbeat Monitor -- throughput_bench\n"
              << "heartbeat_interval=" << heartbeat_interval.count()
              << "ms heartbeat_timeout=" << heartbeat_timeout.count() << "ms ("
              << (interval_overridden ? "overridden via CLI" : "shipped default, see config.h") << ")  repetitions="
              << repetitions << "\n"
              << "steady_window=" << window_for(heartbeat_interval).count() << "ms ("
              << (window_for(heartbeat_interval) / heartbeat_interval) << " heartbeat periods, after a "
              << settle_for(heartbeat_interval).count() << "ms settle)\n\n";

    bool all_complete = true;
    if (!naive_only) {
        std::cout << "=== connection count ramp (epoll_max_events=256) ===\n";
        for (const size_t n : counts) {
            all_complete &= run_and_print_repeated(n, 256, heartbeat_interval, heartbeat_timeout, repetitions);
        }

        std::cout << "\n=== epoll_max_events batch size (N=" << counts.back() << ") ===\n";
        for (const int batch : {16, 64, 256, 1024}) {
            all_complete &= run_and_print_repeated(counts.back(), batch, heartbeat_interval, heartbeat_timeout, repetitions);
        }
    }

    if (naive_mode) {
        // Same counts and cadence as the epoll section, for a line-by-line comparison.
        std::cout << "\n=== naive blocking-thread-per-connection baseline (same N, same cadence) ===\n";
        for (const size_t n : counts) {
            all_complete &= run_and_print_naive_repeated(n, heartbeat_interval, repetitions);
        }
    }

    if (!all_complete) {
        std::cerr << "[throughput_bench] FAILED: at least one trial did not bring every connection to HEALTHY/connected.\n";
        return 1;
    }
    return 0;
}
