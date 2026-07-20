// Measures how FeedMonitor's epoll reactor scales with connection count:
// time to bring N connections to HEALTHY, steady-state CPU and RSS, and the
// effect of epoll_max_events batch size. See PROJECT_PLAN.md section 12.
//
// Needs a peer that can answer many distinct feed_ids on one port.
// FeedSimulator (src/feed_simulator.h) is intentionally one feed per
// instance -- each simulator represents one exchange -- so ramping to
// thousands of distinct feed_ids would mean thousands of FeedSimulator
// instances (thousands of OS threads/listening sockets), which would measure
// thread-scheduling overhead from the *benchmark's own scaffolding*, not
// FeedMonitor. Instead, MultiFeedEchoPeer below answers on one port for any
// feed_id, using heartbeat.h's encode/decode directly rather than the full
// Connection class -- Connection's feed_id must be fixed at construction,
// before the first byte (which carries the feed_id) has even been read, so
// it structurally can't serve this role.

#include "bench_common.h"
#include "epoll_utils.h"
#include "feed_monitor.h"
#include "heartbeat.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
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
            clients_[fd];
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
                continue;  // this echo peer only answers hello/ping, benchmark never sends anything else
            }
            HeartbeatWireBuffer out_wire;
            encode_heartbeat(reply, out_wire);
            // Best-effort single send: a 28-byte reply on a freshly-accepted
            // socket's send buffer essentially never partial-writes at this
            // scale, and this peer exists to generate load, not to
            // demonstrate partial-write handling (Connection already has
            // dedicated coverage for that in connection_test.cpp).
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
    size_t connection_count = 0;
    int epoll_max_events = 0;
    int64_t heartbeat_interval_ms = 0;
    bool all_healthy = false;
    double setup_seconds = 0.0;
    double cpu_percent = 0.0;
    int64_t rss_bytes = 0;
    double epoll_wait_calls_per_sec = 0.0;
    double events_per_sec = 0.0;
};

// CPU-seconds consumed by one specific thread so far, via its own
// CPU-time clockid -- not process-wide getrusage(RUSAGE_SELF, ...), which
// can't separate FeedMonitor's reactor thread from MultiFeedEchoPeer's
// thread (both live in this benchmark's own process) and so overstated
// every reported steady_cpu% by however much the echo peer itself cost
// (PROJECT_PLAN.md F8). Returns 0.0 if the clockid lookup fails (e.g. the
// thread has already exited), which is treated as "no CPU time observed"
// rather than a fatal error since this is benchmark instrumentation, not
// correctness-critical code.
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

ThroughputResult run_throughput_trial(size_t connection_count, int epoll_max_events,
                                       std::chrono::milliseconds heartbeat_interval,
                                       std::chrono::milliseconds heartbeat_timeout) {
    MultiFeedEchoPeer peer(0);
    const uint16_t port = peer.port();
    // Both threads below would otherwise silently inherit main()'s
    // single-CPU pin (see bench::reset_affinity_unpinned's doc comment),
    // forcing the peer and the monitor under test onto the same core and
    // adding contention noise to exactly what's being measured.
    std::thread peer_thread([&peer] {
        bench::reset_affinity_unpinned();
        peer.run();
    });

    Config config;
    config.heartbeat_interval = heartbeat_interval;
    config.heartbeat_timeout = heartbeat_timeout;
    config.epoll_max_events = epoll_max_events;

    FeedMonitor monitor(config);

    std::mutex mu;
    AggregateStats last_snapshot;
    monitor.set_tick_callback([&](const AggregateStats& s) {
        std::lock_guard<std::mutex> lock(mu);
        last_snapshot = s;
    });

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
        {
            std::lock_guard<std::mutex> lock(mu);
            if (last_snapshot.healthy_count == connection_count) {
                all_healthy = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const int64_t setup_end = now_monotonic_ns();

    const double cpu_before = thread_cpu_seconds(monitor_thread.native_handle());
    const uint64_t epoll_wait_before = monitor.debug_epoll_wait_count();
    const uint64_t events_before = monitor.debug_events_processed_count();
    const int64_t window_start = now_monotonic_ns();
    std::this_thread::sleep_for(std::chrono::seconds(2));  // steady-state measurement window
    const double cpu_after = thread_cpu_seconds(monitor_thread.native_handle());
    const uint64_t epoll_wait_after = monitor.debug_epoll_wait_count();
    const uint64_t events_after = monitor.debug_events_processed_count();
    const int64_t window_end = now_monotonic_ns();

    const double wall_seconds = static_cast<double>(window_end - window_start) / 1e9;
    const double cpu_delta = cpu_after - cpu_before;
    const double cpu_percent = wall_seconds > 0.0 ? (cpu_delta / wall_seconds) * 100.0 : 0.0;
    const int64_t rss = bench::current_rss_bytes();
    // PROJECT_PLAN.md section 12 promised this pair of metrics from the
    // start; throughput_bench never actually measured them until this pass.
    const double epoll_wait_calls_per_sec =
        wall_seconds > 0.0 ? static_cast<double>(epoll_wait_after - epoll_wait_before) / wall_seconds : 0.0;
    const double events_per_sec =
        wall_seconds > 0.0 ? static_cast<double>(events_after - events_before) / wall_seconds : 0.0;

    size_t final_healthy;
    {
        std::lock_guard<std::mutex> lock(mu);
        final_healthy = last_snapshot.healthy_count;
    }

    monitor.stop();
    monitor_thread.join();
    peer.stop();
    peer_thread.join();

    if (!all_healthy) {
        std::cerr << "[throughput_bench] warning: only " << final_healthy << "/" << connection_count
                   << " connections reached HEALTHY within the timeout\n";
    }

    ThroughputResult result;
    result.connection_count = connection_count;
    result.epoll_max_events = epoll_max_events;
    result.heartbeat_interval_ms = heartbeat_interval.count();
    result.all_healthy = all_healthy;
    result.setup_seconds = static_cast<double>(setup_end - setup_start) / 1e9;
    result.cpu_percent = cpu_percent;
    result.rss_bytes = rss;
    result.epoll_wait_calls_per_sec = epoll_wait_calls_per_sec;
    result.events_per_sec = events_per_sec;
    return result;
}

// Runs run_throughput_trial `repetitions` times for one (connection_count,
// epoll_max_events) pair and prints the min/median/max spread across those
// repetitions for the metrics that vary run to run (setup time, steady-state
// CPU%, RSS) -- neither benchmark repeated a configuration before this audit
// pass, so every number was a single-sample point estimate with no reported
// variance (PROJECT_PLAN.md F10).
void run_and_print_repeated(size_t connection_count, int epoll_max_events,
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
              << "  events/s:     min=" << events.min << "  median=" << events.median << "  max=" << events.max << "\n"
              << "  setup:      min=" << setup.min << "s  median=" << setup.median << "s  max=" << setup.max << "s\n"
              << "  steady_cpu: min=" << cpu.min << "%  median=" << cpu.median << "%  max=" << cpu.max << "%\n"
              << "  rss:        min=" << (rss.min / (1024 * 1024)) << "MB  median=" << (rss.median / (1024 * 1024))
              << "MB  max=" << (rss.max / (1024 * 1024)) << "MB\n";
}

void raise_fd_limit() {
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) return;
    if (limit.rlim_cur < limit.rlim_max) {
        rlimit raised{limit.rlim_max, limit.rlim_max};
        ::setrlimit(RLIMIT_NOFILE, &raised);
    }
}

// ---------------------------------------------------------------------------
// Naive blocking-thread-per-connection baseline (PROJECT_PLAN.md section 12
// asked for an epoll-vs-blocking-I/O comparison; it was scoped out at build
// time as "real work, out of proportion to what's left" -- built here since
// it's now specifically what's being asked for). One real OS thread per
// connection, each doing a blocking connect() + blocking handshake + a
// blocking send-PING/recv-PONG loop paced by heartbeat_interval. This is
// deliberately the simplest possible correct implementation of "watch N
// feeds without an event loop" -- exactly the naive baseline the comparison
// needs to be meaningful, not a tuned alternative architecture.
// ---------------------------------------------------------------------------

struct NaiveWorkerState {
    std::atomic<bool> connected{false};
};

void naive_worker(uint16_t port, std::chrono::milliseconds heartbeat_interval, std::atomic<bool>& stop_flag,
                   NaiveWorkerState& state) {
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

    // Bounds every blocking recv() so this thread still notices stop_flag
    // rather than blocking forever if the peer never replies -- a benchmark
    // worker concern, not something production code needs (Connection is
    // non-blocking and never calls recv() without epoll saying it's ready).
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

        // Sleep in short slices so stop_flag is checked responsively instead
        // of blocking the full interval in one call.
        auto remaining = heartbeat_interval;
        while (remaining.count() > 0 && !stop_flag.load(std::memory_order_relaxed)) {
            const auto slice = std::min(remaining, std::chrono::milliseconds(50));
            std::this_thread::sleep_for(slice);
            remaining -= slice;
        }
    }
    ::close(fd);
}

struct NaiveThroughputResult {
    size_t connection_count = 0;
    bool all_connected = false;
    double setup_seconds = 0.0;
    double cpu_percent = 0.0;
    int64_t rss_bytes = 0;
};

NaiveThroughputResult run_naive_throughput_trial(size_t connection_count, std::chrono::milliseconds heartbeat_interval) {
    MultiFeedEchoPeer peer(0);
    const uint16_t port = peer.port();
    // This thread, and every worker thread spawned below, would otherwise
    // silently inherit main()'s single-CPU pin (see
    // bench::reset_affinity_unpinned's doc comment) -- forcing potentially
    // hundreds of worker threads plus the echo peer onto one core, which
    // would partly measure thread-contention-on-one-core rather than the
    // naive thread-per-connection architecture's real cost. A real
    // deployment of this naive approach would use all available cores.
    std::thread peer_thread([&peer] {
        bench::reset_affinity_unpinned();
        peer.run();
    });

    // unique_ptr so pointers into `states` stay valid even if the vector
    // reallocates as more connections are pushed -- the naive_worker
    // threads capture their NaiveWorkerState by reference.
    std::vector<std::unique_ptr<NaiveWorkerState>> states;
    std::vector<std::thread> workers;
    states.reserve(connection_count);
    workers.reserve(connection_count);
    std::atomic<bool> stop_flag{false};

    const int64_t setup_start = now_monotonic_ns();
    for (size_t i = 0; i < connection_count; ++i) {
        states.push_back(std::make_unique<NaiveWorkerState>());
        NaiveWorkerState& state = *states.back();
        workers.emplace_back([port, heartbeat_interval, &stop_flag, &state] {
            bench::reset_affinity_unpinned();
            naive_worker(port, heartbeat_interval, stop_flag, state);
        });
    }

    bool all_connected = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        const size_t connected = static_cast<size_t>(std::count_if(
            states.begin(), states.end(),
            [](const std::unique_ptr<NaiveWorkerState>& s) { return s->connected.load(std::memory_order_relaxed); }));
        if (connected == connection_count) {
            all_connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const int64_t setup_end = now_monotonic_ns();

    // Sum per-thread CPU time across all N worker threads, not process-wide
    // getrusage(RUSAGE_SELF, ...) -- same F8 reasoning as the epoll-based
    // trial: the echo peer's own thread lives in this same process and must
    // not be counted as the naive implementation's cost.
    double cpu_before = 0.0;
    for (auto& w : workers) {
        cpu_before += thread_cpu_seconds(w.native_handle());
    }
    const int64_t window_start = now_monotonic_ns();
    std::this_thread::sleep_for(std::chrono::seconds(2));  // steady-state measurement window
    double cpu_after = 0.0;
    for (auto& w : workers) {
        cpu_after += thread_cpu_seconds(w.native_handle());
    }
    const int64_t window_end = now_monotonic_ns();

    const double wall_seconds = static_cast<double>(window_end - window_start) / 1e9;
    const double cpu_percent = wall_seconds > 0.0 ? ((cpu_after - cpu_before) / wall_seconds) * 100.0 : 0.0;
    const int64_t rss = bench::current_rss_bytes();

    stop_flag.store(true, std::memory_order_relaxed);
    for (auto& w : workers) {
        if (w.joinable()) {
            w.join();
        }
    }
    peer.stop();
    peer_thread.join();

    if (!all_connected) {
        const size_t final_connected = static_cast<size_t>(std::count_if(
            states.begin(), states.end(),
            [](const std::unique_ptr<NaiveWorkerState>& s) { return s->connected.load(std::memory_order_relaxed); }));
        std::cerr << "[throughput_bench] warning: naive baseline only got " << final_connected << "/" << connection_count
                   << " connections established within the timeout\n";
    }

    NaiveThroughputResult result;
    result.connection_count = connection_count;
    result.all_connected = all_connected;
    result.setup_seconds = static_cast<double>(setup_end - setup_start) / 1e9;
    result.cpu_percent = cpu_percent;
    result.rss_bytes = rss;
    return result;
}

void run_and_print_naive_repeated(size_t connection_count, std::chrono::milliseconds heartbeat_interval,
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
              << "\n"
              << "  setup:      min=" << setup.min << "s  median=" << setup.median << "s  max=" << setup.max << "s\n"
              << "  steady_cpu: min=" << cpu.min << "%  median=" << cpu.median << "%  max=" << cpu.max << "%\n"
              << "  rss:        min=" << (rss.min / (1024 * 1024)) << "MB  median=" << (rss.median / (1024 * 1024))
              << "MB  max=" << (rss.max / (1024 * 1024)) << "MB\n";
}

}  // namespace

int main(int argc, char** argv) {
    raise_fd_limit();
    bench::try_pin_and_prioritize(0);

    // --naive/--naive-only can appear anywhere on the command line; strip
    // them out first so the remaining positional args (counts,
    // heartbeat_interval, repetitions) keep their usual meaning regardless
    // of where the flags were placed.
    //
    // --naive-only exists specifically so RSS numbers are trustworthy: RSS
    // is an absolute snapshot (unlike CPU%, which is a before/after delta
    // within its own window and so immune to this), and glibc's allocator
    // doesn't return freed memory to the OS after a large trial finishes --
    // running --naive in the same process as the epoll section left every
    // naive-section RSS reading inflated by the epoll section's high-water
    // mark (confirmed: N=10 read 72MB combined vs. 4.7MB run standalone).
    // --naive-only skips the epoll section entirely so the naive baseline's
    // RSS reflects only its own allocations.
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

    std::vector<size_t> counts = {10, 100, 500, 1000};
    if (!positional.empty()) {
        counts.clear();
        const std::string& arg = positional[0];
        size_t pos = 0;
        while (pos < arg.size()) {
            const size_t comma = arg.find(',', pos);
            counts.push_back(static_cast<size_t>(std::stoul(arg.substr(pos, comma - pos))));
            if (comma == std::string::npos) break;
            pos = comma + 1;
        }
    }

    // Defaults to config.h's own shipped defaults (1000ms interval, 3000ms
    // timeout) rather than an ad-hoc 5x-more-aggressive cadence: the old
    // 200ms/2000ms trial config made every reported CPU% roughly 5x what a
    // default-config deployment would show at the same connection count,
    // with nothing in the output saying so (PROJECT_PLAN.md F9). Overridable
    // via the second positional arg for deliberately exploring other cadences.
    std::chrono::milliseconds heartbeat_interval{1000};
    const bool interval_overridden = positional.size() > 1;
    if (interval_overridden) {
        heartbeat_interval = std::chrono::milliseconds(std::atoll(positional[1].c_str()));
    }
    const std::chrono::milliseconds heartbeat_timeout = heartbeat_interval * 3;

    size_t repetitions = 3;
    if (positional.size() > 2) {
        repetitions = static_cast<size_t>(std::atoll(positional[2].c_str()));
    }

    std::cout << "Low-Latency TCP Heartbeat Monitor -- throughput_bench\n"
              << "heartbeat_interval=" << heartbeat_interval.count()
              << "ms heartbeat_timeout=" << heartbeat_timeout.count() << "ms ("
              << (interval_overridden ? "overridden via CLI" : "shipped default, see config.h") << ")  repetitions="
              << repetitions << "\n\n";

    if (!naive_only) {
        std::cout << "=== connection count ramp (epoll_max_events=256) ===\n";
        for (const size_t n : counts) {
            run_and_print_repeated(n, 256, heartbeat_interval, heartbeat_timeout, repetitions);
        }

        std::cout << "\n=== epoll_max_events batch size (N=" << counts.back() << ") ===\n";
        for (const int batch : {16, 64, 256, 1024}) {
            run_and_print_repeated(counts.back(), batch, heartbeat_interval, heartbeat_timeout, repetitions);
        }
    }

    if (naive_mode) {
        // PROJECT_PLAN.md section 12's other explicit ask: the epoll vs.
        // naive-blocking-I/O comparison the original spec called for, which
        // had been scoped out at build time. Reuses the same connection
        // counts and cadence as the epoll run above, so the two sections are
        // directly comparable line by line.
        std::cout << "\n=== naive blocking-thread-per-connection baseline (same N, same cadence) ===\n";
        for (const size_t n : counts) {
            run_and_print_naive_repeated(n, heartbeat_interval, repetitions);
        }
    }

    return 0;
}
