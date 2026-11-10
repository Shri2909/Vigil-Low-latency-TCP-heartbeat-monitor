// Kernel-floor baseline for latency_bench: the same 28-byte ping/pong over loopback TCP with
// bare send/epoll_wait/recv on both ends -- no framing, CRC, validation or state machine.
// Run: ./bin/raw_socket_baseline [measured_iters [warmup_iters [repetitions]]]
//
// Mirrors latency_bench: same message size, tcp_nodelay=true, back-to-back closed loop, client
// pinned like latency_bench's, echo thread unpinned, RTT = (clock after epoll_wait returns) -
// (clock just before send). The gap between the two benchmarks is what Vigil's protocol layer
// (encode, CRC32, decode, PONG validation, stats) plus the harness's epoll_ctl(MOD) adds.

#include "bench_common.h"
#include "heartbeat.h"
#include "time_utils.h"

#include <cerrno>
#include <fcntl.h>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr size_t kMessageSize = kHeartbeatMessageSize;

void set_nonblocking_nodelay(int fd) {
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

// Reads until EAGAIN; returns bytes read, or -1 on EOF/error.
ssize_t drain(int fd, uint8_t* buf, size_t cap) {
    ssize_t total = 0;
    while (static_cast<size_t>(total) < cap) {
        const ssize_t n = ::recv(fd, buf + total, cap - static_cast<size_t>(total), 0);
        if (n > 0) {
            total += n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        return -1;  // EOF or a real error
    }
    return total;
}

// Echoes every byte it receives until `stop_fd` fires. Edge-triggered with drain-to-EAGAIN,
// like FeedSimulator's default.
void echo_server(int listen_fd, int stop_fd) {
    bench::reset_affinity_unpinned();
    const int conn_fd = ::accept(listen_fd, nullptr, nullptr);
    if (conn_fd < 0) return;
    set_nonblocking_nodelay(conn_fd);

    const int epoll_fd = ::epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = conn_fd;
    ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev);
    ev.events = EPOLLIN;
    ev.data.fd = stop_fd;
    ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, stop_fd, &ev);

    uint8_t buf[4096];
    epoll_event events[4];
    bool running = true;
    while (running) {
        const int n = ::epoll_wait(epoll_fd, events, 4, -1);
        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == stop_fd) {
                running = false;
                break;
            }
            const ssize_t got = drain(conn_fd, buf, sizeof(buf));
            if (got < 0) {
                running = false;
                break;
            }
            if (got > 0) ::send(conn_fd, buf, static_cast<size_t>(got), MSG_NOSIGNAL);
        }
    }
    ::close(epoll_fd);
    ::close(conn_fd);
}

struct TrialResult {
    bool ok = false;
    std::vector<int64_t> samples;
};

TrialResult run_trial(size_t warmup_iters, size_t measured_iters) {
    const int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (listen_fd < 0 || ::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(listen_fd, 1) != 0) {
        std::cerr << "[raw_socket_baseline] could not set up the listener\n";
        if (listen_fd >= 0) ::close(listen_fd);
        return {};
    }
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &len);

    const int stop_fd = ::eventfd(0, EFD_NONBLOCK);
    std::thread server([&] { echo_server(listen_fd, stop_fd); });

    const int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    TrialResult result;
    if (client_fd < 0 || ::connect(client_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[raw_socket_baseline] connect failed\n";
    } else {
        set_nonblocking_nodelay(client_fd);
        const int epoll_fd = ::epoll_create1(0);
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

        uint8_t out[kMessageSize] = {};
        uint8_t in[kMessageSize];
        result.samples.reserve(measured_iters);
        const size_t total = warmup_iters + measured_iters;
        size_t completed = 0;
        epoll_event events[2];
        for (; completed < total; ++completed) {
            const int64_t t0 = now_monotonic_ns();
            if (::send(client_fd, out, sizeof(out), MSG_NOSIGNAL) != static_cast<ssize_t>(sizeof(out))) break;
            int64_t t1 = 0;
            size_t received = 0;
            while (received < kMessageSize) {
                if (::epoll_wait(epoll_fd, events, 2, 5000) <= 0) break;
                if (received == 0) t1 = now_monotonic_ns();  // same clock point as latency_bench
                const ssize_t got = drain(client_fd, in, kMessageSize - received);
                if (got < 0) break;
                received += static_cast<size_t>(got);
            }
            if (received < kMessageSize) break;
            if (completed >= warmup_iters) result.samples.push_back(t1 - t0);
        }
        ::close(epoll_fd);
        result.ok = (completed == total);
        if (!result.ok) {
            std::cerr << "[raw_socket_baseline] trial incomplete (" << completed << "/" << total << ") -- FAILED\n";
        }
    }

    const uint64_t one = 1;
    [[maybe_unused]] const ssize_t rc = ::write(stop_fd, &one, sizeof(one));
    ::shutdown(client_fd, SHUT_RDWR);
    server.join();
    ::close(client_fd);
    ::close(stop_fd);
    ::close(listen_fd);
    return result;
}

}  // namespace

int main(int argc, char** argv) {
    size_t warmup = 500;
    size_t measured = 20000;
    size_t repetitions = 5;
    const auto usage = [&] {
        std::cerr << "usage: " << argv[0] << " [measured_iters [warmup_iters [repetitions]]]  (all positive integers)\n";
        return 2;
    };
    if (argc > 4) return usage();
    if (argc > 1 && !bench::parse_positive_size(argv[1], measured)) return usage();
    if (argc > 2 && !bench::parse_positive_size(argv[2], warmup)) return usage();
    if (argc > 3 && !bench::parse_positive_size(argv[3], repetitions)) return usage();

    bench::try_pin_and_prioritize(0);

    std::cout << "Raw-socket loopback baseline -- raw_socket_baseline\n"
              << "warmup=" << warmup << " measured=" << measured << " repetitions=" << repetitions
              << " (loopback, " << kMessageSize << "-byte messages, tcp_nodelay=true)\n\n";

    std::vector<bench::PercentileReport> reports;
    for (size_t rep = 1; rep <= repetitions; ++rep) {
        const auto trial = run_trial(warmup, measured);
        if (!trial.ok) {
            std::cerr << "[raw_socket_baseline] FAILED: a trial did not complete; no summary printed.\n";
            return 1;
        }
        reports.push_back(bench::summarize(trial.samples));
        char label[64];
        std::snprintf(label, sizeof(label), "rep %zu raw sockets", rep);
        bench::print_report(label, reports.back());
    }

    std::cout << "\n";
    bench::print_spread_report("raw sockets", reports);
    return 0;
}
