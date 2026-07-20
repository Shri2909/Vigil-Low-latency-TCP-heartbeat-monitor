// Measures end-to-end PING -> PONG round-trip latency over loopback.
//
// Deliberately drives Connection directly through a minimal epoll loop here,
// rather than through the real FeedMonitor::run(). FeedMonitor paces pings
// off config.heartbeat_interval via its periodic tick (by design -- see
// PROJECT_PLAN.md section 5), which is the right behavior for a monitoring
// daemon but would make "send the next ping the instant the previous pong
// arrives" awkward to express, and would conflate the tick's granularity
// with the protocol's actual round-trip cost. This benchmark isolates the
// latter: real Connection encode/decode/socket-syscall cost against a real
// FeedSimulator (not a benchmark-only stub), with back-to-back pacing.

#include "bench_common.h"
#include "connection.h"
#include "epoll_utils.h"
#include "feed_simulator.h"

#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// Runs one back-to-back ping/pong latency measurement against a fresh
// FeedSimulator and returns the sorted RTT samples (nanoseconds), collected
// directly from Connection's own stats() after each pong -- bypassing the
// live-display ring buffer's 256-sample cap, which exists for bounded memory
// use in a long-running process, not for benchmark data collection.
std::vector<int64_t> run_latency_trial(bool tcp_nodelay, size_t warmup_iters, size_t measured_iters) {
    Config config;
    config.tcp_nodelay = tcp_nodelay;
    config.heartbeat_timeout = std::chrono::milliseconds(30000);  // generous; not exercised by this bench
    config.max_missed_heartbeats = 1000;

    FeedSimulator sim(0, /*feed_id=*/1, config);
    const uint16_t port = sim.port();
    // This thread would otherwise silently inherit main()'s single-CPU pin
    // (see bench::reset_affinity_unpinned's doc comment) -- forcing the
    // simulator onto the exact same core as the client whose latency is
    // being measured, adding contention/scheduling noise to precisely the
    // measurement the pin was meant to clean up.
    std::thread sim_thread([&sim] {
        bench::reset_affinity_unpinned();
        sim.run();
    });

    const int client_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    ::connect(client_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    Connection conn(client_fd, Role::kInitiator, /*feed_id=*/1, "127.0.0.1", port, config);

    const int epoll_fd = ::epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.fd = client_fd;
    ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

    std::vector<int64_t> samples;
    samples.reserve(measured_iters);
    const size_t total_iters = warmup_iters + measured_iters;
    size_t completed = 0;
    uint64_t last_acked = 0;
    bool first_ping_sent = false;

    epoll_event events[8];
    while (completed < total_iters) {
        const int n = ::epoll_wait(epoll_fd, events, 8, 5000);
        if (n <= 0) {
            std::cerr << "[latency_bench] epoll_wait timed out or failed -- aborting trial\n";
            break;
        }
        const int64_t now = now_monotonic_ns();
        for (int i = 0; i < n; ++i) {
            if (events[i].events & EPOLLOUT) {
                conn.on_writable(now);
            }
            if (events[i].events & EPOLLIN) {
                conn.on_readable(now);
            }
        }

        // Kick off the very first ping once the handshake completes.
        if (!first_ping_sent && conn.state() == ConnectionState::kHealthy) {
            first_ping_sent = true;
            conn.send_ping(now_monotonic_ns());
        }

        // Every fresh pong immediately triggers the next ping -- maximum
        // back-to-back rate, limited only by real socket/epoll round-trip
        // cost, which is exactly what this benchmark measures.
        if (conn.stats().heartbeats_acked > last_acked) {
            last_acked = conn.stats().heartbeats_acked;
            if (completed >= warmup_iters) {
                samples.push_back(conn.stats().last_rtt_ns);
            }
            ++completed;
            conn.send_ping(now_monotonic_ns());
        }

        // Keep EPOLLOUT armed only while there's something queued to write.
        ev.events = epoll_interest_flags(/*readable=*/true, conn.wants_write(), /*edge_triggered=*/false);
        ::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
    }

    ::close(epoll_fd);
    sim.stop();
    sim_thread.join();
    return samples;
}

// Prints the min/median/max of one percentile metric across a set of
// repeated-trial reports for one tcp_nodelay configuration -- the "spread
// across repetitions" that replaces the old single-point-estimate output
// (PROJECT_PLAN.md F7/F10).
void print_spread_report(const std::string& label, const std::vector<bench::PercentileReport>& reports) {
    const auto p50 = bench::summarize_spread(reports, [](const bench::PercentileReport& r) { return r.p50; });
    const auto p90 = bench::summarize_spread(reports, [](const bench::PercentileReport& r) { return r.p90; });
    const auto p99 = bench::summarize_spread(reports, [](const bench::PercentileReport& r) { return r.p99; });
    std::cout << label << " across " << reports.size() << " repetitions:\n"
              << "  p50: min=" << format_duration_ns(p50.min) << " median=" << format_duration_ns(p50.median)
              << " max=" << format_duration_ns(p50.max) << "\n"
              << "  p90: min=" << format_duration_ns(p90.min) << " median=" << format_duration_ns(p90.median)
              << " max=" << format_duration_ns(p90.max) << "\n"
              << "  p99: min=" << format_duration_ns(p99.min) << " median=" << format_duration_ns(p99.median)
              << " max=" << format_duration_ns(p99.max) << "\n";
}

}  // namespace

int main(int argc, char** argv) {
    size_t warmup = 500;
    size_t measured = 20000;
    size_t repetitions = 5;
    if (argc > 1) measured = static_cast<size_t>(std::atoll(argv[1]));
    if (argc > 2) warmup = static_cast<size_t>(std::atoll(argv[2]));
    if (argc > 3) repetitions = static_cast<size_t>(std::atoll(argv[3]));

    bench::try_pin_and_prioritize(0);

    std::cout << "Low-Latency TCP Heartbeat Monitor -- latency_bench\n"
              << "warmup=" << warmup << " measured=" << measured << " repetitions=" << repetitions
              << " (loopback)\n\n";

    // Both configurations are re-run `repetitions` times via bench::repeat_trials,
    // alternating which one goes first each repetition. A single fixed
    // true-then-false ordering (the old behavior) confounds the comparison
    // with whichever trial happens to run on a more CPU-frequency-warmed
    // core -- exactly the "false beats true" pattern seen on live runs of
    // this benchmark on a powersave-governor machine. Alternating the
    // starting side cancels that "ran second" advantage out across
    // repetitions instead of consistently handing it to one configuration
    // (PROJECT_PLAN.md F7).
    struct PairedReport {
        bench::PercentileReport nodelay_true;
        bench::PercentileReport nodelay_false;
    };
    size_t rep_counter = 0;
    const auto paired_reports = bench::repeat_trials(
        [&]() -> PairedReport {
            const bool true_first = (rep_counter++ % 2 == 0);
            PairedReport paired;
            const bool order[2] = {true_first, !true_first};
            for (const bool nodelay : order) {
                auto samples = run_latency_trial(nodelay, warmup, measured);
                (nodelay ? paired.nodelay_true : paired.nodelay_false) = bench::summarize(samples);
            }
            return paired;
        },
        repetitions);

    std::vector<bench::PercentileReport> reports_nodelay_true;
    std::vector<bench::PercentileReport> reports_nodelay_false;
    reports_nodelay_true.reserve(paired_reports.size());
    reports_nodelay_false.reserve(paired_reports.size());
    for (const auto& p : paired_reports) {
        reports_nodelay_true.push_back(p.nodelay_true);
        reports_nodelay_false.push_back(p.nodelay_false);
    }

    print_spread_report("tcp_nodelay=true ", reports_nodelay_true);
    print_spread_report("tcp_nodelay=false", reports_nodelay_false);

    return 0;
}
