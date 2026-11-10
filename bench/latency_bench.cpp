// PING -> PONG round-trip latency over loopback, tcp_nodelay true vs false.
// Run: ./bin/latency_bench [measured_iters [warmup_iters [repetitions]]]
//
// Drives Connection through a minimal epoll loop (not FeedMonitor::run, whose
// tick pacing would hide the protocol's own cost) against a real FeedSimulator,
// sending the next ping the instant a pong arrives.

#include "bench_common.h"
#include "connection.h"
#include "epoll_utils.h"
#include "feed_simulator.h"

#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// ok == false: the trial did not complete (handshake never reached HEALTHY,
// epoll stalled, or the deadline hit); partial samples must not be summarized.
struct TrialResult {
    bool ok = false;
    std::vector<int64_t> samples;
};

// One back-to-back ping/pong run against a fresh FeedSimulator. RTT samples come
// from Connection::stats().last_rtt_ns after each pong.
TrialResult run_latency_trial(bool tcp_nodelay, size_t warmup_iters, size_t measured_iters) {
    Config config;
    config.tcp_nodelay = tcp_nodelay;
    config.heartbeat_timeout = std::chrono::milliseconds(30000);  // generous; not exercised by this bench
    config.max_missed_heartbeats = 1000;

    FeedSimulator sim(0, /*feed_id=*/1, config);
    const uint16_t port = sim.port();
    // Otherwise the simulator inherits main()'s CPU pin and competes with the
    // measured client for one core.
    std::thread sim_thread([&sim] {
        bench::reset_affinity_unpinned();
        sim.run();
    });

    const int client_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    // Non-blocking connect: EINPROGRESS is expected; completion shows up as EPOLLOUT.
    if (client_fd >= 0) {
        [[maybe_unused]] const int connect_rc = ::connect(client_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    }

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

    // Whole-trial wall-clock cap (healthy runs take well under a second).
    const int64_t deadline_ns = now_monotonic_ns() + 120'000'000'000LL;

    epoll_event events[8];
    while (completed < total_iters) {
        const int n = ::epoll_wait(epoll_fd, events, 8, 5000);
        if (n <= 0) {
            std::cerr << "[latency_bench] epoll_wait timed out or failed (state="
                      << to_string(conn.state()) << ", completed " << completed << "/" << total_iters
                      << ") -- trial FAILED\n";
            break;
        }
        if (now_monotonic_ns() > deadline_ns) {
            std::cerr << "[latency_bench] trial exceeded its 120s deadline (completed " << completed << "/"
                      << total_iters << ") -- trial FAILED\n";
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

        // First ping once the handshake completes.
        if (!first_ping_sent && conn.state() == ConnectionState::kHealthy) {
            first_ping_sent = true;
            conn.send_ping(now_monotonic_ns());
        }

        // Each fresh pong immediately triggers the next ping.
        if (conn.stats().heartbeats_acked > last_acked) {
            last_acked = conn.stats().heartbeats_acked;
            if (completed >= warmup_iters) {
                samples.push_back(conn.stats().last_rtt_ns);
            }
            ++completed;
            conn.send_ping(now_monotonic_ns());
        }

        // Arm EPOLLOUT only while there is something queued to write.
        ev.events = epoll_interest_flags(/*readable=*/true, conn.wants_write(), /*edge_triggered=*/false);
        ::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
    }

    ::close(epoll_fd);
    sim.stop();
    sim_thread.join();
    TrialResult result;
    result.ok = (completed == total_iters) && samples.size() == measured_iters;
    result.samples = std::move(samples);
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

    std::cout << "Low-Latency TCP Heartbeat Monitor -- latency_bench\n"
              << "warmup=" << warmup << " measured=" << measured << " repetitions=" << repetitions
              << " (loopback)\n\n";

    // Alternate which configuration runs first each repetition, so a "ran
    // second on a warmer CPU" advantage doesn't always go to the same side.
    struct PairedReport {
        bench::PercentileReport nodelay_true;
        bench::PercentileReport nodelay_false;
    };
    size_t rep_counter = 0;
    bool any_failed = false;
    const auto paired_reports = bench::repeat_trials(
        [&]() -> PairedReport {
            const bool true_first = (rep_counter++ % 2 == 0);
            PairedReport paired;
            const bool order[2] = {true_first, !true_first};
            for (const bool nodelay : order) {
                const auto trial = run_latency_trial(nodelay, warmup, measured);
                if (!trial.ok) {
                    any_failed = true;
                    continue;  // never summarize a partial trial
                }
                auto& slot = nodelay ? paired.nodelay_true : paired.nodelay_false;
                slot = bench::summarize(trial.samples);
                char label[64];
                std::snprintf(label, sizeof(label), "rep %zu tcp_nodelay=%s", rep_counter, nodelay ? "true " : "false");
                bench::print_report(label, slot);
            }
            return paired;
        },
        repetitions);

    if (any_failed) {
        std::cerr << "[latency_bench] FAILED: at least one trial did not complete; no summary printed.\n";
        return 1;
    }

    std::cout << "\n";
    std::vector<bench::PercentileReport> reports_nodelay_true;
    std::vector<bench::PercentileReport> reports_nodelay_false;
    reports_nodelay_true.reserve(paired_reports.size());
    reports_nodelay_false.reserve(paired_reports.size());
    for (const auto& p : paired_reports) {
        reports_nodelay_true.push_back(p.nodelay_true);
        reports_nodelay_false.push_back(p.nodelay_false);
    }

    bench::print_spread_report("tcp_nodelay=true ", reports_nodelay_true);
    bench::print_spread_report("tcp_nodelay=false", reports_nodelay_false);

    return 0;
}
