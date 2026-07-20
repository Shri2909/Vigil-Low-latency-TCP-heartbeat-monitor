// Live demonstration of the project's actual motive: detect silent feed
// failure, mark the connection unhealthy, stop trusting it, reconnect
// automatically, and resume monitoring -- *without* disturbing any other
// feed being watched at the same time. latency_bench/throughput_bench
// (bench/) prove this is fast and scales; this proves it is *correct*, for
// one feed failing and for its neighbors staying untouched.
//
// Drives a real FeedMonitor against three real FeedSimulator instances --
// the same production classes the shipped binaries use, over real loopback
// TCP sockets and real timers, not stubs. Only NASDAQ ever receives a fault
// command; NYSE and LSE are never touched at all. Every line printed below
// is triggered by an actual state transition observed on FeedMonitor's tick
// callback; nothing here is a pre-scripted transcript.
//
// Usage: ./bin/lifecycle_demo

#include "config.h"
#include "feed_monitor.h"
#include "feed_simulator.h"

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace {

std::mutex g_print_mutex;

// Multiple threads (main() and FeedMonitor's reactor thread, via the tick
// callback) call this, so both the shared stdout stream and the
// non-reentrant time-formatting path need explicit protection.
void print_line(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_print_mutex);
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
    ::localtime_r(&t, &tm_buf);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &tm_buf);
    std::cout << "[" << buf << "] " << msg << std::endl;
}

struct FeedSpec {
    uint32_t feed_id;
    const char* name;
};

const char* name_for(uint32_t feed_id) {
    switch (feed_id) {
        case 1: return "NASDAQ";
        case 2: return "NYSE";
        case 3: return "LSE";
        default: return "UNKNOWN";
    }
}

}  // namespace

int main() {
    // Every socket send() already passes MSG_NOSIGNAL (see connection.cpp),
    // so this is pure defense-in-depth, not a live bug -- insurance against
    // any write path that doesn't go through that flag.
    (void)::signal(SIGPIPE, SIG_IGN);  // return value (previous handler) deliberately unused

    std::cout << "=== Low-Latency TCP Heartbeat Monitor -- live fault-injection demonstration ===\n"
              << "Three real feeds, one real FeedMonitor -- only NASDAQ will be faulted, to\n"
              << "demonstrate that one feed dying doesn't perturb the others.\n\n";

    const std::vector<FeedSpec> specs = {{1, "NASDAQ"}, {2, "NYSE"}, {3, "LSE"}};

    std::vector<std::unique_ptr<FeedSimulator>> sims;
    for (const auto& spec : specs) {
        sims.push_back(std::make_unique<FeedSimulator>(0, spec.feed_id, Config{}));
    }

    // Only NASDAQ's simulator gets a command pipe for fault injection --
    // NYSE's and LSE's simulators never receive any command at all, and
    // their FaultConfig stays at its default (no drop, no latency) for the
    // entire run. Same reactor-thread-only reasoning as the single-feed
    // version: set_fault_config is called from this callback, not directly
    // from main()'s thread.
    int pipe_fds[2];
    [[maybe_unused]] const int pipe_rc = ::pipe(pipe_fds);
    const int flags = ::fcntl(pipe_fds[0], F_GETFL, 0);
    ::fcntl(pipe_fds[0], F_SETFL, flags | O_NONBLOCK);
    FaultConfig faults;
    FeedSimulator& nasdaq_sim = *sims[0];
    nasdaq_sim.set_command_input(pipe_fds[0], [&](const std::string& line) {
        if (line == "drop-on") {
            faults.drop_probability = 1.0;
            nasdaq_sim.set_fault_config(faults);
        } else if (line == "drop-off") {
            faults.drop_probability = 0.0;
            nasdaq_sim.set_fault_config(faults);
        }
    });

    std::vector<std::thread> sim_threads;
    for (auto& sim : sims) {
        FeedSimulator* raw = sim.get();
        sim_threads.emplace_back([raw] { raw->run(); });
    }

    // Shipped production defaults (config.h), not a sped-up demo cadence: a
    // sub-second miss deadline doesn't read as a believable "missed
    // heartbeat" -- 3 seconds does. This makes the whole cycle take longer
    // to watch, but it's the actual number a deployed instance would use.
    Config config;
    config.heartbeat_interval = std::chrono::milliseconds(1000);
    config.heartbeat_timeout = std::chrono::milliseconds(3000);
    config.max_missed_heartbeats = 2;
    config.reconnect_base_delay = std::chrono::milliseconds(1000);
    config.reconnect_max_delay = std::chrono::milliseconds(5000);
    config.tick_interval = std::chrono::milliseconds(100);

    FeedMonitor monitor(config);

    // Indexed by feed_id (1..3); index 0 unused.
    std::array<std::atomic<ConnectionState>, 4> current_state;
    for (auto& s : current_state) {
        s.store(ConnectionState::kDisconnected, std::memory_order_relaxed);
    }
    std::atomic<bool> others_ever_left_healthy{false};

    monitor.set_tick_callback([&](const AggregateStats& stats) {
        for (const auto& f : stats.feeds) {
            const ConnectionState prev = current_state[f.feed_id].exchange(f.state, std::memory_order_relaxed);
            if (prev != f.state) {
                print_line(std::string(name_for(f.feed_id)) + ": " + to_string(f.state));
                if (f.feed_id == 1 && f.state == ConnectionState::kFailed) {
                    print_line("Reconnect attempt scheduled (exponential backoff + jitter)");
                }
                if (f.feed_id != 1 && f.state != ConnectionState::kHealthy) {
                    // Would mean NYSE or LSE got disturbed by NASDAQ's fault
                    // -- the isolation claim this demo exists to check.
                    others_ever_left_healthy.store(true, std::memory_order_relaxed);
                }
            }
        }
    });

    for (const auto& spec : specs) {
        monitor.add_feed(FeedEndpoint{spec.feed_id, spec.name, "127.0.0.1", sims[spec.feed_id - 1]->port()});
    }
    std::thread monitor_thread([&monitor] { monitor.run(); });

    auto wait_for_state = [&](uint32_t feed_id, ConnectionState target) {
        while (current_state[feed_id].load(std::memory_order_relaxed) != target) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    };

    wait_for_state(1, ConnectionState::kHealthy);
    wait_for_state(2, ConnectionState::kHealthy);
    wait_for_state(3, ConnectionState::kHealthy);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));  // a few real healthy heartbeats first

    print_line("--- Simulator stops replying (NASDAQ only -- NYSE and LSE are never touched) ---");
    const std::string drop_on = "drop-on\n";
    [[maybe_unused]] ssize_t drop_on_rc = ::write(pipe_fds[1], drop_on.data(), drop_on.size());

    // No reply ever arrives for NASDAQ -> timeout detected -> HEALTHY ->
    // DEGRADED -> miss limit reached -> DEGRADED -> FAILED. NYSE/LSE keep
    // ticking on their own independent Connections and simulators the
    // whole time -- the tick callback above would flag it immediately if
    // either one so much as blinked.
    wait_for_state(1, ConnectionState::kFailed);

    // Restored well before reconnect_base_delay (1000ms) elapses, so the
    // reconnect that's already scheduled will find a working peer.
    print_line("--- Simulator becomes available again ---");
    const std::string drop_off = "drop-off\n";
    [[maybe_unused]] ssize_t drop_off_rc = ::write(pipe_fds[1], drop_off.data(), drop_off.size());

    // New socket, handshake repeated, HEALTHY again -- proven here by
    // actually observing it, not asserting it happened.
    wait_for_state(1, ConnectionState::kHealthy);
    std::this_thread::sleep_for(std::chrono::milliseconds(900));  // stay healthy for a visible stretch

    const bool isolation_held = !others_ever_left_healthy.load(std::memory_order_relaxed);
    std::cout << "\n=== Demonstration complete: NASDAQ's silent failure was detected, distrusted,\n"
                 "    and recovered automatically. NYSE and LSE stayed HEALTHY throughout ("
              << (isolation_held ? "confirmed" : "FAILED -- one of them was perturbed") << "). ===\n";

    monitor.stop();
    monitor_thread.join();
    for (auto& sim : sims) {
        sim->stop();
    }
    for (auto& t : sim_threads) {
        t.join();
    }
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
    return isolation_held ? 0 : 1;
}
