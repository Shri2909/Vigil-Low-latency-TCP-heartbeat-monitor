// Live fault-injection demo: three real FeedSimulators and one real FeedMonitor
// over loopback. Only NASDAQ is faulted; the demo checks that it fails, is
// distrusted, reconnects and recovers while NYSE and LSE never leave HEALTHY.
// Run: ./bin/lifecycle_demo
// Exit: 0 = lifecycle observed and neighbors stayed healthy, 1 = a neighbor was
// perturbed, 2 = a transition did not happen within kStateDeadline.

#include "config.h"
#include "feed_monitor.h"
#include "feed_simulator.h"

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
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

// Upper bound on any single wait; the slowest legitimate step takes ~6s.
constexpr std::chrono::seconds kStateDeadline{30};

// Called from main() and the monitor's tick callback, hence the mutex.
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
    (void)::signal(SIGPIPE, SIG_IGN);  // defense in depth; sends already use MSG_NOSIGNAL

    std::cout << "=== Low-Latency TCP Heartbeat Monitor -- live fault-injection demonstration ===\n"
              << "Three real feeds, one real FeedMonitor -- only NASDAQ will be faulted, to\n"
              << "demonstrate that one feed dying doesn't perturb the others.\n\n";

    const std::vector<FeedSpec> specs = {{1, "NASDAQ"}, {2, "NYSE"}, {3, "LSE"}};

    std::vector<std::unique_ptr<FeedSimulator>> sims;
    for (const auto& spec : specs) {
        sims.push_back(std::make_unique<FeedSimulator>(0, spec.feed_id, Config{}));
    }

    // Only NASDAQ's simulator gets a command pipe; the command callback runs on
    // its reactor thread, which is where set_fault_config must be called.
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

    // Shipped defaults rather than a sped-up cadence, so the timings are realistic.
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
    // A neighbor only counts as perturbed after it has been HEALTHY once; the
    // initial connect is legitimately CONNECTING/HANDSHAKING.
    std::array<std::atomic<bool>, 4> was_healthy{};
    std::atomic<bool> others_ever_left_healthy{false};

    monitor.set_tick_callback([&](const AggregateStats& stats) {
        for (const auto& f : stats.feeds) {
            const ConnectionState prev = current_state[f.feed_id].exchange(f.state, std::memory_order_relaxed);
            if (prev != f.state) {
                print_line(std::string(name_for(f.feed_id)) + ": " + to_string(f.state));
                if (f.feed_id == 1 && f.state == ConnectionState::kFailed) {
                    print_line("Reconnect attempt scheduled (exponential backoff + jitter)");
                }
                if (f.feed_id != 1) {
                    if (f.state == ConnectionState::kHealthy) {
                        was_healthy[f.feed_id].store(true, std::memory_order_relaxed);
                    } else if (was_healthy[f.feed_id].load(std::memory_order_relaxed)) {
                        // A neighbor left HEALTHY: isolation violated.
                        others_ever_left_healthy.store(true, std::memory_order_relaxed);
                    }
                }
            }
        }
    });

    for (const auto& spec : specs) {
        monitor.add_feed(FeedEndpoint{spec.feed_id, spec.name, "127.0.0.1", sims[spec.feed_id - 1]->port()});
    }
    std::thread monitor_thread([&monitor] { monitor.run(); });

    // Returns false (after printing why) if `target` isn't reached in time.
    auto wait_for_state = [&](uint32_t feed_id, ConnectionState target) {
        const auto deadline = std::chrono::steady_clock::now() + kStateDeadline;
        while (current_state[feed_id].load(std::memory_order_relaxed) != target) {
            if (std::chrono::steady_clock::now() >= deadline) {
                print_line(std::string("TIMEOUT: ") + name_for(feed_id) + " did not reach " + to_string(target) +
                           " within " + std::to_string(kStateDeadline.count()) + "s (stuck in " +
                           to_string(current_state[feed_id].load(std::memory_order_relaxed)) + ")");
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return true;
    };

    bool timed_out = false;
    do {
        if (!wait_for_state(1, ConnectionState::kHealthy) || !wait_for_state(2, ConnectionState::kHealthy) ||
            !wait_for_state(3, ConnectionState::kHealthy)) {
            timed_out = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(600));  // a few real healthy heartbeats first

        print_line("--- Simulator stops replying (NASDAQ only -- NYSE and LSE are never touched) ---");
        const std::string drop_on = "drop-on\n";
        [[maybe_unused]] ssize_t drop_on_rc = ::write(pipe_fds[1], drop_on.data(), drop_on.size());

        // HEALTHY -> DEGRADED -> FAILED once the miss limit is reached.
        if (!wait_for_state(1, ConnectionState::kFailed)) {
            timed_out = true;
            break;
        }

        // Restored before the reconnect backoff elapses, so the retry finds a working peer.
        print_line("--- Simulator becomes available again ---");
        const std::string drop_off = "drop-off\n";
        [[maybe_unused]] ssize_t drop_off_rc = ::write(pipe_fds[1], drop_off.data(), drop_off.size());

        // New socket and handshake, then HEALTHY again.
        if (!wait_for_state(1, ConnectionState::kHealthy)) {
            timed_out = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(900));  // stay healthy for a visible stretch
    } while (false);

    const bool isolation_held = !others_ever_left_healthy.load(std::memory_order_relaxed);
    if (timed_out) {
        std::cout << "\n=== Demonstration ABORTED: a state transition did not happen in time (see TIMEOUT above). ===\n";
    } else {
        std::cout << "\n=== Demonstration complete: NASDAQ's silent failure was detected, distrusted,\n"
                     "    and recovered automatically. NYSE and LSE stayed HEALTHY throughout ("
                  << (isolation_held ? "confirmed" : "FAILED -- one of them was perturbed") << "). ===\n";
    }

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
    if (timed_out) return 2;
    return isolation_held ? 0 : 1;
}
