#include "config.h"
#include "feed_simulator.h"

#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>

#include <csignal>
#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>

namespace {

FeedSimulator* g_simulator = nullptr;

// Signal-safe: only touches g_simulator (a plain pointer, read-only here) and
// calls FeedSimulator::stop(), which itself only does an atomic store and a
// write() to an eventfd -- both on the POSIX async-signal-safe list.
void handle_signal(int) {
    if (g_simulator != nullptr) {
        g_simulator->stop();
    }
}

void print_simulator_usage(const char* prog_name) {
    std::cout
        << "Usage: " << prog_name << " --port <n> [options]\n"
           "\n"
           "  --port <n>                      required; TCP port to listen on (0 = OS-assigned)\n"
           "  --feed-id <n>                    [default 1]\n"
           "  --tcp-nodelay <true|false>       [default true]\n"
           "  --edge-triggered <true|false>    [default true]\n"
           "  --drop-rate <0..1>               probability a PONG reply is silently dropped [default 0]\n"
           "  --extra-latency-ms <n>           artificial delay before replying [default 0]\n"
           "  --jitter <true|false>            randomize extra latency +/-50%% [default false]\n"
           "  -h, --help                       show this message\n"
           "\n"
           "Stdin commands while running: kill | set-drop-rate <0..1> | quit\n";
}

bool parse_bool_flag(const char* value) {
    return value != nullptr && (std::strcmp(value, "true") == 0 || std::strcmp(value, "1") == 0);
}

// clang-tidy (cert-err34-c) flagged this file's original std::atoi/std::atof
// calls: unlike config.cpp's require_int_at_least (already hardened against
// exactly this class of gap), they silently return 0 on garbage input
// instead of signaling an error -- so `--port abc` would silently bind to
// port 0 (OS-assigned) rather than telling the operator they mistyped the
// flag. strtol/strtod's errno+endptr pair distinguishes "parsed as zero" from
// "failed to parse" and "out of range" (ERANGE), which atoi/atof cannot.
std::optional<long long> parse_int_arg(const char* raw, const char* flag_name) {
    if (raw == nullptr || *raw == '\0') {
        std::cerr << "error: " << flag_name << " requires a value\n";
        return std::nullopt;
    }
    char* end = nullptr;
    errno = 0;
    const long long value = std::strtoll(raw, &end, 10);
    if (end == raw || *end != '\0' || errno == ERANGE) {
        std::cerr << "error: " << flag_name << ": invalid integer '" << raw << "'\n";
        return std::nullopt;
    }
    return value;
}

std::optional<double> parse_double_arg(const char* raw, const char* flag_name) {
    if (raw == nullptr || *raw == '\0') {
        std::cerr << "error: " << flag_name << " requires a value\n";
        return std::nullopt;
    }
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(raw, &end);
    if (end == raw || *end != '\0' || errno == ERANGE) {
        std::cerr << "error: " << flag_name << ": invalid number '" << raw << "'\n";
        return std::nullopt;
    }
    return value;
}

// Runs on the reactor thread (invoked from FeedSimulator's command-input
// callback), so calling simulator's mutating methods here is not a
// cross-thread concern -- same reasoning as main.cpp's handle_command_line.
void handle_command_line(FeedSimulator& simulator, FaultConfig& faults, const std::string& line) {
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;
    if (cmd.empty()) {
        return;
    }

    if (cmd == "quit" || cmd == "exit") {
        simulator.stop();
    } else if (cmd == "kill") {
        simulator.kill_random_connection();
    } else if (cmd == "set-drop-rate") {
        double rate;
        if (iss >> rate) {
            faults.drop_probability = rate;
            simulator.set_fault_config(faults);
        } else {
            std::cerr << "usage: set-drop-rate <0..1>\n";
        }
    } else {
        std::cerr << "[feed_simulator] unknown command '" << cmd << "' (try: kill | set-drop-rate <p> | quit)\n";
    }
}

}  // namespace

int main(int argc, char** argv) {
    // Every socket send() already passes MSG_NOSIGNAL (see connection.cpp),
    // so this is pure defense-in-depth, not a live bug -- insurance against
    // any write path that doesn't go through that flag (stdout/stderr piped
    // to something that closes early, or a future send() call someone adds
    // without remembering it).
    (void)::signal(SIGPIPE, SIG_IGN);  // return value (previous handler) deliberately unused

    uint16_t port = 0;
    bool port_set = false;
    uint32_t feed_id = 1;
    Config config;
    FaultConfig faults;

    enum LongOptId { kOptDropRate = 1000, kOptExtraLatencyMs, kOptJitter };
    const struct option kLongOptions[] = {
        {"port", required_argument, nullptr, 'p'},
        {"feed-id", required_argument, nullptr, 'f'},
        {"tcp-nodelay", required_argument, nullptr, 'n'},
        {"edge-triggered", required_argument, nullptr, 'e'},
        {"drop-rate", required_argument, nullptr, kOptDropRate},
        {"extra-latency-ms", required_argument, nullptr, kOptExtraLatencyMs},
        {"jitter", required_argument, nullptr, kOptJitter},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "p:f:n:e:h", kLongOptions, nullptr)) != -1) {
        switch (c) {
            case 'p': {
                const auto v = parse_int_arg(optarg, "--port");
                if (!v) return 1;
                port = static_cast<uint16_t>(*v);
                port_set = true;
                break;
            }
            case 'f': {
                const auto v = parse_int_arg(optarg, "--feed-id");
                if (!v) return 1;
                feed_id = static_cast<uint32_t>(*v);
                break;
            }
            case 'n':
                config.tcp_nodelay = parse_bool_flag(optarg);
                break;
            case 'e':
                config.edge_triggered = parse_bool_flag(optarg);
                break;
            case kOptDropRate: {
                const auto v = parse_double_arg(optarg, "--drop-rate");
                if (!v) return 1;
                faults.drop_probability = *v;
                break;
            }
            case kOptExtraLatencyMs: {
                const auto v = parse_int_arg(optarg, "--extra-latency-ms");
                if (!v) return 1;
                faults.extra_latency = std::chrono::milliseconds(*v);
                break;
            }
            case kOptJitter:
                faults.jitter = parse_bool_flag(optarg);
                break;
            case 'h':
                print_simulator_usage(argv[0]);
                return 0;
            default:
                print_simulator_usage(argv[0]);
                return 1;
        }
    }

    if (!port_set) {
        std::cerr << "error: --port is required\n";
        print_simulator_usage(argv[0]);
        return 1;
    }

    const int stdin_flags = ::fcntl(STDIN_FILENO, F_GETFL, 0);
    ::fcntl(STDIN_FILENO, F_SETFL, stdin_flags | O_NONBLOCK);

    try {
        FeedSimulator simulator(port, feed_id, config, faults);
        g_simulator = &simulator;

        struct sigaction sa{};
        sa.sa_handler = handle_signal;
        ::sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        ::sigaction(SIGINT, &sa, nullptr);
        ::sigaction(SIGTERM, &sa, nullptr);

        simulator.set_command_input(STDIN_FILENO, [&simulator, &faults](const std::string& line) {
            handle_command_line(simulator, faults, line);
        });

        // Explicit flush: unlike a TTY, a piped/redirected stdout (a log
        // collector, a supervisor, or a test driving this binary as a real
        // subprocess) is fully buffered by default -- without this, this
        // line sits in libstdc++'s buffer and is never actually delivered
        // while simulator.run() blocks for the rest of the process's
        // lifetime right after it.
        std::cout << "[feed_simulator] listening on port " << simulator.port() << " as feed_id " << feed_id
                  << " (drop_rate=" << faults.drop_probability
                  << " extra_latency_ms=" << faults.extra_latency.count() << " jitter=" << std::boolalpha
                  << faults.jitter << ")\n"
                  << std::flush;
        simulator.run();
        std::cout << "[feed_simulator] shut down (" << simulator.client_count() << " clients still tracked)\n";
    } catch (const std::exception& e) {
        std::cerr << "[feed_simulator] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
