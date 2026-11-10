#include "config.h"
#include "feed_simulator.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>

namespace {

// Exit codes: 0 = clean shutdown / --help, 1 = runtime failure, 2 = bad command line.
constexpr int kExitRuntimeError = 1;
constexpr int kExitUsageError = 2;

// Atomic so main()'s publish/clear and the signal handler's read are ordered.
std::atomic<FeedSimulator*> g_simulator{nullptr};

// Async-signal-safe: stop() is an atomic store plus an eventfd write(). errno
// is saved/restored since the write() can clobber it.
void handle_signal(int) {
    const int saved_errno = errno;
    if (FeedSimulator* sim = g_simulator.load()) {
        sim->stop();
    }
    errno = saved_errno;
}

// Sets stdin O_NONBLOCK for its lifetime, then restores the original flags.
// The flag lives on the open file description shared with the parent shell, so
// leaving it set would make the shell's own reads fail with EAGAIN.
class StdinNonBlockGuard {
public:
    StdinNonBlockGuard() {
        original_flags_ = ::fcntl(STDIN_FILENO, F_GETFL, 0);
        if (original_flags_ < 0) {
            return;
        }
        if (::fcntl(STDIN_FILENO, F_SETFL, original_flags_ | O_NONBLOCK) == 0) {
            active_ = true;
        }
    }
    ~StdinNonBlockGuard() {
        if (active_) {
            ::fcntl(STDIN_FILENO, F_SETFL, original_flags_);
        }
    }
    StdinNonBlockGuard(const StdinNonBlockGuard&) = delete;
    StdinNonBlockGuard& operator=(const StdinNonBlockGuard&) = delete;
    bool active() const { return active_; }

private:
    int original_flags_ = -1;
    bool active_ = false;
};

void print_simulator_usage(const char* prog_name) {
    std::cout
        << "Usage: " << prog_name << " --port <n> [options]\n"
           "\n"
           "  --port <n>                      required; TCP port to listen on, 0-65535 (0 = OS-assigned)\n"
           "  --bind <ipv4>                   address to listen on [default 127.0.0.1; use 0.0.0.0 for all interfaces]\n"
           "  --max-clients <n>               max simultaneous clients, 1-1000000; extras are closed [default 1024]\n"
           "  --feed-id <n>                   0-4294967295 [default 1]\n"
           "  --tcp-nodelay <bool>            [default true]\n"
           "  --edge-triggered <bool>         [default true]\n"
           "  --heartbeat-timeout-ms <n>      reap a client that sends no PING for this long, >=1 [default 3000]\n"
           "  --tick-interval-ms <n>          how often timeouts/deferred replies are checked, >=1 [default 50]\n"
           "  --drop-rate <0..1>              probability a PONG reply is silently dropped [default 0]\n"
           "  --extra-latency-ms <n>          artificial delay before replying, 0-3600000 [default 0]\n"
           "  --jitter <bool>                 randomize extra latency +/-50% [default false]\n"
           "  -h, --help                      show this message\n"
           "\n"
           "<bool> is one of true|false|1|0|yes|no (case-insensitive).\n"
           "Invalid arguments exit with status 2.\n"
           "Stdin commands while running: kill | set-drop-rate <0..1> | quit\n";
}

// Accepts true/1/yes and false/0/no, case-insensitive; anything else is an error.
std::optional<bool> parse_bool_arg(const char* raw, const char* flag_name) {
    std::string v = raw != nullptr ? raw : "";
    std::transform(v.begin(), v.end(), v.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (v == "true" || v == "1" || v == "yes") return true;
    if (v == "false" || v == "0" || v == "no") return false;
    std::cerr << "error: " << flag_name << ": invalid boolean '" << (raw != nullptr ? raw : "")
              << "' (expected true|false|1|0|yes|no)\n";
    return std::nullopt;
}

// Strict integer parse (strtoll + errno/endptr, unlike atoi) with a [lo, hi]
// range check so later narrowing casts can't silently truncate.
std::optional<long long> parse_int_arg(const char* raw, const char* flag_name, long long lo, long long hi) {
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
    if (value < lo || value > hi) {
        std::cerr << "error: " << flag_name << ": " << value << " is out of range [" << lo << ", " << hi << "]\n";
        return std::nullopt;
    }
    return value;
}

// Probability in [0, 1]. Written as !(v >= 0 && v <= 1) so NaN is rejected too.
std::optional<double> parse_probability(const char* raw, const char* flag_name) {
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
    if (!(value >= 0.0 && value <= 1.0)) {
        std::cerr << "error: " << flag_name << ": '" << raw << "' is out of range [0, 1]\n";
        return std::nullopt;
    }
    return value;
}

// Runs on the reactor thread (FeedSimulator's command callback).
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
        std::string arg, extra;
        if ((iss >> arg) && !(iss >> extra)) {
            if (const auto rate = parse_probability(arg.c_str(), "set-drop-rate")) {
                faults.drop_probability = *rate;
                simulator.set_fault_config(faults);
            }  // else: parse_probability already reported the error
        } else {
            std::cerr << "usage: set-drop-rate <0..1>\n";
        }
    } else {
        std::cerr << "[feed_simulator] unknown command '" << cmd << "' (try: kill | set-drop-rate <p> | quit)\n";
    }
}

struct SimulatorArgs {
    uint16_t port = 0;
    uint32_t feed_id = 1;
    Config config;
    FaultConfig faults;
    SimulatorOptions options;
};

enum class ParseOutcome { kRun, kHelp, kUsageError };

// Prints its own diagnostics (and usage where appropriate); main() only maps the outcome to an exit code.
ParseOutcome parse_args(int argc, char** argv, SimulatorArgs& args) {
    bool port_set = false;
    enum LongOptId {
        kOptDropRate = 1000,
        kOptExtraLatencyMs,
        kOptJitter,
        kOptBind,
        kOptMaxClients,
        kOptHeartbeatTimeoutMs,
        kOptTickIntervalMs,
    };
    const struct option kLongOptions[] = {
        {"port", required_argument, nullptr, 'p'},
        {"feed-id", required_argument, nullptr, 'f'},
        {"tcp-nodelay", required_argument, nullptr, 'n'},
        {"edge-triggered", required_argument, nullptr, 'e'},
        {"drop-rate", required_argument, nullptr, kOptDropRate},
        {"extra-latency-ms", required_argument, nullptr, kOptExtraLatencyMs},
        {"jitter", required_argument, nullptr, kOptJitter},
        {"bind", required_argument, nullptr, kOptBind},
        {"max-clients", required_argument, nullptr, kOptMaxClients},
        {"heartbeat-timeout-ms", required_argument, nullptr, kOptHeartbeatTimeoutMs},
        {"tick-interval-ms", required_argument, nullptr, kOptTickIntervalMs},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    constexpr long long kMaxDurationMs = 24LL * 60 * 60 * 1000;  // 1 day: far past any sane value, far from overflow

    int c;
    while ((c = getopt_long(argc, argv, "p:f:n:e:h", kLongOptions, nullptr)) != -1) {
        switch (c) {
            case 'p': {
                const auto v = parse_int_arg(optarg, "--port", 0, 65535);
                if (!v) return ParseOutcome::kUsageError;
                args.port = static_cast<uint16_t>(*v);
                port_set = true;
                break;
            }
            case 'f': {
                const auto v = parse_int_arg(optarg, "--feed-id", 0, std::numeric_limits<uint32_t>::max());
                if (!v) return ParseOutcome::kUsageError;
                args.feed_id = static_cast<uint32_t>(*v);
                break;
            }
            case 'n': {
                const auto v = parse_bool_arg(optarg, "--tcp-nodelay");
                if (!v) return ParseOutcome::kUsageError;
                args.config.tcp_nodelay = *v;
                break;
            }
            case 'e': {
                const auto v = parse_bool_arg(optarg, "--edge-triggered");
                if (!v) return ParseOutcome::kUsageError;
                args.config.edge_triggered = *v;
                break;
            }
            case kOptDropRate: {
                const auto v = parse_probability(optarg, "--drop-rate");
                if (!v) return ParseOutcome::kUsageError;
                args.faults.drop_probability = *v;
                break;
            }
            case kOptExtraLatencyMs: {
                const auto v = parse_int_arg(optarg, "--extra-latency-ms", 0, 3'600'000);
                if (!v) return ParseOutcome::kUsageError;
                args.faults.extra_latency = std::chrono::milliseconds(*v);
                break;
            }
            case kOptJitter: {
                const auto v = parse_bool_arg(optarg, "--jitter");
                if (!v) return ParseOutcome::kUsageError;
                args.faults.jitter = *v;
                break;
            }
            case kOptBind: {
                in_addr probe{};
                if (::inet_pton(AF_INET, optarg, &probe) != 1) {
                    std::cerr << "error: --bind: '" << optarg << "' is not a valid IPv4 address\n";
                    return ParseOutcome::kUsageError;
                }
                args.options.bind_address = optarg;
                break;
            }
            case kOptMaxClients: {
                const auto v = parse_int_arg(optarg, "--max-clients", 1, 1'000'000);
                if (!v) return ParseOutcome::kUsageError;
                args.options.max_clients = static_cast<size_t>(*v);
                break;
            }
            case kOptHeartbeatTimeoutMs: {
                const auto v = parse_int_arg(optarg, "--heartbeat-timeout-ms", 1, kMaxDurationMs);
                if (!v) return ParseOutcome::kUsageError;
                args.config.heartbeat_timeout = std::chrono::milliseconds(*v);
                break;
            }
            case kOptTickIntervalMs: {
                const auto v = parse_int_arg(optarg, "--tick-interval-ms", 1, kMaxDurationMs);
                if (!v) return ParseOutcome::kUsageError;
                args.config.tick_interval = std::chrono::milliseconds(*v);
                break;
            }
            case 'h':
                print_simulator_usage(argv[0]);
                return ParseOutcome::kHelp;
            default:
                print_simulator_usage(argv[0]);
                return ParseOutcome::kUsageError;
        }
    }

    if (!port_set) {
        std::cerr << "error: --port is required\n";
        print_simulator_usage(argv[0]);
        return ParseOutcome::kUsageError;
    }
    return ParseOutcome::kRun;
}

}  // namespace

int main(int argc, char** argv) {
    // Every send() uses MSG_NOSIGNAL; this guards stdout/stderr pipes closing early.
    (void)::signal(SIGPIPE, SIG_IGN);

    SimulatorArgs args;
    switch (parse_args(argc, argv, args)) {
        case ParseOutcome::kHelp:
            return 0;
        case ParseOutcome::kUsageError:
            return kExitUsageError;
        case ParseOutcome::kRun:
            break;
    }
    FaultConfig& faults = args.faults;  // mutated by the set-drop-rate command

    // Declared before the simulator so stdin's flags are restored after it is destroyed.
    StdinNonBlockGuard stdin_guard;
    if (!stdin_guard.active()) {
        std::cerr << "[feed_simulator] warning: could not set stdin non-blocking: " << std::strerror(errno)
                  << " -- stdin commands disabled\n";
    }

    try {
        FeedSimulator simulator(args.port, args.feed_id, args.config, faults, args.options);
        g_simulator.store(&simulator);
        // Destroyed before simulator, so the handler never sees a dangling pointer.
        struct ClearOnExit {
            ~ClearOnExit() { g_simulator.store(nullptr); }
        } clear_on_exit;

        struct sigaction sa{};
        sa.sa_handler = handle_signal;
        ::sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        ::sigaction(SIGINT, &sa, nullptr);
        ::sigaction(SIGTERM, &sa, nullptr);

        if (stdin_guard.active()) {
            try {
                simulator.set_command_input(STDIN_FILENO, [&simulator, &faults](const std::string& line) {
                    handle_command_line(simulator, faults, line);
                });
            } catch (const std::exception& e) {
                // e.g. stdin is /dev/null or a regular file (epoll EPERM); not fatal.
                std::cerr << "[feed_simulator] warning: stdin commands disabled: " << e.what() << "\n";
            }
        }

        // Flush explicitly: piped stdout is fully buffered and run() blocks.
        std::cout << "[feed_simulator] listening on port " << simulator.port() << " as feed_id " << args.feed_id
                  << " (bind=" << args.options.bind_address << " max_clients=" << args.options.max_clients
                  << " drop_rate=" << faults.drop_probability
                  << " extra_latency_ms=" << faults.extra_latency.count() << " jitter=" << std::boolalpha
                  << faults.jitter << ")\n"
                  << std::flush;
        simulator.run();
        std::cout << "[feed_simulator] shut down (" << simulator.client_count() << " clients still tracked)\n";
    } catch (const std::exception& e) {
        std::cerr << "[feed_simulator] fatal: " << e.what() << "\n";
        return kExitRuntimeError;
    }

    return 0;
}
