#include "config.h"

#include <cctype>
#include <charconv>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace {

void warn(const std::string& message) {
    std::cerr << "[config] warning: " << message << "\n";
}

std::string trim(const std::string& s) {
    size_t begin = 0;
    while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin]))) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(begin, end - begin);
}

std::optional<long long> try_parse_int(const std::string& s) {
    if (s.empty()) {
        return std::nullopt;
    }
    long long value = 0;
    const char* begin = s.data();
    const char* end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

std::optional<bool> try_parse_bool(const std::string& s) {
    if (s == "true" || s == "1" || s == "yes") return true;
    if (s == "false" || s == "0" || s == "no") return false;
    return std::nullopt;
}

bool is_valid_log_level(const std::string& s) {
    static const std::unordered_set<std::string> kLevels = {"trace", "debug", "info", "warn",
                                                              "error"};
    return kLevels.count(s) != 0;
}

// Parses raw as an integer and requires it to be >= min_value; otherwise logs
// via `context` (e.g. "line 12" or "--heartbeat-interval-ms") and returns
// nullopt so the caller keeps its current/default value untouched. Shared by
// both the file parser (apply_global_key) and the CLI (apply_cli_overrides)
// so every numeric duration/count field is validated the same way -- before
// this, only feed port/id had range checks; fields like heartbeat_interval_ms
// or epoll_max_events silently accepted 0/negative/absurd values, despite
// config.h's own doc comment promising invalid values are rejected.
std::optional<long long> require_int_at_least(const std::string& raw, const std::string& field_name,
                                               const std::string& context, long long min_value) {
    auto v = try_parse_int(raw);
    if (!v) {
        warn(context + ": invalid integer for '" + field_name + "': '" + raw + "', keeping default");
        return std::nullopt;
    }
    if (*v < min_value) {
        warn(context + ": '" + field_name + "' must be >= " + std::to_string(min_value) + ", got '" +
             raw + "', keeping default");
        return std::nullopt;
    }
    return v;
}

// Applies a single global (non-[feed]) key=value pair to config. Shared by the
// file parser; the CLI has its own flags for the same fields (getopt_long
// doesn't let us reuse a key=value dispatch table cleanly), see
// apply_cli_overrides.
void apply_global_key(Config& config, const std::string& key, const std::string& value,
                       int line_number) {
    const std::string context = "line " + std::to_string(line_number);

    if (key == "heartbeat_interval_ms") {
        if (auto v = require_int_at_least(value, key, context, 1))
            config.heartbeat_interval = std::chrono::milliseconds(*v);
    } else if (key == "heartbeat_timeout_ms") {
        if (auto v = require_int_at_least(value, key, context, 1))
            config.heartbeat_timeout = std::chrono::milliseconds(*v);
    } else if (key == "max_missed_heartbeats") {
        if (auto v = require_int_at_least(value, key, context, 1)) config.max_missed_heartbeats = static_cast<int>(*v);
    } else if (key == "reconnect_base_delay_ms") {
        if (auto v = require_int_at_least(value, key, context, 1))
            config.reconnect_base_delay = std::chrono::milliseconds(*v);
    } else if (key == "reconnect_max_delay_ms") {
        if (auto v = require_int_at_least(value, key, context, 1))
            config.reconnect_max_delay = std::chrono::milliseconds(*v);
    } else if (key == "max_reconnect_attempts") {
        // -1 is a legitimate sentinel ("unlimited", see config.h), not an error.
        if (auto v = require_int_at_least(value, key, context, -1))
            config.max_reconnect_attempts = static_cast<int>(*v);
    } else if (key == "epoll_max_events") {
        if (auto v = require_int_at_least(value, key, context, 1)) config.epoll_max_events = static_cast<int>(*v);
    } else if (key == "tick_interval_ms") {
        if (auto v = require_int_at_least(value, key, context, 1))
            config.tick_interval = std::chrono::milliseconds(*v);
    } else if (key == "tcp_nodelay") {
        if (auto v = try_parse_bool(value)) {
            config.tcp_nodelay = *v;
        } else {
            warn("line " + std::to_string(line_number) + ": invalid boolean for 'tcp_nodelay': '" +
                 value + "', keeping default");
        }
    } else if (key == "edge_triggered") {
        if (auto v = try_parse_bool(value)) {
            config.edge_triggered = *v;
        } else {
            warn("line " + std::to_string(line_number) +
                 ": invalid boolean for 'edge_triggered': '" + value + "', keeping default");
        }
    } else if (key == "log_level") {
        if (is_valid_log_level(value)) {
            config.log_level = value;
        } else {
            warn("line " + std::to_string(line_number) + ": invalid log_level '" + value +
                 "', keeping default '" + config.log_level + "'");
        }
    } else {
        warn("line " + std::to_string(line_number) + ": unknown config key '" + key +
             "', ignoring");
    }
}

// A [feed] section accumulated line-by-line before validation and commit.
struct PendingFeed {
    std::optional<std::string> id;
    std::optional<std::string> name;
    std::optional<std::string> host;
    std::optional<std::string> port;
    int start_line = 0;
};

// Validates and, if valid, appends the feed to config.feeds. Invalid or
// duplicate-id feeds are logged and dropped -- one bad [feed] section never
// prevents the other feeds in the file (or on the CLI) from being monitored.
// Shared by the file parser and the CLI's --feed flag.
void validate_and_add_feed(Config& config, const std::optional<std::string>& id_str,
                            const std::optional<std::string>& name,
                            const std::optional<std::string>& host,
                            const std::optional<std::string>& port_str,
                            const std::string& context) {
    if (!id_str || !name || !host || !port_str) {
        warn(context + ": incomplete feed definition (need id, name, host, port), skipping");
        return;
    }
    auto id_value = try_parse_int(*id_str);
    // feed_id is stored as uint32_t (FeedEndpoint::feed_id, config.h); without
    // the upper-bound check, a too-large value (a typo, or a pasted unrelated
    // number) would pass this check and then silently wrap around at the
    // static_cast<uint32_t> below instead of being rejected -- e.g.
    // id=99999999999 would silently become feed_id 1410065407.
    if (!id_value || *id_value < 0 || *id_value > static_cast<long long>(std::numeric_limits<uint32_t>::max())) {
        warn(context + ": invalid feed id '" + *id_str + "' (must be 0-" +
             std::to_string(std::numeric_limits<uint32_t>::max()) + "), skipping");
        return;
    }
    if (host->empty()) {
        warn(context + ": empty feed host, skipping");
        return;
    }
    auto port_value = try_parse_int(*port_str);
    if (!port_value || *port_value < 1 || *port_value > 65535) {
        warn(context + ": invalid feed port '" + *port_str + "' (must be 1-65535), skipping");
        return;
    }
    const uint32_t feed_id = static_cast<uint32_t>(*id_value);
    for (const auto& existing : config.feeds) {
        if (existing.feed_id == feed_id) {
            warn(context + ": duplicate feed id " + std::to_string(feed_id) + ", skipping");
            return;
        }
    }
    config.feeds.push_back(FeedEndpoint{feed_id, *name, *host, static_cast<uint16_t>(*port_value)});
}

void commit_pending_feed(Config& config, const PendingFeed& pending) {
    if (pending.start_line == 0) {
        return;  // no [feed] section has been opened yet
    }
    validate_and_add_feed(config, pending.id, pending.name, pending.host, pending.port,
                           "[feed] at line " + std::to_string(pending.start_line));
}

}  // namespace

Config parse_config_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("could not open config file: " + path);
    }

    Config config;
    PendingFeed pending;
    bool in_feed_section = false;
    std::string raw_line;
    int line_number = 0;

    while (std::getline(file, raw_line)) {
        ++line_number;
        const std::string line = trim(raw_line);
        if (line.empty() || line[0] == '#' || line[0] == ';') {
            continue;
        }

        if (line == "[feed]") {
            commit_pending_feed(config, pending);
            pending = PendingFeed{};
            pending.start_line = line_number;
            in_feed_section = true;
            continue;
        }

        const size_t eq = line.find('=');
        if (eq == std::string::npos || eq == 0) {
            warn("line " + std::to_string(line_number) + ": malformed line '" + line +
                 "' (expected key=value), skipping");
            continue;
        }
        const std::string key = trim(line.substr(0, eq));
        const std::string value = trim(line.substr(eq + 1));

        if (in_feed_section) {
            if (key == "id") pending.id = value;
            else if (key == "name") pending.name = value;
            else if (key == "host") pending.host = value;
            else if (key == "port") pending.port = value;
            else warn("line " + std::to_string(line_number) + ": unknown feed key '" + key + "', ignoring");
        } else {
            apply_global_key(config, key, value, line_number);
        }
    }
    commit_pending_feed(config, pending);

    return config;
}

namespace {

enum LongOptId {
    kOptHeartbeatIntervalMs = 1000,
    kOptHeartbeatTimeoutMs,
    kOptMaxMissed,
    kOptReconnectBaseDelayMs,
    kOptReconnectMaxDelayMs,
    kOptMaxReconnectAttempts,
    kOptEpollMaxEvents,
    kOptTickIntervalMs,
    kOptTcpNodelay,
    kOptEdgeTriggered,
    kOptLogLevel,
    kOptFeed,
    kOptConfig,
};

// clang-format off
const struct option kLongOptions[] = {
    {"config",                     required_argument, nullptr, kOptConfig},
    {"heartbeat-interval-ms",      required_argument, nullptr, kOptHeartbeatIntervalMs},
    {"heartbeat-timeout-ms",       required_argument, nullptr, kOptHeartbeatTimeoutMs},
    {"max-missed",                 required_argument, nullptr, kOptMaxMissed},
    {"reconnect-base-delay-ms",    required_argument, nullptr, kOptReconnectBaseDelayMs},
    {"reconnect-max-delay-ms",     required_argument, nullptr, kOptReconnectMaxDelayMs},
    {"max-reconnect-attempts",     required_argument, nullptr, kOptMaxReconnectAttempts},
    {"epoll-max-events",           required_argument, nullptr, kOptEpollMaxEvents},
    {"tick-interval-ms",           required_argument, nullptr, kOptTickIntervalMs},
    {"tcp-nodelay",                required_argument, nullptr, kOptTcpNodelay},
    {"edge-triggered",             required_argument, nullptr, kOptEdgeTriggered},
    {"log-level",                  required_argument, nullptr, kOptLogLevel},
    {"feed",                       required_argument, nullptr, kOptFeed},
    {"help",                       no_argument,        nullptr, 'h'},
    {nullptr, 0, nullptr, 0},
};
// clang-format on

// Splits "id:name:host:port" for --feed. Returns false if the shape is wrong
// (wrong field count); validate_and_add_feed still validates field contents.
bool split_feed_spec(const std::string& spec, std::string* id, std::string* name,
                      std::string* host, std::string* port) {
    std::vector<std::string> parts;
    std::stringstream ss(spec);
    std::string part;
    while (std::getline(ss, part, ':')) {
        parts.push_back(part);
    }
    if (parts.size() != 4) {
        return false;
    }
    *id = parts[0];
    *name = parts[1];
    *host = parts[2];
    *port = parts[3];
    return true;
}

}  // namespace

Config apply_cli_overrides(Config base, int argc, char** argv) {
    // getopt_long is process-global state (optind); reset it so repeated calls
    // in the same process -- e.g. from config_test.cpp's individual test
    // cases -- don't see stale state from a previous call.
    optind = 1;
    opterr = 0;  // we print our own diagnostics below

    Config config = std::move(base);

    // First pass: --config only. If present, it becomes the new base, and CLI
    // flags (applied in the second pass) still win over whatever it contains --
    // matching the "CLI > --config file > built-in defaults" precedence
    // documented in config.h.
    int c;
    while ((c = getopt_long(argc, argv, "h", kLongOptions, nullptr)) != -1) {
        if (c == kOptConfig) {
            try {
                config = parse_config_file(optarg);
            } catch (const std::exception& e) {
                warn(std::string("--config: ") + e.what() + ", ignoring");
            }
        }
    }

    optind = 1;
    while ((c = getopt_long(argc, argv, "h", kLongOptions, nullptr)) != -1) {
        switch (c) {
            case kOptConfig:
                break;  // handled in the first pass
            case 'h':
                config.help_requested = true;
                break;
            case kOptHeartbeatIntervalMs:
                if (auto v = require_int_at_least(optarg, "heartbeat-interval-ms", "--heartbeat-interval-ms", 1))
                    config.heartbeat_interval = std::chrono::milliseconds(*v);
                break;
            case kOptHeartbeatTimeoutMs:
                if (auto v = require_int_at_least(optarg, "heartbeat-timeout-ms", "--heartbeat-timeout-ms", 1))
                    config.heartbeat_timeout = std::chrono::milliseconds(*v);
                break;
            case kOptMaxMissed:
                if (auto v = require_int_at_least(optarg, "max-missed", "--max-missed", 1))
                    config.max_missed_heartbeats = static_cast<int>(*v);
                break;
            case kOptReconnectBaseDelayMs:
                if (auto v = require_int_at_least(optarg, "reconnect-base-delay-ms", "--reconnect-base-delay-ms", 1))
                    config.reconnect_base_delay = std::chrono::milliseconds(*v);
                break;
            case kOptReconnectMaxDelayMs:
                if (auto v = require_int_at_least(optarg, "reconnect-max-delay-ms", "--reconnect-max-delay-ms", 1))
                    config.reconnect_max_delay = std::chrono::milliseconds(*v);
                break;
            case kOptMaxReconnectAttempts:
                // -1 is a legitimate sentinel ("unlimited", see config.h), not an error.
                if (auto v = require_int_at_least(optarg, "max-reconnect-attempts", "--max-reconnect-attempts", -1))
                    config.max_reconnect_attempts = static_cast<int>(*v);
                break;
            case kOptEpollMaxEvents:
                if (auto v = require_int_at_least(optarg, "epoll-max-events", "--epoll-max-events", 1))
                    config.epoll_max_events = static_cast<int>(*v);
                break;
            case kOptTickIntervalMs:
                if (auto v = require_int_at_least(optarg, "tick-interval-ms", "--tick-interval-ms", 1))
                    config.tick_interval = std::chrono::milliseconds(*v);
                break;
            case kOptTcpNodelay:
                if (auto v = try_parse_bool(optarg)) config.tcp_nodelay = *v;
                else warn(std::string("--tcp-nodelay: invalid boolean '") + optarg + "'");
                break;
            case kOptEdgeTriggered:
                if (auto v = try_parse_bool(optarg)) config.edge_triggered = *v;
                else warn(std::string("--edge-triggered: invalid boolean '") + optarg + "'");
                break;
            case kOptLogLevel:
                if (is_valid_log_level(optarg)) config.log_level = optarg;
                else warn(std::string("--log-level: invalid level '") + optarg + "'");
                break;
            case kOptFeed: {
                std::string id, name, host, port;
                if (split_feed_spec(optarg, &id, &name, &host, &port)) {
                    validate_and_add_feed(config, id, name, host, port,
                                          std::string("--feed '") + optarg + "'");
                } else {
                    warn(std::string("--feed: expected id:name:host:port, got '") + optarg + "'");
                }
                break;
            }
            case '?':
            default:
                warn("unrecognized command-line option, ignoring");
                break;
        }
    }

    return config;
}

void print_usage(const char* prog_name) {
    std::cout <<
        "Usage: " << prog_name << " [options]\n"
        "\n"
        "  --config <path>                   load a config file (CLI flags below still override it)\n"
        "  --feed <id>:<name>:<host>:<port>   add a feed endpoint (repeatable)\n"
        "  --heartbeat-interval-ms <n>        [default 1000]\n"
        "  --heartbeat-timeout-ms <n>         [default 3000]\n"
        "  --max-missed <n>                   consecutive missed heartbeats before FAILED [default 3]\n"
        "  --reconnect-base-delay-ms <n>      [default 500]\n"
        "  --reconnect-max-delay-ms <n>       [default 30000]\n"
        "  --max-reconnect-attempts <n>       -1 = unlimited [default -1]\n"
        "  --epoll-max-events <n>             epoll_wait batch size [default 256]\n"
        "  --tick-interval-ms <n>             heartbeat/timeout check cadence [default 50]\n"
        "  --tcp-nodelay <true|false>         [default true]\n"
        "  --edge-triggered <true|false>      [default true]\n"
        "  --log-level <trace|debug|info|warn|error>  [default info]\n"
        "  -h, --help                         show this message\n";
}
