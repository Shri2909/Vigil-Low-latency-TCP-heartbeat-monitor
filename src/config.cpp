#include "config.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <climits>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>

#include <getopt.h>

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

// Case-insensitive: "TRUE", "True", "No" are all accepted.
std::optional<bool> try_parse_bool(const std::string& raw) {
    std::string s = raw;
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (s == "true" || s == "1" || s == "yes") return true;
    if (s == "false" || s == "0" || s == "no") return false;
    return std::nullopt;
}

// Upper bounds: ints are narrowed from long long (cap at INT_MAX); durations are capped at one day so
// duration_cast<nanoseconds> cannot overflow and typo'd values are rejected.
constexpr long long kMaxDurationMs = 86'400'000;
// epoll_max_events sizes the epoll_wait output array, so INT_MAX would mean a ~25 GB allocation.
constexpr long long kMaxEpollEvents = 1'000'000;

// Parses raw as an integer in [min_value, max_value]; otherwise warns (prefixed with context) and
// returns nullopt so the caller keeps its current value. Shared by the file parser and the CLI.
std::optional<long long> require_int_in_range(const std::string& raw, const std::string& field_name,
                                               const std::string& context, long long min_value,
                                               long long max_value) {
    auto v = try_parse_int(raw);
    if (!v) {
        warn(context + ": invalid integer for '" + field_name + "': '" + raw + "', keeping default");
        return std::nullopt;
    }
    if (*v < min_value || *v > max_value) {
        warn(context + ": '" + field_name + "' must be in [" + std::to_string(min_value) + ", " +
             std::to_string(max_value) + "], got '" + raw + "', keeping default");
        return std::nullopt;
    }
    return v;
}

std::optional<std::chrono::milliseconds> parse_duration_ms(const std::string& raw, const std::string& field_name,
                                                            const std::string& context) {
    if (auto v = require_int_in_range(raw, field_name, context, 1, kMaxDurationMs)) {
        return std::chrono::milliseconds(*v);
    }
    return std::nullopt;
}

std::optional<int> parse_int_field(const std::string& raw, const std::string& field_name,
                                    const std::string& context, long long min_value,
                                    long long max_value = INT_MAX) {
    if (auto v = require_int_in_range(raw, field_name, context, min_value, max_value)) {
        return static_cast<int>(*v);
    }
    return std::nullopt;
}

// Cross-field checks, run once after a whole file/CLI is applied. A violating pair reverts to the
// (mutually consistent) defaults.
void validate_cross_fields(Config& config, const std::string& source) {
    const Config defaults;
    if (config.reconnect_base_delay > config.reconnect_max_delay) {
        warn(source + ": reconnect_base_delay_ms (" + std::to_string(config.reconnect_base_delay.count()) +
             ") must be <= reconnect_max_delay_ms (" + std::to_string(config.reconnect_max_delay.count()) +
             "), reverting both to defaults");
        config.reconnect_base_delay = defaults.reconnect_base_delay;
        config.reconnect_max_delay = defaults.reconnect_max_delay;
    }
    if (config.heartbeat_interval >= config.heartbeat_timeout) {
        warn(source + ": heartbeat_interval_ms (" + std::to_string(config.heartbeat_interval.count()) +
             ") must be < heartbeat_timeout_ms (" + std::to_string(config.heartbeat_timeout.count()) +
             "), reverting both to defaults");
        config.heartbeat_interval = defaults.heartbeat_interval;
        config.heartbeat_timeout = defaults.heartbeat_timeout;
    }
    if (config.tick_interval > config.heartbeat_interval) {
        // A tick coarser than the heartbeat interval can't send pings on time.
        const auto fixed = std::min(defaults.tick_interval, config.heartbeat_interval);
        warn(source + ": tick_interval_ms (" + std::to_string(config.tick_interval.count()) +
             ") must be <= heartbeat_interval_ms (" + std::to_string(config.heartbeat_interval.count()) +
             "), using " + std::to_string(fixed.count()));
        config.tick_interval = fixed;
    }
}

// Applies one global (non-[feed]) key=value pair; the CLI has its own getopt_long dispatch.
void apply_global_key(Config& config, const std::string& key, const std::string& value,
                       int line_number) {
    const std::string context = "line " + std::to_string(line_number);

    if (key == "heartbeat_interval_ms") {
        if (auto v = parse_duration_ms(value, key, context)) config.heartbeat_interval = *v;
    } else if (key == "heartbeat_timeout_ms") {
        if (auto v = parse_duration_ms(value, key, context)) config.heartbeat_timeout = *v;
    } else if (key == "max_missed_heartbeats") {
        if (auto v = parse_int_field(value, key, context, 1)) config.max_missed_heartbeats = *v;
    } else if (key == "reconnect_base_delay_ms") {
        if (auto v = parse_duration_ms(value, key, context)) config.reconnect_base_delay = *v;
    } else if (key == "reconnect_max_delay_ms") {
        if (auto v = parse_duration_ms(value, key, context)) config.reconnect_max_delay = *v;
    } else if (key == "max_reconnect_attempts") {
        // -1 means unlimited.
        if (auto v = parse_int_field(value, key, context, -1)) config.max_reconnect_attempts = *v;
    } else if (key == "epoll_max_events") {
        if (auto v = parse_int_field(value, key, context, 1, kMaxEpollEvents)) config.epoll_max_events = *v;
    } else if (key == "tick_interval_ms") {
        if (auto v = parse_duration_ms(value, key, context)) config.tick_interval = *v;
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

// Validates and appends a feed; invalid or duplicate-id feeds are warned about and dropped. Shared by
// the file parser and --feed.
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
    // Bound-check before the uint32_t cast so a huge id is rejected instead of wrapping.
    if (!id_value || *id_value < 0 || *id_value > static_cast<long long>(std::numeric_limits<uint32_t>::max())) {
        warn(context + ": invalid feed id '" + *id_str + "' (must be 0-" +
             std::to_string(std::numeric_limits<uint32_t>::max()) + "), skipping");
        return;
    }
    if (name->empty()) {
        warn(context + ": empty feed name, skipping");
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
        return;  // no [feed] section open
    }
    validate_and_add_feed(config, pending.id, pending.name, pending.host, pending.port,
                           "[feed] at line " + std::to_string(pending.start_line));
}

}  // namespace

Config parse_config_file(const std::string& path) {
    // ifstream opens a directory "successfully" and then fails every read; reject it up front.
    std::error_code ec;
    if (std::filesystem::is_directory(path, ec)) {
        throw std::runtime_error("config path is a directory, not a file: " + path);
    }
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("could not open config file: " + path);
    }

    Config config;
    PendingFeed pending;
    enum class Section { kGlobal, kFeed, kUnknown } section = Section::kGlobal;
    std::string raw_line;
    int line_number = 0;

    while (std::getline(file, raw_line)) {
        ++line_number;
        // A '#' preceded by whitespace starts an inline comment; a glued '#' stays in the value.
        for (size_t i = 1; i < raw_line.size(); ++i) {
            if (raw_line[i] == '#' && std::isspace(static_cast<unsigned char>(raw_line[i - 1]))) {
                raw_line.erase(i);
                break;
            }
        }
        const std::string line = trim(raw_line);
        if (line.empty() || line[0] == '#' || line[0] == ';') {
            continue;
        }

        if (line.front() == '[' && line.back() == ']') {
            // A section header ends the previous section.
            commit_pending_feed(config, pending);
            pending = PendingFeed{};
            if (line == "[feed]") {
                pending.start_line = line_number;
                section = Section::kFeed;
            } else {
                warn("line " + std::to_string(line_number) + ": unknown section '" + line +
                     "', skipping its keys");
                section = Section::kUnknown;
            }
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

        switch (section) {
            case Section::kFeed:
                if (key == "id") pending.id = value;
                else if (key == "name") pending.name = value;
                else if (key == "host") pending.host = value;
                else if (key == "port") pending.port = value;
                else warn("line " + std::to_string(line_number) + ": unknown feed key '" + key + "', ignoring");
                break;
            case Section::kUnknown:
                break;  // warned at the section header
            case Section::kGlobal:
                apply_global_key(config, key, value, line_number);
                break;
        }
    }
    if (file.bad()) {
        throw std::runtime_error("error reading config file: " + path);
    }
    commit_pending_feed(config, pending);
    validate_cross_fields(config, "config file '" + path + "'");

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
    {"feed",                       required_argument, nullptr, kOptFeed},
    {"help",                       no_argument,        nullptr, 'h'},
    {nullptr, 0, nullptr, 0},
};
// clang-format on

// Splits "id:name:host:port". Empty fields are kept so "1::host:9" is an empty name, not 3 fields;
// validate_and_add_feed checks the contents.
bool split_feed_spec(const std::string& spec, std::string* id, std::string* name,
                      std::string* host, std::string* port) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        const size_t colon = spec.find(':', start);
        if (colon == std::string::npos) {
            parts.push_back(spec.substr(start));
            break;
        }
        parts.push_back(spec.substr(start, colon - start));
        start = colon + 1;
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
    // getopt_long keeps global state; reset it so repeated calls (tests) start clean.
    optind = 1;
    opterr = 0;  // diagnostics are printed below

    Config config = std::move(base);

    // First pass: --config only, so it becomes the base that the CLI flags then override.
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
                if (auto v = parse_duration_ms(optarg, "heartbeat-interval-ms", "--heartbeat-interval-ms"))
                    config.heartbeat_interval = *v;
                break;
            case kOptHeartbeatTimeoutMs:
                if (auto v = parse_duration_ms(optarg, "heartbeat-timeout-ms", "--heartbeat-timeout-ms"))
                    config.heartbeat_timeout = *v;
                break;
            case kOptMaxMissed:
                if (auto v = parse_int_field(optarg, "max-missed", "--max-missed", 1))
                    config.max_missed_heartbeats = *v;
                break;
            case kOptReconnectBaseDelayMs:
                if (auto v = parse_duration_ms(optarg, "reconnect-base-delay-ms", "--reconnect-base-delay-ms"))
                    config.reconnect_base_delay = *v;
                break;
            case kOptReconnectMaxDelayMs:
                if (auto v = parse_duration_ms(optarg, "reconnect-max-delay-ms", "--reconnect-max-delay-ms"))
                    config.reconnect_max_delay = *v;
                break;
            case kOptMaxReconnectAttempts:
                // -1 means unlimited.
                if (auto v = parse_int_field(optarg, "max-reconnect-attempts", "--max-reconnect-attempts", -1))
                    config.max_reconnect_attempts = *v;
                break;
            case kOptEpollMaxEvents:
                if (auto v = parse_int_field(optarg, "epoll-max-events", "--epoll-max-events", 1, kMaxEpollEvents))
                    config.epoll_max_events = *v;
                break;
            case kOptTickIntervalMs:
                if (auto v = parse_duration_ms(optarg, "tick-interval-ms", "--tick-interval-ms"))
                    config.tick_interval = *v;
                break;
            case kOptTcpNodelay:
                if (auto v = try_parse_bool(optarg)) config.tcp_nodelay = *v;
                else warn(std::string("--tcp-nodelay: invalid boolean '") + optarg + "'");
                break;
            case kOptEdgeTriggered:
                if (auto v = try_parse_bool(optarg)) config.edge_triggered = *v;
                else warn(std::string("--edge-triggered: invalid boolean '") + optarg + "'");
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
                warn(std::string("unrecognized or incomplete command-line option '") + argv[optind - 1] +
                     "', ignoring");
                break;
        }
    }

    validate_cross_fields(config, "command line");
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
        "  -h, --help                         show this message\n";
}
