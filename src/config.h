#pragma once

// Configuration: defaults, config-file parsing, and CLI overrides.
// See PROJECT_PLAN.md section 8. No parser dependency -- both the file format
// and the CLI flags are hand-rolled (key=value + repeated [feed] sections;
// getopt_long for the CLI).

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

struct FeedEndpoint {
    uint32_t feed_id = 0;
    std::string name;
    std::string host;
    uint16_t port = 0;
};

struct Config {
    std::chrono::milliseconds heartbeat_interval{1000};
    std::chrono::milliseconds heartbeat_timeout{3000};
    int max_missed_heartbeats = 3;

    std::chrono::milliseconds reconnect_base_delay{500};
    std::chrono::milliseconds reconnect_max_delay{30000};
    int max_reconnect_attempts = -1;  // -1 = unlimited

    int epoll_max_events = 256;
    std::chrono::milliseconds tick_interval{50};
    bool tcp_nodelay = true;   // exposed so latency_bench can A/B it
    bool edge_triggered = true;

    std::vector<FeedEndpoint> feeds;
    std::string log_level = "info";  // trace|debug|info|warn|error

    // Set by apply_cli_overrides when -h/--help is present. Not read back from
    // a config file. Kept on Config (rather than a bare exit() call inside the
    // parser) so parsing stays a pure function main.cpp can act on -- important
    // for testability, since config_test.cpp calls apply_cli_overrides directly
    // and must never have it terminate the test process.
    bool help_requested = false;
};

// Parses a key=value config file with repeated [feed] sections (format
// documented in PROJECT_PLAN.md section 8). Malformed lines, unknown keys, and
// invalid values are logged to stderr and skipped -- one bad line never aborts
// the rest of the file. Only a file that can't be opened throws
// (std::runtime_error): that's a system-boundary failure, not a content one.
Config parse_config_file(const std::string& path);

// Applies CLI flags on top of base. If argv contains --config <path>, that
// file is loaded first (via parse_config_file) and used as the new base before
// the remaining flags are applied -- so precedence is: CLI flags > --config
// file > base's built-in defaults. Never calls exit()/throws for bad flags;
// unrecognized or malformed flags are logged to stderr and ignored, matching
// the config-file parser's "never abort on bad input" policy.
Config apply_cli_overrides(Config base, int argc, char** argv);

void print_usage(const char* prog_name);
