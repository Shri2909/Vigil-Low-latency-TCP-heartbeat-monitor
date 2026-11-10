#pragma once

// Configuration: built-in defaults, a key=value config file with [feed] sections, and CLI overrides.

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

    // Set by apply_cli_overrides on -h/--help (never read from a file); the caller acts on it.
    bool help_requested = false;
};

// Parses a config file. Bad lines, unknown keys and out-of-range values are warned about and skipped
// (default kept); cross-field violations revert the pair to defaults. Throws std::runtime_error only
// if the file cannot be opened or read.
Config parse_config_file(const std::string& path);

// Applies CLI flags on top of base. Precedence: CLI flags > --config file > base. Bad flags are
// warned about and ignored; never throws or exits.
Config apply_cli_overrides(Config base, int argc, char** argv);

void print_usage(const char* prog_name);
