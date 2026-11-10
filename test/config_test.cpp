#include "config.h"
#include "mini_test.h"

#include <climits>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

namespace {

// Writes contents to a unique temp file and removes it on destruction.
class TempConfigFile {
public:
    explicit TempConfigFile(const std::string& contents) {
        path_ = std::filesystem::temp_directory_path() /
                ("tcp_config_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".conf");
        std::ofstream out(path_);
        out << contents;
    }
    ~TempConfigFile() { std::filesystem::remove(path_); }
    TempConfigFile(const TempConfigFile&) = delete;
    TempConfigFile& operator=(const TempConfigFile&) = delete;

    const std::string& path() const { return path_str_ = path_.string(); }

private:
    std::filesystem::path path_;
    mutable std::string path_str_;
};

// Builds an argv array from tokens; argv[0] is a dummy program name that getopt_long skips.
class Args {
public:
    explicit Args(std::vector<std::string> tokens) : tokens_(std::move(tokens)) {
        pointers_.push_back(const_cast<char*>("prog"));
        for (auto& t : tokens_) {
            pointers_.push_back(t.data());
        }
    }
    int argc() const { return static_cast<int>(pointers_.size()); }
    char** argv() { return pointers_.data(); }

private:
    std::vector<std::string> tokens_;
    std::vector<char*> pointers_;
};

}  // namespace

TEST_CASE("missing optional keys fall back to defaults") {
    TempConfigFile file("heartbeat_interval_ms=2000\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(2000));
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(3000));
    CHECK(config.max_missed_heartbeats == 3);
    CHECK(config.tcp_nodelay == true);
}

TEST_CASE("multiple [feed] sections parsed correctly") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=NASDAQ\n"
        "host=127.0.0.1\n"
        "port=9001\n"
        "\n"
        "[feed]\n"
        "id=2\n"
        "name=NYSE\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 2);
    CHECK(config.feeds[0].feed_id == 1);
    CHECK(config.feeds[0].name == "NASDAQ");
    CHECK(config.feeds[0].port == 9001);
    CHECK(config.feeds[1].feed_id == 2);
    CHECK(config.feeds[1].name == "NYSE");
    CHECK(config.feeds[1].port == 9002);
}

TEST_CASE("malformed line is skipped, rest of the file still parses") {
    TempConfigFile file(
        "heartbeat_interval_ms=2000\n"
        "this line has no equals sign\n"
        "heartbeat_timeout_ms=5000\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(2000));
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(5000));
}

TEST_CASE("invalid feed port is rejected, other feeds survive") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=BadFeed\n"
        "host=127.0.0.1\n"
        "port=99999\n"  // out of range
        "\n"
        "[feed]\n"
        "id=2\n"
        "name=GoodFeed\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "GoodFeed");
}

TEST_CASE("an oversized feed id is rejected rather than silently wrapping around") {
    // 99999999999 would truncate to 1410065407 in a uint32_t cast.
    TempConfigFile file(
        "[feed]\n"
        "id=99999999999\n"
        "name=BadFeed\n"
        "host=127.0.0.1\n"
        "port=9001\n"
        "\n"
        "[feed]\n"
        "id=2\n"
        "name=GoodFeed\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "GoodFeed");
}

TEST_CASE("non-positive numeric config fields are rejected, not silently accepted") {
    TempConfigFile file(
        "heartbeat_interval_ms=0\n"
        "heartbeat_timeout_ms=-100\n"
        "tick_interval_ms=-1\n"
        "epoll_max_events=0\n"
        "max_missed_heartbeats=-5\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(1000));  // built-in default, untouched
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(3000));
    CHECK(config.tick_interval == std::chrono::milliseconds(50));
    CHECK(config.epoll_max_events == 256);
    CHECK(config.max_missed_heartbeats == 3);
}

TEST_CASE("max_reconnect_attempts=-1 is accepted as the documented 'unlimited' sentinel") {
    TempConfigFile file("max_reconnect_attempts=-1\n");
    Config config = parse_config_file(file.path());
    CHECK(config.max_reconnect_attempts == -1);
}

TEST_CASE("--epoll-max-events 0 on the CLI is rejected, not silently applied") {
    Args args({"--epoll-max-events", "0"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.epoll_max_events == 256);  // built-in default, untouched
}

TEST_CASE("invalid feed host (empty) is rejected") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=NoHost\n"
        "host=\n"
        "port=9001\n");
    Config config = parse_config_file(file.path());
    CHECK(config.feeds.empty());
}

TEST_CASE("duplicate feed id is rejected, first one wins") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=First\n"
        "host=127.0.0.1\n"
        "port=9001\n"
        "\n"
        "[feed]\n"
        "id=1\n"
        "name=Second\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "First");
}

TEST_CASE("unknown top-level key is ignored without affecting other keys") {
    TempConfigFile file(
        "totally_made_up_key=123\n"
        "heartbeat_interval_ms=750\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(750));
}

TEST_CASE("nonexistent config file throws") {
    bool threw = false;
    try {
        parse_config_file("/nonexistent/path/that/should/not/exist.conf");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

TEST_CASE("CLI overrides win over built-in defaults") {
    Args args({"--heartbeat-interval-ms", "1234"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(1234));
}

TEST_CASE("CLI overrides win over a loaded --config file") {
    TempConfigFile file("heartbeat_interval_ms=2000\n");
    Args args({"--config", file.path(), "--heartbeat-interval-ms", "2500"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(2500));
}

TEST_CASE("fields not overridden on the CLI keep the --config file's values") {
    TempConfigFile file("heartbeat_interval_ms=2000\nheartbeat_timeout_ms=6000\n");
    Args args({"--config", file.path(), "--heartbeat-interval-ms", "4000"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(4000));
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(6000));
}

TEST_CASE("--feed adds a validated feed endpoint") {
    Args args({"--feed", "3:LSE:127.0.0.1:9003"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].feed_id == 3);
    CHECK(config.feeds[0].name == "LSE");
    CHECK(config.feeds[0].host == "127.0.0.1");
    CHECK(config.feeds[0].port == 9003);
}

TEST_CASE("--feed with wrong field count is rejected") {
    Args args({"--feed", "not:enough:fields"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.feeds.empty());
}

TEST_CASE("--help sets help_requested without side effects") {
    Args args({"--help"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.help_requested == true);
}

TEST_CASE("repeated apply_cli_overrides calls do not interfere with each other") {
    // getopt_long keeps global state (optind); each call must reset it.
    Args first({"--heartbeat-interval-ms", "111"});
    Config c1 = apply_cli_overrides(Config{}, first.argc(), first.argv());
    Args second({"--heartbeat-interval-ms", "222"});
    Config c2 = apply_cli_overrides(Config{}, second.argc(), second.argv());
    CHECK(c1.heartbeat_interval == std::chrono::milliseconds(111));
    CHECK(c2.heartbeat_interval == std::chrono::milliseconds(222));
}

// Upper bounds
TEST_CASE("int fields above INT_MAX and durations above one day are rejected, not truncated") {
    // 4294967297 would wrap to 1 in a static_cast<int>; 86'400'001 ms is just over the one-day cap.
    TempConfigFile file(
        "max_missed_heartbeats=4294967297\n"
        "max_reconnect_attempts=2147483648\n"
        "epoll_max_events=2147483647\n"
        "heartbeat_interval_ms=99999999999\n"
        "heartbeat_timeout_ms=86400001\n"
        "tick_interval_ms=9223372036854775807\n"
        "reconnect_base_delay_ms=86400001\n"
        "reconnect_max_delay_ms=9999999999999\n");
    Config config = parse_config_file(file.path());
    const Config defaults;
    CHECK(config.max_missed_heartbeats == defaults.max_missed_heartbeats);
    CHECK(config.max_reconnect_attempts == defaults.max_reconnect_attempts);
    CHECK(config.epoll_max_events == defaults.epoll_max_events);
    CHECK(config.heartbeat_interval == defaults.heartbeat_interval);
    CHECK(config.heartbeat_timeout == defaults.heartbeat_timeout);
    CHECK(config.tick_interval == defaults.tick_interval);
    CHECK(config.reconnect_base_delay == defaults.reconnect_base_delay);
    CHECK(config.reconnect_max_delay == defaults.reconnect_max_delay);
}

TEST_CASE("the maximum allowed values are accepted") {
    TempConfigFile file(
        "max_missed_heartbeats=2147483647\n"
        "max_reconnect_attempts=2147483647\n"
        "heartbeat_interval_ms=86399999\n"
        "heartbeat_timeout_ms=86400000\n");
    Config config = parse_config_file(file.path());
    CHECK(config.max_missed_heartbeats == INT_MAX);
    CHECK(config.max_reconnect_attempts == INT_MAX);
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(86'399'999));
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(86'400'000));
}

TEST_CASE("CLI rejects out-of-range values the same way as the file parser") {
    Args args({"--max-missed", "4294967297", "--heartbeat-timeout-ms", "86400001", "--max-reconnect-attempts",
               "2147483648", "--tick-interval-ms", "99999999999"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    const Config defaults;
    CHECK(config.max_missed_heartbeats == defaults.max_missed_heartbeats);
    CHECK(config.heartbeat_timeout == defaults.heartbeat_timeout);
    CHECK(config.max_reconnect_attempts == defaults.max_reconnect_attempts);
    CHECK(config.tick_interval == defaults.tick_interval);
}

// Cross-field validation
TEST_CASE("heartbeat_interval >= heartbeat_timeout is rejected (both revert to defaults)") {
    TempConfigFile file("heartbeat_interval_ms=5000\nheartbeat_timeout_ms=2000\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(1000));
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(3000));

    TempConfigFile equal("heartbeat_interval_ms=2000\nheartbeat_timeout_ms=2000\n");
    Config c2 = parse_config_file(equal.path());
    CHECK(c2.heartbeat_interval == std::chrono::milliseconds(1000));
    CHECK(c2.heartbeat_timeout == std::chrono::milliseconds(3000));
}

TEST_CASE("reconnect_base_delay > reconnect_max_delay is rejected (both revert to defaults)") {
    TempConfigFile file("reconnect_base_delay_ms=9000\nreconnect_max_delay_ms=1000\n");
    Config config = parse_config_file(file.path());
    CHECK(config.reconnect_base_delay == std::chrono::milliseconds(500));
    CHECK(config.reconnect_max_delay == std::chrono::milliseconds(30000));

    TempConfigFile equal("reconnect_base_delay_ms=1000\nreconnect_max_delay_ms=1000\n");  // equal is allowed
    Config c2 = parse_config_file(equal.path());
    CHECK(c2.reconnect_base_delay == std::chrono::milliseconds(1000));
    CHECK(c2.reconnect_max_delay == std::chrono::milliseconds(1000));
}

TEST_CASE("tick_interval > heartbeat_interval is not accepted") {
    TempConfigFile file("heartbeat_interval_ms=100\ntick_interval_ms=500\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(100));
    CHECK(config.tick_interval <= config.heartbeat_interval);
}

TEST_CASE("cross-field rules also apply to CLI overrides") {
    Args bad({"--heartbeat-interval-ms", "5000"});  // >= default timeout of 3000
    Config config = apply_cli_overrides(Config{}, bad.argc(), bad.argv());
    CHECK(config.heartbeat_interval < config.heartbeat_timeout);

    Args bad_reconnect({"--reconnect-base-delay-ms", "60000"});  // > default max of 30000
    Config c2 = apply_cli_overrides(Config{}, bad_reconnect.argc(), bad_reconnect.argv());
    CHECK(c2.reconnect_base_delay <= c2.reconnect_max_delay);

    Args ok({"--heartbeat-interval-ms", "500", "--heartbeat-timeout-ms", "1500", "--tick-interval-ms", "100"});
    Config c3 = apply_cli_overrides(Config{}, ok.argc(), ok.argv());
    CHECK(c3.heartbeat_interval == std::chrono::milliseconds(500));
    CHECK(c3.heartbeat_timeout == std::chrono::milliseconds(1500));
    CHECK(c3.tick_interval == std::chrono::milliseconds(100));
}

// File format: sections, comments, path errors, booleans
TEST_CASE("an unknown [section] is skipped and its keys are not attributed to the previous feed") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=A\n"
        "host=127.0.0.1\n"
        "port=9001\n"
        "[logging]\n"
        "name=SHOULD_NOT_OVERWRITE\n"
        "port=1\n"
        "heartbeat_interval_ms=1\n"
        "[feed]\n"
        "id=2\n"
        "name=B\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 2);
    CHECK(config.feeds[0].name == "A");
    CHECK(config.feeds[0].port == 9001);
    CHECK(config.feeds[1].name == "B");
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(1000));  // the key inside [logging] was skipped
}

TEST_CASE("inline ' #' comments are stripped from values; '#' glued to a value is kept") {
    TempConfigFile file(
        "heartbeat_interval_ms=750   # faster pings\n"
        "max_missed_heartbeats=4\t# trailing\n"
        "[feed]\n"
        "id=1\n"
        "name=Ex#1\n"
        "host=127.0.0.1 # loopback\n"
        "port=9001\n");
    Config config = parse_config_file(file.path());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(750));
    CHECK(config.max_missed_heartbeats == 4);
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "Ex#1");
    CHECK(config.feeds[0].host == "127.0.0.1");
}

TEST_CASE("a config path that is a directory throws") {
    bool threw = false;
    try {
        parse_config_file(std::filesystem::temp_directory_path().string());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

TEST_CASE("booleans are case-insensitive") {
    TempConfigFile file("tcp_nodelay=FALSE\nedge_triggered=No\n");
    Config config = parse_config_file(file.path());
    CHECK(config.tcp_nodelay == false);
    CHECK(config.edge_triggered == false);

    TempConfigFile on("tcp_nodelay=Yes\nedge_triggered=TRUE\n");
    Config c2 = parse_config_file(on.path());
    CHECK(c2.tcp_nodelay == true);
    CHECK(c2.edge_triggered == true);
}

TEST_CASE("tcp_nodelay and edge_triggered: valid values apply, invalid ones keep the default") {
    TempConfigFile file("tcp_nodelay=0\nedge_triggered=0\n");
    Config config = parse_config_file(file.path());
    CHECK(config.tcp_nodelay == false);
    CHECK(config.edge_triggered == false);

    TempConfigFile bad("tcp_nodelay=maybe\nedge_triggered=2\n");
    Config c2 = parse_config_file(bad.path());
    CHECK(c2.tcp_nodelay == true);
    CHECK(c2.edge_triggered == true);
}

TEST_CASE("reconnect delays, max_reconnect_attempts and tick_interval parse from a file") {
    TempConfigFile file(
        "reconnect_base_delay_ms=100\n"
        "reconnect_max_delay_ms=2000\n"
        "max_reconnect_attempts=5\n"
        "tick_interval_ms=25\n");
    Config config = parse_config_file(file.path());
    CHECK(config.reconnect_base_delay == std::chrono::milliseconds(100));
    CHECK(config.reconnect_max_delay == std::chrono::milliseconds(2000));
    CHECK(config.max_reconnect_attempts == 5);
    CHECK(config.tick_interval == std::chrono::milliseconds(25));
}

TEST_CASE("an incomplete [feed] section is skipped") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=NoPort\n"
        "host=127.0.0.1\n"
        "[feed]\n"
        "id=2\n"
        "name=Complete\n"
        "host=127.0.0.1\n"
        "port=9002\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "Complete");
}

TEST_CASE("an unknown key inside a [feed] section is ignored, the feed still loads") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=A\n"
        "color=blue\n"
        "host=127.0.0.1\n"
        "port=9001\n");
    Config config = parse_config_file(file.path());
    REQUIRE(config.feeds.size() == 1);
    CHECK(config.feeds[0].name == "A");
}

TEST_CASE("an empty feed name is rejected") {
    TempConfigFile file(
        "[feed]\n"
        "id=1\n"
        "name=\n"
        "host=127.0.0.1\n"
        "port=9001\n");
    CHECK(parse_config_file(file.path()).feeds.empty());
}

// CLI flags
TEST_CASE("--feed rejects an empty name or host, and a trailing/extra field") {
    for (const char* spec : {"1::127.0.0.1:9001", "1:A::9001", "1:A:127.0.0.1:", "1:A:127.0.0.1:9001:extra",
                              "1:A:127.0.0.1"}) {
        Args args({"--feed", spec});
        Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
        CHECK(config.feeds.empty());
    }
}

TEST_CASE("CLI flags for reconnect attempts, tick interval, tcp-nodelay and edge-triggered apply") {
    Args args({"--max-reconnect-attempts", "7", "--tick-interval-ms", "20", "--tcp-nodelay", "false",
               "--edge-triggered", "NO", "--reconnect-base-delay-ms", "200",
               "--reconnect-max-delay-ms", "4000", "--max-missed", "5", "--epoll-max-events", "64"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.max_reconnect_attempts == 7);
    CHECK(config.tick_interval == std::chrono::milliseconds(20));
    CHECK(config.tcp_nodelay == false);
    CHECK(config.edge_triggered == false);
    CHECK(config.reconnect_base_delay == std::chrono::milliseconds(200));
    CHECK(config.reconnect_max_delay == std::chrono::milliseconds(4000));
    CHECK(config.max_missed_heartbeats == 5);
    CHECK(config.epoll_max_events == 64);
}

TEST_CASE("--max-reconnect-attempts -1 is accepted on the CLI; below -1 is rejected") {
    Args ok({"--max-reconnect-attempts", "-1"});
    CHECK(apply_cli_overrides(Config{}, ok.argc(), ok.argv()).max_reconnect_attempts == -1);
    Args bad({"--max-reconnect-attempts", "-2"});
    CHECK(apply_cli_overrides(Config{}, bad.argc(), bad.argv()).max_reconnect_attempts == -1);  // default is -1 too
    Config base;
    base.max_reconnect_attempts = 4;
    Args bad2({"--max-reconnect-attempts", "-2"});
    CHECK(apply_cli_overrides(base, bad2.argc(), bad2.argv()).max_reconnect_attempts == 4);
}

TEST_CASE("invalid CLI booleans keep the existing value") {
    Args args({"--tcp-nodelay", "maybe", "--edge-triggered", "2"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.tcp_nodelay == true);
    CHECK(config.edge_triggered == true);
}

TEST_CASE("an unknown CLI option is ignored without affecting the rest") {
    Args args({"--no-such-flag", "--heartbeat-interval-ms", "800"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(800));
    CHECK(config.help_requested == false);
}

TEST_CASE("a --config path that cannot be read is ignored, later flags still apply") {
    Args args({"--config", "/nonexistent/dir/x.conf", "--max-missed", "6"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.max_missed_heartbeats == 6);
}
