#include "config.h"
#include "mini_test.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

namespace {

// Writes contents to a uniquely-named file under the system temp dir and
// removes it on destruction, so each test leaves no state behind for the next
// one -- config_test.cpp tests run in-process, back to back.
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

// Builds an argv-style array from a list of strings whose lifetime covers the
// call. getopt_long expects argv[0] to be a program name it skips.
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
    // everything else untouched by the file keeps Config{}'s built-in defaults
    CHECK(config.heartbeat_timeout == std::chrono::milliseconds(3000));
    CHECK(config.max_missed_heartbeats == 3);
    CHECK(config.tcp_nodelay == true);
    CHECK(config.log_level == "info");
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
    // Regression test for a real bug: id_value was only checked for < 0, not
    // for exceeding uint32_t's range, before being static_cast<uint32_t> --
    // an oversized value like this one used to silently truncate to whatever
    // value % 2^32 comes out to (1410065407 for this exact input) instead of
    // being rejected, like every other invalid field in this file is.
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
    // Regression test: these fields used to accept any parseable integer
    // including 0/negative, contradicting config.h's doc comment that
    // invalid values are rejected. Each bad line should be warned-and-skipped
    // (default kept), same as every other validated field, not silently used.
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
    // -1 must NOT be rejected by the same non-positive check applied to every
    // other numeric field above -- it's a legitimate, documented value here.
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
    Args args({"--heartbeat-interval-ms", "1234", "--log-level", "debug"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(1234));
    CHECK(config.log_level == "debug");
}

TEST_CASE("CLI overrides win over a loaded --config file") {
    TempConfigFile file("heartbeat_interval_ms=2000\n");
    Args args({"--config", file.path(), "--heartbeat-interval-ms", "9999"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(9999));
}

TEST_CASE("fields not overridden on the CLI keep the --config file's values") {
    TempConfigFile file("heartbeat_interval_ms=2000\nheartbeat_timeout_ms=6000\n");
    Args args({"--config", file.path(), "--heartbeat-interval-ms", "9999"});
    Config config = apply_cli_overrides(Config{}, args.argc(), args.argv());
    CHECK(config.heartbeat_interval == std::chrono::milliseconds(9999));
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
    // Regression guard for getopt_long's global optind: two calls in the same
    // process, back to back, must each parse their own argv correctly.
    Args first({"--heartbeat-interval-ms", "111"});
    Config c1 = apply_cli_overrides(Config{}, first.argc(), first.argv());
    Args second({"--heartbeat-interval-ms", "222"});
    Config c2 = apply_cli_overrides(Config{}, second.argc(), second.argv());
    CHECK(c1.heartbeat_interval == std::chrono::milliseconds(111));
    CHECK(c2.heartbeat_interval == std::chrono::milliseconds(222));
}
