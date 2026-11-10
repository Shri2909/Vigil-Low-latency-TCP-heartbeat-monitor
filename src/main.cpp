#include "cli_display.h"
#include "config.h"
#include "feed_monitor.h"

#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

namespace {

FeedMonitor* g_monitor = nullptr;

// Async-signal-safe: stop() is an atomic store plus an eventfd write(); errno is preserved.
void handle_signal(int) {
    const int saved_errno = errno;
    if (g_monitor != nullptr) {
        g_monitor->stop();
    }
    errno = saved_errno;
}

// Makes a fd non-blocking and restores its flags on destruction (O_NONBLOCK is
// shared with the parent shell via the open file description).
class NonBlockingGuard {
public:
    explicit NonBlockingGuard(int fd) : fd_(fd) {
        const int flags = ::fcntl(fd_, F_GETFL, 0);
        if (flags < 0) {
            std::cerr << "[feed_monitor] fcntl(F_GETFL) failed: " << std::strerror(errno) << "\n";
            return;
        }
        if ((flags & O_NONBLOCK) == 0 && ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::cerr << "[feed_monitor] fcntl(F_SETFL, O_NONBLOCK) failed: " << std::strerror(errno) << "\n";
            return;
        }
        original_flags_ = (flags & O_NONBLOCK) == 0 ? flags : -1;  // already non-blocking: nothing to restore
        ok_ = true;
    }
    ~NonBlockingGuard() {
        if (original_flags_ >= 0) {
            ::fcntl(fd_, F_SETFL, original_flags_);
        }
    }
    NonBlockingGuard(const NonBlockingGuard&) = delete;
    NonBlockingGuard& operator=(const NonBlockingGuard&) = delete;

    bool ok() const noexcept { return ok_; }

private:
    int fd_;
    int original_flags_ = -1;
    bool ok_ = false;
};

// Parses exactly one whole decimal argument of type T from iss; prints an error
// and returns false if it is missing, malformed, out of range, or followed by extras.
template <typename T>
bool parse_single_arg(std::istringstream& iss, const std::string& cmd, const char* usage, T& out) {
    std::string token;
    if (!(iss >> token)) {
        std::cerr << "usage: " << usage << "\n";
        return false;
    }
    const char* first = token.data();
    const char* last = first + token.size();
    const auto [ptr, ec] = std::from_chars(first, last, out);
    if (ec != std::errc{} || ptr != last) {
        std::cerr << "[feed_monitor] " << cmd << ": invalid argument '" << token << "' (usage: " << usage << ")\n";
        return false;
    }
    std::string extra;
    if (iss >> extra) {
        std::cerr << "[feed_monitor] " << cmd << ": unexpected extra argument '" << extra << "' (usage: " << usage
                  << ")\n";
        return false;
    }
    return true;
}

// Raises RLIMIT_NOFILE to the hard limit so thousands of feeds fit; logs the result.
void raise_fd_limit() {
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        std::cerr << "[feed_monitor] getrlimit(RLIMIT_NOFILE) failed: " << std::strerror(errno) << "\n";
        return;
    }
    if (limit.rlim_cur < limit.rlim_max) {
        rlimit raised{limit.rlim_max, limit.rlim_max};
        if (::setrlimit(RLIMIT_NOFILE, &raised) == 0) {
            limit.rlim_cur = limit.rlim_max;
        }
    }
    std::cerr << "[feed_monitor] fd limit: soft=" << limit.rlim_cur << " hard=" << limit.rlim_max << "\n";
}

// Parses and dispatches one stdin command (runs on the reactor thread).
void handle_command_line(FeedMonitor& monitor, const std::string& line) {
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;
    if (cmd.empty()) {
        return;
    }

    if (cmd == "quit" || cmd == "exit") {
        monitor.stop();
    } else if (cmd == "disconnect") {
        uint32_t feed_id;
        if (parse_single_arg(iss, cmd, "disconnect <feed_id>", feed_id)) {
            monitor.force_disconnect(feed_id);
        }
    } else if (cmd == "reconnect") {
        uint32_t feed_id;
        if (parse_single_arg(iss, cmd, "reconnect <feed_id>", feed_id)) {
            monitor.force_reconnect(feed_id);
        }
    } else if (cmd == "set-interval") {
        int64_t ms;
        if (parse_single_arg(iss, cmd, "set-interval <ms>", ms)) {
            if (!monitor.set_heartbeat_interval(std::chrono::milliseconds(ms))) {
                std::cerr << "[feed_monitor] set-interval: <ms> must be greater than 0\n";
            }
        }
    } else if (cmd == "set-timeout") {
        int64_t ms;
        if (parse_single_arg(iss, cmd, "set-timeout <ms>", ms)) {
            if (!monitor.set_heartbeat_timeout(std::chrono::milliseconds(ms))) {
                std::cerr << "[feed_monitor] set-timeout: <ms> must be greater than 0\n";
            }
        }
    } else {
        std::cerr << "[feed_monitor] unknown command '" << cmd
                   << "' (try: disconnect <id> | reconnect <id> | set-interval <ms> | set-timeout <ms> | quit)\n";
    }
}

}  // namespace

int main(int argc, char** argv) {
    // Defense in depth: socket sends already use MSG_NOSIGNAL.
    (void)::signal(SIGPIPE, SIG_IGN);

    Config config = apply_cli_overrides(Config{}, argc, argv);
    if (config.help_requested) {
        print_usage(argv[0]);
        return 0;
    }
    if (config.feeds.empty()) {
        std::cerr << "[feed_monitor] no feeds configured -- use --feed id:name:host:port or --config <file>\n";
        print_usage(argv[0]);
        return 1;
    }

    raise_fd_limit();

    // The command reader requires non-blocking stdin; restored when main() returns.
    NonBlockingGuard stdin_guard(STDIN_FILENO);

    try {
        FeedMonitor monitor(config);
        g_monitor = &monitor;

        struct sigaction sa{};
        sa.sa_handler = handle_signal;
        ::sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        ::sigaction(SIGINT, &sa, nullptr);
        ::sigaction(SIGTERM, &sa, nullptr);

        for (const FeedEndpoint& endpoint : config.feeds) {
            if (!monitor.add_feed(endpoint)) {
                std::cerr << "[feed_monitor] failed to add feed " << endpoint.feed_id << " (\""
                           << endpoint.name << "\")\n";
            }
        }

        monitor.set_tick_callback([](const AggregateStats& stats) { cli_display::render(stats); });
        monitor.set_alert_callback([](const std::string& message) { cli_display::add_alert(message); });
        if (!stdin_guard.ok() ||
            !monitor.set_command_input(STDIN_FILENO, [&monitor](const std::string& line) {
                handle_command_line(monitor, line);
            })) {
            std::cerr << "[feed_monitor] stdin commands unavailable; stop with SIGINT/SIGTERM\n";
        }

        monitor.run();
        g_monitor = nullptr;  // monitor is about to be destroyed
        std::cout << "[feed_monitor] shut down\n";
    } catch (const std::exception& e) {
        std::cerr << "[feed_monitor] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
