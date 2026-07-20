#include "cli_display.h"
#include "config.h"
#include "feed_monitor.h"

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

// Signal-safe: only touches g_monitor (a plain pointer, read-only here) and
// calls FeedMonitor::stop(), which itself only does an atomic store and a
// write() to an eventfd -- both on the POSIX async-signal-safe list.
void handle_signal(int) {
    if (g_monitor != nullptr) {
        g_monitor->stop();
    }
}

// Attempts to raise RLIMIT_NOFILE to its hard limit before any feeds are
// added -- the spec's "monitor 1000s of connections" claim would otherwise
// silently cap out at whatever the shell's default soft limit is (often
// 1024). Logs the effective limit either way; never fatal on its own since a
// lower limit just means fewer feeds fit, not that the program is broken.
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

// Parses and dispatches one stdin command line. Runs on the reactor thread
// (invoked from FeedMonitor's command-input callback, itself called from
// inside run()'s event loop), so calling monitor's mutating methods here is
// not a cross-thread concern -- see feed_monitor.h's set_command_input.
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
        if (iss >> feed_id) {
            monitor.force_disconnect(feed_id);
        } else {
            std::cerr << "usage: disconnect <feed_id>\n";
        }
    } else if (cmd == "reconnect") {
        uint32_t feed_id;
        if (iss >> feed_id) {
            monitor.force_reconnect(feed_id);
        } else {
            std::cerr << "usage: reconnect <feed_id>\n";
        }
    } else if (cmd == "set-interval") {
        int ms;
        if (iss >> ms) {
            monitor.set_heartbeat_interval(std::chrono::milliseconds(ms));
        } else {
            std::cerr << "usage: set-interval <ms>\n";
        }
    } else if (cmd == "set-timeout") {
        int ms;
        if (iss >> ms) {
            monitor.set_heartbeat_timeout(std::chrono::milliseconds(ms));
        } else {
            std::cerr << "usage: set-timeout <ms>\n";
        }
    } else {
        std::cerr << "[feed_monitor] unknown command '" << cmd
                   << "' (try: disconnect <id> | reconnect <id> | set-interval <ms> | set-timeout <ms> | quit)\n";
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

    // FeedMonitor's command-input reader requires a non-blocking fd (see
    // on_command_fd_readable's drain-to-EAGAIN loop); stdin is blocking by
    // default.
    const int stdin_flags = ::fcntl(STDIN_FILENO, F_GETFL, 0);
    ::fcntl(STDIN_FILENO, F_SETFL, stdin_flags | O_NONBLOCK);

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
        monitor.set_command_input(
            STDIN_FILENO, [&monitor](const std::string& line) { handle_command_line(monitor, line); });

        monitor.run();
        std::cout << "[feed_monitor] shut down\n";
    } catch (const std::exception& e) {
        std::cerr << "[feed_monitor] fatal: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
