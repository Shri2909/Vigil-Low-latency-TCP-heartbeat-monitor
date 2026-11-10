// End-to-end tests: fork()+exec() the real bin/feed_monitor and bin/feed_simulator
// and drive them as an operator would (argv, stdin commands, the live table on
// stdout, signals). The Makefile bakes in PROJECT_BIN_DIR and PROJECT_SOURCE_DIR
// as absolute paths so the test works from any cwd.
//
// Each child is owned by a ChildProcess guard that SIGTERMs, SIGKILLs and reaps
// it on destruction, so a failed REQUIRE never leaves an orphan. Pipe fds are
// O_CLOEXEC and SIGPIPE is ignored so a dead child yields EPIPE, not a crash.

#include "mini_test.h"

#include <cctype>
#include <chrono>
#include <csignal>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef PROJECT_BIN_DIR
#error "PROJECT_BIN_DIR must be defined (see Makefile's e2e_binary_test.o rule)"
#endif
#ifndef PROJECT_SOURCE_DIR
#error "PROJECT_SOURCE_DIR must be defined (see Makefile's e2e_binary_test.o rule)"
#endif

namespace {

// Process-wide, set at static init: writing to an exited child must give EPIPE.
[[maybe_unused]] const bool g_sigpipe_ignored = (std::signal(SIGPIPE, SIG_IGN), true);

// Cap on retained child output; beyond it output is still read (so the child
// never blocks on a full pipe) but discarded.
constexpr size_t kMaxRetainedOutputBytes = size_t{8} * 1024 * 1024;
constexpr int kStartupTimeoutMs = 5000;
constexpr int kStateChangeTimeoutMs = 10000;
constexpr int kExitTimeoutMs = 5000;

std::string bin_path(const char* name) {
    return std::string(PROJECT_BIN_DIR) + "/" + name;
}

// RAII owner of one child process + its stdin/stdout pipes. Move-only.
class ChildProcess {
public:
    ChildProcess() = default;
    ChildProcess(pid_t pid, int stdin_fd, int stdout_fd)
        : pid_(pid), stdin_fd_(stdin_fd), stdout_fd_(stdout_fd) {}
    ChildProcess(const ChildProcess&) = delete;
    ChildProcess& operator=(const ChildProcess&) = delete;
    ChildProcess(ChildProcess&& other) noexcept { *this = std::move(other); }
    ChildProcess& operator=(ChildProcess&& other) noexcept {
        if (this != &other) {
            terminate_and_reap();
            pid_ = std::exchange(other.pid_, -1);
            stdin_fd_ = std::exchange(other.stdin_fd_, -1);
            stdout_fd_ = std::exchange(other.stdout_fd_, -1);
            output = std::move(other.output);
            reaped_ = other.reaped_;
        }
        return *this;
    }
    ~ChildProcess() { terminate_and_reap(); }

    pid_t pid() const { return pid_; }

    // Everything the child has written to stdout+stderr (merged) so far.
    std::string output;

    // Writes one command line (a '\n' is appended) to the child's stdin.
    bool send_line(const std::string& line) {
        if (stdin_fd_ < 0) {
            return false;
        }
        const std::string data = line + "\n";
        size_t off = 0;
        while (off < data.size()) {
            const ssize_t n = ::write(stdin_fd_, data.data() + off, data.size() - off);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                return false;
            }
            off += static_cast<size_t>(n);
        }
        return true;
    }

    void close_stdin() {
        if (stdin_fd_ >= 0) {
            ::close(stdin_fd_);
            stdin_fd_ = -1;
        }
    }

    // Reads whatever is currently available without blocking.
    void drain_output() {
        if (stdout_fd_ < 0) {
            return;
        }
        char buf[4096];
        while (true) {
            const ssize_t n = ::read(stdout_fd_, buf, sizeof(buf));
            if (n > 0) {
                if (output.size() < kMaxRetainedOutputBytes) {
                    output.append(buf, static_cast<size_t>(n));
                }
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return;  // EAGAIN, EOF, or error: nothing more right now
        }
    }

    // Polls the child's stdout until pred() is true or timeout_ms elapses.
    // Returns the final truth of pred() either way.
    bool wait_for_output(int timeout_ms, const std::function<bool()>& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (true) {
            drain_output();
            if (pred()) {
                return true;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            if (stdout_fd_ < 0) {
                return false;
            }
            pollfd pfd{stdout_fd_, POLLIN, 0};
            ::poll(&pfd, 1, 50);
            if (pfd.revents & (POLLHUP | POLLERR)) {
                drain_output();
                // EOF: no more output will ever arrive; give pred() one last look.
                return pred();
            }
        }
    }

    // Waits (bounded) for the child to exit on its own, draining its output
    // meanwhile so it can't block on a full pipe. Reaps it on success.
    bool wait_for_exit(int timeout_ms, int* out_status) {
        if (reaped_) {
            return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            drain_output();
            int status = 0;
            const pid_t rc = ::waitpid(pid_, &status, WNOHANG);
            if (rc == pid_) {
                reaped_ = true;
                drain_output();
                *out_status = status;
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void send_signal(int sig) const {
        if (pid_ > 0 && !reaped_) {
            ::kill(pid_, sig);
        }
    }

private:
    // SIGTERM, bounded wait, SIGKILL, blocking reap; then close fds. Safe to
    // call on an already-reaped or empty guard.
    void terminate_and_reap() noexcept {
        if (pid_ > 0 && !reaped_) {
            int status = 0;
            if (::waitpid(pid_, &status, WNOHANG) != pid_) {
                ::kill(pid_, SIGTERM);
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                bool done = false;
                while (std::chrono::steady_clock::now() < deadline) {
                    if (::waitpid(pid_, &status, WNOHANG) == pid_) {
                        done = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                if (!done) {
                    ::kill(pid_, SIGKILL);
                    while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
                    }
                }
            }
            reaped_ = true;
        }
        close_stdin();
        if (stdout_fd_ >= 0) {
            ::close(stdout_fd_);
            stdout_fd_ = -1;
        }
        pid_ = -1;
    }

    pid_t pid_ = -1;
    int stdin_fd_ = -1;   // parent writes here -> child's stdin
    int stdout_fd_ = -1;  // parent reads here <- child's stdout+stderr (merged)
    bool reaped_ = false;
};

// Spawns path with stdin and stdout+stderr piped to the parent. Only
// async-signal-safe calls run between fork() and execv(), which keeps forking
// safe after earlier tests have used threads.
ChildProcess spawn(const std::string& path, const std::vector<std::string>& args) {
    int in_pipe[2];
    int out_pipe[2];
    REQUIRE(::pipe2(in_pipe, O_CLOEXEC) == 0);
    if (::pipe2(out_pipe, O_CLOEXEC) != 0) {
        ::close(in_pipe[0]);
        ::close(in_pipe[1]);
        REQUIRE(false);
    }

    // Built before fork(): no allocation in the child.
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(path.c_str()));
    for (const auto& a : args) {
        argv.push_back(const_cast<char*>(a.c_str()));
    }
    argv.push_back(nullptr);

    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(in_pipe[0]);
        ::close(in_pipe[1]);
        ::close(out_pipe[0]);
        ::close(out_pipe[1]);
        REQUIRE(false);
    }
    if (pid == 0) {
        // dup2 clears O_CLOEXEC on 0/1/2; the original pipe fds close at exec.
        ::dup2(in_pipe[0], STDIN_FILENO);
        ::dup2(out_pipe[1], STDOUT_FILENO);
        ::dup2(out_pipe[1], STDERR_FILENO);
        ::execv(path.c_str(), argv.data());
        ::_exit(127);  // execv only returns on failure
    }

    ::close(in_pipe[0]);
    ::close(out_pipe[1]);
    const int flags = ::fcntl(out_pipe[0], F_GETFL, 0);
    ::fcntl(out_pipe[0], F_SETFL, flags | O_NONBLOCK);
    return ChildProcess(pid, in_pipe[1], out_pipe[0]);
}

// True once some line in accumulated[search_from:] contains both name and state
// (a table row). The checkpoint stops an earlier occurrence from matching.
bool output_shows_feed_state(const std::string& accumulated, size_t search_from, const std::string& name,
                              const std::string& state) {
    if (search_from > accumulated.size()) {
        return false;
    }
    const std::string tail = accumulated.substr(search_from);
    size_t pos = 0;
    while ((pos = tail.find(name, pos)) != std::string::npos) {
        const size_t line_end = tail.find('\n', pos);
        const std::string line =
            tail.substr(pos, line_end == std::string::npos ? std::string::npos : line_end - pos);
        if (line.find(state) != std::string::npos) {
            return true;
        }
        pos += name.size();
    }
    return false;
}

uint16_t extract_listening_port(const std::string& output) {
    const std::string marker = "listening on port ";
    const size_t pos = output.find(marker);
    REQUIRE(pos != std::string::npos);
    size_t start = pos + marker.size();
    size_t end = start;
    while (end < output.size() && std::isdigit(static_cast<unsigned char>(output[end]))) {
        ++end;
    }
    REQUIRE(end > start);
    return static_cast<uint16_t>(std::stoi(output.substr(start, end - start)));
}

// Starts a simulator on an OS-assigned port and returns once it is listening.
ChildProcess start_simulator(uint16_t* out_port, const std::vector<std::string>& extra_args = {}) {
    std::vector<std::string> args = {"--port", "0", "--feed-id", "1"};
    args.insert(args.end(), extra_args.begin(), extra_args.end());
    ChildProcess sim = spawn(bin_path("feed_simulator"), args);
    REQUIRE(sim.wait_for_output(kStartupTimeoutMs,
                                [&] { return sim.output.find("listening on port") != std::string::npos; }));
    *out_port = extract_listening_port(sim.output);
    return sim;
}

// Starts a monitor with one feed named E2ETEST against `port`, with fast
// timers so state changes are visible within a second or two.
ChildProcess start_monitor(uint16_t port, const std::vector<std::string>& extra_args = {}) {
    std::vector<std::string> args = {"--feed", "1:E2ETEST:127.0.0.1:" + std::to_string(port),
                                     "--heartbeat-interval-ms", "200", "--heartbeat-timeout-ms", "600",
                                     "--max-missed", "2", "--reconnect-base-delay-ms", "200",
                                     "--reconnect-max-delay-ms", "1000"};
    args.insert(args.end(), extra_args.begin(), extra_args.end());
    return spawn(bin_path("feed_monitor"), args);
}

bool wait_for_feed_state(ChildProcess& proc, size_t checkpoint, const std::string& state,
                         int timeout_ms = kStateChangeTimeoutMs) {
    return proc.wait_for_output(
        timeout_ms, [&] { return output_shows_feed_state(proc.output, checkpoint, "E2ETEST", state); });
}

// True once the feed shows any "not healthy" row after checkpoint.
bool wait_for_feed_unhealthy(ChildProcess& proc, size_t checkpoint, int timeout_ms = kStateChangeTimeoutMs) {
    return proc.wait_for_output(timeout_ms, [&] {
        return output_shows_feed_state(proc.output, checkpoint, "E2ETEST", "FAILED") ||
               output_shows_feed_state(proc.output, checkpoint, "E2ETEST", "DISCONNECTED");
    });
}

// Runs a binary expected to exit by itself and returns its status + output.
int run_to_exit(const std::string& path, const std::vector<std::string>& args, std::string* out_output) {
    ChildProcess proc = spawn(path, args);
    int status = 0;
    REQUIRE(proc.wait_for_exit(kExitTimeoutMs, &status));
    *out_output = proc.output;
    REQUIRE(WIFEXITED(status));
    return WEXITSTATUS(status);
}

}  // namespace

TEST_CASE("the real feed_monitor and feed_simulator binaries detect, fail, and recover a feed under a live command-driven fault") {
    uint16_t port = 0;
    ChildProcess sim = start_simulator(&port);
    ChildProcess mon = start_monitor(port);

    // 1. Argv parsing, config loading and handshake: the feed reaches HEALTHY.
    REQUIRE(wait_for_feed_state(mon, 0, "HEALTHY", kStartupTimeoutMs));

    // 2. Simulator stdin command: stop replying.
    size_t checkpoint = mon.output.size();
    REQUIRE(sim.send_line("set-drop-rate 1"));
    REQUIRE(wait_for_feed_state(mon, checkpoint, "FAILED"));

    // 3. Restore; the monitor must reconnect on its own.
    checkpoint = mon.output.size();
    REQUIRE(sim.send_line("set-drop-rate 0"));
    REQUIRE(wait_for_feed_state(mon, checkpoint, "HEALTHY"));

    // 4. SIGINT: prompt, clean exit.
    mon.send_signal(SIGINT);
    int mon_status = 0;
    REQUIRE(mon.wait_for_exit(kExitTimeoutMs, &mon_status));
    CHECK(WIFEXITED(mon_status));
    CHECK(WEXITSTATUS(mon_status) == 0);

    REQUIRE(sim.send_line("quit"));
    int sim_status = 0;
    REQUIRE(sim.wait_for_exit(kExitTimeoutMs, &sim_status));
    CHECK(WIFEXITED(sim_status));
    CHECK(WEXITSTATUS(sim_status) == 0);
}

TEST_CASE("the real feed_monitor's stdin commands disconnect, reconnect, set-interval and set-timeout drive a live feed") {
    uint16_t port = 0;
    ChildProcess sim = start_simulator(&port);
    // Long backoff so `disconnect` stays down until `reconnect` cuts the wait short.
    ChildProcess mon = start_monitor(port, {"--reconnect-base-delay-ms", "5000", "--reconnect-max-delay-ms", "10000"});
    REQUIRE(wait_for_feed_state(mon, 0, "HEALTHY", kStartupTimeoutMs));

    size_t checkpoint = mon.output.size();
    REQUIRE(mon.send_line("disconnect 1"));
    REQUIRE(wait_for_feed_unhealthy(mon, checkpoint));

    // `reconnect` must beat the 5 s backoff.
    checkpoint = mon.output.size();
    const auto t0 = std::chrono::steady_clock::now();
    REQUIRE(mon.send_line("reconnect 1"));
    REQUIRE(wait_for_feed_state(mon, checkpoint, "HEALTHY", 4000));
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(4));

    // Tuning commands are accepted, and a second disconnect/reconnect round trip
    // still works. (The table redraws only on state change, so an active round
    // trip is the only way to observe "still managed".)
    REQUIRE(mon.send_line("set-interval 100"));
    REQUIRE(mon.send_line("set-timeout 1000"));
    checkpoint = mon.output.size();
    REQUIRE(mon.send_line("disconnect 1"));
    REQUIRE(wait_for_feed_unhealthy(mon, checkpoint));
    checkpoint = mon.output.size();
    REQUIRE(mon.send_line("reconnect 1"));
    REQUIRE(wait_for_feed_state(mon, checkpoint, "HEALTHY", 4000));
    CHECK(mon.output.find("usage:") == std::string::npos);
    CHECK(mon.output.find("invalid argument") == std::string::npos);

    // A bogus command is reported but must not kill the process.
    REQUIRE(mon.send_line("definitely-not-a-command"));
    REQUIRE(mon.wait_for_output(3000, [&] { return mon.output.find("unknown command") != std::string::npos; }));

    REQUIRE(mon.send_line("quit"));
    int status = 0;
    REQUIRE(mon.wait_for_exit(kExitTimeoutMs, &status));
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);

    sim.send_signal(SIGTERM);
    REQUIRE(sim.wait_for_exit(kExitTimeoutMs, &status));
    CHECK(WIFEXITED(status));
}

TEST_CASE("the real feed_simulator's kill command drops the connection and the monitor reconnects on its own") {
    uint16_t port = 0;
    ChildProcess sim = start_simulator(&port);
    ChildProcess mon = start_monitor(port);
    REQUIRE(wait_for_feed_state(mon, 0, "HEALTHY", kStartupTimeoutMs));

    size_t checkpoint = mon.output.size();
    REQUIRE(sim.send_line("kill"));
    REQUIRE(wait_for_feed_unhealthy(mon, checkpoint));

    checkpoint = mon.output.size();
    REQUIRE(wait_for_feed_state(mon, checkpoint, "HEALTHY"));

    mon.send_signal(SIGTERM);
    int status = 0;
    REQUIRE(mon.wait_for_exit(kExitTimeoutMs, &status));
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}

TEST_CASE("the real binaries print usage for --help and exit 0") {
    std::string out;
    CHECK(run_to_exit(bin_path("feed_monitor"), {"--help"}, &out) == 0);
    CHECK(out.find("Usage") != std::string::npos);
    CHECK(out.find("--feed") != std::string::npos);

    CHECK(run_to_exit(bin_path("feed_simulator"), {"--help"}, &out) == 0);
    CHECK(out.find("Usage") != std::string::npos);
    CHECK(out.find("--port") != std::string::npos);
}

TEST_CASE("the real feed_monitor with no feeds configured exits non-zero with an explanation") {
    std::string out;
    CHECK(run_to_exit(bin_path("feed_monitor"), {}, &out) != 0);
    CHECK(out.find("no feeds") != std::string::npos);

    // A feed spec that fails validation leaves zero valid feeds: same outcome.
    CHECK(run_to_exit(bin_path("feed_monitor"), {"--feed", "not-a-valid-spec"}, &out) != 0);
    CHECK(out.find("no feeds") != std::string::npos);
}

TEST_CASE("the real feed_simulator rejects a malformed --port instead of silently binding to port 0") {
    // A mistyped port must be an error (exit 2), not a silent bind to port 0.
    std::string out;
    CHECK(run_to_exit(bin_path("feed_simulator"), {"--port", "abc", "--feed-id", "1"}, &out) == 2);
    CHECK(out.find("invalid integer") != std::string::npos);
    CHECK(out.find("--port") != std::string::npos);
}

TEST_CASE("the real feed_simulator rejects bad invocations with a usage-error exit code") {
    std::string out;
    // --port is required.
    CHECK(run_to_exit(bin_path("feed_simulator"), {}, &out) == 2);
    CHECK(out.find("--port") != std::string::npos);
    // Out of range for uint16_t (would silently wrap to 4464 if unchecked).
    CHECK(run_to_exit(bin_path("feed_simulator"), {"--port", "70000"}, &out) == 2);
    // Probability outside [0, 1].
    CHECK(run_to_exit(bin_path("feed_simulator"), {"--port", "0", "--drop-rate", "1.5"}, &out) == 2);
    // Unknown flag.
    CHECK(run_to_exit(bin_path("feed_simulator"), {"--port", "0", "--no-such-flag"}, &out) == 2);
}

TEST_CASE("the shipped sample config config/feeds.example.conf parses cleanly in the real feed_monitor") {
    // Nothing listens on the sample's ports, so the feeds just retry; both must
    // still load (names in the table) with no parser warnings.
    ChildProcess mon = spawn(bin_path("feed_monitor"),
                             {"--config", std::string(PROJECT_SOURCE_DIR) + "/config/feeds.example.conf"});
    REQUIRE(mon.wait_for_output(kStartupTimeoutMs, [&] {
        return mon.output.find("NYSE-PRIMARY") != std::string::npos &&
               mon.output.find("NASDAQ-BACKUP") != std::string::npos;
    }));
    CHECK(mon.output.find("[config]") == std::string::npos);  // no parser warnings

    mon.send_signal(SIGINT);
    int status = 0;
    REQUIRE(mon.wait_for_exit(kExitTimeoutMs, &status));
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}
