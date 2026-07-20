// Every other test in this suite links src/*.cpp directly and drives
// FeedMonitor/FeedSimulator in-process. None of that exercises the actual
// shipped ./bin/feed_monitor / ./bin/feed_simulator processes: real argv
// parsing (main.cpp/main_simulator.cpp's getopt_long paths), real config
// loading, real stdin command handling, real SIGINT/SIGTERM shutdown. This
// file closes that gap by fork()+exec()-ing the real compiled binaries as
// actual child processes and driving them exactly the way an operator
// would -- piping commands to stdin, reading the live CLI table off
// stdout, sending a real signal to shut down.
//
// PROJECT_BIN_DIR (an absolute path, see the Makefile's dedicated rule for
// this file) is used instead of a cwd-relative guess, since this binary
// might be invoked from somewhere other than the repo root (an IDE run
// config, a sanitizer build run from a subdirectory, etc.).

#include "mini_test.h"

#include <chrono>
#include <cctype>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef PROJECT_BIN_DIR
#error "PROJECT_BIN_DIR must be defined (see Makefile's e2e_binary_test.o rule)"
#endif

namespace {

struct ChildProcess {
    pid_t pid = -1;
    int stdin_fd = -1;   // parent writes here -> child's stdin
    int stdout_fd = -1;  // parent reads here <- child's stdout+stderr (merged)
};

// Launches path with args as a real child process, stdin/stdout+stderr
// wired to pipes the parent controls. Only async-signal-safe operations run
// between fork() and execv() (dup2/close), which is what makes forking
// safe here even though this test binary has used std::thread extensively
// in every test that ran before this one.
ChildProcess spawn(const std::string& path, const std::vector<std::string>& args) {
    int in_pipe[2];
    int out_pipe[2];
    REQUIRE(::pipe(in_pipe) == 0);
    REQUIRE(::pipe(out_pipe) == 0);

    const pid_t pid = ::fork();
    REQUIRE(pid >= 0);
    if (pid == 0) {
        ::dup2(in_pipe[0], STDIN_FILENO);
        ::dup2(out_pipe[1], STDOUT_FILENO);
        ::dup2(out_pipe[1], STDERR_FILENO);
        ::close(in_pipe[0]);
        ::close(in_pipe[1]);
        ::close(out_pipe[0]);
        ::close(out_pipe[1]);

        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(path.c_str()));
        for (const auto& a : args) {
            argv.push_back(const_cast<char*>(a.c_str()));
        }
        argv.push_back(nullptr);
        ::execv(path.c_str(), argv.data());
        ::_exit(127);  // execv only returns on failure
    }

    ::close(in_pipe[0]);
    ::close(out_pipe[1]);
    ChildProcess proc;
    proc.pid = pid;
    proc.stdin_fd = in_pipe[1];
    proc.stdout_fd = out_pipe[0];
    const int flags = ::fcntl(proc.stdout_fd, F_GETFL, 0);
    ::fcntl(proc.stdout_fd, F_SETFL, flags | O_NONBLOCK);
    return proc;
}

// Drains whatever's available on proc.stdout_fd into accumulated, polling
// until pred(accumulated) is true or timeout_ms elapses. Returns the final
// truth of pred(accumulated) either way.
bool wait_for_output(ChildProcess& proc, std::string& accumulated, int timeout_ms,
                      const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    char buf[4096];
    while (true) {
        if (pred()) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return pred();
        }
        pollfd pfd{proc.stdout_fd, POLLIN, 0};
        const int rc = ::poll(&pfd, 1, 50);
        if (rc > 0 && (pfd.revents & POLLIN)) {
            const ssize_t n = ::read(proc.stdout_fd, buf, sizeof(buf));
            if (n > 0) {
                accumulated.append(buf, static_cast<size_t>(n));
            }
        }
    }
}

// True once some line in accumulated[search_from:] contains both name and
// state -- e.g. a live CLI table row showing a specific feed at a specific
// ConnectionState. Restricting to [search_from:] (a checkpoint captured
// right before the action under test) instead of the whole buffer avoids a
// stale earlier occurrence of the same state satisfying the check.
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

bool wait_for_exit(pid_t pid, int timeout_ms, int* out_status) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        const pid_t rc = ::waitpid(pid, &status, WNOHANG);
        if (rc == pid) {
            *out_status = status;
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

}  // namespace

TEST_CASE("the real feed_monitor and feed_simulator binaries detect, fail, and recover a feed under a live command-driven fault") {
    const std::string simulator_path = std::string(PROJECT_BIN_DIR) + "/feed_simulator";
    const std::string monitor_path = std::string(PROJECT_BIN_DIR) + "/feed_monitor";

    ChildProcess sim = spawn(simulator_path, {"--port", "0", "--feed-id", "1"});
    std::string sim_output;
    REQUIRE(wait_for_output(sim, sim_output, 5000,
                             [&] { return sim_output.find("listening on port") != std::string::npos; }));
    const uint16_t port = extract_listening_port(sim_output);

    ChildProcess mon = spawn(monitor_path, {"--feed", "1:E2ETEST:127.0.0.1:" + std::to_string(port),
                                             "--heartbeat-interval-ms", "200", "--heartbeat-timeout-ms", "600",
                                             "--max-missed", "2", "--reconnect-base-delay-ms", "200",
                                             "--reconnect-max-delay-ms", "1000"});
    std::string mon_output;

    // 1. Real argv parsing + real config loading + real handshake: the feed
    // reaches HEALTHY in the actual rendered live table.
    REQUIRE(wait_for_output(mon, mon_output, 5000,
                             [&] { return output_shows_feed_state(mon_output, 0, "E2ETEST", "HEALTHY"); }));

    // 2. Real stdin command handling on the simulator side: drive it to stop
    // replying, exactly as an operator would via main_simulator.cpp's
    // documented "set-drop-rate <p>" command.
    size_t checkpoint = mon_output.size();
    const std::string drop_cmd = "set-drop-rate 1\n";
    REQUIRE(::write(sim.stdin_fd, drop_cmd.data(), drop_cmd.size()) == static_cast<ssize_t>(drop_cmd.size()));

    REQUIRE(wait_for_output(
        mon, mon_output, 10000,
        [&] { return output_shows_feed_state(mon_output, checkpoint, "E2ETEST", "FAILED"); }));

    // 3. Restore, and confirm the real monitor process reconnects and
    // recovers on its own, entirely through the real binaries with no
    // in-process shortcuts.
    checkpoint = mon_output.size();
    const std::string restore_cmd = "set-drop-rate 0\n";
    REQUIRE(::write(sim.stdin_fd, restore_cmd.data(), restore_cmd.size()) ==
            static_cast<ssize_t>(restore_cmd.size()));

    REQUIRE(wait_for_output(
        mon, mon_output, 10000,
        [&] { return output_shows_feed_state(mon_output, checkpoint, "E2ETEST", "HEALTHY"); }));

    // 4. Real SIGINT handling: prompt, clean exit -- WIFEXITED/WEXITSTATUS,
    // not scanning for a farewell string (main.cpp's shutdown path has no
    // interactive text to wait for beyond what's already asserted above).
    REQUIRE(::kill(mon.pid, SIGINT) == 0);
    int mon_status = 0;
    REQUIRE(wait_for_exit(mon.pid, 5000, &mon_status));
    CHECK(WIFEXITED(mon_status));
    CHECK(WEXITSTATUS(mon_status) == 0);

    const std::string quit_cmd = "quit\n";
    REQUIRE(::write(sim.stdin_fd, quit_cmd.data(), quit_cmd.size()) == static_cast<ssize_t>(quit_cmd.size()));
    int sim_status = 0;
    REQUIRE(wait_for_exit(sim.pid, 5000, &sim_status));
    CHECK(WIFEXITED(sim_status));
    CHECK(WEXITSTATUS(sim_status) == 0);

    ::close(mon.stdin_fd);
    ::close(mon.stdout_fd);
    ::close(sim.stdin_fd);
    ::close(sim.stdout_fd);
}

TEST_CASE("the real feed_simulator binary rejects a malformed --port instead of silently binding to port 0") {
    // Regression test for a clang-tidy (cert-err34-c) finding: main_simulator.cpp's
    // CLI parsing used to call std::atoi/std::atof directly, which silently
    // returns 0 on unparseable input instead of signaling an error -- unlike
    // config.cpp's require_int_at_least (already hardened against exactly
    // this class of gap for the monitor side). `--port abc` used to silently
    // bind to an OS-assigned port instead of telling the operator they
    // mistyped the flag.
    const std::string simulator_path = std::string(PROJECT_BIN_DIR) + "/feed_simulator";

    ChildProcess sim = spawn(simulator_path, {"--port", "abc", "--feed-id", "1"});
    int status = 0;
    REQUIRE(wait_for_exit(sim.pid, 5000, &status));
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 1);

    std::string output;
    wait_for_output(sim, output, 500, [&] { return !output.empty(); });
    CHECK(output.find("invalid integer") != std::string::npos);
    CHECK(output.find("--port") != std::string::npos);

    ::close(sim.stdin_fd);
    ::close(sim.stdout_fd);
}
