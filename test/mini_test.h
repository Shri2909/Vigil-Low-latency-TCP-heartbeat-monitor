#pragma once

// Minimal header-only test harness (no GoogleTest/Catch2 dependency).
//
//   TEST_CASE("some behavior") {
//       REQUIRE(1 + 1 == 2);   // fails and aborts this test
//       CHECK(2 + 2 == 4);     // records a failure and keeps going
//   }
//
// Exactly one translation unit defines MINI_TEST_MAIN before including this
// header (test/test_main.cpp) to emit main().
//
// ./run_tests [--list] [name-substring ...]
// Exits non-zero if a test fails, none are registered, or a filter matches
// nothing. A per-test SIGALRM watchdog (60 s; MINI_TEST_TIMEOUT_SEC overrides,
// 0 disables) aborts with the hung test's name.

#include <cstdio>
#include <exception>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace mini_test {

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

// Function-local static: safe against static-init order across translation units.
inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

struct Registrar {
    Registrar(std::string name, std::function<void()> fn) {
        registry().push_back({std::move(name), std::move(fn)});
    }
};

// Thrown by REQUIRE to unwind the current test.
struct AssertionFailure {
    std::string message;
};

inline int& soft_failures() {
    static int n = 0;
    return n;
}

}  // namespace mini_test

#define MINI_TEST_CONCAT_(a, b) a##b
#define MINI_TEST_CONCAT(a, b) MINI_TEST_CONCAT_(a, b)

#define TEST_CASE(name_str)                                                       \
    static void MINI_TEST_CONCAT(mini_test_fn_, __LINE__)();                      \
    static ::mini_test::Registrar MINI_TEST_CONCAT(mini_test_reg_, __LINE__)(      \
        (name_str), &MINI_TEST_CONCAT(mini_test_fn_, __LINE__));                  \
    static void MINI_TEST_CONCAT(mini_test_fn_, __LINE__)()

#define REQUIRE(cond)                                                             \
    do {                                                                          \
        if (!(cond)) {                                                            \
            throw ::mini_test::AssertionFailure{                                  \
                std::string(__FILE__) + ":" + std::to_string(__LINE__) +          \
                ": REQUIRE(" #cond ") failed"};                                   \
        }                                                                         \
    } while (0)

#define CHECK(cond)                                                                \
    do {                                                                          \
        if (!(cond)) {                                                            \
            std::fprintf(stderr, "%s:%d: CHECK(%s) failed\n", __FILE__, __LINE__, \
                          #cond);                                                 \
            ::mini_test::soft_failures()++;                                       \
        }                                                                         \
    } while (0)

#ifdef MINI_TEST_MAIN

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <set>

#include <csignal>
#include <unistd.h>

namespace mini_test {

// Current test name in a fixed buffer the signal handler can read.
inline char* current_test_name() {
    static char buf[512];
    return buf;
}

// Async-signal-safe: write(2) + abort() only.
inline void watchdog_handler(int) {
    static const char kPrefix[] = "\n[ WATCHDOG ] test exceeded its time limit and is presumed hung: ";
    [[maybe_unused]] ssize_t r1 = ::write(STDERR_FILENO, kPrefix, sizeof(kPrefix) - 1);
    const char* name = current_test_name();
    [[maybe_unused]] ssize_t r2 = ::write(STDERR_FILENO, name, std::strlen(name));
    [[maybe_unused]] ssize_t r3 = ::write(STDERR_FILENO, "\n", 1);
    std::abort();
}

inline unsigned watchdog_seconds() {
    const char* env = std::getenv("MINI_TEST_TIMEOUT_SEC");
    if (env != nullptr && *env != '\0') {
        char* end = nullptr;
        const long v = std::strtol(env, &end, 10);
        if (*end == '\0' && v >= 0) {
            return static_cast<unsigned>(v);
        }
    }
    return 60;
}

inline bool matches_filters(const std::string& name, const std::vector<std::string>& filters) {
    if (filters.empty()) {
        return true;
    }
    return std::any_of(filters.begin(), filters.end(),
                       [&](const std::string& f) { return name.find(f) != std::string::npos; });
}

}  // namespace mini_test

int main(int argc, char** argv) {
    std::vector<std::string> filters;
    bool list_only = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--list") {
            list_only = true;
        } else if (arg == "-h" || arg == "--help") {
            std::printf("usage: %s [--list] [name-substring ...]\n", argv[0]);
            return 0;
        } else {
            filters.push_back(arg);
        }
    }

    auto& tests = ::mini_test::registry();
    if (tests.empty()) {
        std::fprintf(stderr, "error: no tests registered -- refusing to report success on an empty suite\n");
        return 1;
    }

    // Duplicate names make filters and failure lists ambiguous.
    std::set<std::string> seen;
    for (const auto& t : tests) {
        if (!seen.insert(t.name).second) {
            std::fprintf(stderr, "warning: duplicate test name: %s\n", t.name.c_str());
        }
    }

    if (list_only) {
        for (const auto& t : tests) {
            std::printf("%s\n", t.name.c_str());
        }
        return 0;
    }

    const unsigned timeout_s = ::mini_test::watchdog_seconds();
    if (timeout_s > 0) {
        struct sigaction sa{};
        sa.sa_handler = ::mini_test::watchdog_handler;
        ::sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        ::sigaction(SIGALRM, &sa, nullptr);
    }

    int total = 0;
    int passed = 0;
    std::vector<std::string> failed_names;
    for (auto& t : tests) {
        if (!::mini_test::matches_filters(t.name, filters)) {
            continue;
        }
        ++total;
        ::mini_test::soft_failures() = 0;
        std::printf("[ RUN      ] %s\n", t.name.c_str());
        std::fflush(stdout);
        std::snprintf(::mini_test::current_test_name(), 512, "%s", t.name.c_str());
        if (timeout_s > 0) {
            ::alarm(timeout_s);
        }
        bool ok = true;
        try {
            t.fn();
        } catch (const ::mini_test::AssertionFailure& e) {
            std::fprintf(stderr, "%s\n", e.message.c_str());
            ok = false;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "uncaught exception: %s\n", e.what());
            ok = false;
        } catch (...) {
            std::fprintf(stderr, "uncaught non-exception value thrown\n");
            ok = false;
        }
        if (timeout_s > 0) {
            ::alarm(0);
        }
        if (::mini_test::soft_failures() > 0) {
            ok = false;
        }
        if (ok) {
            std::printf("[       OK ] %s\n", t.name.c_str());
            ++passed;
        } else {
            std::printf("[  FAILED  ] %s\n", t.name.c_str());
            failed_names.push_back(t.name);
        }
    }

    if (total == 0) {
        std::fprintf(stderr, "error: no registered test matches the given filter(s)\n");
        return 1;
    }
    std::printf("%d/%d tests passed\n", passed, total);
    if (!failed_names.empty()) {
        std::printf("FAILED tests (%zu):\n", failed_names.size());
        for (const auto& n : failed_names) {
            std::printf("  %s\n", n.c_str());
        }
    }
    return (passed == total) ? 0 : 1;
}
#endif  // MINI_TEST_MAIN
