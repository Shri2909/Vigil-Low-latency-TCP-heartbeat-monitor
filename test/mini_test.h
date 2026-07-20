#pragma once

// Minimal header-only test harness. No external dependency (Catch2/GoogleTest)
// deliberately, to keep the whole project buildable with nothing but g++ and make.
//
// Usage:
//   TEST_CASE("some behavior") {
//       REQUIRE(1 + 1 == 2);   // aborts this test immediately on failure
//       CHECK(2 + 2 == 4);     // records failure but keeps running the test
//   }
//
// Exactly one translation unit must `#define MINI_TEST_MAIN` before including
// this header (see test/test_main.cpp) -- that TU emits main(); every other
// test .cpp just includes this header normally and links in.

#include <cstdio>
#include <exception>
#include <functional>
#include <string>
#include <vector>

namespace mini_test {

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

// Function-local static avoids static-initialization-order-fiasco between
// translation units registering into it via global Registrar instances.
inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

struct Registrar {
    Registrar(std::string name, std::function<void()> fn) {
        registry().push_back({std::move(name), std::move(fn)});
    }
};

// Thrown by REQUIRE to unwind out of the current test immediately.
struct AssertionFailure {
    std::string message;
};

inline int& soft_failures() {
    static int n = 0;
    return n;
}

} // namespace mini_test

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
int main() {
    int total = 0;
    int passed = 0;
    for (auto& t : ::mini_test::registry()) {
        ++total;
        ::mini_test::soft_failures() = 0;
        std::printf("[ RUN      ] %s\n", t.name.c_str());
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
        if (::mini_test::soft_failures() > 0) {
            ok = false;
        }
        if (ok) {
            std::printf("[       OK ] %s\n", t.name.c_str());
            ++passed;
        } else {
            std::printf("[  FAILED  ] %s\n", t.name.c_str());
        }
    }
    std::printf("%d/%d tests passed\n", passed, total);
    return (passed == total) ? 0 : 1;
}
#endif // MINI_TEST_MAIN
