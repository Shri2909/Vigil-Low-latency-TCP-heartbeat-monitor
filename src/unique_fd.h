#pragma once

// Move-only RAII file descriptor. Added while building Connection: a class
// storing a raw `int fd_` cannot be safely moved by the compiler-generated
// (or `= default`ed) move constructor -- a plain member-wise move copies the
// int without invalidating the source, so both the moved-from and moved-to
// objects would close() the same fd on destruction. That matters here
// specifically because Connection is stored by value in a std::vector (see
// PROJECT_PLAN.md section 5's flat-vector amendment), so it must be safely
// movable for reallocation/erase to work correctly.

#include <unistd.h>

#include <utility>

class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    ~UniqueFd() { reset(); }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    int get() const noexcept { return fd_; }
    bool valid() const noexcept { return fd_ >= 0; }

    int release() noexcept {
        const int f = fd_;
        fd_ = -1;
        return f;
    }

    void reset(int new_fd = -1) noexcept {
        if (fd_ >= 0 && fd_ != new_fd) {
            ::close(fd_);
        }
        fd_ = new_fd;
    }

private:
    int fd_ = -1;
};
