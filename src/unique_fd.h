#pragma once

// Move-only RAII file descriptor (Connection is stored by value in a std::vector, so it must be safely movable).

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
