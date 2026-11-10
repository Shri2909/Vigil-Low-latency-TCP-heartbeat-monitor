// libFuzzer harness for Connection's accumulate-and-parse pipeline
// (on_readable -> drain_read_buffer -> decode_heartbeat -> handle_message) on
// arbitrary-length byte streams, delivered over a real AF_UNIX socketpair.
//
// Input layout:
//   byte 0   bit0     role: 0 = Role::kInitiator, 1 = Role::kResponder
//            bit1     1 = skip the scripted handshake (stream starts in
//                         kConnecting/kHandshaking, so CONNECT_HELLO/ACK
//                         handling is itself fuzzed)
//            bit2     1 = close the peer end after the last chunk (EOF path)
//            bits3-7  chunk size selector: 0 = whole stream in one send,
//                     n = n-byte chunks (stresses partial-frame accumulation)
//   byte 1-2          little-endian milliseconds added to the data-phase
//                     now_ns (0..65 s), so RTT/timeout arithmetic varies
//   byte 3-           the byte stream sent by the peer
//
// Unless bit1 is set, a scripted handshake completes first (asserted to reach
// kHealthy; an initiator also sends PINGs seq 0..3) so the fuzzed bytes hit the
// steady-state path (PING/PONG handling, dedup, RTT checks, read_accum_ cap).
//
// Seeds are in fuzz/corpus/drain_read_buffer_fuzz/ (fuzz/gen_corpus.py); the
// custom mutator re-fixes magic/type/CRC for each aligned frame.
//
// Run: make fuzz-smoke, or
//   ./bin/drain_read_buffer_fuzz -max_total_time=60 -dict=fuzz/heartbeat.dict \
//       fuzz-work/drain fuzz/corpus/drain_read_buffer_fuzz

#include "config.h"
#include "connection.h"
#include "fuzz_common.h"
#include "heartbeat.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>

#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr size_t kHeaderSize = 3;
constexpr uint32_t kFeedId = 1;

void send_all(int fd, const uint8_t* data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        const ssize_t n = ::send(fd, data + sent, size - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            break;  // socket buffer full or peer gone; the rest is dropped
        }
        sent += static_cast<size_t>(n);
    }
}

void send_message(int fd, MessageType type) {
    HeartbeatMessage msg{};
    msg.type = static_cast<uint8_t>(type);
    msg.feed_id = kFeedId;
    HeartbeatWireBuffer wire;
    encode_heartbeat(msg, wire);
    send_all(fd, wire.data(), wire.size());
}

// Throws away whatever the Connection wrote to its peer (HELLO/ACK/PONG...),
// so a long input can never fill the socket buffer and stall write paths.
void drain_peer(int fd) {
    uint8_t sink[4096];
    while (::recv(fd, sink, sizeof(sink), MSG_DONTWAIT) > 0) {
    }
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    // Connection logs protocol violations to std::cerr; silence it so it doesn't
    // flood the fuzzer's output.
    std::cerr.setstate(std::ios_base::failbit);
    return 0;
}

extern "C" size_t LLVMFuzzerCustomMutator(uint8_t* data, size_t size, size_t max_size,
                                           unsigned int seed) {
    if (max_size < kHeaderSize) {
        return size;
    }
    if (size < kHeaderSize) {
        std::fill(data + size, data + kHeaderSize, uint8_t{0});
        size = kHeaderSize;
    }
    size = LLVMFuzzerMutate(data, size, max_size);
    if (size < kHeaderSize) {
        std::fill(data + size, data + kHeaderSize, uint8_t{0});
        size = kHeaderSize;
    }
    std::mt19937 rng(seed);
    fuzz::fix_frames(data, size, kHeaderSize, rng);
    return size;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < kHeaderSize) {
        return 0;
    }
    const uint8_t control = data[0];
    const Role role = (control & 0x01) ? Role::kResponder : Role::kInitiator;
    const bool skip_handshake = (control & 0x02) != 0;
    const bool close_peer_at_end = (control & 0x04) != 0;
    const size_t chunk_selector = control >> 3;  // 0..31
    const int64_t data_now_ns =
        3'000 + static_cast<int64_t>(data[1] | (data[2] << 8)) * 1'000'000;
    const uint8_t* stream = data + kHeaderSize;
    const size_t stream_size = size - kHeaderSize;

    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds) != 0) {
        return 0;
    }
    const int peer_fd = fds[1];

    {
        Config config;
        Connection conn(fds[0], role, kFeedId, "fuzz", 1, config);

        if (!skip_handshake) {
            if (role == Role::kInitiator) {
                conn.on_writable(1'000);  // completes "connect" -> kHandshaking, queues CONNECT_HELLO
                drain_peer(peer_fd);
                send_message(peer_fd, MessageType::kConnectAck);
            } else {
                send_message(peer_fd, MessageType::kConnectHello);
            }
            conn.on_readable(2'000);  // processes ACK/HELLO -> kHealthy
            drain_peer(peer_fd);
            FUZZ_ASSERT(conn.state() == ConnectionState::kHealthy);
            if (role == Role::kInitiator) {
                // PONGs for never-sent sequences are rejected, so send PINGs 0..3
                // for the fuzzer's PONGs to match.
                for (int i = 0; i < 4; ++i) {
                    conn.send_ping(2'500);
                }
                drain_peer(peer_fd);
            }
        }

        // Feed the fuzzer's bytes: unaligned, possibly chunked, possibly garbage or
        // past read_accum_'s cap. One on_readable() per chunk, mirroring epoll wakeups.
        const size_t chunk = (chunk_selector == 0) ? std::max<size_t>(stream_size, 1) : chunk_selector;
        int64_t now = data_now_ns;
        for (size_t off = 0; off < stream_size && conn.state() != ConnectionState::kFailed;
             off += chunk) {
            send_all(peer_fd, stream + off, std::min(chunk, stream_size - off));
            conn.on_readable(now);
            drain_peer(peer_fd);
            now += 1'000;
        }
        if (close_peer_at_end) {
            ::shutdown(peer_fd, SHUT_WR);
            conn.on_readable(now);
        }
        if (conn.state() != ConnectionState::kFailed) {
            conn.on_writable(now);
            conn.check_timeout(now + 10'000'000'000LL);  // well past any default timeout
        }
        // conn's destructor closes fds[0].
    }
    ::close(peer_fd);
    return 0;
}
