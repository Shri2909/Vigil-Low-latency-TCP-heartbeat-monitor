// libFuzzer harness for Connection's real accumulate-and-parse pipeline
// (on_readable -> drain_read_buffer -> decode_heartbeat -> handle_message),
// fuzzing arbitrary-length byte streams -- the dimension
// decode_heartbeat_fuzz.cpp structurally can't reach, since decode_heartbeat
// itself only ever sees a fixed 28-byte buffer. This answers "is the wire
// protocol robust to an arbitrary byte stream arriving off a real socket,"
// not just "is one 28-byte frame parsed correctly."
//
// Uses a real AF_UNIX socketpair (not a stub) so the exact code path a real
// TCP peer would drive -- recv() in a loop, accumulate, drain -- is what
// gets fuzzed. A real handshake completes first (deterministic, not
// fuzzer-controlled) so the fuzzer's bytes land on the more interesting
// kHealthy-state path (handle_ping/handle_pong, sequence dedup, the
// staleness/negative-RTT checks, the read_accum_ cap) rather than being
// short-circuited immediately as "unexpected" in kConnecting.
//
// Build/run manually (not part of make all/test/CI):
//   make fuzz
//   ./bin/drain_read_buffer_fuzz -max_total_time=60

#include "config.h"
#include "connection.h"
#include "heartbeat.h"

#include <cstddef>
#include <cstdint>

#include <sys/socket.h>
#include <unistd.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds) != 0) {
        return 0;
    }
    const int peer_fd = fds[1];

    {
        Config config;
        Connection conn(fds[0], Role::kInitiator, /*feed_id=*/1, "fuzz", 1, config);
        conn.on_writable(1'000);  // completes "connect" -> kHandshaking, sends CONNECT_HELLO

        uint8_t hello_buf[kHeartbeatMessageSize];
        const ssize_t hello_n = ::recv(peer_fd, hello_buf, sizeof(hello_buf), 0);
        if (hello_n == static_cast<ssize_t>(kHeartbeatMessageSize)) {
            HeartbeatMessage ack{};
            ack.type = static_cast<uint8_t>(MessageType::kConnectAck);
            ack.feed_id = 1;
            HeartbeatWireBuffer ack_wire;
            encode_heartbeat(ack, ack_wire);
            [[maybe_unused]] ssize_t ack_sent = ::send(peer_fd, ack_wire.data(), ack_wire.size(), MSG_NOSIGNAL);
            conn.on_readable(2'000);  // processes CONNECT_ACK -> kHealthy
        }

        // Now feed the fuzzer-controlled bytes -- arbitrary length, not
        // aligned to message boundaries, may span multiple messages, may be
        // pure garbage, may be well past read_accum_'s cap.
        size_t sent = 0;
        while (sent < size) {
            const ssize_t n = ::send(peer_fd, data + sent, size - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                break;
            }
            sent += static_cast<size_t>(n);
        }
        conn.on_readable(3'000);
        // conn destructs here, closing fds[0] via UniqueFd.
    }
    ::close(peer_fd);
    return 0;
}
