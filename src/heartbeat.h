#pragma once

// Wire protocol for the feed heartbeat exchange. See PROJECT_PLAN.md section 3.
//
// Fixed 28-byte message, chosen so a TCP stream can always be parsed as
// "accumulate until kHeartbeatMessageSize bytes, then decode one message" --
// no length-prefix or delimiter logic needed anywhere that reads a socket.
//
// The monitor is always the PING sender; the peer echoes PING's sequence and
// timestamp back unchanged in PONG, so round-trip latency can be computed
// entirely from the monitor's own CLOCK_MONOTONIC reading -- no clock sync
// between processes is required.

#include <array>
#include <cstddef>
#include <cstdint>

enum class MessageType : uint8_t {
    kConnectHello = 1,  // monitor -> peer, first message after connect()
    kConnectAck   = 2,  // peer -> monitor, handshake complete
    kPing         = 3,  // monitor -> peer, periodic heartbeat
    kPong         = 4,  // peer -> monitor, echoes ping's sequence + timestamp
    kDisconnect   = 5,  // either side, graceful close notice
};

constexpr uint16_t kProtocolMagic = 0xFEED;
constexpr uint8_t kProtocolVersion = 1;

#pragma pack(push, 1)
struct HeartbeatMessage {
    uint16_t magic = kProtocolMagic;
    uint8_t version = kProtocolVersion;
    uint8_t type = 0;
    uint32_t feed_id = 0;
    uint64_t sequence = 0;
    int64_t timestamp_ns = 0;  // CLOCK_MONOTONIC at PING send; echoed unchanged in PONG
    uint32_t crc32 = 0;        // CRC32 (IEEE 802.3) over all preceding fields
};
#pragma pack(pop)

constexpr size_t kHeartbeatMessageSize = 28;
static_assert(sizeof(HeartbeatMessage) == kHeartbeatMessageSize,
              "HeartbeatMessage must have no padding -- it is the on-wire layout");

using HeartbeatWireBuffer = std::array<uint8_t, kHeartbeatMessageSize>;

// Serializes msg into out in network byte order and fills in msg's crc32 field's
// on-wire counterpart. Does not validate msg.type -- callers construct messages
// with a known-valid type, so there is nothing to reject on the encode side.
void encode_heartbeat(const HeartbeatMessage& msg, HeartbeatWireBuffer& out);

// Deserializes a full kHeartbeatMessageSize-byte buffer into *out. Returns false
// if magic, version, type, or crc32 don't validate; callers must treat a false
// return as a protocol error (stream desync or a non-protocol peer), not as an
// empty/missing message -- see the "loud, not silent" note in PROJECT_PLAN.md
// finding #2.
bool decode_heartbeat(const HeartbeatWireBuffer& in, HeartbeatMessage* out);

uint32_t crc32_ieee(const uint8_t* data, size_t len);
