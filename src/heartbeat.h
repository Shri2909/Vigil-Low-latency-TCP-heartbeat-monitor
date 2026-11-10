#pragma once

// Wire protocol: fixed 28-byte big-endian messages, so a stream is parsed by accumulating
// kHeartbeatMessageSize bytes and decoding one message. The monitor sends PING; the peer
// echoes its sequence and timestamp in PONG, so RTT needs only the monitor's own clock.

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
    int64_t timestamp_ns = 0;  // CLOCK_BOOTTIME at PING send; echoed unchanged in PONG
    uint32_t crc32 = 0;        // CRC32 (IEEE) over all preceding fields
};
#pragma pack(pop)

constexpr size_t kHeartbeatMessageSize = 28;
static_assert(sizeof(HeartbeatMessage) == kHeartbeatMessageSize,
              "HeartbeatMessage must have no padding: it is the on-wire layout");

using HeartbeatWireBuffer = std::array<uint8_t, kHeartbeatMessageSize>;

// Serializes msg (network byte order, CRC computed here) into out. Does not validate msg.type.
void encode_heartbeat(const HeartbeatMessage& msg, HeartbeatWireBuffer& out);

// Decodes one full message into *out. Returns false on bad magic/version/type/CRC; callers
// must treat that as a protocol error (stream desync), not as a missing message.
bool decode_heartbeat(const HeartbeatWireBuffer& in, HeartbeatMessage* out);

uint32_t crc32_ieee(const uint8_t* data, size_t len);
