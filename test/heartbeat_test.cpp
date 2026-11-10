#include "heartbeat.h"
#include "mini_test.h"

namespace {

HeartbeatMessage make_message() {
    HeartbeatMessage msg;
    msg.type = static_cast<uint8_t>(MessageType::kPing);
    msg.feed_id = 0x11223344;
    msg.sequence = 0x0102030405060708ULL;
    msg.timestamp_ns = 1234567890123LL;
    return msg;
}

}  // namespace

TEST_CASE("encode/decode round-trip preserves all fields") {
    const HeartbeatMessage original = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(original, wire);

    HeartbeatMessage decoded{};
    REQUIRE(decode_heartbeat(wire, &decoded));
    CHECK(decoded.magic == original.magic);
    CHECK(decoded.version == original.version);
    CHECK(decoded.type == original.type);
    CHECK(decoded.feed_id == original.feed_id);
    CHECK(decoded.sequence == original.sequence);
    CHECK(decoded.timestamp_ns == original.timestamp_ns);
}

TEST_CASE("round-trip preserves a negative timestamp") {
    HeartbeatMessage original = make_message();
    original.timestamp_ns = -42;
    HeartbeatWireBuffer wire{};
    encode_heartbeat(original, wire);

    HeartbeatMessage decoded{};
    REQUIRE(decode_heartbeat(wire, &decoded));
    CHECK(decoded.timestamp_ns == -42);
}

TEST_CASE("encode writes big-endian (network byte order) fields") {
    HeartbeatMessage msg = make_message();
    msg.feed_id = 0x11223344;
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);

    CHECK(wire[0] == 0xFE);
    CHECK(wire[1] == 0xED);
    CHECK(wire[2] == kProtocolVersion);
    CHECK(wire[3] == static_cast<uint8_t>(MessageType::kPing));
    CHECK(wire[4] == 0x11);
    CHECK(wire[5] == 0x22);
    CHECK(wire[6] == 0x33);
    CHECK(wire[7] == 0x44);
}

TEST_CASE("decode rejects wrong magic") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);
    wire[0] ^= 0xFF;  // corrupt magic's high byte

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects wrong version") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);
    wire[2] = kProtocolVersion + 1;  // also breaks the CRC; the point is that decode rejects it

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects unknown message type") {
    HeartbeatMessage msg = make_message();
    msg.type = 0;  // not a defined MessageType; encode still computes a valid CRC
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects a single corrupted bit anywhere in the payload") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);

    for (size_t byte_index = 0; byte_index < kHeartbeatMessageSize - sizeof(uint32_t); ++byte_index) {
        HeartbeatWireBuffer corrupted = wire;
        corrupted[byte_index] ^= 0x01;  // flip the low bit of one payload byte
        HeartbeatMessage decoded{};
        CHECK(!decode_heartbeat(corrupted, &decoded));
    }
}

TEST_CASE("HeartbeatMessage has no padding and is exactly the wire size") {
    REQUIRE(sizeof(HeartbeatMessage) == kHeartbeatMessageSize);
    REQUIRE(kHeartbeatMessageSize == 28);
}

TEST_CASE("crc32_ieee matches the standard test vector") {
    // Canonical CRC-32/ISO-HDLC check value for "123456789".
    const uint8_t data[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    CHECK(crc32_ieee(data, sizeof(data)) == 0xCBF43926u);
}
