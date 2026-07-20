// Connection is tested against a minimal raw-socket TCP peer written directly
// in this file, not against FeedSimulator (which doesn't exist yet -- and
// even once it does, is itself built *from* Connection, so testing Connection
// against it would be circular; see PROJECT_PLAN.md section 10/15).

#include "connection.h"
#include "time_utils.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

void set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool wait_for(int fd, short events, int timeout_ms) {
    pollfd pfd{fd, events, 0};
    const int rc = ::poll(&pfd, 1, timeout_ms);
    return rc > 0 && (pfd.revents & events) != 0;
}

bool wait_readable(int fd, int timeout_ms = 1000) { return wait_for(fd, POLLIN, timeout_ms); }
bool wait_writable(int fd, int timeout_ms = 1000) { return wait_for(fd, POLLOUT, timeout_ms); }

// A blocking TCP listener representing "the other side" of whatever
// Connection under test is exercising. Owns exactly one accepted peer socket
// at a time, which is all these tests need.
class TestPeerListener {
public:
    TestPeerListener() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // ephemeral port
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 1);

        sockaddr_in bound{};
        socklen_t bound_len = sizeof(bound);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len);
        port_ = ntohs(bound.sin_port);
    }
    ~TestPeerListener() {
        if (peer_fd_ >= 0) ::close(peer_fd_);
        ::close(listen_fd_);
    }
    TestPeerListener(const TestPeerListener&) = delete;
    TestPeerListener& operator=(const TestPeerListener&) = delete;

    uint16_t port() const { return port_; }

    // Blocks (briefly -- loopback, test-local) until a client connects.
    int accept_one() {
        peer_fd_ = ::accept(listen_fd_, nullptr, nullptr);
        return peer_fd_;
    }

    // Same contract as UniqueFd::release(): hands ownership of the accepted
    // fd back to the caller, so this class's destructor won't also close it.
    // Needed by any test that deliberately closes the peer fd mid-test (e.g.
    // to trigger an EOF scenario) rather than leaving cleanup to RAII.
    int release_peer_fd() {
        const int fd = peer_fd_;
        peer_fd_ = -1;
        return fd;
    }

private:
    int listen_fd_ = -1;
    int peer_fd_ = -1;
    uint16_t port_ = 0;
};

// Non-blocking client socket connect()ed toward the given port, returned
// immediately (likely EINPROGRESS) -- exactly the state Connection expects to
// be constructed with for Role::kInitiator.
int connect_nonblocking(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    set_nonblocking(fd);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    return fd;
}

void send_wire(int fd, const HeartbeatMessage& msg) {
    HeartbeatWireBuffer wire;
    encode_heartbeat(msg, wire);
    ssize_t total = 0;
    while (total < static_cast<ssize_t>(wire.size())) {
        const ssize_t n = ::send(fd, wire.data() + total, wire.size() - static_cast<size_t>(total), 0);
        REQUIRE(n > 0);
        total += n;
    }
}

// Reads exactly kHeartbeatMessageSize bytes (blocking on a short timeout) and
// decodes them. REQUIREs success -- a test that expects no message should not
// call this.
HeartbeatMessage recv_wire(int fd) {
    HeartbeatWireBuffer wire{};
    size_t total = 0;
    while (total < wire.size()) {
        REQUIRE(wait_readable(fd, 1000));
        const ssize_t n = ::recv(fd, wire.data() + total, wire.size() - total, 0);
        REQUIRE(n > 0);
        total += static_cast<size_t>(n);
    }
    HeartbeatMessage msg{};
    REQUIRE(decode_heartbeat(wire, &msg));
    return msg;
}

Config make_test_config() {
    Config config;
    config.heartbeat_interval = std::chrono::milliseconds(1000);
    config.heartbeat_timeout = std::chrono::milliseconds(3000);
    config.max_missed_heartbeats = 3;
    config.tcp_nodelay = true;
    return config;
}

// Drives a fresh Role::kInitiator Connection through connect + handshake
// against `peer`, leaving both the Connection and the raw peer_fd in
// kHealthy/handshake-complete state. Returns the peer's accepted fd.
struct InitiatorFixture {
    TestPeerListener peer;
    Config config = make_test_config();
    int client_fd;
    Connection connection;
    int peer_fd = -1;

    InitiatorFixture()
        : client_fd(connect_nonblocking(peer.port())),
          connection(client_fd, Role::kInitiator, /*feed_id=*/42, "127.0.0.1", peer.port(), config) {
        peer_fd = peer.accept_one();
        REQUIRE(wait_writable(client_fd));
        connection.on_writable(1'000);  // connect() completion -> kHandshaking, sends CONNECT_HELLO

        const HeartbeatMessage hello = recv_wire(peer_fd);
        REQUIRE(static_cast<MessageType>(hello.type) == MessageType::kConnectHello);
        REQUIRE(hello.feed_id == 42u);

        HeartbeatMessage ack{};
        ack.type = static_cast<uint8_t>(MessageType::kConnectAck);
        ack.feed_id = 42;
        send_wire(peer_fd, ack);

        REQUIRE(wait_readable(client_fd));
        connection.on_readable(2'000);
    }
};

}  // namespace

TEST_CASE("initiator reaches HEALTHY after CONNECT_HELLO/CONNECT_ACK handshake") {
    InitiatorFixture fx;
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
    // handshake completed via on_readable(2'000) in the fixture -- see
    // handle_connect_ack, which stamps connected_since_ns there.
    CHECK(fx.connection.stats().connected_since_ns == 2'000);
}

TEST_CASE("send_ping increments sequence and produces correct on-wire bytes") {
    InitiatorFixture fx;

    fx.connection.send_ping(10'000);
    HeartbeatMessage ping0 = recv_wire(fx.peer_fd);
    CHECK(static_cast<MessageType>(ping0.type) == MessageType::kPing);
    CHECK(ping0.sequence == 0u);
    CHECK(ping0.timestamp_ns == 10'000);

    fx.connection.send_ping(20'000);
    HeartbeatMessage ping1 = recv_wire(fx.peer_fd);
    CHECK(ping1.sequence == 1u);
    CHECK(ping1.timestamp_ns == 20'000);

    CHECK(fx.connection.stats().heartbeats_sent == 2u);
}

TEST_CASE("valid PONG updates RTT stats and clears missed-count") {
    InitiatorFixture fx;

    fx.connection.send_ping(10'000);
    recv_wire(fx.peer_fd);  // drain the PING off the wire

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 10'000;  // echoed unchanged, as the real peer would
    send_wire(fx.peer_fd, pong);

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(10'500);  // "now" 500ns after the ping was sent

    CHECK(fx.connection.stats().heartbeats_acked == 1u);
    CHECK(fx.connection.stats().last_rtt_ns == 500);
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
}

TEST_CASE("a final PONG arriving in the same on_readable call as peer EOF is still processed") {
    // Regression test for a real bug: on_readable used to call
    // handle_peer_closed() and return immediately on recv()==0 (EOF), before
    // ever draining whatever had just been accumulated into read_accum_ in
    // that same call -- silently dropping an already-fully-received final
    // message. Reproduced by having the peer send a PONG and close
    // immediately with no delay, so both the data and the EOF are visible to
    // a single on_readable() call (this is exactly what happens in practice
    // when a fault-injected deferred PONG is immediately followed by the
    // peer disconnecting, or on any graceful shutdown that sends a last
    // message before closing).
    InitiatorFixture fx;

    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);  // drain the PING off the wire

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 1'000;
    send_wire(fx.peer_fd, pong);
    ::close(fx.peer.release_peer_fd());  // release first: avoid a double-close via TestPeerListener's own destructor

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(1'500);

    CHECK(fx.connection.stats().heartbeats_acked == 1u);  // the PONG must have been processed, not dropped
    CHECK(fx.connection.stats().last_rtt_ns == 500);
    CHECK(fx.connection.state() == ConnectionState::kFailed);  // and the connection is still correctly torn down
}

TEST_CASE("a reordered PONG (earlier sequence arriving after a later one) is still counted, not dropped as stale") {
    // Regression test for a real bug: handle_pong used to compare
    // msg.sequence against "the highest sequence acked so far" and drop
    // anything <= that as stale/duplicate. But set_ping_interceptor (built
    // for FeedSimulator's extra_latency fault injection) explicitly allows
    // deferring an earlier PING's reply behind a later one's, so PONGs do
    // not always arrive in non-decreasing sequence order -- a legitimate
    // reordered PONG was silently misclassified as stale and dropped.
    // Reproduced directly here (no simulator/fault-injection needed): send
    // two pings, then feed their PONGs back in reverse order.
    InitiatorFixture fx;

    fx.connection.send_ping(1'000);  // sequence 0
    recv_wire(fx.peer_fd);
    fx.connection.send_ping(2'000);  // sequence 1
    recv_wire(fx.peer_fd);

    HeartbeatMessage pong1{};
    pong1.type = static_cast<uint8_t>(MessageType::kPong);
    pong1.feed_id = 42;
    pong1.sequence = 1;
    pong1.timestamp_ns = 2'000;
    send_wire(fx.peer_fd, pong1);
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(2'300);
    CHECK(fx.connection.stats().heartbeats_acked == 1u);

    // Now the *earlier* sequence's (deferred) PONG arrives, out of order.
    HeartbeatMessage pong0{};
    pong0.type = static_cast<uint8_t>(MessageType::kPong);
    pong0.feed_id = 42;
    pong0.sequence = 0;
    pong0.timestamp_ns = 1'000;
    send_wire(fx.peer_fd, pong0);
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(2'600);

    CHECK(fx.connection.stats().heartbeats_acked == 2u);  // both pongs counted, not just the first
}

TEST_CASE("N consecutive missed heartbeats transitions HEALTHY -> DEGRADED -> FAILED") {
    InitiatorFixture fx;
    fx.config.max_missed_heartbeats = 2;
    const int64_t timeout_ns = 3'000'000'000LL;  // matches make_test_config()'s 3000ms

    // InitiatorFixture's handshake completes via on_readable(2'000), which sets
    // last_liveness_ns_ = 2000 -- start from that same baseline, not an
    // independent guess, or "elapsed" comes out short.
    int64_t t = 2'000;
    // Advance strictly past the timeout window each call -- injected time, no real sleeping.
    t += timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == false);
    CHECK(fx.connection.state() == ConnectionState::kDegraded);

    t += timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == true);  // this call is the one that fails it
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("a PONG whose round trip already exceeds heartbeat_timeout does not resurrect a degraded connection") {
    // Regression test for a real gap: handle_pong only checked "have I
    // already counted this sequence," never "did this reply arrive within a
    // timeframe that actually proves current health." A PONG for a PING
    // sent long enough ago that its round trip already exceeds
    // heartbeat_timeout doesn't prove the feed is healthy *now* -- by the
    // same timing rule check_timeout itself uses to declare a miss, this
    // reply is already too old to trust. Before the fix, accepting it would
    // reset consecutive_missed_ to 0 and flip a kDegraded connection
    // straight back to kHealthy, silently masking every real miss that
    // happened after the stale PING was originally sent.
    InitiatorFixture fx;
    const int64_t timeout_ns = 3'000'000'000LL;  // matches make_test_config()'s 3000ms

    fx.connection.send_ping(2'000);
    const HeartbeatMessage ping = recv_wire(fx.peer_fd);
    REQUIRE(ping.sequence == 0u);

    // No reply arrives in time -> one miss, DEGRADED (make_test_config()'s
    // max_missed_heartbeats=3, so this single miss alone doesn't fail it).
    int64_t t = 2'000 + timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == false);
    CHECK(fx.connection.state() == ConnectionState::kDegraded);

    // The PONG for that very first PING finally arrives -- but its implied
    // RTT (t - ping.timestamp_ns) is already well past heartbeat_timeout.
    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = ping.sequence;
    pong.timestamp_ns = ping.timestamp_ns;
    send_wire(fx.peer_fd, pong);
    t += 1'000;
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(t);

    CHECK(fx.connection.state() == ConnectionState::kDegraded);  // NOT resurrected to kHealthy
    CHECK(fx.connection.stats().heartbeats_acked == 0u);          // not counted as a real ack
}

TEST_CASE("a PONG with a future timestamp is not accepted as a valid heartbeat") {
    // Regression test: handle_pong's staleness check only rejected RTTs that
    // were too *large* (rtt_ns >= heartbeat_timeout), never negative ones. A
    // PONG whose echoed timestamp_ns claims to be in the future relative to
    // now_ns (clock skew, a buggy peer, or a forged message) produced a
    // negative rtt_ns that slipped straight through -- accepted as a valid
    // heartbeat, incrementing heartbeats_acked and polluting
    // stats().last_rtt_ns/ewma_rtt_ns with a nonsensical negative value.
    // Verified empirically before this fix: last_rtt_ns came out as
    // -999999999998 for a PONG claiming a timestamp ~1000s in the future.
    InitiatorFixture fx;

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 999'999'999'999LL;  // far in the "future" relative to the on_readable() call below
    send_wire(fx.peer_fd, pong);

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(2'001);  // now_ns barely after handshake -- nowhere near the claimed timestamp

    CHECK(fx.connection.stats().heartbeats_acked == 0u);
    CHECK(fx.connection.stats().last_rtt_ns == 0);
    CHECK(fx.connection.state() == ConnectionState::kHealthy);  // unaffected either way, just not counted
}

TEST_CASE("a message split across two on_readable calls reassembles correctly") {
    InitiatorFixture fx;

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 1'000;
    HeartbeatWireBuffer wire;
    encode_heartbeat(pong, wire);

    // send the first half, let the Connection see it, then the second half.
    ssize_t n = ::send(fx.peer_fd, wire.data(), 10, 0);
    REQUIRE(n == 10);
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(5'000);
    CHECK(fx.connection.stats().heartbeats_acked == 0u);  // nothing to parse yet -- only 10/28 bytes arrived

    n = ::send(fx.peer_fd, wire.data() + 10, wire.size() - 10, 0);
    REQUIRE(n == static_cast<ssize_t>(wire.size() - 10));
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(5'100);
    CHECK(fx.connection.stats().heartbeats_acked == 1u);  // now complete
}

TEST_CASE("write_pending_ queues under a full send buffer and flushes on the next writable event") {
    InitiatorFixture fx;

    // Shrink the client's send buffer so a burst of pings can't all fit in the
    // kernel socket buffer at once, forcing queue_write's immediate-send
    // attempt to short-write and leave bytes in write_pending_.
    int small_buf = 256;
    ::setsockopt(fx.client_fd, SOL_SOCKET, SO_SNDBUF, &small_buf, sizeof(small_buf));

    bool saw_queued_write = false;
    for (int i = 0; i < 200 && !saw_queued_write; ++i) {
        fx.connection.send_ping(static_cast<int64_t>(i));
        if (fx.connection.wants_write()) {
            saw_queued_write = true;
        }
    }
    REQUIRE(saw_queued_write);

    // Drain everything the peer received so far, then let the Connection
    // finish flushing once the socket is writable again.
    uint8_t drain_buf[65536];
    while (wait_readable(fx.peer_fd, 100)) {
        ::recv(fx.peer_fd, drain_buf, sizeof(drain_buf), 0);
    }
    REQUIRE(wait_writable(fx.client_fd));
    fx.connection.on_writable(999'999);
    CHECK(fx.connection.wants_write() == false);
}

TEST_CASE("write_pending_ exceeding the cap fails the connection instead of growing forever") {
    // Regression test: a peer that stops reading (wedged, slow-loris) used
    // to let write_pending_ grow one heartbeat's worth of bytes at a time,
    // forever, with no drain guarantee -- unlike command_line_buffer_
    // (capped separately), nothing bounded this. Same SO_SNDBUF-shrinking
    // technique as the neighboring test forces real accumulation instead of
    // the kernel silently absorbing everything.
    InitiatorFixture fx;

    int small_buf = 256;
    ::setsockopt(fx.client_fd, SOL_SOCKET, SO_SNDBUF, &small_buf, sizeof(small_buf));

    // Never read from fx.peer_fd -- the peer intentionally never drains.
    const size_t iterations = Connection::kMaxWritePendingBytes / kHeartbeatMessageSize + 10;
    for (size_t i = 0; i < iterations && fx.connection.state() != ConnectionState::kFailed; ++i) {
        fx.connection.send_ping(static_cast<int64_t>(i));
    }

    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("read_accum_ exceeding the cap within a single on_readable() call fails the connection, even when every individual message is well-formed") {
    // Regression test: drain_read_buffer() only runs once, after
    // on_readable()'s recv-until-EAGAIN loop exits -- so read_accum_ isn't
    // bounded by "steady-state leftover between calls is small" *within* one
    // call. A peer sustaining a flood faster than the drain rate could
    // otherwise grow it to an arbitrary size in one shot.
    //
    // Deliberately NOT a garbage/corrupt burst: a flood of garbage would
    // still end up kFailed via the pre-existing corrupt-message detection
    // once drain_read_buffer eventually ran, whether or not the cap exists
    // -- that wouldn't actually prove the cap does anything. Instead this
    // sends many individually well-formed, individually-acceptable PONGs
    // (each one alone would be happily processed) -- without the cap,
    // drain_read_buffer would eventually run and accept the entire burst,
    // ending HEALTHY with a huge acked count; the cap must fail the
    // connection before that ever happens, purely because of accumulated
    // size, independent of message validity.
    InitiatorFixture fx;

    // Large enough that the peer's blocking send() below completes without
    // blocking on our end draining it -- this test controls exactly when
    // on_readable() runs, so nothing drains the socket until then.
    int big_buf = 512 * 1024;
    ::setsockopt(fx.client_fd, SOL_SOCKET, SO_RCVBUF, &big_buf, sizeof(big_buf));

    const size_t message_count = Connection::kMaxReadAccumBytes / kHeartbeatMessageSize + 10;
    std::vector<uint8_t> burst;
    burst.reserve(message_count * kHeartbeatMessageSize);
    for (size_t i = 0; i < message_count; ++i) {
        HeartbeatMessage pong{};
        pong.type = static_cast<uint8_t>(MessageType::kPong);
        pong.feed_id = 42;
        pong.sequence = i;
        pong.timestamp_ns = 900;  // well within heartbeat_timeout of the on_readable() call below
        HeartbeatWireBuffer wire;
        encode_heartbeat(pong, wire);
        burst.insert(burst.end(), wire.begin(), wire.end());
    }

    size_t sent = 0;
    while (sent < burst.size()) {
        const ssize_t n = ::send(fx.peer_fd, burst.data() + sent, burst.size() - sent, 0);
        REQUIRE(n > 0);
        sent += static_cast<size_t>(n);
    }

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(1'000);

    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("peer closing the connection transitions to FAILED") {
    InitiatorFixture fx;
    ::close(fx.peer_fd);
    fx.peer_fd = -1;

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(50'000);
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("on_hangup_or_error transitions directly to FAILED") {
    InitiatorFixture fx;
    fx.connection.on_hangup_or_error(60'000, ECONNRESET);
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("rebind clears per-attempt state but preserves cumulative stats") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);
    CHECK(fx.connection.stats().heartbeats_sent == 1u);

    TestPeerListener new_peer;
    const int new_client_fd = connect_nonblocking(new_peer.port());
    fx.connection.rebind(new_client_fd, 100'000);

    CHECK(fx.connection.state() == ConnectionState::kConnecting);
    CHECK(fx.connection.fd() == new_client_fd);
    // cumulative stats survive a reconnect -- only per-attempt sequence/buffer
    // state resets.
    CHECK(fx.connection.stats().heartbeats_sent == 1u);
    CHECK(fx.connection.stats().reconnects == 1u);
    CHECK(fx.connection.stats().connected_since_ns == 0);  // not connected again yet

    // next ping after rebind starts sequence back at 0
    const int new_peer_fd = new_peer.accept_one();
    REQUIRE(wait_writable(new_client_fd));
    fx.connection.on_writable(100'100);
    recv_wire(new_peer_fd);  // CONNECT_HELLO
    HeartbeatMessage ack{};
    ack.type = static_cast<uint8_t>(MessageType::kConnectAck);
    ack.feed_id = 42;
    send_wire(new_peer_fd, ack);
    REQUIRE(wait_readable(new_client_fd));
    fx.connection.on_readable(100'200);
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
    CHECK(fx.connection.stats().connected_since_ns == 100'200);  // set again by the new handshake

    fx.connection.send_ping(100'300);
    HeartbeatMessage ping = recv_wire(new_peer_fd);
    CHECK(ping.sequence == 0u);  // reset, not continuing from the old connection's counter

    // no explicit close(new_peer_fd) here: new_peer (TestPeerListener) already
    // owns and closes it via RAII -- closing it again here would be a
    // double-close on whatever fd number happens to get reused next.
}

TEST_CASE("responder replies to PING with PONG echoing sequence and timestamp") {
    TestPeerListener peer;
    Config config = make_test_config();
    const int client_fd = connect_nonblocking(peer.port());
    const int accepted_fd = peer.accept_one();
    set_nonblocking(accepted_fd);

    Connection responder(accepted_fd, Role::kResponder, /*feed_id=*/7, "127.0.0.1", peer.port(), config);
    CHECK(responder.state() == ConnectionState::kHandshaking);

    HeartbeatMessage hello{};
    hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
    hello.feed_id = 7;
    send_wire(client_fd, hello);

    REQUIRE(wait_readable(accepted_fd));
    responder.on_readable(1'000);
    CHECK(responder.state() == ConnectionState::kHealthy);

    const HeartbeatMessage ack = recv_wire(client_fd);
    CHECK(static_cast<MessageType>(ack.type) == MessageType::kConnectAck);

    HeartbeatMessage ping{};
    ping.type = static_cast<uint8_t>(MessageType::kPing);
    ping.feed_id = 7;
    ping.sequence = 99;
    ping.timestamp_ns = 12345;
    send_wire(client_fd, ping);

    REQUIRE(wait_readable(accepted_fd));
    responder.on_readable(2'000);

    const HeartbeatMessage pong = recv_wire(client_fd);
    CHECK(static_cast<MessageType>(pong.type) == MessageType::kPong);
    CHECK(pong.sequence == 99u);
    CHECK(pong.timestamp_ns == 12345);  // echoed unchanged

    ::close(client_fd);
}

TEST_CASE("check_timeout fails a responder stuck in kHandshaking forever (no CONNECT_HELLO ever arrives)") {
    // Regression test for F13: Role::kResponder connections used to never
    // time out at all (check_timeout returned false unconditionally), so a
    // client that connects and then goes silent mid-handshake would occupy a
    // Connection+fd on FeedSimulator indefinitely -- a real, unbounded
    // resource-exhaustion vector for a hung or malicious test client.
    //
    // Unlike the handshake-progressing tests elsewhere in this file, nothing
    // here ever calls a handler with a test-controlled now_ns before this
    // check, so state_entered_ns_ still holds whatever now_monotonic_ns()
    // the constructor itself read -- t0 below captures that same real clock
    // right after construction so the injected "later" timestamps are
    // correctly relative to it, not to an arbitrary small literal.
    TestPeerListener peer;
    Config config = make_test_config();  // heartbeat_timeout = 3000ms
    const int client_fd = connect_nonblocking(peer.port());
    const int accepted_fd = peer.accept_one();
    set_nonblocking(accepted_fd);
    Connection responder(accepted_fd, Role::kResponder, 7, "127.0.0.1", peer.port(), config);
    const int64_t t0 = now_monotonic_ns();

    CHECK(responder.state() == ConnectionState::kHandshaking);

    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config.heartbeat_timeout).count();

    CHECK(responder.check_timeout(t0 + 100) == false);  // not yet timed out
    CHECK(responder.state() == ConnectionState::kHandshaking);

    CHECK(responder.check_timeout(t0 + timeout_ns + 1'000'000) == true);  // well past heartbeat_timeout
    CHECK(responder.state() == ConnectionState::kFailed);

    ::close(client_fd);
}

TEST_CASE("check_timeout fails a healthy responder whose client stops sending PING") {
    // Regression test for F13, the other half: a responder that completed
    // its handshake and received at least one PING, but then the peer goes
    // quiet (crashes without closing its socket, or is a deliberately hung
    // test client) -- this used to be undetectable from the responder side.
    TestPeerListener peer;
    Config config = make_test_config();  // heartbeat_timeout = 3000ms
    const int client_fd = connect_nonblocking(peer.port());
    const int accepted_fd = peer.accept_one();
    set_nonblocking(accepted_fd);
    Connection responder(accepted_fd, Role::kResponder, 7, "127.0.0.1", peer.port(), config);

    HeartbeatMessage hello{};
    hello.type = static_cast<uint8_t>(MessageType::kConnectHello);
    hello.feed_id = 7;
    send_wire(client_fd, hello);
    REQUIRE(wait_readable(accepted_fd));
    responder.on_readable(1'000);
    CHECK(responder.state() == ConnectionState::kHealthy);
    recv_wire(client_fd);  // drain the CONNECT_ACK

    HeartbeatMessage ping{};
    ping.type = static_cast<uint8_t>(MessageType::kPing);
    ping.feed_id = 7;
    ping.sequence = 0;
    ping.timestamp_ns = 1'000;
    send_wire(client_fd, ping);
    REQUIRE(wait_readable(accepted_fd));
    responder.on_readable(2'000);  // last_liveness_ns_ becomes 2000 here
    recv_wire(client_fd);          // drain the PONG

    const int64_t timeout_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(config.heartbeat_timeout).count();

    // ...then the client goes silent. Not yet timed out shortly after the last PING.
    CHECK(responder.check_timeout(2'000 + 100) == false);
    CHECK(responder.state() == ConnectionState::kHealthy);

    // Well past heartbeat_timeout with no further PING.
    CHECK(responder.check_timeout(2'000 + timeout_ns + 1) == true);
    CHECK(responder.state() == ConnectionState::kFailed);

    ::close(client_fd);
}
