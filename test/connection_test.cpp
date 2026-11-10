// Connection is tested against a minimal raw-socket peer defined in this file rather than
// FeedSimulator, which is itself built on Connection (that would be circular).

#include "connection.h"
#include "epoll_utils.h"
#include "time_utils.h"
#include "heartbeat.h"
#include "mini_test.h"

#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
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

// Blocking loopback listener acting as the remote side; owns one accepted peer socket.
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

    int accept_one() {
        peer_fd_ = ::accept(listen_fd_, nullptr, nullptr);
        return peer_fd_;
    }

    // Hands ownership of the accepted fd to the caller (for tests that close it mid-test).
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

// Non-blocking socket with connect() issued (likely EINPROGRESS): what a kInitiator expects.
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

// Reads and decodes exactly one message; REQUIREs success.
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

HeartbeatMessage make_msg(MessageType type, uint32_t feed_id, uint64_t sequence = 0, int64_t timestamp_ns = 0) {
    HeartbeatMessage msg{};
    msg.type = static_cast<uint8_t>(type);
    msg.feed_id = feed_id;
    msg.sequence = sequence;
    msg.timestamp_ns = timestamp_ns;
    return msg;
}

// A kInitiator Connection driven through connect + handshake to kHealthy against a raw peer.
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

// A kResponder Connection (feed 7) with `client_fd` as the raw peer; optionally completes the handshake.
struct ResponderFixture {
    TestPeerListener peer;
    Config config = make_test_config();
    int client_fd;
    int accepted_fd;
    Connection responder;

    explicit ResponderFixture(bool handshake)
        : client_fd(connect_nonblocking(peer.port())),
          accepted_fd(setup_accepted(peer)),
          responder(accepted_fd, Role::kResponder, /*feed_id=*/7, "127.0.0.1", peer.port(), config) {
        if (handshake) {
            send_wire(client_fd, make_msg(MessageType::kConnectHello, 7));
            REQUIRE(wait_readable(accepted_fd));
            responder.on_readable(1'000);
            REQUIRE(responder.state() == ConnectionState::kHealthy);
            recv_wire(client_fd);  // drain the CONNECT_ACK
        }
    }
    ~ResponderFixture() { ::close(client_fd); }

    // Sends `msg` from the peer and lets the responder process it.
    void deliver(const HeartbeatMessage& msg, int64_t now_ns = 2'000) {
        send_wire(client_fd, msg);
        REQUIRE(wait_readable(accepted_fd));
        responder.on_readable(now_ns);
    }

private:
    static int setup_accepted(TestPeerListener& peer) {
        const int fd = peer.accept_one();
        set_nonblocking(fd);
        return fd;
    }
};

// Delivers `msg` from the peer to the initiator's Connection and runs on_readable.
void deliver_to_initiator(InitiatorFixture& fx, const HeartbeatMessage& msg, int64_t now_ns) {
    send_wire(fx.peer_fd, msg);
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(now_ns);
}

}  // namespace

TEST_CASE("initiator reaches HEALTHY after CONNECT_HELLO/CONNECT_ACK handshake") {
    InitiatorFixture fx;
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
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
    // The peer sends a PONG and closes at once, so data and EOF are both visible to one on_readable().
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
    // Send two pings, then return their PONGs in reverse order.
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

    // The earlier sequence arrives late.
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

    int64_t t = 2'000;  // the fixture's handshake time is the liveness baseline
    t += timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == false);
    CHECK(fx.connection.state() == ConnectionState::kDegraded);

    t += timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == true);  // this call is the one that fails it
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("a PONG whose round trip already exceeds heartbeat_timeout does not resurrect a degraded connection") {
    InitiatorFixture fx;
    const int64_t timeout_ns = 3'000'000'000LL;  // matches make_test_config()'s 3000ms

    fx.connection.send_ping(2'000);
    const HeartbeatMessage ping = recv_wire(fx.peer_fd);
    REQUIRE(ping.sequence == 0u);

    // No reply in time: one miss -> DEGRADED (max_missed_heartbeats is 3).
    int64_t t = 2'000 + timeout_ns + 1;
    CHECK(fx.connection.check_timeout(t) == false);
    CHECK(fx.connection.state() == ConnectionState::kDegraded);

    // The PONG finally arrives, but its RTT already exceeds heartbeat_timeout.
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
    InitiatorFixture fx;
    fx.connection.send_ping(2'000);  // sequence 0 must exist for its PONG to be plausible
    recv_wire(fx.peer_fd);

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 999'999'999'999LL;  // far in the future relative to the on_readable() below
    send_wire(fx.peer_fd, pong);

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(2'001);

    CHECK(fx.connection.stats().heartbeats_acked == 0u);
    CHECK(fx.connection.stats().last_rtt_ns == 0);
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
}

TEST_CASE("a message split across two on_readable calls reassembles correctly") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);

    HeartbeatMessage pong{};
    pong.type = static_cast<uint8_t>(MessageType::kPong);
    pong.feed_id = 42;
    pong.sequence = 0;
    pong.timestamp_ns = 1'000;
    HeartbeatWireBuffer wire;
    encode_heartbeat(pong, wire);

    ssize_t n = ::send(fx.peer_fd, wire.data(), 10, 0);
    REQUIRE(n == 10);
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(5'000);
    CHECK(fx.connection.stats().heartbeats_acked == 0u);  // only 10/28 bytes so far

    n = ::send(fx.peer_fd, wire.data() + 10, wire.size() - 10, 0);
    REQUIRE(n == static_cast<ssize_t>(wire.size() - 10));
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(5'100);
    CHECK(fx.connection.stats().heartbeats_acked == 1u);  // now complete
}

TEST_CASE("write_pending_ queues under a full send buffer and flushes on the next writable event") {
    InitiatorFixture fx;

    // A tiny send buffer forces short writes, leaving bytes queued.
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

    uint8_t drain_buf[65536];
    while (wait_readable(fx.peer_fd, 100)) {
        ::recv(fx.peer_fd, drain_buf, sizeof(drain_buf), 0);
    }
    REQUIRE(wait_writable(fx.client_fd));
    fx.connection.on_writable(999'999);
    CHECK(fx.connection.wants_write() == false);
}

TEST_CASE("write_pending_ exceeding the cap fails the connection instead of growing forever") {
    InitiatorFixture fx;

    int small_buf = 256;
    ::setsockopt(fx.client_fd, SOL_SOCKET, SO_SNDBUF, &small_buf, sizeof(small_buf));

    // fx.peer_fd is never read, so the queue backs up.
    const size_t iterations = Connection::kMaxWritePendingBytes / kHeartbeatMessageSize + 10;
    for (size_t i = 0; i < iterations && fx.connection.state() != ConnectionState::kFailed; ++i) {
        fx.connection.send_ping(static_cast<int64_t>(i));
    }

    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("a burst larger than the read cap made of valid messages is processed, not failed") {
    // The read cap applies to the unparsed remainder, not to the size of a burst.
    InitiatorFixture fx;

    const size_t message_count = Connection::kMaxReadAccumBytes / kHeartbeatMessageSize + 10;
    for (size_t i = 0; i < message_count; ++i) {
        fx.connection.send_ping(900);
        if (i % 256 == 255) {
            uint8_t sink[16384];  // keep the peer draining
            while (wait_readable(fx.peer_fd, 0)) ::recv(fx.peer_fd, sink, sizeof(sink), 0);
        }
    }
    uint8_t sink[16384];
    while (wait_readable(fx.peer_fd, 0)) ::recv(fx.peer_fd, sink, sizeof(sink), 0);
    REQUIRE(fx.connection.state() == ConnectionState::kHealthy);

    // Big receive buffer so the peer's blocking send() completes before on_readable() runs.
    int big_buf = 512 * 1024;
    ::setsockopt(fx.client_fd, SOL_SOCKET, SO_RCVBUF, &big_buf, sizeof(big_buf));

    std::vector<uint8_t> burst;
    burst.reserve(message_count * kHeartbeatMessageSize);
    for (size_t i = 0; i < message_count; ++i) {
        HeartbeatWireBuffer wire;
        encode_heartbeat(make_msg(MessageType::kPong, 42, i, 900), wire);
        burst.insert(burst.end(), wire.begin(), wire.end());
    }
    size_t sent = 0;
    while (sent < burst.size()) {
        const ssize_t n = ::send(fx.peer_fd, burst.data() + sent, burst.size() - sent, 0);
        REQUIRE(n > 0);
        sent += static_cast<size_t>(n);
    }

    size_t guard = 0;
    while (fx.connection.stats().heartbeats_acked < message_count && guard++ < 1000) {
        if (wait_readable(fx.client_fd, 10)) fx.connection.on_readable(1'000);
    }
    CHECK(fx.connection.state() == ConnectionState::kHealthy);
    CHECK(fx.connection.stats().heartbeats_acked == message_count);
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
    CHECK(fx.connection.stats().heartbeats_sent == 1u);
    CHECK(fx.connection.stats().reconnects == 1u);
    CHECK(fx.connection.stats().connected_since_ns == 0);

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
    CHECK(ping.sequence == 0u);  // sequence restarts after rebind
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
    // The constructor stamped state_entered_ns_ from the real clock, so injected times are relative to t0.
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

    // The client goes silent: not timed out shortly after the last PING...
    CHECK(responder.check_timeout(2'000 + 100) == false);
    CHECK(responder.state() == ConnectionState::kHealthy);

    // ...but timed out once heartbeat_timeout passes with no PING.
    CHECK(responder.check_timeout(2'000 + timeout_ns + 1) == true);
    CHECK(responder.state() == ConnectionState::kFailed);

    ::close(client_fd);
}

// PONG validation (initiator side)
TEST_CASE("a PONG for a sequence that was never sent fails the connection and is not recorded as acked") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);  // only sequence 0 exists
    recv_wire(fx.peer_fd);

    deliver_to_initiator(fx, make_msg(MessageType::kPong, 42, /*sequence=*/5, 1'000), 1'500);

    CHECK(fx.connection.state() == ConnectionState::kFailed);
    CHECK(fx.connection.stats().heartbeats_acked == 0u);
}

TEST_CASE("a PONG with a forged extreme timestamp is ignored without overflow") {
    // now_ns - INT64_MIN would overflow; the range check must precede the subtraction.
    InitiatorFixture fx;
    for (int64_t forged : {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max(), int64_t{-1}}) {
        fx.connection.send_ping(1'000);
        recv_wire(fx.peer_fd);
        const uint64_t seq = fx.connection.stats().heartbeats_sent - 1;
        deliver_to_initiator(fx, make_msg(MessageType::kPong, 42, seq, forged), 1'500);
        CHECK(fx.connection.state() == ConnectionState::kHealthy);
        CHECK(fx.connection.stats().heartbeats_acked == 0u);
        CHECK(fx.connection.stats().last_rtt_ns == 0);
    }
}

TEST_CASE("a PONG with the wrong feed_id fails the connection") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);
    deliver_to_initiator(fx, make_msg(MessageType::kPong, /*feed_id=*/43, 0, 1'000), 1'500);
    CHECK(fx.connection.state() == ConnectionState::kFailed);
    CHECK(fx.connection.stats().heartbeats_acked == 0u);
}

TEST_CASE("a duplicate PONG is ignored and not double-counted") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);
    deliver_to_initiator(fx, make_msg(MessageType::kPong, 42, 0, 1'000), 1'500);
    CHECK(fx.connection.stats().heartbeats_acked == 1u);
    deliver_to_initiator(fx, make_msg(MessageType::kPong, 42, 0, 1'000), 1'600);
    CHECK(fx.connection.stats().heartbeats_acked == 1u);
    CHECK(fx.connection.state() == ConnectionState::kHealthy);  // benign, not a violation
}

TEST_CASE("an unexpected PONG (responder role, or initiator before the handshake finishes) fails the connection") {
    {
        ResponderFixture fx(/*handshake=*/true);
        fx.deliver(make_msg(MessageType::kPong, 7, 0, 1'000));
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
    {
        // initiator still in kHandshaking: connect done, CONNECT_ACK not yet received
        TestPeerListener peer;
        Config config = make_test_config();
        const int client_fd = connect_nonblocking(peer.port());
        Connection conn(client_fd, Role::kInitiator, 42, "127.0.0.1", peer.port(), config);
        const int peer_fd = peer.accept_one();
        REQUIRE(wait_writable(client_fd));
        conn.on_writable(1'000);
        REQUIRE(conn.state() == ConnectionState::kHandshaking);
        send_wire(peer_fd, make_msg(MessageType::kPong, 42, 0, 1'000));
        REQUIRE(wait_readable(client_fd));
        conn.on_readable(2'000);
        CHECK(conn.state() == ConnectionState::kFailed);
    }
}

// Other protocol-violation paths
TEST_CASE("a corrupt message mid-stream fails the connection, after the valid message before it was processed") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);

    HeartbeatWireBuffer good, bad;
    encode_heartbeat(make_msg(MessageType::kPong, 42, 0, 1'000), good);
    encode_heartbeat(make_msg(MessageType::kPong, 42, 0, 1'000), bad);
    bad[10] ^= 0xFF;  // payload byte flipped: CRC no longer matches
    uint8_t stream[2 * kHeartbeatMessageSize];
    std::memcpy(stream, good.data(), good.size());
    std::memcpy(stream + good.size(), bad.data(), bad.size());
    REQUIRE(::send(fx.peer_fd, stream, sizeof(stream), 0) == static_cast<ssize_t>(sizeof(stream)));

    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(1'500);

    CHECK(fx.connection.stats().heartbeats_acked == 1u);
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("an incoming DISCONNECT fails the connection") {
    InitiatorFixture fx;
    deliver_to_initiator(fx, make_msg(MessageType::kDisconnect, 42), 3'000);
    CHECK(fx.connection.state() == ConnectionState::kFailed);
}

TEST_CASE("unexpected CONNECT_HELLO: wrong role, repeated, or feed_id mismatch all fail the connection") {
    {
        InitiatorFixture fx;  // an initiator never accepts a HELLO
        deliver_to_initiator(fx, make_msg(MessageType::kConnectHello, 42), 3'000);
        CHECK(fx.connection.state() == ConnectionState::kFailed);
    }
    {
        ResponderFixture fx(/*handshake=*/true);  // a second HELLO after the handshake
        fx.deliver(make_msg(MessageType::kConnectHello, 7));
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
    {
        ResponderFixture fx(/*handshake=*/false);
        fx.deliver(make_msg(MessageType::kConnectHello, /*feed_id=*/8), 1'000);
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
}

TEST_CASE("unexpected CONNECT_ACK: wrong role, repeated, or feed_id mismatch all fail the connection") {
    {
        ResponderFixture fx(/*handshake=*/false);  // a responder never accepts an ACK
        fx.deliver(make_msg(MessageType::kConnectAck, 7), 1'000);
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
    {
        InitiatorFixture fx;  // already healthy, a second ACK is a violation
        deliver_to_initiator(fx, make_msg(MessageType::kConnectAck, 42), 3'000);
        CHECK(fx.connection.state() == ConnectionState::kFailed);
    }
    {
        TestPeerListener peer;
        Config config = make_test_config();
        const int client_fd = connect_nonblocking(peer.port());
        Connection conn(client_fd, Role::kInitiator, 42, "127.0.0.1", peer.port(), config);
        const int peer_fd = peer.accept_one();
        REQUIRE(wait_writable(client_fd));
        conn.on_writable(1'000);
        send_wire(peer_fd, make_msg(MessageType::kConnectAck, /*feed_id=*/99));
        REQUIRE(wait_readable(client_fd));
        conn.on_readable(2'000);
        CHECK(conn.state() == ConnectionState::kFailed);
    }
}

TEST_CASE("unexpected PING: initiator role, responder before handshake, or feed_id mismatch fail the connection") {
    {
        InitiatorFixture fx;
        deliver_to_initiator(fx, make_msg(MessageType::kPing, 42, 0, 1'000), 3'000);
        CHECK(fx.connection.state() == ConnectionState::kFailed);
    }
    {
        ResponderFixture fx(/*handshake=*/false);
        fx.deliver(make_msg(MessageType::kPing, 7, 0, 1'000), 1'000);
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
    {
        ResponderFixture fx(/*handshake=*/true);
        fx.deliver(make_msg(MessageType::kPing, /*feed_id=*/8, 0, 1'000));
        CHECK(fx.responder.state() == ConnectionState::kFailed);
    }
}

TEST_CASE("a failing CONNECT_ACK write is not overwritten by the transition to HEALTHY") {
    ResponderFixture fx(/*handshake=*/false);
    send_wire(fx.client_fd, make_msg(MessageType::kConnectHello, 7));
    REQUIRE(wait_readable(fx.accepted_fd));
    ::shutdown(fx.accepted_fd, SHUT_WR);  // makes the ACK send() fail with EPIPE
    fx.responder.on_readable(1'000);
    CHECK(fx.responder.state() == ConnectionState::kFailed);
}

TEST_CASE("on_readable and on_writable are no-ops once the connection has failed") {
    InitiatorFixture fx;
    fx.connection.send_ping(1'000);
    recv_wire(fx.peer_fd);
    fx.connection.on_hangup_or_error(1'100, ECONNRESET);
    REQUIRE(fx.connection.state() == ConnectionState::kFailed);

    send_wire(fx.peer_fd, make_msg(MessageType::kPong, 42, 0, 1'000));
    REQUIRE(wait_readable(fx.client_fd));
    fx.connection.on_readable(1'200);
    fx.connection.on_writable(1'300);

    CHECK(fx.connection.state() == ConnectionState::kFailed);
    CHECK(fx.connection.stats().heartbeats_acked == 0u);  // the PONG was never read
}

// epoll_utils.h
TEST_CASE("arm_periodic_timer arms a timerfd and throws on an invalid fd") {
    const int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    REQUIRE(tfd >= 0);
    arm_periodic_timer(tfd, std::chrono::milliseconds(10));
    itimerspec cur{};
    REQUIRE(::timerfd_gettime(tfd, &cur) == 0);
    CHECK(cur.it_interval.tv_sec == 0);
    CHECK(cur.it_interval.tv_nsec == 10'000'000);
    ::close(tfd);

    bool threw = false;
    try {
        arm_periodic_timer(-1, std::chrono::milliseconds(10));
    } catch (const std::runtime_error& e) {
        threw = std::string(e.what()).find("timerfd_settime") != std::string::npos;
    }
    CHECK(threw);
}

TEST_CASE("throw_errno reports the errno that was current at the call") {
    errno = ENOENT;
    bool ok = false;
    try {
        throw_errno("open thing");
    } catch (const std::runtime_error& e) {
        ok = std::string(e.what()) == std::string("open thing: ") + std::strerror(ENOENT);
    }
    CHECK(ok);
}
