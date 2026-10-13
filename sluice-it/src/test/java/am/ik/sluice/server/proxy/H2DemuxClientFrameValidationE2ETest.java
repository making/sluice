package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux checks every client frame RFC 9113 makes a connection error of before the
 * frame has any effect: the connection ends with a GOAWAY of the error's code, nothing of
 * the frame reaches the upstream, and the relay lets go of the connection, its leg and
 * its streams. The well-formed counterpart of each keeps the connection. A frame making a
 * stream error resets only its stream. In the relay's package for its meters.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxClientFrameValidationE2ETest {

	private static final String ROUTE = "fv.test";

	/**
	 * A route whose upstream announces a stream window of 0 and answers heads at once.
	 */
	private static final String HELD_ROUTE = "fv-held.test";

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int FLOW_CONTROL_ERROR = 0x3;

	private static final int FRAME_SIZE_ERROR = 0x6;

	private static final int CANCEL = 0x8;

	private static final int PRIORITY = 0x2;

	private static final int PUSH_PROMISE = 0x5;

	private static final int UNKNOWN = 0xfa;

	private static final int FLAG_PRIORITY = 0x20;

	private static final int SETTINGS_ENABLE_PUSH = 0x2;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int SETTINGS_MAX_FRAME_SIZE = 0x5;

	private static final int SETTINGS_ENABLE_CONNECT_PROTOCOL = 0x8;

	private static final int SETTINGS_NO_RFC7540_PRIORITIES = 0x9;

	private static final int MAX_WINDOW = Integer.MAX_VALUE;

	private static final int WINDOW = H2TestClient.INITIAL_WINDOW;

	private static final AtomicLong PINGS = new AtomicLong();

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	/** The upstream connections accepted, in order. */
	private final BlockingQueue<UpstreamConnection> upstreams = new LinkedBlockingQueue<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startRoutes() {
		startClient(ROUTE, upstreamServer(false));
		startClient(HELD_ROUTE, upstreamServer(true));
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxClientFrameValidationE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Sent while stream 1 of the route is open (its request unfinished), the frame ends
	 * the connection with a GOAWAY of the error's code; nothing more reaches the
	 * upstream, whose connection is closed, and the relay tracks no stream and holds no
	 * connection any more.
	 */
	@ParameterizedTest(name = "{0}")
	@MethodSource("violations")
	void violationEndsTheConnection(Violation violation) throws Exception {
		try (Session session = open(ROUTE, "/")) {
			int before = session.upstream().frames().size();
			session.client().raw(violation.bytes());
			assertThat(session.client().awaitTrue(session.client()::closed, Duration.ofSeconds(5)))
				.as("the relay closed the connection")
				.isTrue();
			assertThat(session.client().goAwayCode()).isEqualTo(violation.code());
			assertThat(session.upstream().eof().await(5, TimeUnit.SECONDS)).as("the leg closed").isTrue();
			assertThat(session.upstream().frames().subList(before, session.upstream().frames().size()))
				.as("nothing reached the upstream")
				.isEmpty();
			awaitReleased(ROUTE);
		}
	}

	static Stream<Violation> violations() {
		byte[] get3 = get(ROUTE);
		return Stream.of(
				// frame sizes (RFC 9113 6.4-6.9, 4.2)
				new Violation("PING of 7", FRAME_SIZE_ERROR, frame(TestH2.PING, 0, 0, new byte[7])),
				new Violation("PING of 9", FRAME_SIZE_ERROR, frame(TestH2.PING, 0, 0, new byte[9])),
				new Violation("PING ack of 7", FRAME_SIZE_ERROR, frame(TestH2.PING, TestH2.FLAG_ACK, 0, new byte[7])),
				new Violation("RST_STREAM of 3", FRAME_SIZE_ERROR, frame(TestH2.RST_STREAM, 0, 1, new byte[3])),
				new Violation("RST_STREAM of 5", FRAME_SIZE_ERROR, frame(TestH2.RST_STREAM, 0, 1, new byte[5])),
				new Violation("SETTINGS of 5", FRAME_SIZE_ERROR, frame(TestH2.SETTINGS, 0, 0, new byte[5])),
				new Violation("SETTINGS of 7", FRAME_SIZE_ERROR,
						frame(TestH2.SETTINGS, 0, 0,
								Arrays.copyOf(WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 7), 7))),
				new Violation("SETTINGS ack of 6", FRAME_SIZE_ERROR,
						frame(TestH2.SETTINGS, TestH2.FLAG_ACK, 0,
								WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 7))),
				new Violation("GOAWAY of 7", FRAME_SIZE_ERROR, frame(TestH2.GOAWAY, 0, 0, new byte[7])),
				new Violation("padded HEADERS of 0", FRAME_SIZE_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_PADDED | TestH2.FLAG_END_HEADERS, 3, new byte[0])),
				new Violation("HEADERS with priority of 4", FRAME_SIZE_ERROR,
						frame(TestH2.HEADERS, FLAG_PRIORITY | TestH2.FLAG_END_HEADERS, 3, new byte[4])),
				new Violation("padded HEADERS with priority of 5", FRAME_SIZE_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_PADDED | FLAG_PRIORITY | TestH2.FLAG_END_HEADERS, 3,
								new byte[5])),
				// padding past the payload (RFC 9113 6.1, 6.2)
				new Violation("DATA padded past its payload", PROTOCOL_ERROR,
						frame(TestH2.DATA, TestH2.FLAG_PADDED, 1, new byte[] { 3, 0, 0 })),
				new Violation("HEADERS padded past its payload", PROTOCOL_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_PADDED | TestH2.FLAG_END_HEADERS, 3,
								concat(new byte[] { (byte) (get3.length + 1) }, get3))),
				new Violation("HEADERS with priority padded past its payload", PROTOCOL_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_PADDED | FLAG_PRIORITY | TestH2.FLAG_END_HEADERS, 3,
								new byte[] { 2, 0, 0, 0, 0, 0, 0 })),
				// stream 0 (RFC 9113 6.1-6.8, 6.10) and PUSH_PROMISE (8.4)
				new Violation("DATA on stream 0", PROTOCOL_ERROR, frame(TestH2.DATA, 0, 0, new byte[1])),
				new Violation("HEADERS on stream 0", PROTOCOL_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 0, get3)),
				new Violation("CONTINUATION on stream 0", PROTOCOL_ERROR,
						frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 0, get3)),
				new Violation("RST_STREAM on stream 0", PROTOCOL_ERROR,
						frame(TestH2.RST_STREAM, 0, 0, TestH2.int32(CANCEL))),
				new Violation("PRIORITY on stream 0", PROTOCOL_ERROR, frame(PRIORITY, 0, 0, new byte[5])),
				new Violation("SETTINGS on stream 1", PROTOCOL_ERROR, frame(TestH2.SETTINGS, 0, 1, new byte[0])),
				new Violation("PING on stream 1", PROTOCOL_ERROR, frame(TestH2.PING, 0, 1, new byte[8])),
				new Violation("GOAWAY on stream 1", PROTOCOL_ERROR, frame(TestH2.GOAWAY, 0, 1, new byte[8])),
				new Violation("PUSH_PROMISE", PROTOCOL_ERROR,
						frame(PUSH_PROMISE, TestH2.FLAG_END_HEADERS, 1, concat(TestH2.int32(2), get3))),
				// idle streams (RFC 9113 5.1, 5.1.1)
				new Violation("DATA on an idle stream", PROTOCOL_ERROR, frame(TestH2.DATA, 0, 3, new byte[1])),
				new Violation("RST_STREAM on an idle stream", PROTOCOL_ERROR,
						frame(TestH2.RST_STREAM, 0, 3, TestH2.int32(CANCEL))),
				new Violation("WINDOW_UPDATE on an idle stream", PROTOCOL_ERROR,
						frame(TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32(1))),
				new Violation("DATA on an even stream", PROTOCOL_ERROR, frame(TestH2.DATA, 0, 2, new byte[1])),
				new Violation("HEADERS on an even stream", PROTOCOL_ERROR,
						frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 2, get3)),
				new Violation("CONTINUATION outside a header block", PROTOCOL_ERROR,
						frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 1, get3)),
				new Violation("CONTINUATION opening a stream", PROTOCOL_ERROR,
						frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 3, get3)),
				// inside a header block (RFC 9113 6.10, 5.5)
				insideBlock("HEADERS of another stream",
						frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 5, get(ROUTE))),
				insideBlock("CONTINUATION of another stream",
						frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 5, new byte[0])),
				insideBlock("DATA", frame(TestH2.DATA, 0, 1, new byte[1])),
				insideBlock("PING", frame(TestH2.PING, 0, 0, new byte[8])),
				insideBlock("SETTINGS", frame(TestH2.SETTINGS, 0, 0, new byte[0])),
				insideBlock("WINDOW_UPDATE", frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(1))),
				insideBlock("PRIORITY", frame(PRIORITY, 0, 3, new byte[5])),
				insideBlock("RST_STREAM", frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL))),
				insideBlock("an unknown frame type", frame(UNKNOWN, 0, 0, new byte[3])),
				// settings values (RFC 9113 6.5.2, RFC 8441 3, RFC 9218 2.1)
				setting(SETTINGS_ENABLE_PUSH, 2, PROTOCOL_ERROR),
				setting(SETTINGS_INITIAL_WINDOW_SIZE, MAX_WINDOW + 1L, FLOW_CONTROL_ERROR),
				setting(SETTINGS_INITIAL_WINDOW_SIZE, 0xffffffffL, FLOW_CONTROL_ERROR),
				setting(SETTINGS_MAX_FRAME_SIZE, 16 * 1024 - 1, PROTOCOL_ERROR),
				setting(SETTINGS_MAX_FRAME_SIZE, 1 << 24, PROTOCOL_ERROR),
				setting(SETTINGS_ENABLE_CONNECT_PROTOCOL, 2, PROTOCOL_ERROR),
				new Violation("SETTINGS with a valid entry before an invalid one", PROTOCOL_ERROR,
						frame(TestH2.SETTINGS, 0, 0,
								concat(WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 7),
										WindowedH2Upstream.setting(SETTINGS_ENABLE_PUSH, 2)))));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("violationsTheCodecAnswersOtherwise")
	@Disabled("netty's codec does not end the connection on these frames: to be rewritten for its answers")
	void violationTheCodecAnswersOtherwiseEndsTheConnection(Violation violation) throws Exception {
		violationEndsTheConnection(violation);
	}

	static Stream<Violation> violationsTheCodecAnswersOtherwise() {
		return Stream.of(
				new Violation("PRIORITY of 4 on an idle stream", FRAME_SIZE_ERROR, frame(PRIORITY, 0, 3, new byte[4])),
				new Violation("PRIORITY of 6 on an idle stream", FRAME_SIZE_ERROR, frame(PRIORITY, 0, 3, new byte[6])),
				new Violation("padded DATA of 0", FRAME_SIZE_ERROR,
						frame(TestH2.DATA, TestH2.FLAG_PADDED, 1, new byte[0])),
				setting(SETTINGS_NO_RFC7540_PRIORITIES, 2, PROTOCOL_ERROR));
	}

	/**
	 * Sent while stream 1 of the route is open, the frame keeps the connection: a stream
	 * the frames open gets its response, and so does a stream opened after them.
	 */
	@ParameterizedTest(name = "{0}")
	@MethodSource("counterparts")
	void counterpartKeepsTheConnection(Counterpart counterpart) throws Exception {
		try (Session session = open(ROUTE, "/")) {
			H2TestClient client = session.client();
			client.raw(counterpart.bytes());
			if (counterpart.opened() != 0) {
				assertThat(client.awaitEnd(counterpart.opened(), Duration.ofSeconds(5))).as("the stream opened")
					.isTrue();
				assertThat(TestH2.bodyOf(client.frames(counterpart.opened()))).isEqualTo("ok");
			}
			client.headers(5, true, get(ROUTE));
			assertThat(client.awaitEnd(5, Duration.ofSeconds(5))).as("a later stream completes").isTrue();
			assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("ok");
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	static Stream<Counterpart> counterparts() {
		byte[] get3 = get(ROUTE);
		int half = get3.length / 2;
		return Stream.of(new Counterpart("PING of 8", 0, frame(TestH2.PING, 0, 0, new byte[8])),
				new Counterpart("PING ack of 8", 0, frame(TestH2.PING, TestH2.FLAG_ACK, 0, new byte[8])),
				new Counterpart("RST_STREAM of 4", 0, frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL))),
				new Counterpart("SETTINGS of 6", 0,
						frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 7))),
				new Counterpart("SETTINGS ack of 0", 0, frame(TestH2.SETTINGS, TestH2.FLAG_ACK, 0, new byte[0])),
				new Counterpart("GOAWAY of 8", 0, frame(TestH2.GOAWAY, 0, 0, new byte[8])),
				new Counterpart("GOAWAY with debug data", 0, frame(TestH2.GOAWAY, 0, 0, new byte[12])),
				new Counterpart("PRIORITY of 5 on an open stream", 0, frame(PRIORITY, 0, 1, new byte[5])),
				new Counterpart("PRIORITY of 5 on an idle stream", 0, frame(PRIORITY, 0, 3, new byte[5])),
				new Counterpart("PRIORITY of 5 on an even stream", 0, frame(PRIORITY, 0, 2, new byte[5])),
				new Counterpart("DATA padded to its payload", 0,
						frame(TestH2.DATA, TestH2.FLAG_PADDED, 1, new byte[] { 2, 0, 0 })),
				new Counterpart("HEADERS padded to its payload", 3,
						frame(TestH2.HEADERS, TestH2.FLAG_PADDED | TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
								padded(get3, 4))),
				new Counterpart("HEADERS with priority and padding", 3,
						frame(TestH2.HEADERS,
								TestH2.FLAG_PADDED | FLAG_PRIORITY | TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM,
								3, padded(concat(new byte[5], get3), 2))),
				new Counterpart("an unknown frame type on an idle stream", 0, frame(UNKNOWN, 0, 9, new byte[3])),
				new Counterpart("an unknown frame type on stream 0", 0, frame(UNKNOWN, 0xff, 0, new byte[3])),
				new Counterpart("a header block split by a CONTINUATION", 3,
						concat(frame(TestH2.HEADERS, TestH2.FLAG_END_STREAM, 3, Arrays.copyOf(get3, half)),
								frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 3,
										Arrays.copyOfRange(get3, half, get3.length)))),
				new Counterpart("a CONTINUATION with flags it does not define", 3,
						concat(frame(TestH2.HEADERS, TestH2.FLAG_END_STREAM, 3, Arrays.copyOf(get3, half)),
								frame(TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS | TestH2.FLAG_PADDED | FLAG_PRIORITY,
										3, Arrays.copyOfRange(get3, half, get3.length)))),
				counterpartSetting(SETTINGS_ENABLE_PUSH, 1),
				counterpartSetting(SETTINGS_INITIAL_WINDOW_SIZE, MAX_WINDOW),
				counterpartSetting(SETTINGS_MAX_FRAME_SIZE, 16 * 1024),
				counterpartSetting(SETTINGS_MAX_FRAME_SIZE, (1 << 24) - 1),
				counterpartSetting(SETTINGS_ENABLE_CONNECT_PROTOCOL, 1),
				counterpartSetting(SETTINGS_NO_RFC7540_PRIORITIES, 1), counterpartSetting(0xff, 0xffffffffL));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("counterpartsTheCodecAnswersOtherwise")
	@Disabled("netty's codec ends the connection on these frames: to be rewritten for its answers")
	void counterpartTheCodecAnswersOtherwiseKeepsTheConnection(Counterpart counterpart) throws Exception {
		counterpartKeepsTheConnection(counterpart);
	}

	static Stream<Counterpart> counterpartsTheCodecAnswersOtherwise() {
		return Stream.of(
				new Counterpart("RST_STREAM on an even stream", 0,
						frame(TestH2.RST_STREAM, 0, 2, TestH2.int32(CANCEL))),
				new Counterpart("WINDOW_UPDATE on an even stream", 0,
						frame(TestH2.WINDOW_UPDATE, 0, 2, TestH2.int32(1))));
	}

	/**
	 * A PRIORITY of a length other than 5 on an open stream is a stream error (RFC 9113
	 * 6.3): RST_STREAM {@code FRAME_SIZE_ERROR} on that stream only, {@code CANCEL} to
	 * the upstream, counted once with reason {@code protocol}; the other stream
	 * completes. Once the relay forgot the reset stream, another such PRIORITY on it ends
	 * the connection with GOAWAY {@code FRAME_SIZE_ERROR}.
	 */
	@Test
	@Disabled("counts the protocol reset meter (sluice.h2demux.streams.reset, reason protocol), "
			+ "not yet on the h2 demux")
	void priorityOfAWrongLengthResetsOnlyItsStream() throws Exception {
		double protocolResets = resets("protocol");
		try (Session session = open(ROUTE, "/")) {
			H2TestClient client = session.client();
			client.headers(3, false, post(ROUTE));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> session.upstream().has(TestH2.HEADERS, 3));
			assertThat(trackedStreams(ROUTE)).as("both streams open").isEqualTo(2);
			client.frame(PRIORITY, 0, 1, new byte[4]);
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(1)).isEqualTo(FRAME_SIZE_ERROR);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(session.upstream().resets()).containsExactly(List.of(1, CANCEL)));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(ROUTE) == 1);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
			client.sendData(3, "x".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok");
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			client.frame(PRIORITY, 0, 1, new byte[4]);
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).isTrue();
			assertThat(client.goAwayCode()).isEqualTo(FRAME_SIZE_ERROR);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
		}
	}

	/**
	 * Two WINDOW_UPDATEs of one stream in one write, together taking its window past
	 * 2^31-1 -- coalesced or not by the relay -- reset that stream with
	 * {@code FLOW_CONTROL_ERROR} (RFC 9113 6.9.1), {@code CANCEL} to the upstream, whose
	 * window never passes 2^31-1; counted once. Another stream granted exactly up to
	 * 2^31-1 goes on and completes.
	 */
	@Test
	@Disabled("counts the protocol reset meter (sluice.h2demux.streams.reset, reason protocol), "
			+ "not yet on the h2 demux")
	void streamWindowPastTheMaximumResetsOnlyItsStream() throws Exception {
		double protocolResets = resets("protocol");
		try (Session session = open(ROUTE, "/")) {
			H2TestClient client = session.client();
			client.headers(3, false, post(ROUTE));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> session.upstream().has(TestH2.HEADERS, 3));
			client.raw(concat(frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(MAX_WINDOW - WINDOW)),
					frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(1))));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(1)).isEqualTo(FLOW_CONTROL_ERROR);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(session.upstream().resets()).containsExactly(List.of(1, CANCEL)));
			assertThat(WINDOW + session.upstream().windowGranted(1)).as("the upstream's window of stream 1")
				.isLessThanOrEqualTo(MAX_WINDOW);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
			client.frame(TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32(MAX_WINDOW - WINDOW));
			client.sendData(3, "x".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok");
			assertThat(client.reset(3)).isNull();
			assertThat(session.upstream().windowGranted(3)).as("relayed").isEqualTo(MAX_WINDOW - WINDOW);
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
		}
	}

	/**
	 * A stream the client ended, whose head-only response ended too, stays tracked while
	 * its request DATA waits for the upstream's stream window of 0: closed both ways, it
	 * is not reset by a WINDOW_UPDATE past 2^31-1, which is not relayed either, nor by a
	 * PRIORITY of a wrong length, which ends the connection with GOAWAY
	 * {@code FRAME_SIZE_ERROR} instead.
	 */
	@Test
	@Disabled("counts the protocol reset meter (sluice.h2demux.streams.reset, reason protocol), "
			+ "not yet on the h2 demux")
	void streamClosedBothWaysIsNotReset() throws Exception {
		double protocolResets = resets("protocol");
		try (Session session = open(HELD_ROUTE, "/")) {
			H2TestClient client = session.client();
			// the upstream's SETTINGS precede its response on the leg: applied by now
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.sendData(1, "abc".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			awaitPingAck(client);
			assertThat(trackedStreams(HELD_ROUTE)).as("tracked while its DATA waits for the upstream's window")
				.isEqualTo(1);
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(MAX_WINDOW));
			awaitPingAck(client);
			assertThat(client.resets()).isEmpty();
			client.frame(PRIORITY, 0, 1, new byte[4]);
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).isTrue();
			assertThat(client.goAwayCode()).isEqualTo(FRAME_SIZE_ERROR);
			assertThat(client.resets()).isEmpty();
			assertThat(session.upstream().resets()).isEmpty();
			assertThat(session.upstream().windowGranted(1)).as("nothing relayed").isZero();
			assertThat(resets("protocol") - protocolResets).isZero();
			awaitReleased(HELD_ROUTE);
		}
	}

	/**
	 * A change of SETTINGS_INITIAL_WINDOW_SIZE taking the window of a stream whose
	 * response goes on past 2^31-1 ends the connection with GOAWAY
	 * {@code FLOW_CONTROL_ERROR} (RFC 9113 6.9.2); the setting never reaches the
	 * upstream.
	 */
	@Test
	@Disabled("pins the hand-written relay's handling of the client's "
			+ "SETTINGS_INITIAL_WINDOW_SIZE against the upstream's stream windows; the relay "
			+ "keeps windows per hop: to be rewritten for it")
	void initialWindowChangePastTheMaximumEndsTheConnection() throws Exception {
		try (Session session = open(ROUTE, "/")) {
			H2TestClient client = session.client();
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(MAX_WINDOW - WINDOW));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> session.upstream().windowGranted(1) == MAX_WINDOW - WINDOW);
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, WINDOW + 1));
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).isTrue();
			assertThat(client.goAwayCode()).isEqualTo(FLOW_CONTROL_ERROR);
			assertThat(session.upstream().eof().await(5, TimeUnit.SECONDS)).isTrue();
			assertThat(session.upstream().settings(SETTINGS_INITIAL_WINDOW_SIZE)).isEmpty();
			awaitReleased(ROUTE);
		}
	}

	/**
	 * A change of SETTINGS_INITIAL_WINDOW_SIZE taking a stream's window to 2^31-1 exactly
	 * keeps the connection, and the upstream gets the setting.
	 */
	@Test
	@Disabled("pins the hand-written relay's handling of the client's "
			+ "SETTINGS_INITIAL_WINDOW_SIZE against the upstream's stream windows; the relay "
			+ "keeps windows per hop: to be rewritten for it")
	void initialWindowChangeToTheMaximumKeepsTheConnection() throws Exception {
		try (Session session = open(ROUTE, "/")) {
			H2TestClient client = session.client();
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(MAX_WINDOW - WINDOW - 1));
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, WINDOW + 1));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> session.upstream().settings(SETTINGS_INITIAL_WINDOW_SIZE).equals(List.of(WINDOW + 1L)));
			client.sendData(1, "x".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * A stream whose response ended, its request still going on, has no window the
	 * relay's peer sends into any more: a change of SETTINGS_INITIAL_WINDOW_SIZE that
	 * would take its window past 2^31-1 keeps the connection.
	 */
	@Test
	@Disabled("pins the hand-written relay's handling of the client's "
			+ "SETTINGS_INITIAL_WINDOW_SIZE against the upstream's stream windows; the relay "
			+ "keeps windows per hop: to be rewritten for it")
	void initialWindowChangeSparesAStreamWhoseResponseEnded() throws Exception {
		try (Session session = open(ROUTE, "/early")) {
			H2TestClient client = session.client();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).as("the response ended").isTrue();
			// the window, less the response's two bytes, at the maximum
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(MAX_WINDOW - WINDOW + 2));
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, WINDOW + 1));
			awaitPingAck(client);
			assertThat(trackedStreams(ROUTE)).as("stream 1 still tracked: its request goes on").isEqualTo(1);
			client.headers(3, true, get(ROUTE));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.resets()).isEmpty();
		}
	}

	private static Violation insideBlock(String name, byte[] offending) {
		byte[] get3 = get(ROUTE);
		return new Violation(name + " inside a header block", PROTOCOL_ERROR, concat(
				frame(TestH2.HEADERS, TestH2.FLAG_END_STREAM, 3, Arrays.copyOf(get3, get3.length / 2)), offending));
	}

	private static Violation setting(int id, long value, int code) {
		return new Violation("setting 0x%x of %d".formatted(id, value), code,
				frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(id, value)));
	}

	private static Counterpart counterpartSetting(int id, long value) {
		return new Counterpart("setting 0x%x of %d".formatted(id, value), 0,
				frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(id, value)));
	}

	/**
	 * Opens stream 1 of the route with a POST to the path whose end never comes, once the
	 * previous connection is gone; returns when the upstream acked the relay's SETTINGS
	 * and got the head.
	 */
	private Session open(String route, String path) throws Exception {
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> proxy.activeConnections() == 0 && trackedStreams(route) == 0);
		this.upstreams.clear();
		H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5);
		client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=" + path));
		UpstreamConnection upstream = Objects.requireNonNull(this.upstreams.poll(10, TimeUnit.SECONDS));
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> upstream.has(TestH2.HEADERS, 1) && upstream.settingsAcked());
		assertThat(trackedStreams(route)).as("stream 1 open").isEqualTo(1);
		return new Session(client, upstream);
	}

	/** The relay let go of the connection and of every stream of the route. */
	private void awaitReleased(String route) {
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> proxy.activeConnections() == 0 && trackedStreams(route) == 0);
	}

	/**
	 * Pings the relay and waits for the ack: the outbound parse handled every frame sent
	 * before.
	 */
	private static void awaitPingAck(H2TestClient client) throws Exception {
		byte[] ping = Arrays.copyOf(TestH2.int32((int) PINGS.incrementAndGet()), 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
				Duration.ofSeconds(5)))
			.as("the relay acked the PING")
			.isTrue();
	}

	private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			TestH2.writeFrame(out, type, flags, streamId, payload);
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return out.toByteArray();
	}

	/** A padded payload: the Pad Length, the content, then {@code padLength} zeros. */
	private static byte[] padded(byte[] content, int padLength) {
		byte[] payload = new byte[1 + content.length + padLength];
		payload[0] = (byte) padLength;
		System.arraycopy(content, 0, payload, 1, content.length);
		return payload;
	}

	private static byte[] concat(byte[]... parts) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private double resets(String reason) {
		return Objects.requireNonNull(this.dataProxyServer)
			.h2DemuxMeters()
			.registry()
			.get("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-fv").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/**
	 * A test upstream publishing every accepted connection to {@link #upstreams}. It
	 * answers a request ({@code :status 200}, body {@code ok}) once the request ended, or
	 * at once for the path {@code /early}; a held upstream announces a stream window of 0
	 * and answers every head at once without a body.
	 */
	private ServerSocket upstreamServer(boolean held) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-fv-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> serve(socket, held));
					}
					catch (IOException e) {
						return;
					}
				}
			});
			return server;
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private void serve(Socket socket, boolean held) {
		UpstreamConnection connection = new UpstreamConnection();
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0,
					held ? WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, 0) : new byte[0]);
			this.upstreams.add(connection);
			DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);
			Set<Integer> answered = new HashSet<>();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				connection.frames().add(frame);
				int streamId = frame.streamId();
				boolean ended = (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (frame.type() == TestH2.HEADERS) {
					// the relay's blocks are table-less and fit one frame here
					Http2Headers headers = decoder.decodeHeaders(streamId, Unpooled.wrappedBuffer(frame.payload()));
					if (held) {
						TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM,
								streamId, new byte[] { (byte) 0x88 });
						answered.add(streamId);
					}
					else if (ended || "/early".contentEquals(Objects.requireNonNull(headers.path()))) {
						answer(out, streamId);
						answered.add(streamId);
					}
				}
				else if (frame.type() == TestH2.DATA && ended && answered.add(streamId)) {
					answer(out, streamId);
				}
			}
		}
		catch (Exception e) {
			// the relay went away
		}
		finally {
			connection.eof().countDown();
		}
	}

	private static void answer(OutputStream out, int streamId) throws IOException {
		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, new byte[] { (byte) 0x88 });
		TestH2.writeFrame(out, TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, "ok".getBytes(StandardCharsets.US_ASCII));
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/** A client frame RFC 9113 makes a connection error of, and the code. */
	record Violation(String name, int code, byte[] bytes) {

		@Override
		public String toString() {
			return this.name;
		}

	}

	/**
	 * A well-formed counterpart of a violation, and the stream its frames open with a GET
	 * ending the request, or 0.
	 */
	record Counterpart(String name, int opened, byte[] bytes) {

		@Override
		public String toString() {
			return this.name;
		}

	}

	/** The frames one upstream connection received, and its end. */
	record UpstreamConnection(List<TestH2.Frame> frames, CountDownLatch eof) {

		UpstreamConnection() {
			this(new CopyOnWriteArrayList<>(), new CountDownLatch(1));
		}

		boolean has(int type, int streamId) {
			return this.frames.stream().anyMatch(frame -> frame.type() == type && frame.streamId() == streamId);
		}

		boolean settingsAcked() {
			return this.frames.stream()
				.anyMatch(frame -> frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) != 0);
		}

		/** The RST_STREAM frames received: stream id and error code. */
		List<List<Integer>> resets() {
			return this.frames.stream()
				.filter(frame -> frame.type() == TestH2.RST_STREAM)
				.map(frame -> List.of(frame.streamId(), TestH2.errorCode(frame)))
				.toList();
		}

		/**
		 * The window the relay granted the upstream on the stream, all updates summed.
		 */
		long windowGranted(int streamId) {
			return this.frames.stream()
				.filter(frame -> frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() == streamId)
				.mapToLong(TestH2::increment)
				.sum();
		}

		/** The values of the setting in the SETTINGS frames received, in order. */
		List<Long> settings(int id) {
			List<Long> values = new ArrayList<>();
			for (TestH2.Frame frame : this.frames) {
				if (frame.type() != TestH2.SETTINGS || (frame.flags() & TestH2.FLAG_ACK) != 0) {
					continue;
				}
				byte[] p = frame.payload();
				for (int i = 0; i + 6 <= p.length; i += 6) {
					if (((p[i] & 0xff) << 8 | (p[i + 1] & 0xff)) == id) {
						values.add((p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8
								| (p[i + 5] & 0xff));
					}
				}
			}
			return values;
		}

	}

	/** One client connection and the upstream connection of its leg. */
	record Session(H2TestClient client, UpstreamConnection upstream) implements AutoCloseable {

		@Override
		public void close() throws IOException {
			this.client.close();
		}

	}

}
