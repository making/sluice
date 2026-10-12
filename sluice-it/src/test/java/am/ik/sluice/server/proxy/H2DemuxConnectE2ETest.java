package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An h2 connection whose first stream is a CONNECT (RFC 9113 8.5: {@code :authority}, no
 * {@code :scheme} nor {@code :path}) is demultiplexed like any other: the CONNECT stream
 * is a tunnel to the upstream of its authority, relaying data and its half-close or reset
 * either way, every later stream is routed, access-checked and logged on its own, and the
 * connection keeps the idle and stall timeouts. A request head the relay cannot take for
 * a request -- a CONNECT without {@code :authority}, a head without {@code :method} -- is
 * reset with {@code PROTOCOL_ERROR} (RFC 9113 8.1.1), never relayed; a header block that
 * does not decode ends the connection with {@code COMPRESSION_ERROR} (RFC 9113 4.3), the
 * first head's as a later one's. In the relay's package for its meters and the deadline
 * queue of {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxConnectE2ETest {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(2);

	private static final Duration STALL_TIMEOUT = Duration.ofSeconds(2);

	/** The latest a timeout cut may come: two stall samples past it, plus slack. */
	private static final Duration CUT_SLACK = Duration.ofSeconds(3);

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int CANCEL = 0x8;

	private static final int COMPRESSION_ERROR = 0x9;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	private static final String TUNNEL_HOST = "tunnel.h2connect.test";

	private static final String OTHER_HOST = "other.h2connect.test";

	/** A route admitting a documentation network only: the loopback peer is refused. */
	private static final String FORBIDDEN_HOST = "forbidden.h2connect.test";

	private static final String UNROUTED_HOST = "nowhere.h2connect.test";

	/** What the tunnel upstream resets its stream on, with {@code CANCEL}. */
	private static final String RESET_ME = "reset-me";

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	/** The request heads the tunnel upstream received. */
	private static final Queue<Http2Headers> TUNNEL_HEADS = new ConcurrentLinkedQueue<>();

	/** The resets the tunnel upstream received, as {@code <authority>:<code>}. */
	private static final Queue<String> TUNNEL_RESETS = new ConcurrentLinkedQueue<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.idle-timeout", () -> IDLE_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.limits.stall-timeout", () -> STALL_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		ServerSocket tunnel = upstream(H2DemuxConnectE2ETest::serveTunnel);
		ServerSocket other = upstream(H2DemuxConnectE2ETest::serveOther);
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.token("it-token")
			.upstream(Upstream.builder().host(TUNNEL_HOST).target("tcp://127.0.0.1:" + tunnel.getLocalPort()).build())
			.upstream(Upstream.builder().host(OTHER_HOST).target("tcp://127.0.0.1:" + other.getLocalPort()).build())
			.upstream(Upstream.builder()
				.host(FORBIDDEN_HOST)
				.target("tcp://127.0.0.1:" + other.getLocalPort())
				.allowedCidrs(List.of("198.51.100.0/24"))
				.build())
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-h2connect-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> List.of(TUNNEL_HOST, OTHER_HOST, FORBIDDEN_HOST)
				.stream()
				.allMatch(host -> router().lookup(host).isPresent()));
	}

	@BeforeEach
	void awaitQuiet() {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(H2DemuxConnectE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	/**
	 * The CONNECT on the first stream is a demuxed tunnel, data relayed both ways and
	 * each side's end of it too; a later stream reaches the upstream of its own
	 * authority. Each stream has a request line of its own, on an h2 connection.
	 */
	@Test
	void aConnectionOpeningWithAConnectIsDemuxed() throws Exception {
		String authority = TUNNEL_HOST + ":1001";
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, connect(authority));
			awaitTunnel(client, 1);
			client.sendData(1, bytes("ping"), 0);
			assertThat(client.awaitTrue(() -> TestH2.bodyOf(client.frames(1)).equals("echo:ping"), TIMEOUT))
				.as("relayed to the upstream and back: %s", TestH2.bodyOf(client.frames(1)))
				.isTrue();
			// a previous connection's streams are untracked once its relay settled
			Awaitility.await("the tunnel is a stream of the demux")
				.atMost(Duration.ofSeconds(5))
				.until(() -> trackedStreams(TUNNEL_HOST) == 1);
			client.headers(3, true, get(OTHER_HOST, "/after-connect"));
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).as("answered by the route of its authority").isEqualTo("other");
			client.sendData(1, bytes("bye"), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the upstream ended its side once the client did").isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("echo:pingecho:bye");
			assertThat(client.resets()).isEmpty();
		}
		Http2Headers head = tunnelHead(authority).orElseThrow();
		assertThat(head.method()).hasToString("CONNECT");
		assertThat(head.path()).as("relayed without a :path").isNull();
		assertThat(head.scheme()).as("relayed without a :scheme").isNull();
		String connect = awaitRequestLine("route=" + TUNNEL_HOST + " method=CONNECT path=" + authority + " http=2 ");
		String later = awaitRequestLine("route=" + OTHER_HOST + " method=GET path=/after-connect http=2 ");
		assertThat(idOf(later)).as("one connection").isEqualTo(idOf(connect));
		assertThat(awaitCloseLine(idOf(connect))).contains(" transport=h2 ");
	}

	@Test
	void aConnectionOpeningWithAConnectIdlesOutAtTheIdleTimeout() throws Exception {
		double idle = counter("sluice.data.connections.idle.closed");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, connect(TUNNEL_HOST + ":1002"));
			awaitTunnel(client, 1);
			client.sendData(1, bytes("bye"), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			long ended = System.nanoTime();
			assertThat(client.awaitTrue(client::closed, IDLE_TIMEOUT.plus(CUT_SLACK))).as("closed").isTrue();
			// the relay starts the timeout before the client reads the end: slack below
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).as("not before the idle timeout")
				.isGreaterThan(IDLE_TIMEOUT.minusSeconds(1));
			assertThat(client.goAwayCode()).as("GOAWAY NO_ERROR").isZero();
			assertThat(client.goAwayLastStreamId()).isEqualTo(1);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		assertThat(counter("sluice.data.connections.idle.closed")).isEqualTo(idle + 1);
	}

	/**
	 * A later stream on the CONNECT's own route has its request due and never sent: the
	 * stall timeout cuts the connection, the tunnel at rest not counting.
	 */
	@Test
	void aRequestStalledOnAConnectionOpeningWithAConnectIsCutAtTheStallTimeout() throws Exception {
		double stalls = counter("sluice.data.connections.stall.closed");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, connect(TUNNEL_HOST + ":1003"));
			awaitTunnel(client, 1);
			client.headers(3, false,
					TestH2.headerBlock(":authority=" + OTHER_HOST, ":method=POST", ":scheme=http", ":path=/stalled"));
			long start = System.nanoTime();
			assertThat(client.awaitTrue(client::closed, STALL_TIMEOUT.plus(CUT_SLACK))).as("closed").isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("not before the stall timeout")
				.isGreaterThan(STALL_TIMEOUT.minusMillis(500));
			assertThat(client.goAwayCode()).as("GOAWAY ENHANCE_YOUR_CALM").isEqualTo(ENHANCE_YOUR_CALM);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		assertThat(counter("sluice.data.connections.stall.closed")).isEqualTo(stalls + 1);
	}

	/**
	 * Behind an open tunnel, a request and a CONNECT to a route the peer may not reach
	 * are answered 403, a CONNECT to an unrouted authority 503, each on its own stream:
	 * the tunnel goes on.
	 */
	@Test
	void laterStreamsAreRoutedAndAccessCheckedEachOnItsOwn() throws Exception {
		String authority = TUNNEL_HOST + ":1004";
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			client.headers(1, false, connect(authority));
			awaitTunnel(client, 1);
			client.headers(3, true, get(FORBIDDEN_HOST, "/forbidden"));
			client.headers(5, false, connect(FORBIDDEN_HOST + ":1004"));
			client.headers(7, false, connect(UNROUTED_HOST + ":1004"));
			for (int streamId : new int[] { 3, 5, 7 }) {
				assertThat(client.awaitEnd(streamId, TIMEOUT)).as("stream %d answered", streamId).isTrue();
			}
			assertThat(TestH2.status(client.frames(3))).isEqualTo("403");
			assertThat(TestH2.status(client.frames(5))).isEqualTo("403");
			assertThat(TestH2.status(client.frames(7))).isEqualTo("503");
			client.sendData(1, bytes("still"), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("echo:still");
		}
		assertThat(TUNNEL_HEADS).as("the refused streams never reached the tunnel's upstream")
			.noneMatch(head -> String.valueOf(head.authority()).endsWith(":1004")
					&& !authority.equals(String.valueOf(head.authority())));
		String connect = awaitRequestLine("route=" + TUNNEL_HOST + " method=CONNECT path=" + authority + " http=2 ");
		String forbidden = awaitRequestLine("route=- method=CONNECT path=" + FORBIDDEN_HOST + ":1004 http=2 ");
		String unrouted = awaitRequestLine("route=- method=CONNECT path=" + UNROUTED_HOST + ":1004 http=2 ");
		assertThat(List.of(idOf(forbidden), idOf(unrouted))).containsOnly(idOf(connect));
	}

	/**
	 * A CONNECT without {@code :authority} is malformed, first or later on a connection:
	 * reset with {@code PROTOCOL_ERROR}, the connection going on. The first is routed by
	 * its {@code host} header, so the connection is admitted and demuxed.
	 */
	@Test
	void aConnectWithoutAnAuthorityIsResetWithProtocolError() throws Exception {
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, false, TestH2.headerBlock(":method=CONNECT", "host=" + TUNNEL_HOST, "x-case=1005"));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, TIMEOUT)).as("stream 1 reset").isTrue();
			assertThat(client.reset(1)).isEqualTo(PROTOCOL_ERROR);
			client.headers(3, true, get(OTHER_HOST, "/after-malformed-connect"));
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("other");
			client.headers(5, false, TestH2.headerBlock(":method=CONNECT", "x-case=1005"));
			assertThat(client.awaitTrue(() -> client.reset(5) != null, TIMEOUT)).as("stream 5 reset").isTrue();
			assertThat(client.reset(5)).isEqualTo(PROTOCOL_ERROR);
			assertThat(client.goAwayCode()).as("no GOAWAY").isEqualTo(-1);
		}
		assertThat(TUNNEL_HEADS).as("relayed to no upstream")
			.noneMatch(head -> "1005".contentEquals(Objects.requireNonNullElse(head.get("x-case"), "")));
		String later = awaitRequestLine("route=" + OTHER_HOST + " method=GET path=/after-malformed-connect http=2 ");
		assertThat(awaitCloseLine(idOf(later))).contains(" transport=h2 ");
		assertThat(accessLines()).as("each reset stream logged")
			.filteredOn(line -> line.startsWith("type=req id=" + idOf(later) + " "))
			.filteredOn(line -> line.contains(" route=- method=CONNECT path=- http=2 "))
			.hasSize(2);
	}

	/**
	 * A first head without {@code :method} is malformed too: reset, not relayed verbatim
	 * to the route of its authority; so are later ones without {@code :path} or with a
	 * field the decoder rejects.
	 */
	@Test
	void headsWithoutAMethodOrAPathAreResetNotRelayed() throws Exception {
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, true,
					TestH2.headerBlock(":authority=" + TUNNEL_HOST, ":scheme=http", ":path=/", "x-case=1006"));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, TIMEOUT)).as("stream 1 reset").isTrue();
			assertThat(client.reset(1)).isEqualTo(PROTOCOL_ERROR);
			client.headers(3, true, get(OTHER_HOST, "/after-methodless"));
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("other");
			client.headers(5, true,
					TestH2.headerBlock(":authority=" + TUNNEL_HOST, ":method=GET", ":scheme=http", "x-case=1006"));
			assertThat(client.awaitTrue(() -> client.reset(5) != null, TIMEOUT)).as("stream 5 reset").isTrue();
			assertThat(client.reset(5)).isEqualTo(PROTOCOL_ERROR);
			// a connection-specific field (RFC 9113 8.2.2) is a stream error, decoded in
			// full: the connection goes on
			client.headers(7, true, TestH2.headerBlock(":authority=" + TUNNEL_HOST, ":method=GET", ":scheme=http",
					":path=/", "connection=close", "x-case=1006"));
			assertThat(client.awaitTrue(() -> client.reset(7) != null, TIMEOUT)).as("stream 7 reset").isTrue();
			assertThat(client.reset(7)).isEqualTo(PROTOCOL_ERROR);
			client.headers(9, true, get(OTHER_HOST, "/after-connection-field"));
			assertThat(client.awaitEnd(9, TIMEOUT)).isTrue();
			assertThat(client.goAwayCode()).as("no GOAWAY").isEqualTo(-1);
		}
		assertThat(TUNNEL_HEADS).as("relayed to no upstream")
			.noneMatch(head -> "1006".contentEquals(Objects.requireNonNullElse(head.get("x-case"), "")));
	}

	/**
	 * A first header block that does not decode is a connection error (RFC 9113 4.3):
	 * GOAWAY {@code COMPRESSION_ERROR} naming no stream, nothing relayed, the connection
	 * closed and logged.
	 */
	@Test
	void anUndecodableFirstHeadEndsTheConnectionWithACompressionError() throws Exception {
		int heads = TUNNEL_HEADS.size();
		int port;
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			port = client.localPort();
			client.headers(1, true, undecodable(TUNNEL_HOST));
			assertThat(client.awaitTrue(client::closed, TIMEOUT)).as("closed").isTrue();
			assertThat(client.goAwayCode()).as("GOAWAY COMPRESSION_ERROR").isEqualTo(COMPRESSION_ERROR);
			assertThat(client.goAwayLastStreamId()).isZero();
		}
		assertThat(TUNNEL_HEADS).as("relayed to no upstream").hasSize(heads);
		assertCompressionErrorLogged(port);
	}

	/**
	 * A later header block that does not decode ends the connection alike, the open
	 * tunnel with it: the decoder's state is unknown from there on, so no later block
	 * could be trusted.
	 */
	@Test
	void anUndecodableLaterHeadEndsTheConnectionWithACompressionError() throws Exception {
		int port;
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			port = client.localPort();
			client.headers(1, false, connect(TUNNEL_HOST + ":1009"));
			awaitTunnel(client, 1);
			int heads = TUNNEL_HEADS.size();
			client.headers(3, true, undecodable(TUNNEL_HOST));
			assertThat(client.awaitTrue(client::closed, TIMEOUT)).as("closed").isTrue();
			assertThat(client.goAwayCode()).as("GOAWAY COMPRESSION_ERROR").isEqualTo(COMPRESSION_ERROR);
			assertThat(client.goAwayLastStreamId()).isEqualTo(1);
			assertThat(client.resets()).as("no stream error instead").isEmpty();
			assertThat(TUNNEL_HEADS).as("relayed to no upstream").hasSize(heads);
		}
		assertCompressionErrorLogged(port);
	}

	/**
	 * The connection's request line names neither route, method nor path, and its close
	 * line an h2 transport.
	 */
	private static void assertCompressionErrorLogged(int port) {
		String suffix = " route=- method=- path=- http=2 remote=127.0.0.1:" + port;
		Awaitility.await("the request line")
			.atMost(Duration.ofSeconds(5))
			.until(() -> accessLines().stream()
				.anyMatch(line -> line.startsWith("type=req ") && line.endsWith(suffix)));
		String line = accessLines().stream()
			.filter(candidate -> candidate.startsWith("type=req ") && candidate.endsWith(suffix))
			.findFirst()
			.orElseThrow();
		assertThat(awaitCloseLine(idOf(line))).contains(" transport=h2 ");
	}

	/**
	 * A header block whose last field references index 0, which HPACK forbids (RFC 7541
	 * 6.1): the decoder fails after the routable fields before it.
	 */
	private static byte[] undecodable(String authority) {
		byte[] fields = TestH2.headerBlock(":authority=" + authority, ":method=GET", ":scheme=http", ":path=/");
		byte[] block = Arrays.copyOf(fields, fields.length + 1);
		block[fields.length] = (byte) 0x80;
		return block;
	}

	/** A reset of the tunnel by either side reaches the other. */
	@Test
	void aResetOfTheTunnelIsRelayedEitherWay() throws Exception {
		String byClient = TUNNEL_HOST + ":1007";
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, connect(byClient));
			awaitTunnel(client, 1);
			client.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			Awaitility.await().atMost(TIMEOUT).until(() -> TUNNEL_RESETS.contains(byClient + ":" + CANCEL));
			client.headers(3, false, connect(TUNNEL_HOST + ":1008"));
			awaitTunnel(client, 3);
			client.sendData(3, bytes(RESET_ME), 0);
			assertThat(client.awaitTrue(() -> client.reset(3) != null, TIMEOUT)).as("stream 3 reset").isTrue();
			assertThat(client.reset(3)).isEqualTo(CANCEL);
			assertThat(client.goAwayCode()).as("no GOAWAY").isEqualTo(-1);
		}
	}

	/**
	 * Answers every CONNECT with a 200 head, echoes its data, ends it after the client.
	 */
	private static void serveTunnel(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		Map<Integer, String> authorities = new HashMap<>();
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			int streamId = frame.streamId();
			switch (frame.type()) {
				case TestH2.HEADERS -> {
					Http2Headers head = decode(frame);
					TUNNEL_HEADS.add(head);
					authorities.put(streamId, String.valueOf(head.authority()));
					upstream.headers(streamId, false);
				}
				case TestH2.DATA -> {
					String text = new String(frame.payload(), StandardCharsets.UTF_8);
					if (text.equals(RESET_ME)) {
						upstream.reset(streamId, CANCEL);
					}
					else {
						upstream.sendData(streamId, bytes("echo:" + text),
								(frame.flags() & TestH2.FLAG_END_STREAM) != 0);
					}
				}
				case TestH2.RST_STREAM -> TUNNEL_RESETS.add(authorities.get(streamId) + ":" + TestH2.errorCode(frame));
				default -> {
					// connection housekeeping
				}
			}
		}
	}

	/** Answers every ended request with {@code other}. */
	private static void serveOther(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			boolean request = frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA;
			if (request && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
				upstream.headers(frame.streamId(), false);
				upstream.sendData(frame.streamId(), bytes("other"), true);
			}
		}
	}

	private static void awaitTunnel(H2TestClient client, int streamId) throws InterruptedException {
		assertThat(client.awaitTrue(
				() -> client.frames(streamId).stream().anyMatch(frame -> frame.type() == TestH2.HEADERS), TIMEOUT))
			.as("the tunnel's response head on stream %d", streamId)
			.isTrue();
		assertThat(TestH2.status(client.frames(streamId))).isEqualTo("200");
	}

	private static byte[] connect(String authority) {
		return TestH2.headerBlock(":method=CONNECT", ":authority=" + authority);
	}

	private static byte[] get(String authority, String path) {
		return TestH2.headerBlock(":authority=" + authority, ":method=GET", ":scheme=http", ":path=" + path);
	}

	private static byte[] bytes(String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}

	private static Http2Headers decode(TestH2.Frame frame) throws Http2Exception {
		return new DefaultHttp2HeadersDecoder(true).decodeHeaders(frame.streamId(),
				Unpooled.wrappedBuffer(frame.payload()));
	}

	private static Optional<Http2Headers> tunnelHead(String authority) {
		return TUNNEL_HEADS.stream().filter(head -> authority.equals(String.valueOf(head.authority()))).findFirst();
	}

	/** The {@code type=req} line carrying the fragment, once logged. */
	private static String awaitRequestLine(String fragment) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> requestLine(fragment).isPresent());
		return requestLine(fragment).orElseThrow();
	}

	private static Optional<String> requestLine(String fragment) {
		return accessLines().stream()
			.filter(line -> line.startsWith("type=req ") && line.contains(fragment))
			.findFirst();
	}

	/** The {@code event=close} line of the connection, once logged. */
	private static String awaitCloseLine(String id) {
		String prefix = "type=conn id=" + id + " event=close ";
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> accessLines().stream().anyMatch(line -> line.startsWith(prefix)));
		return accessLines().stream().filter(line -> line.startsWith(prefix)).findFirst().orElseThrow();
	}

	private static List<String> accessLines() {
		return List.copyOf(ACCESS_EVENTS.list).stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	/** The connection id of an access log line. */
	private static String idOf(String line) {
		int start = line.indexOf(" id=") + " id=".length();
		return line.substring(start, line.indexOf(' ', start));
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double counter(String name) {
		return meterRegistry().get(name).counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
	}

	private ServerSocket upstream(Handler handler) {
		return stub(socket -> {
			try (socket) {
				WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
				if (upstream != null) {
					handler.serve(upstream);
				}
			}
			catch (Exception e) {
				// connection ended
			}
		});
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-h2connect-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> handler.accept(socket));
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

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws Exception;

	}

}
