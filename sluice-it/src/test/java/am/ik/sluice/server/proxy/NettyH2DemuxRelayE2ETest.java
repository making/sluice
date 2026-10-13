package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.util.ResourceLeakDetector;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.ScriptedH2Upstream;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.route.Router;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What only the netty engine does, and how its relay ends: a header list past the 64KiB
 * limit is answered {@code 431} by netty's codec; a page the relay answers itself -- the
 * no-route page, the bad-gateway page of a stream its leg lost before a response head --
 * is held to the client's stream window; an upstream's malformed frame ends its leg with
 * netty's GOAWAY while the client's connection goes on; a response the upstream completed
 * outlives the upstream's reset and its connection's end; the client going away
 * mid-stream cancels the upstream's stream and ends the relay -- connection count,
 * admission slot, hop threads, the legs' virtual connections. The relay's exception sink
 * stays silent throughout (no listener failure, no channel operation without the loop's
 * lock), and with netty's paranoid leak detection on no buffer leaks.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class NettyH2DemuxRelayE2ETest {

	private static final String OK_HOST = "ok.netty-h2demux.test";

	/** A route whose upstream port refuses connections. */
	private static final String GONE_HOST = "gone.netty-h2demux.test";

	/** A route whose upstream answers a head, part of a body, then closes. */
	private static final String CUT_HOST = "cut.netty-h2demux.test";

	/** A route whose upstream answers a head and never ends the response. */
	private static final String HOLD_HOST = "hold.netty-h2demux.test";

	private static final String UNROUTED_HOST = "nowhere.netty-h2demux.test";

	/** A route whose upstream admits one stream at a time and holds it. */
	private static final String LIMITED_HOST = "limited.netty-h2demux.test";

	/** A route whose upstream answers every head with a connection error. */
	private static final String FAIL_HOST = "fail.netty-h2demux.test";

	/**
	 * A route whose upstream answers every head with the malformed frame its path names.
	 */
	private static final String MALFORMED_HOST = "malformed.netty-h2demux.test";

	private static final int NO_ERROR = 0x0;

	private static final int PUSH_PROMISE = 0x5;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int REFUSED_STREAM = 0x7;

	private static final int CANCEL = 0x8;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int INTERNAL_ERROR = 0x2;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static final ListAppender<ILoggingEvent> RELAY_EVENTS = new ListAppender<>();

	private static final ListAppender<ILoggingEvent> LEAK_EVENTS = new ListAppender<>();

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	/** Counted down when a connection of the holding upstream ended. */
	private static volatile CountDownLatch holdEnded = new CountDownLatch(1);

	/** The resets the holding upstream received, as {@code <path>:<code>}. */
	private static final Queue<String> HOLD_RESETS = new ConcurrentLinkedQueue<>();

	/** The GOAWAYs the malformed-frame upstream received, as {@code <path>:<code>}. */
	private static final Queue<String> MALFORMED_GOAWAYS = new ConcurrentLinkedQueue<>();

	/** The hosts whose scripted upstream wrote what follows its complete response. */
	private static final Queue<String> AFTER_WRITTEN = new ConcurrentLinkedQueue<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private ResourceLeakDetector.@Nullable Level leakLevel;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private ErrorResponse errorResponse;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.h2-demux-engine", () -> "netty");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClients() throws IOException {
		this.leakLevel = ResourceLeakDetector.getLevel();
		ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
		attach(RELAY_EVENTS, NettyH2DemuxRelayE2ETest.class.getPackageName() + ".NettyHttp2DemuxRelay");
		attach(LEAK_EVENTS, "io.netty.util.ResourceLeakDetector");
		attach(ACCESS_EVENTS, "sluice.access");
		ServerSocket refusing = new ServerSocket();
		refusing.bind(new InetSocketAddress("127.0.0.1", 0));
		int refusedPort = refusing.getLocalPort();
		refusing.close();
		startClient(OK_HOST, upstream(NettyH2DemuxRelayE2ETest::serveOk).getLocalPort());
		startClient(GONE_HOST, refusedPort);
		startClient(CUT_HOST, upstream(NettyH2DemuxRelayE2ETest::serveCut).getLocalPort());
		startClient(HOLD_HOST, upstream(NettyH2DemuxRelayE2ETest::serveHold).getLocalPort());
		startClient(LIMITED_HOST, upstream(NettyH2DemuxRelayE2ETest::serveLimited).getLocalPort());
		startClient(FAIL_HOST, upstream(NettyH2DemuxRelayE2ETest::serveFailing).getLocalPort());
		startClient(MALFORMED_HOST, upstream(NettyH2DemuxRelayE2ETest::serveMalformed).getLocalPort());
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> List.of(OK_HOST, GONE_HOST, CUT_HOST, HOLD_HOST, LIMITED_HOST, FAIL_HOST, MALFORMED_HOST)
				.stream()
				.allMatch(host -> router().lookup(host).isPresent()));
	}

	@BeforeEach
	void awaitQuiet() {
		Awaitility.await().atMost(TIMEOUT).until(() -> admitted() == 0 && server().activeConnections() == 0);
	}

	@AfterEach
	void relayEnded() {
		Awaitility.await("every relay ended")
			.atMost(TIMEOUT)
			.until(() -> admitted() == 0 && server().activeConnections() == 0 && hopThreads().isEmpty());
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	@AfterAll
	void tearDown() throws Exception {
		try {
			// a leaked buffer is reported once collected and a later allocation polls
			for (int i = 0; i < 3; i++) {
				System.gc();
				try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
					h2.headers(1, true, get(OK_HOST, "/gc"));
					h2.awaitEnd(1, TIMEOUT);
				}
				TimeUnit.MILLISECONDS.sleep(100);
			}
			assertThat(LEAK_EVENTS.list).as("netty's leak reports").isEmpty();
		}
		finally {
			ResourceLeakDetector.setLevel(Objects.requireNonNull(this.leakLevel));
			this.clients.forEach(TunnelClient::stop);
			this.closeables.forEach(NettyH2DemuxRelayE2ETest::close);
			UPSTREAM_EXECUTOR.shutdown();
			((Logger) LoggerFactory
				.getLogger(NettyH2DemuxRelayE2ETest.class.getPackageName() + ".NettyHttp2DemuxRelay"))
				.detachAppender(RELAY_EVENTS);
			((Logger) LoggerFactory.getLogger("io.netty.util.ResourceLeakDetector")).detachAppender(LEAK_EVENTS);
			((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
		}
	}

	/**
	 * A header list past 64KiB (but within netty's 80KiB block limit) is a stream error
	 * netty's codec answers {@code 431}; the connection goes on.
	 */
	@Test
	void aHeaderListPastTheLimitIsAnswered431() throws Exception {
		DefaultHttp2Headers headers = new DefaultHttp2Headers();
		headers.method("GET").scheme("http").authority(OK_HOST).path("/too-big").add("x-big", "x".repeat(70 * 1024));
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5)) {
			h2.headers(1, true, get(OK_HOST, "/before"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, encode(headers));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("stream 3 answered").isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("431");
			h2.headers(5, true, get(OK_HOST, "/after"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).as("the connection goes on").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(5))).isEqualTo("ok");
			assertThat(h2.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * A client announcing a 100-byte initial stream window gets no more of a no-route
	 * page than that until it grants more, then the rest: the page goes through netty's
	 * flow controller.
	 */
	@Test
	void aRelayAnsweredPageIsHeldToTheClientsStreamWindow() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, 100));
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 1,
					get(OK_HOST, "/first"));
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					get(UNROUTED_HOST, "/page"));
			long contentLength = -1;
			int received = 0;
			// reads until stream 3's window is spent, plus a quiet second for anything
			// past it
			socket.setSoTimeout(1000);
			try {
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					if (frame.streamId() == 3 && frame.type() == TestH2.HEADERS) {
						assertThat(TestH2.status(List.of(frame))).isEqualTo("503");
						contentLength = contentLength(frame);
					}
					if (frame.streamId() == 3 && frame.type() == TestH2.DATA) {
						received += frame.payload().length;
					}
				}
			}
			catch (java.net.SocketTimeoutException e) {
				// quiet: the relay holds the rest
			}
			assertThat(contentLength).as("the page's length").isGreaterThan(100);
			assertThat(received).as("DATA within the stream window").isEqualTo(100);
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32((int) contentLength));
			TestH2.@Nullable Frame frame;
			while (received < contentLength && (frame = TestH2.readFrame(in)) != null) {
				if (frame.streamId() == 3 && frame.type() == TestH2.DATA) {
					received += frame.payload().length;
				}
			}
			assertThat(received).as("the rest once granted").isEqualTo(contentLength);
		}
	}

	/**
	 * A route whose upstream cannot be reached loses its stream before any response head:
	 * the relay answers it {@code 502} with the bad-gateway page; the connection goes on,
	 * and the route's next stream opens a new leg.
	 */
	@Test
	void aStreamLostWithItsLegIsAnswered502AndTheConnectionGoesOn() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			h2.headers(1, true, get(OK_HOST, "/first"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, get(GONE_HOST, "/lost"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("stream 3 answered").isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("502");
			assertThat(TestH2.bodyOf(h2.frames(3))).isEqualTo(badGatewayPage(GONE_HOST, "GET", "/lost"));
			assertThat(h2.reset(3)).as("the request had ended").isNull();
			h2.headers(5, true, get(GONE_HOST, "/lost-again"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).as("stream 5 answered").isTrue();
			// lost with a new leg: the closed one would not take its head
			// (REFUSED_STREAM)
			assertThat(TestH2.status(h2.frames(5))).isEqualTo("502");
			assertThat(h2.reset(5)).isNull();
			h2.headers(7, true, get(OK_HOST, "/after"));
			assertThat(h2.awaitEnd(7, TIMEOUT)).as("the connection goes on").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(7))).isEqualTo("ok");
			assertThat(h2.goAwayCode()).isEqualTo(-1);
		}
		String id = requireAccessId(GONE_HOST, "/lost");
		assertThat(accessLines()).filteredOn(line -> line.startsWith("type=req id=" + id + " "))
			.filteredOn(line -> line.contains(" route=" + GONE_HOST + " "))
			.hasSize(2);
	}

	/**
	 * An upstream closing its connection mid-response loses the stream: reset with
	 * {@code INTERNAL_ERROR} after the part relayed; the connection goes on.
	 */
	@Test
	void anUpstreamEndingMidResponseResetsItsStream() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, true, get(CUT_HOST, "/cut"));
			assertThat(h2.awaitTrue(() -> h2.reset(1) != null, TIMEOUT)).as("stream 1 reset").isTrue();
			assertThat(h2.reset(1)).isEqualTo(INTERNAL_ERROR);
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo("part");
			h2.headers(3, true, get(OK_HOST, "/after-cut"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
		}
	}

	/**
	 * The client going away mid-stream closes the legs, so the upstream's connection
	 * ends, and the relay ends: the connection uncounted, its slot released, every hop
	 * thread ended ({@link #relayEnded()}).
	 */
	@Test
	void theClientGoingAwayMidStreamEndsTheUpstreamConnectionAndTheRelay() throws Exception {
		holdEnded = new CountDownLatch(1);
		H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3);
		try {
			h2.headers(1, true, get(OK_HOST, "/first"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, false, post(HOLD_HOST, "/held"));
			assertThat(h2.awaitTrue(() -> !h2.frames(3).isEmpty(), TIMEOUT)).as("the response head").isTrue();
			assertThat(server().activeConnections()).isEqualTo(1);
		}
		finally {
			h2.abort();
		}
		assertThat(holdEnded.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the upstream's connection ended")
			.isTrue();
	}

	/**
	 * A stream the client's codec resets itself -- more DATA than its
	 * {@code content-length} -- cancels its upstream stream; the connection goes on.
	 */
	@Test
	void aStreamTheCodecResetsCancelsItsUpstreamStream() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5)) {
			h2.headers(1, true, get(OK_HOST, "/first"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, false, TestH2.headerBlock(":authority=" + HOLD_HOST, ":method=POST", ":scheme=http",
					":path=/overlong", "content-length=1"));
			assertThat(h2.awaitTrue(() -> !h2.frames(3).isEmpty(), TIMEOUT)).as("the response head").isTrue();
			h2.sendData(3, bytes("too long"), 0);
			assertThat(h2.awaitTrue(() -> h2.reset(3) != null, TIMEOUT)).as("stream 3 reset").isTrue();
			assertThat(h2.reset(3)).isEqualTo(PROTOCOL_ERROR);
			Awaitility.await("the upstream's stream cancelled")
				.atMost(TIMEOUT)
				.until(() -> HOLD_RESETS.contains("/overlong:" + CANCEL));
			h2.headers(5, true, get(OK_HOST, "/after"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).as("the connection goes on").isTrue();
		}
	}

	/**
	 * A head the leg cannot take -- past the concurrency limit its upstream announced --
	 * is refused ({@code REFUSED_STREAM}): the upstream never saw it.
	 */
	@Test
	void aHeadTheLegCannotTakeIsRefused() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5)) {
			h2.headers(1, false, post(LIMITED_HOST, "/held"));
			// the upstream's SETTINGS came before its response head
			assertThat(h2.awaitTrue(() -> !h2.frames(1).isEmpty(), TIMEOUT)).as("the response head").isTrue();
			h2.headers(3, true, get(LIMITED_HOST, "/second"));
			assertThat(h2.awaitTrue(() -> h2.reset(3) != null, TIMEOUT)).as("stream 3 reset").isTrue();
			assertThat(h2.reset(3)).isEqualTo(REFUSED_STREAM);
			h2.headers(5, true, get(OK_HOST, "/after"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).as("the connection goes on").isTrue();
		}
	}

	/**
	 * A head that is no request -- here no {@code :method} -- is reset with
	 * {@code PROTOCOL_ERROR} by the relay, logged without a route.
	 */
	@Test
	void aHeadWithoutAMethodIsResetAndLogged() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, true, get(OK_HOST, "/first"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, TestH2.headerBlock(":authority=" + OK_HOST, ":scheme=http", ":path=/methodless"));
			assertThat(h2.awaitTrue(() -> h2.reset(3) != null, TIMEOUT)).as("stream 3 reset").isTrue();
			assertThat(h2.reset(3)).isEqualTo(PROTOCOL_ERROR);
		}
	}

	/**
	 * A stream lost before its response head, its request still being sent, to a client
	 * whose initial stream window is 0: the {@code 502} head comes alone, the page's DATA
	 * only as the client grants it, and RST_STREAM {@code NO_ERROR} only after the page's
	 * end (RFC 9113 8.1: no more of the request is wanted).
	 */
	@Test
	void aBadGatewayPageIsHeldToTheClientsStreamWindowAndEndsTheRequestAfterIt() throws Exception {
		try (Socket socket = windowlessClient()) {
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, post(FAIL_HOST, "/held-page"));
			TestH2.Frame head = readUntil(in, frame -> frame.streamId() == 1).getLast();
			assertThat(head.type()).as("the first frame of stream 1").isEqualTo(TestH2.HEADERS);
			assertThat(TestH2.status(List.of(head))).isEqualTo("502");
			long contentLength = contentLength(head);
			assertThat(readQuiet(socket, in)).as("nothing more of stream 1 within its window of 0")
				.noneMatch(frame -> frame.streamId() == 1);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32((int) contentLength));
			List<TestH2.Frame> rest = readUntil(in, frame -> frame.streamId() == 1 && frame.type() == TestH2.RST_STREAM)
				.stream()
				.filter(frame -> frame.streamId() == 1)
				.toList();
			assertThat(TestH2.bodyOf(rest)).isEqualTo(badGatewayPage(FAIL_HOST, "POST", "/held-page"));
			assertThat(rest.get(rest.size() - 2).flags() & TestH2.FLAG_END_STREAM).as("the page ended the stream")
				.isNotZero();
			assertThat(TestH2.errorCode(rest.getLast())).isEqualTo(NO_ERROR);
		}
	}

	/**
	 * The client resetting a stream whose bad-gateway page waits for its window drops the
	 * page; the connection goes on.
	 */
	@Test
	void aClientResettingAWaitingBadGatewayPageDropsItAndTheConnectionGoesOn() throws Exception {
		try (Socket socket = windowlessClient()) {
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, post(FAIL_HOST, "/dropped-page"));
			TestH2.Frame head = readUntil(in, frame -> frame.streamId() == 1).getLast();
			assertThat(TestH2.status(List.of(head))).isEqualTo("502");
			TestH2.writeFrame(out, TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					get(OK_HOST, "/after-reset"));
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32(1024));
			List<TestH2.Frame> frames = readUntil(in, frame -> frame.streamId() == 3 && frame.type() == TestH2.DATA
					&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0);
			assertThat(frames.stream()
				.filter(frame -> frame.streamId() == 1)
				.map(frame -> frame.type() + ":" + (frame.type() == TestH2.RST_STREAM ? TestH2.errorCode(frame) : -1))
				.toList()).as("nothing more of stream 1").isEmpty();
			assertThat(TestH2.bodyOf(frames.stream().filter(frame -> frame.streamId() == 3).toList())).isEqualTo("ok");
		}
	}

	/**
	 * The client going away while its stream's bad-gateway page waits for its window ends
	 * the relay ({@link #relayEnded()}): the waiting page is released.
	 */
	@Test
	void theClientGoingAwayWhileItsLegFailsEndsTheRelay() throws Exception {
		try (Socket socket = windowlessClient()) {
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1,
					post(FAIL_HOST, "/gone"));
			TestH2.Frame head = readUntil(socket.getInputStream(), frame -> frame.streamId() == 1).getLast();
			assertThat(TestH2.status(List.of(head))).isEqualTo("502");
			// a reset, not a FIN
			socket.setSoLinger(true, 0);
		}
	}

	/**
	 * An upstream frame RFC 9113 makes a connection error of, sent before any response
	 * head: netty's codec ends the leg with a GOAWAY of the error's code, the stream is
	 * answered {@code 502}, and the client's connection goes on -- no malformed frame
	 * reaches it.
	 */
	@ParameterizedTest(name = "{0}")
	@CsvSource({ "settings-on-a-stream, 1", "ping-on-a-stream, 1", "goaway-on-a-stream, 1", "rst-stream-of-5-octets, 6",
			"settings-of-5-octets, 6", "settings-ack-with-a-payload, 6", "ping-of-7-octets, 6", "data-on-stream-0, 1",
			"headers-on-stream-0, 1", "continuation-without-a-block, 1", "undecodable-header-block, 9",
			"padding-past-the-payload, 1", "push-promise, 1" })
	void anUpstreamsMalformedFrameEndsOnlyItsLeg(String malformed, int code) throws Exception {
		String path = "/" + malformed;
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, true, get(MALFORMED_HOST, path));
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("stream 1 answered").isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("502");
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo(badGatewayPage(MALFORMED_HOST, "GET", path));
			Awaitility.await("the upstream's GOAWAY")
				.atMost(TIMEOUT)
				.until(() -> MALFORMED_GOAWAYS.stream().anyMatch(entry -> entry.startsWith(path + ":")));
			// the last stream id: 2^31-1, netty's on an error of its frame reader
			assertThat(MALFORMED_GOAWAYS).filteredOn(entry -> entry.startsWith(path + ":"))
				.containsExactly(path + ":" + code + ":" + Integer.MAX_VALUE);
			h2.headers(3, true, get(OK_HOST, "/after" + path));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(3))).isEqualTo("ok");
			assertThat(h2.goAwayCode()).isEqualTo(-1);
			assertThat(h2.closed()).isFalse();
		}
	}

	/**
	 * A response its upstream completed and then made a connection error: the leg ends
	 * (GOAWAY {@code PROTOCOL_ERROR}), yet what of the response waits for the client's
	 * stream window is delivered whole once granted, then RST_STREAM {@code NO_ERROR}
	 * ends the request the client was still sending.
	 */
	@Test
	void aResponseTheUpstreamCompletedOutlivesItsConnectionError() throws Exception {
		assertCompletedResponseOutlives("completed.netty-h2demux.test",
				(connection, streamId) -> connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0)), upstream -> {
					TestH2.Frame goAway = upstream.awaitConnection(0, TIMEOUT)
						.await(frame -> frame.type() == TestH2.GOAWAY, TIMEOUT);
					assertThat(goAway).as("the leg ended").isNotNull();
					assertThat(TestH2.errorCode(goAway)).isEqualTo(PROTOCOL_ERROR);
				});
	}

	/**
	 * A response its upstream completed and then reset with {@code NO_ERROR} -- the rest
	 * of the request not wanted, as RFC 9113 8.1 allows: what of the response waits for
	 * the client's stream window is delivered whole once granted, and only then does the
	 * reset reach the client.
	 */
	@Test
	void anUpstreamResetAfterACompleteResponseFollowsTheResponse() throws Exception {
		assertCompletedResponseOutlives("reset-after.netty-h2demux.test",
				(connection, streamId) -> connection.write(TestH2.RST_STREAM, 0, streamId, TestH2.int32(NO_ERROR)),
				upstream -> Awaitility.await("the upstream's reset written")
					.atMost(TIMEOUT)
					.until(() -> AFTER_WRITTEN.contains("reset-after.netty-h2demux.test")));
	}

	/**
	 * An upstream that keeps its connection open past the GOAWAY netty sent it on its
	 * connection error, and past the half-close after it, is closed once the hop's close
	 * grace ran out.
	 */
	@Test
	void anUpstreamIgnoringTheErrorGoAwayIsClosedAfterTheHopsCloseGrace() throws Exception {
		String host = "grace.netty-h2demux.test";
		ScriptedH2Upstream upstream = scripted(host,
				ScriptedH2Upstream.holdingOnEof(new byte[0], (connection, frame) -> {
					if (frame.type() == TestH2.HEADERS) {
						connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
					}
				}));
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, get(host, "/grace"));
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, TIMEOUT);
			assertThat(connection.awaitEof(TIMEOUT)).as("the relay half-closed").isTrue();
			long eof = System.nanoTime();
			assertThat(connection.frames()).anyMatch(frame -> frame.type() == TestH2.GOAWAY);
			assertThat(connection.awaitClosed(TIMEOUT)).as("the relay closed the connection").isTrue();
			long elapsed = System.nanoTime() - eof;
			assertThat(elapsed).as("closed after the close grace")
				.isGreaterThan(Hop.DEFAULT_CLOSE_GRACE.toNanos() / 2)
				.isLessThan(Hop.DEFAULT_CLOSE_GRACE.plusSeconds(2).toNanos());
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("502");
			assertThat(h2.closed()).isFalse();
		}
	}

	/**
	 * The upstream answers a POST with a head and an 8000-byte body ending the response,
	 * then does {@code after}; the client, announcing an initial stream window of 1000
	 * and still sending its request, gets 1000 bytes and nothing more until it grants the
	 * rest, then the whole body, then RST_STREAM {@code NO_ERROR}.
	 */
	private void assertCompletedResponseOutlives(String host, UpstreamStep after, UpstreamCheck check)
			throws Exception {
		int length = 8000;
		ScriptedH2Upstream upstream = scripted(host, ScriptedH2Upstream.start(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.head(frame.streamId(), false);
				connection.write(TestH2.DATA, TestH2.FLAG_END_STREAM, frame.streamId(), new byte[length]);
				after.run(connection, frame.streamId());
				AFTER_WRITTEN.add(host);
			}
		}));
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, 1000));
			h2.headers(1, false, post(host, "/completed"));
			assertThat(h2.awaitTrue(() -> h2.dataReceived(1) == 1000, TIMEOUT)).as("the client's stream window spent")
				.isTrue();
			check.run(upstream);
			// what the upstream wrote after its response reached the relay meanwhile
			Thread.sleep(300);
			assertThat(h2.ended(1)).as("the rest waits for the client's window").isFalse();
			assertThat(h2.reset(1)).as("no reset ahead of the rest").isNull();
			h2.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(length));
			assertThat(h2.awaitTrue(() -> h2.reset(1) != null, TIMEOUT)).as("stream 1 reset").isTrue();
			assertThat(h2.dataReceived(1)).isEqualTo(length);
			assertThat(h2.ended(1)).isTrue();
			assertThat(h2.reset(1)).isEqualTo(NO_ERROR);
			h2.headers(3, true, get(OK_HOST, "/after-completed"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
		}
	}

	/** Answers every ended request with {@code ok}. */
	private static void serveOk(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			boolean request = frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA;
			if (request && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
				upstream.headers(frame.streamId(), false);
				upstream.sendData(frame.streamId(), bytes("ok"), true);
			}
		}
	}

	/** Answers the first head with a 200 head and {@code part}, then closes. */
	private static void serveCut(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				upstream.headers(frame.streamId(), false);
				upstream.sendData(frame.streamId(), bytes("part"), false);
				upstream.close();
				return;
			}
		}
	}

	/** Answers every head with a 200 head, never ends; records the resets. */
	private static void serveHold(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		Map<Integer, String> paths = new HashMap<>();
		try {
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					paths.put(frame.streamId(), pathOf(frame));
					upstream.headers(frame.streamId(), false);
				}
				else if (frame.type() == TestH2.RST_STREAM) {
					HOLD_RESETS.add(paths.get(frame.streamId()) + ":" + TestH2.errorCode(frame));
				}
			}
		}
		finally {
			holdEnded.countDown();
		}
	}

	/** Answers every head with a connection error: a connection WINDOW_UPDATE of 0. */
	private static void serveFailing(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				upstream.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			}
		}
	}

	/**
	 * Answers every head with the malformed frame its path names; records the GOAWAYs it
	 * gets.
	 */
	private static void serveMalformed(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		String path = "-";
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				path = pathOf(frame);
				upstream.raw(malformed(path.substring(1), frame.streamId()));
			}
			else if (frame.type() == TestH2.GOAWAY) {
				MALFORMED_GOAWAYS.add(path + ":" + TestH2.errorCode(frame) + ":" + TestH2.increment(frame));
			}
		}
	}

	/** The malformed frame named, on (or naming) the stream. */
	private static byte[] malformed(String name, int streamId) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		switch (name) {
			case "settings-on-a-stream" -> TestH2.writeFrame(out, TestH2.SETTINGS, 0, streamId, new byte[0]);
			case "ping-on-a-stream" -> TestH2.writeFrame(out, TestH2.PING, 0, streamId, new byte[8]);
			case "goaway-on-a-stream" -> TestH2.writeFrame(out, TestH2.GOAWAY, 0, streamId, new byte[8]);
			case "rst-stream-of-5-octets" -> TestH2.writeFrame(out, TestH2.RST_STREAM, 0, streamId, new byte[5]);
			case "settings-of-5-octets" -> TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[5]);
			case "settings-ack-with-a-payload" ->
				TestH2.writeFrame(out, TestH2.SETTINGS, TestH2.FLAG_ACK, 0, new byte[6]);
			case "ping-of-7-octets" -> TestH2.writeFrame(out, TestH2.PING, 0, 0, new byte[7]);
			case "data-on-stream-0" -> TestH2.writeFrame(out, TestH2.DATA, 0, 0, new byte[1]);
			case "headers-on-stream-0" ->
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 0, new byte[] { (byte) 0x88 });
			case "continuation-without-a-block" -> TestH2.writeFrame(out, TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS,
					streamId, new byte[] { (byte) 0x88 });
			// an indexed field past the static table, the dynamic one empty
			case "undecodable-header-block" ->
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, new byte[] { (byte) 0xbf });
			// a pad length of 10 with one octet of block after it
			case "padding-past-the-payload" -> TestH2.writeFrame(out, TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_PADDED, streamId, new byte[] { 10, (byte) 0x88 });
			// the relay announced SETTINGS_ENABLE_PUSH 0
			case "push-promise" -> TestH2.writeFrame(out, PUSH_PROMISE, TestH2.FLAG_END_HEADERS, streamId,
					new byte[] { 0, 0, 0, 2, (byte) 0x82 });
			default -> throw new IllegalArgumentException(name);
		}
		return out.toByteArray();
	}

	/** Admits one stream at a time; answers every head with a 200 head, never ends. */
	private static void serveLimited(WindowedH2Upstream upstream) throws Exception {
		upstream.write(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 1));
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				upstream.headers(frame.streamId(), false);
			}
		}
	}

	private static String pathOf(TestH2.Frame frame) throws Http2Exception {
		return String.valueOf(new DefaultHttp2HeadersDecoder(true)
			.decodeHeaders(frame.streamId(), Unpooled.wrappedBuffer(frame.payload()))
			.path());
	}

	/**
	 * A raw client connection announcing an initial stream window of 0: no DATA reaches
	 * it until it grants a stream window.
	 */
	private static Socket windowlessClient() throws IOException {
		Socket socket = new Socket("127.0.0.1", dataPort);
		socket.setSoTimeout((int) TIMEOUT.toMillis());
		OutputStream out = socket.getOutputStream();
		out.write(TestH2.PREFACE);
		TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, 0));
		return socket;
	}

	/** Reads frames up to the first matching one, which is the last of the list. */
	private static List<TestH2.Frame> readUntil(InputStream in, Predicate<TestH2.Frame> match) throws IOException {
		List<TestH2.Frame> frames = new ArrayList<>();
		while (true) {
			TestH2.@Nullable Frame frame = TestH2.readFrame(in);
			if (frame == null) {
				throw new AssertionError("EOF before the frame awaited, after " + frames.size() + " frames");
			}
			frames.add(frame);
			if (match.test(frame)) {
				return frames;
			}
		}
	}

	/** Reads frames until the relay sent nothing for a second. */
	private static List<TestH2.Frame> readQuiet(Socket socket, InputStream in) throws IOException {
		List<TestH2.Frame> frames = new ArrayList<>();
		socket.setSoTimeout(1000);
		try {
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				frames.add(frame);
			}
		}
		catch (SocketTimeoutException e) {
			// quiet
		}
		finally {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
		}
		return frames;
	}

	private String badGatewayPage(String host, String method, String path) {
		return new String(Objects.requireNonNull(this.errorResponse)
			.badGatewayPage(host, new ConnectionHeadParser.Head.Request(method, path, "2"))
			.body(), StandardCharsets.UTF_8);
	}

	/** Starts a tunnel client serving the host from the scripted upstream. */
	private ScriptedH2Upstream scripted(String host, ScriptedH2Upstream upstream) {
		this.closeables.add(upstream);
		startClient(host, upstream.port());
		Awaitility.await().atMost(TIMEOUT).until(() -> router().lookup(host).isPresent());
		return upstream;
	}

	private static long contentLength(TestH2.Frame frame) throws Http2Exception {
		CharSequence value = new DefaultHttp2HeadersDecoder(true)
			.decodeHeaders(frame.streamId(), Unpooled.wrappedBuffer(frame.payload()))
			.get("content-length");
		return value == null ? -1 : Long.parseLong(value.toString());
	}

	/** A header block of literals with proper HPACK lengths (TestH2's fit 127 bytes). */
	private static byte[] encode(DefaultHttp2Headers headers) throws Http2Exception {
		ByteBuf out = Unpooled.buffer();
		try {
			DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder((name, value) -> true);
			encoder.configuration().maxHeaderListSize(256 * 1024);
			encoder.encodeHeaders(1, headers, out);
			return ByteBufUtil.getBytes(out);
		}
		finally {
			out.release();
		}
	}

	private static byte[] get(String authority, String path) {
		return TestH2.headerBlock(":authority=" + authority, ":method=GET", ":scheme=http", ":path=" + path);
	}

	private static byte[] post(String authority, String path) {
		return TestH2.headerBlock(":authority=" + authority, ":method=POST", ":scheme=http", ":path=" + path);
	}

	private static byte[] bytes(String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}

	/** The connection id of the request line of the path on the route. */
	private static String requireAccessId(String route, String path) {
		String line = accessLines().stream()
			.filter(candidate -> candidate.startsWith("type=req ") && candidate.contains(" route=" + route + " ")
					&& candidate.contains(" path=" + path + " "))
			.findFirst()
			.orElseThrow(() -> new AssertionError("no request line of " + path));
		int start = line.indexOf(" id=") + " id=".length();
		return line.substring(start, line.indexOf(' ', start));
	}

	private static List<String> accessLines() {
		return List.copyOf(ACCESS_EVENTS.list).stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	private static List<String> warnings() {
		return List.copyOf(RELAY_EVENTS.list)
			.stream()
			.filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
			.map(ILoggingEvent::getFormattedMessage)
			.toList();
	}

	/**
	 * The live threads of the netty relays' hops and loops, virtual ones included (which
	 * {@link Thread#getAllStackTraces()} leaves out).
	 */
	private static List<String> hopThreads() throws IOException {
		Path dump = Files.createTempDirectory("sluice-thread-dump").resolve("threads.txt");
		try {
			// dumpThreads refuses an existing path, hence the fresh directory per call
			ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
				.dumpThreads(dump.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
			return Files.readAllLines(dump).stream().filter(line -> line.contains("\"sluice-h2n")).toList();
		}
		finally {
			Files.deleteIfExists(dump);
			Files.deleteIfExists(dump.getParent());
		}
	}

	private static void attach(ListAppender<ILoggingEvent> appender, String logger) {
		appender.start();
		((Logger) LoggerFactory.getLogger(logger)).addAppender(appender);
	}

	private void startClient(String host, int port) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.token("it-token")
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + port).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-netty-h2demux-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
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
			Thread.ofVirtual().name("e2e-netty-h2demux-accept").start(() -> {
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

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.admitted").gauge().value();
	}

	private DataProxyServer server() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
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

	/** What a scripted upstream writes on the stream after its complete response. */
	private interface UpstreamStep {

		void run(ScriptedH2Upstream.Connection connection, int streamId);

	}

	/** Checks what the scripted upstream did. */
	private interface UpstreamCheck {

		void run(ScriptedH2Upstream upstream) throws Exception;

	}

}
