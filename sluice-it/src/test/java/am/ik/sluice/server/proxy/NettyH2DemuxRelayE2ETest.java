package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
 * What only the netty engine does, and how its relay ends: a header list past the 64KiB
 * limit is answered {@code 431} by netty's codec; a page the relay answers itself is held
 * to the client's stream window; a stream its leg loses is reset with
 * {@code INTERNAL_ERROR} while the connection goes on; the client going away mid-stream
 * cancels the upstream's stream and ends the relay -- connection count, admission slot,
 * hop threads, the legs' virtual connections. The relay's exception sink stays silent
 * throughout (no listener failure, no channel operation without the loop's lock), and
 * with netty's paranoid leak detection on no buffer leaks.
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
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> List.of(OK_HOST, GONE_HOST, CUT_HOST, HOLD_HOST, LIMITED_HOST)
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
	 * A route whose upstream cannot be reached loses its stream, reset with
	 * {@code INTERNAL_ERROR}; the connection goes on, and the route's next stream opens a
	 * new leg.
	 */
	@Test
	void aStreamLostWithItsLegIsResetAndTheConnectionGoesOn() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			h2.headers(1, true, get(OK_HOST, "/first"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, get(GONE_HOST, "/lost"));
			assertThat(h2.awaitTrue(() -> h2.reset(3) != null, TIMEOUT)).as("stream 3 reset").isTrue();
			assertThat(h2.reset(3)).isEqualTo(INTERNAL_ERROR);
			h2.headers(5, true, get(GONE_HOST, "/lost-again"));
			assertThat(h2.awaitTrue(() -> h2.reset(5) != null, TIMEOUT)).as("stream 5 reset").isTrue();
			// lost with a new leg: on the closed one its head would never be taken
			assertThat(h2.reset(5)).isEqualTo(INTERNAL_ERROR);
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

}
