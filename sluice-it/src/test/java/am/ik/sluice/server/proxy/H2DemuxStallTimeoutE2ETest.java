package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.Parameter;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The stall timeout of a relayed h2 connection: a client that does not read, reads but
 * grants no connection window, or grants no stream window to a response begun, and a
 * stream whose request is due but never continued or dripped, end the connection with a
 * GOAWAY {@code ENHANCE_YOUR_CALM} at the stall timeout, releasing its slot. An upstream
 * slower than the timeout, a streaming exchange resting, a CONNECT tunnel, a client
 * awaiting a {@code 100 Continue}, an upload held back by its upstream (no stream window,
 * or no connection window for want of budget), and a client granting its window slowly
 * but steadily are not cut. In the relay's package for the deadline queue of
 * {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
@LegacyOnly("the stall timeout: step 6 (103)")
class H2DemuxStallTimeoutE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final Duration STALL_TIMEOUT = Duration.ofSeconds(2);

	/** The latest a stall is cut: two sample intervals past the timeout, plus slack. */
	private static final Duration CUT_WITHIN = STALL_TIMEOUT.plusMillis(1500);

	/** How long an upstream keeps a client waiting: past the latest a stall is cut. */
	private static final Duration REST = Duration.ofSeconds(4);

	private static final int ENHANCE_YOUR_CALM = 0xb;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int WINDOW = H2TestClient.INITIAL_WINDOW;

	private static final int BIG = 16 * 1024 * 1024;

	private static final String BIG_HOST = "big.h2stall.test";

	private static final String HOLD_HOST = "hold.h2stall.test";

	private static final String SLOW_HOST = "slow.h2stall.test";

	private static final String STREAM_HOST = "stream.h2stall.test";

	private static final String TUNNEL_HOST = "tunnel.h2stall.test";

	private static final String CONTINUE_HOST = "continue.h2stall.test";

	private static final String UPLOAD_HOST = "upload.h2stall.test";

	private static final String PAUSE_HOST = "pause.h2stall.test";

	/** HPACK: {@code :status 100}, a literal with the static table's name. */
	private static final byte[] STATUS_100 = { 0x08, 0x03, '1', '0', '0' };

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits connectionLimits;

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
		registry.add("sluice.limits.stall-timeout", () -> STALL_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.limits.idle-timeout", () -> "30s");
		// a connection past its own allowance has its connection window withheld
		registry.add("sluice.limits.h2-queue-budget", () -> "4KB");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() {
		Map<String, ServerSocket> upstreams = Map.of(BIG_HOST, upstream(H2DemuxStallTimeoutE2ETest::serveBig),
				HOLD_HOST, upstream(H2DemuxStallTimeoutE2ETest::hold), SLOW_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::serveSlowly), STREAM_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::serveStreaming), TUNNEL_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::serveTunnel), CONTINUE_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::serveContinue), UPLOAD_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::serveUpload), PAUSE_HOST,
				upstream(H2DemuxStallTimeoutE2ETest::servePausing));
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.token("it-token");
		upstreams.forEach((host, upstream) -> properties
			.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build()));
		TunnelClient started = TunnelClient.builder()
			.properties(properties.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-h2stall-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> upstreams.keySet().stream().allMatch(host -> router().lookup(host).isPresent()));
	}

	@BeforeEach
	void awaitQuiet() {
		awaitAdmitted(0);
		awaitNoDeadline();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(H2DemuxStallTimeoutE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * A client granting every window up front and reading nothing: the relay's write
	 * blocks once the buffers on the way are full. The GOAWAY blocks too, so the
	 * transport is closed under it after the teardown grace.
	 */
	@Test
	void aClientNotReadingIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = new Socket()) {
			socket.setReceiveBufferSize(16 * 1024);
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort));
			OutputStream out = socket.getOutputStream();
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - WINDOW));
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 1, get(BIG_HOST));
			long start = System.nanoTime();
			Duration grace = ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE;
			Awaitility.await()
				.pollInterval(Duration.ofMillis(20))
				.atMost(CUT_WITHIN.plus(grace).plusSeconds(1))
				.until(() -> admitted() == 0);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(STALL_TIMEOUT)
				.isLessThan(CUT_WITHIN.plus(grace));
		}
		awaitReleased(stalls);
	}

	/**
	 * A client reading everything and granting stream windows, but no connection window:
	 * the response waits in the relay for it.
	 */
	@Test
	void aClientGrantingNoConnectionWindowIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.withholdConnectionWindow();
			streamWindow(client, Integer.MAX_VALUE);
			long start = System.nanoTime();
			client.headers(1, true, get(BIG_HOST));
			assertGoAwayInTime(client, start);
			assertThat(client.dataReceived()).as("the client's connection window").isEqualTo(WINDOW);
		}
		awaitReleased(stalls);
	}

	/**
	 * A client granting its connection window as it reads, but no stream window: the
	 * upstream, bound by the stream window the relay relayed, sends nothing more.
	 */
	@Test
	void aClientGrantingNoStreamWindowIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			client.headers(1, true, get(BIG_HOST));
			assertGoAwayInTime(client, start);
			assertThat(client.dataReceived(1)).as("the stream's initial window").isEqualTo(WINDOW);
			assertThat(client.connectionWindow()).as("the connection window granted back").isEqualTo(WINDOW);
		}
		awaitReleased(stalls);
	}

	@Test
	void aStreamOpenedWithoutEndAndNeverContinuedIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			client.headers(1, false, post(HOLD_HOST));
			assertGoAwayInTime(client, start);
		}
		awaitReleased(stalls);
	}

	/**
	 * Every DATA frame moves the request on, yet less than the stall minimum (16KiB) per
	 * stall timeout: the client is cut as if it sent nothing.
	 */
	@Test
	void aStreamDrippedAByteEveryHalfSecondIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			client.headers(1, false, post(HOLD_HOST));
			AtomicLong dripped = new AtomicLong();
			Thread dripper = Thread.ofVirtual().name("e2e-h2stall-dripper").start(() -> {
				try {
					while (!client.closed()) {
						Thread.sleep(500);
						client.sendData(1, new byte[] { 'x' }, 0);
						dripped.incrementAndGet();
					}
				}
				catch (IOException | InterruptedException e) {
					// cut
				}
			});
			assertGoAwayInTime(client, start);
			dripper.interrupt();
			assertThat(dripped.get()).as("bytes dripped before the cut").isGreaterThanOrEqualTo(3);
		}
		awaitReleased(stalls);
	}

	/**
	 * A head asking for {@code 100 Continue} lets the client hold its body back only
	 * until it sends a byte of it: after that the request is due.
	 */
	@Test
	void aClientSendingPartOfTheBodyItHeldForA100IsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			client.headers(1, false, TestH2.headerBlock(":authority=" + CONTINUE_HOST, ":method=POST", ":path=/",
					"expect=100-continue"));
			client.sendData(1, "o".getBytes(StandardCharsets.US_ASCII), 0);
			assertGoAwayInTime(client, start);
			assertThat(client.headerBlocks()).as("cut before the 100 came").isZero();
		}
		awaitReleased(stalls);
	}

	/**
	 * Also with a client announcing a stream window of 0: an exhausted window counts only
	 * once a response began.
	 */
	@ParameterizedTest
	@ValueSource(ints = { WINDOW, 0 })
	void anUpstreamSlowerThanTheStallTimeoutIsNotCut(int initialWindow) throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			streamWindow(client, initialWindow);
			long start = System.nanoTime();
			client.headers(1, true, get(SLOW_HOST));
			assertThat(client.awaitEnd(1, REST.plusSeconds(3))).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the response")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("ok");
			assertNotCut(client, stalls);
		}
	}

	/**
	 * A response resting after more than the initial stream window, which the client
	 * extended once it got that window -- by raising its initial window setting, which
	 * shifts the windows of the streams open, or by a window update for what it read --,
	 * does not wait on the client.
	 */
	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void aResponseRestingWithinTheStreamWindowTheClientGrantedIsNotCut(boolean bySetting) throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(PAUSE_HOST));
			assertThat(client.awaitTrue(() -> client.dataReceived(1) == WINDOW, Duration.ofSeconds(5))).isTrue();
			if (bySetting) {
				streamWindow(client, 2 * WINDOW);
			}
			else {
				client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(WINDOW));
			}
			assertThat(client.awaitTrue(() -> client.dataReceived(1) == WINDOW + 1000, Duration.ofSeconds(5))).isTrue();
			assertThat(client.awaitEnd(1, REST.plusSeconds(3))).as("the response ended after its rest").isTrue();
			assertNotCut(client, stalls);
		}
	}

	/**
	 * A response begun and not ended makes the stream a streaming exchange: its request
	 * may rest as long as the upstream lets it.
	 */
	@Test
	void aStreamingExchangeRestingLongerThanTheStallTimeoutIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(STREAM_HOST));
			assertThat(client.awaitTrue(() -> client.headerBlocks() == 1, Duration.ofSeconds(5)))
				.as("the response head")
				.isTrue();
			Thread.sleep(REST.toMillis());
			client.sendData(1, "x".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("ok");
			assertNotCut(client, stalls);
		}
	}

	/**
	 * A CONNECT stream waits for its upstream's answer with its request open, then rests
	 * with its stream window exhausted: neither counts. Opened second, behind a request
	 * on another route.
	 */
	@Test
	void aConnectTunnelRestingLongerThanTheStallTimeoutIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, true, get(HOLD_HOST));
			long start = System.nanoTime();
			client.headers(3, false, TestH2.headerBlock(":method=CONNECT", ":authority=" + TUNNEL_HOST));
			assertThat(client.awaitTrue(() -> client.dataReceived(3) == WINDOW, REST.plusSeconds(3)))
				.as("received %d", client.dataReceived(3))
				.isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the answer")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			assertThat(trackedStreams(TUNNEL_HOST)).as("demuxed").isEqualTo(1);
			Thread.sleep(REST.toMillis());
			assertThat(client.dataReceived(3)).as("the stream window held the rest back").isEqualTo(WINDOW);
			assertNotCut(client, stalls);
		}
	}

	@Test
	void aClientAwaitingA100ItsUpstreamDelaysIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			long start = System.nanoTime();
			client.headers(1, false, TestH2.headerBlock(":authority=" + CONTINUE_HOST, ":method=POST", ":path=/",
					"expect=100-continue"));
			assertThat(client.awaitTrue(() -> client.headerBlocks() == 1, REST.plusSeconds(3))).as("the 100").isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the 100")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			client.sendData(1, "ok".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertNotCut(client, stalls);
		}
	}

	/**
	 * An upstream granting no window for a while holds the upload back: the relay grants
	 * the client no stream window meanwhile, which is not the client's stall.
	 */
	@Test
	void anUploadHeldBackByItsUpstreamIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(UPLOAD_HOST));
			AtomicBoolean sent = new AtomicBoolean();
			Thread uploader = Thread.ofVirtual()
				.name("e2e-h2stall-uploader")
				.start(() -> sent.set(upload(client, 1, 1024 * 1024)));
			Thread.sleep(1500);
			assertThat(client.streamWindow(1)).as("the stream window ran out").isLessThanOrEqualTo(0);
			assertThat(client.connectionWindow()).as("the connection window did not").isPositive();
			assertThat(client.awaitEnd(1, REST.plusSeconds(10))).isTrue();
			uploader.join();
			assertThat(sent.get()).isTrue();
			assertNotCut(client, stalls);
		}
	}

	/**
	 * Two uploads take their connection past its allowance with the budget exhausted: the
	 * relay withholds the connection window while the streams still hold stream window,
	 * which is not the client's stall either.
	 */
	@Test
	void uploadsHeldBackByAWithheldConnectionWindowAreNotCut() throws Exception {
		double stalls = stallClosed();
		double withheld = windowWithheld();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(UPLOAD_HOST));
			client.headers(3, false, post(UPLOAD_HOST));
			AtomicBoolean first = new AtomicBoolean();
			AtomicBoolean second = new AtomicBoolean();
			Thread one = Thread.ofVirtual()
				.name("e2e-h2stall-uploader")
				.start(() -> first.set(upload(client, 1, 512 * 1024)));
			Thread three = Thread.ofVirtual()
				.name("e2e-h2stall-uploader")
				.start(() -> second.set(upload(client, 3, 512 * 1024)));
			Thread.sleep(1500);
			// at least once: granted back while the queue drains, it may be withheld
			// again
			assertThat(windowWithheld()).as("the connection window was withheld").isGreaterThan(withheld);
			assertThat(client.connectionWindow()).as("the connection window ran out").isLessThanOrEqualTo(0);
			assertThat(Math.max(client.streamWindow(1), client.streamWindow(3))).as("a stream window did not")
				.isPositive();
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), REST.plusSeconds(10))).isTrue();
			one.join();
			three.join();
			assertThat(first.get() && second.get()).isTrue();
			assertNotCut(client, stalls);
		}
	}

	/**
	 * Granting the connection window 16KiB per second keeps a large response moving: what
	 * the client got stays at what it granted, so the response waited in the relay.
	 */
	@Test
	void aClientGrantingSlowlyButSteadilyIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.paceConnectionGrants(16 * 1024, Duration.ofSeconds(1));
			streamWindow(client, Integer.MAX_VALUE);
			client.headers(1, true, get(BIG_HOST));
			Thread.sleep(STALL_TIMEOUT.multipliedBy(3).toMillis());
			assertThat(client.dataReceived()).as("about the window granted, far from the whole response")
				.isGreaterThanOrEqualTo(WINDOW + 4 * 16 * 1024)
				.isLessThanOrEqualTo(WINDOW + 8 * 16 * 1024);
			assertThat(client.maxReceiveExcess()).as("DATA beyond the window granted").isLessThanOrEqualTo(0);
			assertNotCut(client, stalls);
		}
	}

	/** Sends {@code length} bytes on the stream, waiting for grants; whether it did. */
	private static boolean upload(H2TestClient client, int streamId, int length) {
		try {
			return client.data(streamId, length, true, REST.plusSeconds(10));
		}
		catch (IOException | InterruptedException e) {
			return false;
		}
	}

	private void assertGoAwayInTime(H2TestClient client, long start) throws InterruptedException {
		assertThat(client.awaitTrue(client::closed, CUT_WITHIN.plusSeconds(1))).as("closed").isTrue();
		assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(STALL_TIMEOUT)
			.isLessThan(CUT_WITHIN);
		assertThat(client.goAwayCode()).as("GOAWAY ENHANCE_YOUR_CALM").isEqualTo(ENHANCE_YOUR_CALM);
	}

	private void assertNotCut(H2TestClient client, double stalls) {
		assertThat(client.closed()).as("closed").isFalse();
		assertThat(client.goAwayCode()).as("no GOAWAY").isEqualTo(-1);
		assertThat(stallClosed()).isEqualTo(stalls);
	}

	private void awaitReleased(double stalls) {
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(stallClosed()).isEqualTo(stalls + 1);
	}

	private static void streamWindow(H2TestClient client, int window) throws IOException {
		client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, window));
	}

	private static byte[] get(String host) {
		return TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/");
	}

	private static byte[] post(String host) {
		return TestH2.headerBlock(":authority=" + host, ":method=POST", ":path=/");
	}

	/** Answers every request head with {@link #BIG} bytes within the windows. */
	private static void serveBig(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				int streamId = frame.streamId();
				Thread.ofVirtual().start(() -> {
					try {
						upstream.headers(streamId, false);
						upstream.sendData(streamId, new byte[BIG], true);
					}
					catch (IOException | InterruptedException e) {
						// the relay went away
					}
				});
			}
		}
	}

	/**
	 * Answers every request head with one stream window and 1000 bytes more, then rests
	 * for {@link #REST} before ending the response.
	 */
	private static void servePausing(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				int streamId = frame.streamId();
				later(Duration.ZERO, () -> {
					upstream.headers(streamId, false);
					upstream.sendData(streamId, new byte[WINDOW + 1000], false);
					Thread.sleep(REST.toMillis());
					upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, new byte[0]);
				});
			}
		}
	}

	/** Reads everything, answers nothing. */
	private static void hold(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		while (upstream.next() != null) {
			// held
		}
	}

	/** Answers every ended request after {@link #REST}. */
	private static void serveSlowly(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (ends(frame)) {
				int streamId = frame.streamId();
				later(REST, () -> answer(upstream, streamId));
			}
		}
	}

	/** Sends a response head at once, its end once the request ended. */
	private static void serveStreaming(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				upstream.headers(frame.streamId(), false);
			}
			else if (ends(frame)) {
				upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, frame.streamId(),
						"ok".getBytes(StandardCharsets.US_ASCII));
			}
		}
	}

	/** Answers a CONNECT after {@link #REST} with a head and a stream window of data. */
	private static void serveTunnel(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				int streamId = frame.streamId();
				later(REST, () -> {
					upstream.headers(streamId, false);
					upstream.sendData(streamId, new byte[WINDOW], false);
				});
			}
		}
	}

	/** Sends {@code 100} after {@link #REST}, the response once the request ended. */
	private static void serveContinue(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			int streamId = frame.streamId();
			if (frame.type() == TestH2.HEADERS) {
				later(REST, () -> upstream.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, STATUS_100));
			}
			else if (ends(frame)) {
				answer(upstream, streamId);
			}
		}
	}

	/**
	 * Grants no window for {@link #REST} from the first request head, then enough for
	 * every stream; answers every ended request with a head.
	 */
	private static void serveUpload(WindowedH2Upstream upstream) throws Exception {
		upstream.settings(null);
		Map<Integer, Boolean> streams = new ConcurrentHashMap<>();
		AtomicBoolean released = new AtomicBoolean();
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			int streamId = frame.streamId();
			if (frame.type() == TestH2.HEADERS && streams.isEmpty()) {
				later(REST, () -> {
					synchronized (streams) {
						released.set(true);
						upstream.grant(0, 1 << 30);
						for (Integer id : streams.keySet()) {
							upstream.grant(id, 1 << 30);
						}
					}
				});
			}
			if (frame.type() == TestH2.HEADERS) {
				synchronized (streams) {
					streams.put(streamId, Boolean.TRUE);
					if (released.get()) {
						upstream.grant(streamId, 1 << 30);
					}
				}
			}
			if (ends(frame)) {
				upstream.headers(streamId, true);
			}
		}
	}

	private static boolean ends(TestH2.Frame frame) {
		return frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
				&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
	}

	private static void answer(WindowedH2Upstream upstream, int streamId) throws IOException {
		upstream.headers(streamId, false);
		upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, "ok".getBytes(StandardCharsets.US_ASCII));
	}

	/** Runs the action on a thread of its own after {@code delay}. */
	private static void later(Duration delay, Action action) {
		Thread.ofVirtual().start(() -> {
			try {
				Thread.sleep(delay.toMillis());
				action.run();
			}
			catch (Exception e) {
				// the relay went away
			}
		});
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
			Thread.ofVirtual().name("e2e-h2stall-accept").start(() -> {
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

	private void awaitAdmitted(int expected) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == expected);
	}

	private void awaitNoDeadline() {
		Awaitility.await()
			.atMost(Duration.ofSeconds(3))
			.until(() -> Objects.requireNonNull(this.connectionLimits).scheduledDeadlines() == 0);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double stallClosed() {
		return meterRegistry().get("sluice.data.connections.stall.closed").counter().count();
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private double windowWithheld() {
		return Objects.requireNonNull(this.dataProxyServer)
			.h2DemuxMeters()
			.registry()
			.get("sluice.h2demux.window.withheld")
			.counter()
			.count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
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

	private interface Action {

		void run() throws Exception;

	}

}
