package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
 * Per-hop flow control and the queue budget. What the relay reads of a stream on one hop
 * and has not written on the other stays within the stream window it announced, both
 * ways, while the leg's other streams flow (the connection windows refill as DATA
 * arrives); small frames held for a stream are coalesced, so the bytes held are the bytes
 * accounted. Past its allowance with the budget exhausted, the relay withholds the
 * connection windows of its legs as of its client, so an upstream stops at the window it
 * holds. Whatever path a stream leaves by -- the client's reset, the leg's end, the
 * relay's own reset, the client gone -- what it held leaves the route's gauge and the
 * budget while the connection goes on; a response the upstream completed outlives its
 * leg, and the request's rest outlives a client stream an early response closed. The
 * relay logs no warning throughout (no listener failure, no bytes accounted at its end).
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class NettyH2DemuxBudgetE2ETest {

	private static final int KIB = 1024;

	/** The netty relay's stream window, on both faces. */
	private static final int WINDOW = NettyHttp2DemuxRelay.STREAM_WINDOW;

	private static final long BUDGET = 256 * KIB;

	private static final int CANCEL = 0x8;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final Duration QUIET = Duration.ofMillis(300);

	private static final ListAppender<ILoggingEvent> RELAY_EVENTS = new ListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

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
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void attachRelayLog() {
		Logger logger = (Logger) LoggerFactory.getLogger(NettyHttp2DemuxRelay.class);
		RELAY_EVENTS.start();
		logger.addAppender(RELAY_EVENTS);
	}

	@BeforeEach
	void awaitQuiet() {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		Awaitility.await().atMost(TIMEOUT).until(() -> server().activeConnections() == 0 && budgetUsed() == 0);
	}

	@AfterEach
	void relayEndedSilently() {
		Awaitility.await("every relay ended")
			.atMost(TIMEOUT)
			.until(() -> server().activeConnections() == 0 && budgetUsed() == 0);
		assertThat(RELAY_EVENTS.list).filteredOn(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
			.extracting(ILoggingEvent::getFormattedMessage)
			.as("the relay's warnings")
			.isEmpty();
	}

	@AfterAll
	void tearDown() {
		((Logger) LoggerFactory.getLogger(NettyHttp2DemuxRelay.class)).detachAppender(RELAY_EVENTS);
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(NettyH2DemuxBudgetE2ETest::close);
	}

	/**
	 * An upstream reading promptly but granting stream 1 no window gets exactly that
	 * window of a request the client sends as fast as the relay grants; the relay holds
	 * at most its own stream window on top, so the client got at most two windows out --
	 * the relay grants a stream's window back only for what the leg wrote. Meanwhile a
	 * 200KiB request on the leg's other stream completes: the relay's connection window
	 * refills as DATA arrives, so stream 1's held bytes do not hold it.
	 */
	@Test
	void aRequestHeldForItsUpstreamStaysWithinTheStreamWindowWhileTheLegsOtherStreamFlows() throws Exception {
		String route = route("held-request");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				int streamId = frame.streamId();
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					upstream.grant(0, frame.payload().length);
					if (streamId != 1) {
						upstream.grant(streamId, frame.payload().length);
					}
				}
				if (frame.type() == TestH2.DATA && streamId == 3 && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					upstream.headers(3, true);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			client.fill(1, QUIET);
			WindowedH2Upstream upstream = awaitUpstream(seen);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received(1) == WINDOW);
			long held = settled(() -> queuedBytes(route), () -> client.streamSent(1));
			assertThat(upstream.received(1)).as("the upstream got its window").isEqualTo(WINDOW);
			assertThat(held).as("held for the stream: within the relay's window, a granule of rounding")
				.isGreaterThan(0)
				.isLessThanOrEqualTo(WINDOW + PendingPayload.GRANULE);
			assertThat(client.streamSent(1)).as("what the client got out: the upstream's window and the relay's")
				.isGreaterThan(upstream.received(1))
				.isLessThanOrEqualTo(2L * WINDOW);
			assertThat(held).as("what the relay read and did not write")
				.isGreaterThanOrEqualTo(client.streamSent(1) - upstream.received(1));

			client.headers(3, false, post(route));
			assertThat(client.data(3, 200 * KIB, true, TIMEOUT)).as("the other stream gets the windows").isTrue();
			assertThat(client.awaitEnd(3, TIMEOUT)).as("the other stream completes").isTrue();
			assertThat(upstream.received(3)).isEqualTo(200 * KIB);
			assertThat(upstream.received(1)).as("stream 1 still held").isEqualTo(WINDOW);
		}
	}

	/**
	 * A client granting stream 1 no window gets that window of a 512KiB response; the
	 * relay holds at most its own stream window on top, so the upstream got at most two
	 * windows out. Meanwhile a 60KiB response on the leg's other stream completes: the
	 * leg's connection window refills as DATA arrives.
	 */
	@Test
	void aResponseHeldForItsClientStaysWithinTheStreamWindowWhileTheLegsOtherStreamFlows() throws Exception {
		String route = route("held-response");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					respondAsync(upstream, frame.streamId(), frame.streamId() == 1 ? 512 * KIB : 60 * KIB);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			Awaitility.await().atMost(TIMEOUT).until(() -> client.dataReceived(1) == WINDOW);
			long held = settled(() -> queuedBytes(route), upstream::sent, () -> client.dataReceived(1));
			assertThat(client.dataReceived(1)).as("the client got its stream window").isEqualTo(WINDOW);
			assertThat(held).as("held for the stream: within the relay's window, a granule of rounding")
				.isGreaterThan(0)
				.isLessThanOrEqualTo(WINDOW + PendingPayload.GRANULE);
			assertThat(upstream.sent()).as("what the upstream got out: the client's window and the relay's")
				.isLessThanOrEqualTo(2L * WINDOW);

			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, TIMEOUT)).as("the other stream completes").isTrue();
			assertThat(TestH2.contentOf(client.frames(3))).isEqualTo(pattern(60 * KIB));
			assertThat(client.dataReceived(1)).as("stream 1 still held").isEqualTo(WINDOW);
		}
	}

	/**
	 * 4000 DATA frames of one byte each, held while the upstream grants no window: netty
	 * holds one write of them, the first, and the rest wait in one backlog buffer, so the
	 * route's gauge holds them within a granule; once the upstream grants, they reach it
	 * in no more than three frames, the backlog's end of stream included.
	 */
	@Test
	void smallFramesHeldForAnUpstreamAreCoalesced() throws Exception {
		String route = route("coalesce");
		int frames = 4000;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		AtomicInteger dataFrames = new AtomicInteger();
		AtomicBoolean ended = new AtomicBoolean();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(0);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.DATA && frame.streamId() == 1) {
					if (frame.payload().length > 0) {
						dataFrames.incrementAndGet();
					}
					ended.compareAndSet(false, (frame.flags() & TestH2.FLAG_END_STREAM) != 0);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			Awaitility.await().atMost(TIMEOUT).until(upstream::windowSettingAcked);
			for (int i = 0; i < frames; i++) {
				client.sendData(1, new byte[] { (byte) i }, 0);
			}
			client.sendData(1, new byte[0], TestH2.FLAG_END_STREAM);
			long held = settled(() -> queuedBytes(route));
			assertThat(held).as("held: the bytes, a granule of rounding")
				.isBetween((long) frames, (long) frames + PendingPayload.GRANULE);
			assertThat(server().h2DemuxMeters().writesHeld(route)).as("DATA writes netty holds: the first frame")
				.isEqualTo(1);
			assertThat(upstream.received(1)).isZero();

			upstream.grant(1, WINDOW);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received(1) == frames && ended.get());
			assertThat(dataFrames.get()).as("DATA frames carrying the 4000 bytes").isLessThanOrEqualTo(3);
			Awaitility.await()
				.atMost(TIMEOUT)
				.until(() -> queuedBytes(route) == 0 && server().h2DemuxMeters().writesHeld(route) == 0);
		}
	}

	/**
	 * A client granting 24 streams 1MiB each but no connection window, an upstream
	 * answering each with 128KiB: without the budget the relay would hold a stream window
	 * of each, about 1.5MiB. Once its queue passes its allowance with the budget
	 * exhausted, it withholds the leg's connection window too: the upstream stops at the
	 * window it holds, within the allowance, the budget and what a leg adds outside it.
	 * Once the client grants, every response completes and the budget is released.
	 */
	@Test
	void responsesHeldPastTheBudgetStopTheLegAtItsWithheldWindow() throws Exception {
		String route = route("held-responses");
		int streams = 24;
		int length = 128 * KIB;
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					respondAsync(upstream, frame.streamId(), length);
				}
			}
		}));
		double withheldBefore = withheld();
		Integer[] ids = new Integer[streams];
		for (int i = 0; i < streams; i++) {
			ids[i] = 1 + 2 * i;
		}
		try (H2TestClient client = H2TestClient.connect(dataPort, ids)) {
			client.withholdConnectionWindow();
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, 1024 * KIB));
			for (int i = 0; i < streams; i++) {
				client.headers(1 + 2 * i, true, get(route));
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> client.dataReceived() == WINDOW && budgetUsed() >= BUDGET);
			long held = settled(() -> queuedBytes(route), client::dataReceived);
			assertThat(client.dataReceived()).as("the client got its connection window").isEqualTo(WINDOW);
			assertThat(budgetUsed()).as("the budget is exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(withheld()).as("the windows were withheld").isGreaterThan(withheldBefore);
			assertThat(held).as("held: past the allowance and the budget, within what a leg adds outside them")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + BUDGET)
				.isLessThanOrEqualTo(
						NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + BUDGET + NettyHttp2DemuxRelay.RESPONSE_QUEUE_BYTES);

			client.grantConnectionWindowOnReceipt();
			for (int i = 0; i < streams; i++) {
				int streamId = 1 + 2 * i;
				assertThat(client.awaitEnd(streamId, TIMEOUT)).as("stream %d completes", streamId).isTrue();
				assertThat(TestH2.contentOf(client.frames(streamId))).isEqualTo(pattern(length));
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0 && budgetUsed() == 0);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Three streams' requests held by an upstream announcing a stream window of 0 charge
	 * the budget; each stream the client resets releases what it held, and with the last
	 * one the route's gauge and the budget are back to 0 while the connection goes on.
	 */
	@Test
	void aClientResetReleasesTheBytesItsStreamHeld() throws Exception {
		String route = route("reset-release");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, NettyH2DemuxBudgetE2ETest::holdRequests));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long held = holdThreeRequests(client, route, seen);
			client.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(queuedBytes(route)).as("stream 1's bytes released")
					.isLessThanOrEqualTo(held - WINDOW));
			client.frame(TestH2.RST_STREAM, 0, 3, TestH2.int32(CANCEL));
			client.frame(TestH2.RST_STREAM, 0, 5, TestH2.int32(CANCEL));
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0 && budgetUsed() == 0);
			assertThat(trackedStreams(route)).isZero();
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream answering stream 1 in full -- part of it held for the client's stream
	 * window -- and holding streams 3 and 5's requests, then closing its connection: the
	 * requests' bytes go with the leg (their streams answered {@code 502}), the response
	 * it completed stays held though its leg is gone, and reaches the client once it
	 * grants; then nothing is left held or charged, and the connection goes on.
	 */
	@Test
	void aLegEndingReleasesTheRequestBytesItHeldAndKeepsTheResponseItCompleted() throws Exception {
		String route = route("leg-end");
		int length = 90 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch leave = new CountDownLatch(1);
		CountDownLatch responded = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(0);
			CountDownLatch head = new CountDownLatch(1);
			Thread reader = Thread.ofVirtual().start(() -> read(upstream, head, new CopyOnWriteArrayList<>()));
			assertThat(head.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
			upstream.headers(1, false);
			upstream.sendData(1, pattern(length), true);
			responded.countDown();
			leave.await();
			upstream.close();
			reader.join();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			assertThat(responded.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).as("the upstream answered").isTrue();
			for (int streamId = 3; streamId <= 5; streamId += 2) {
				client.headers(streamId, false, post(route));
				assertThat(client.data(streamId, WINDOW, false, TIMEOUT)).isTrue();
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> client.dataReceived(1) == WINDOW);
			long held = settled(() -> queuedBytes(route), () -> client.dataReceived(1));
			assertThat(client.dataReceived(1)).isEqualTo(WINDOW);
			assertThat(budgetUsed()).as("charged").isPositive();
			long response = length - WINDOW;
			assertThat(held).as("the response's rest and two requests").isGreaterThan(response + WINDOW);

			leave.countDown();
			for (int streamId = 3; streamId <= 5; streamId += 2) {
				assertThat(client.awaitEnd(streamId, TIMEOUT)).as("stream %d answered", streamId).isTrue();
				assertThat(TestH2.status(client.frames(streamId))).isEqualTo("502");
			}
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(queuedBytes(route)).as("the response's rest, its leg gone")
					.isBetween(response, response + PendingPayload.GRANULE));
			Awaitility.await().atMost(TIMEOUT).until(() -> budgetUsed() == 0);

			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(WINDOW));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response completes").isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Stream 1's response held for the client's stream window, eight requests held by an
	 * upstream announcing a stream window of 0: past the allowance with the budget
	 * exhausted, stream 1's trailers do not fit, so the relay resets it ({@code
	 * ENHANCE_YOUR_CALM} to the client, {@code CANCEL} to the upstream) and the response
	 * it held is released; once the client resets the rest, nothing is held or charged.
	 */
	@Test
	void aResponseHeldForAStreamTheRelayResetsIsReleased() throws Exception {
		String route = route("relay-reset");
		int length = 90 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		List<List<Integer>> resets = new CopyOnWriteArrayList<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(0);
			CountDownLatch head = new CountDownLatch(1);
			Thread reader = Thread.ofVirtual().start(() -> read(upstream, head, resets));
			assertThat(head.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
			upstream.headers(1, false);
			upstream.sendData(1, pattern(length), false);
			reader.join();
		}));
		double budgetResets = counter("sluice.h2demux.streams.reset", "budget");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			for (int streamId = 3; streamId <= 17; streamId += 2) {
				client.headers(streamId, false, post(route));
				client.fill(streamId, QUIET);
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> client.dataReceived(1) == WINDOW && budgetUsed() >= BUDGET);
			long held = settled(() -> queuedBytes(route), () -> client.dataReceived(1));

			client.headers(1, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, TIMEOUT)).isTrue();
			assertThat(client.reset(1)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(counter("sluice.h2demux.streams.reset", "budget") - budgetResets).isEqualTo(1);
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(resets).as("the upstream was told").contains(List.of(1, CANCEL)));
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(queuedBytes(route)).as("stream 1's response released")
					.isLessThanOrEqualTo(held - (length - WINDOW)));
			for (int streamId = 3; streamId <= 17; streamId += 2) {
				client.frame(TestH2.RST_STREAM, 0, streamId, TestH2.int32(CANCEL));
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0 && budgetUsed() == 0);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * A client gone without a word while three streams' requests are held and charged:
	 * the relay ends and releases what it held and its share of the budget.
	 */
	@Test
	void aClientGoneWithItsBytesChargedReleasesThem() throws Exception {
		String route = route("gone");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, NettyH2DemuxBudgetE2ETest::holdRequests));
		H2TestClient client = H2TestClient.connect(dataPort);
		holdThreeRequests(client, route, seen);
		client.abort();
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> server().activeConnections() == 0 && queuedBytes(route) == 0 && budgetUsed() == 0);
	}

	/**
	 * An upstream answering a request in full on its head while it holds the request's
	 * body (a stream window of 0): the client's stream closes with the response's end,
	 * and the body the relay holds, its end included, still reaches the upstream once it
	 * grants.
	 */
	@Test
	void theRequestsRestOutlivesItsClientStreamClosingOnAnEarlyResponse() throws Exception {
		String route = route("early");
		int body = 40_000;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		AtomicBoolean ended = new AtomicBoolean();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(0);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					upstream.headers(frame.streamId(), true);
				}
				if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					ended.set(true);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			Awaitility.await().atMost(TIMEOUT).until(upstream::windowSettingAcked);
			assertThat(client.data(1, body, true, TIMEOUT)).isTrue();
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the early response").isTrue();
			Awaitility.await().atMost(TIMEOUT).until(() -> trackedStreams(route) == 0);
			assertThat(settled(() -> queuedBytes(route))).as("the body held, its client stream closed")
				.isBetween((long) body, (long) body + PendingPayload.GRANULE);

			upstream.grant(1, WINDOW);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received(1) == body && ended.get());
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0);
			assertThat(client.resets()).isEmpty();
		}
	}

	/** Holds every request body: a stream window of 0, read and never granted. */
	private static void holdRequests(WindowedH2Upstream upstream) throws IOException {
		upstream.settings(0);
		drain(upstream);
	}

	/**
	 * Opens streams 1, 3 and 5, each sending what the relay's windows admit, held by the
	 * upstream: past the allowance, so the budget is charged. Returns what the route's
	 * gauge holds.
	 */
	private long holdThreeRequests(H2TestClient client, String route, AtomicReference<WindowedH2Upstream> seen)
			throws Exception {
		client.headers(1, false, post(route));
		WindowedH2Upstream upstream = awaitUpstream(seen);
		Awaitility.await().atMost(TIMEOUT).until(upstream::windowSettingAcked);
		for (int streamId = 1; streamId <= 5; streamId += 2) {
			if (streamId > 1) {
				client.headers(streamId, false, post(route));
			}
			assertThat(client.data(streamId, WINDOW, false, TIMEOUT)).isTrue();
		}
		Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) >= 3L * WINDOW);
		long held = settled(() -> queuedBytes(route));
		assertThat(held).as("three stream windows held").isGreaterThanOrEqualTo(3L * WINDOW);
		// the client hop's writer queue counts too, nothing at rest
		assertThat(budgetUsed()).as("charged past the allowance")
			.isBetween(held - NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE,
					held - NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + KIB);
		return held;
	}

	private static void drain(WindowedH2Upstream upstream) {
		try {
			while (upstream.next() != null) {
				// held
			}
		}
		catch (IOException e) {
			// the relay went away
		}
	}

	/**
	 * Reads the relay's frames until EOF: counts {@code head} down on stream 1's head,
	 * records each RST_STREAM as its stream and code.
	 */
	private static void read(WindowedH2Upstream upstream, CountDownLatch head, List<List<Integer>> resets) {
		try {
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					head.countDown();
				}
				else if (frame.type() == TestH2.RST_STREAM) {
					resets.add(List.of(frame.streamId(), TestH2.errorCode(frame)));
				}
			}
		}
		catch (IOException e) {
			// the relay went away
		}
	}

	/** Answers the stream with a head and the body pattern, on a thread of its own. */
	private static void respondAsync(WindowedH2Upstream upstream, int streamId, int length) {
		Thread.ofVirtual().start(() -> {
			try {
				upstream.headers(streamId, false);
				upstream.sendData(streamId, pattern(length), true);
			}
			catch (IOException | InterruptedException e) {
				// the relay went away
			}
		});
	}

	/**
	 * Polls until the values stopped changing for a second (20s at most), and returns the
	 * first: the point where flow control has stopped every sender.
	 */
	private static long settled(LongSupplier... values) throws InterruptedException {
		long deadline = System.nanoTime() + 2 * TIMEOUT.toNanos();
		long[] last = sample(values);
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			Thread.sleep(50);
			long[] now = sample(values);
			if (!java.util.Arrays.equals(now, last)) {
				last = now;
				since = System.nanoTime();
			}
			else if (System.nanoTime() - since >= TimeUnit.SECONDS.toNanos(1)) {
				return now[0];
			}
		}
		throw new AssertionError("still changing after 20s: " + java.util.Arrays.toString(last));
	}

	private static long[] sample(LongSupplier... values) {
		long[] sample = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			sample[i] = values[i].getAsLong();
		}
		return sample;
	}

	/** A body whose bytes tell their offset, so loss or reordering shows. */
	private static byte[] pattern(int length) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) (i % 251);
		}
		return bytes;
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private static String route(String name) {
		return name + ".netty-budget.test";
	}

	private static WindowedH2Upstream awaitUpstream(AtomicReference<WindowedH2Upstream> seen) {
		Awaitility.await().atMost(TIMEOUT).until(() -> seen.get() != null);
		return seen.get();
	}

	private long queuedBytes(String route) {
		return Math.round(meters().find(H2DemuxMeters.QUEUED_GAUGE_NAME)
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(gauge -> gauge.value())
			.sum());
	}

	private long budgetUsed() {
		return Math.round(meters().get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private long budgetGauge() {
		return Math.round(meters().get("sluice.h2demux.budget.bytes").gauge().value());
	}

	private double withheld() {
		return meters().get("sluice.h2demux.window.withheld").counter().count();
	}

	private double counter(String name, String reason) {
		return meters().get(name).tag("reason", reason).counter().count();
	}

	private long trackedStreams(String route) {
		return server().h2DemuxMeters().trackedStreams(route);
	}

	private MeterRegistry meters() {
		return Objects.requireNonNull(this.meterRegistry);
	}

	private DataProxyServer server() {
		return Objects.requireNonNull(this.dataProxyServer);
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-nb").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/**
	 * A test upstream serving every accepted connection with the handler; the first
	 * connection is published to {@code first}.
	 */
	private ServerSocket upstream(AtomicReference<WindowedH2Upstream> first, Handler handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-nb-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						Thread.ofVirtual().name("e2e-nb-upstream").start(() -> serve(socket, first, handler));
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

	private static void serve(Socket socket, AtomicReference<WindowedH2Upstream> first, Handler handler) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			synchronized (first) {
				if (first.get() == null) {
					first.set(upstream);
				}
			}
			handler.serve(upstream);
		}
		catch (Exception e) {
			// connection ended
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

	/** A test upstream's connection handler. */
	@FunctionalInterface
	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws Exception;

	}

}
