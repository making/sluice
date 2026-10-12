package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The queue budget the h2 demux relays share: connections whose upstreams do not read
 * stop at the budget plus what each may queue outside it, by having their connection
 * window withheld, not by being torn down; a connection within its own allowance keeps
 * flowing however exhausted the budget is; a withheld window is granted once any relay
 * frees budget; and every exit releases what a relay held. In the relay's package for the
 * budget's waiters and the relay's constants.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxBudgetE2ETest {

	private static final long BUDGET = 512 * 1024;

	/**
	 * Streams per stalled connection: their stream budgets add up far past the budget.
	 */
	private static final int STREAMS = 20;

	/** Request body per stream, past what the tunnel and the upstream's socket absorb. */
	private static final int BODY = 512 * 1024;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private static final int NO_ERROR = 0x0;

	private static final int CANCEL = 0x8;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	/** A frame type RFC 9113 does not define, relayed as it is. */
	private static final int UNKNOWN_FRAME = 0xfa;

	/** A RST_STREAM frame: its header and error code. */
	private static final int RST_STREAM_BYTES = 9 + 4;

	private static final AtomicLong PINGS = new AtomicLong();

	/** The kernel's TCP connection tables, where readable (Linux). */
	private static final List<Path> TCP_TABLES = Stream.of("/proc/net/tcp", "/proc/net/tcp6")
		.map(Path::of)
		.filter(Files::isReadable)
		.toList();

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits limits;

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
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxBudgetE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Two connections whose upstream does not read, twenty streams each: together they
	 * stop at the budget plus what each queues outside it -- far below the 5MiB their
	 * stream budgets allow -- with the budget exhausted and their windows withheld, yet
	 * no stream is reset and no connection torn down; once the upstream reads, every
	 * stream completes and the budget is released.
	 */
	@Test
	void stalledConnectionsStopAtTheBudgetAndCompleteOnceTheUpstreamReads() throws Exception {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		String route = "bud.stall.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		double withheldBefore = withheld();
		try (Stalled first = stall(route); Stalled second = stall(route)) {
			long maxQueued = awaitStable(() -> queuedBytes(route));
			assertThat(maxQueued).as("queued at most: the budget plus each connection's unbudgeted bound")
				.isLessThanOrEqualTo(BUDGET + 2 * Http2DemuxRelay.UNBUDGETED_BYTES)
				.isGreaterThanOrEqualTo(BUDGET);
			assertThat(budgetUsed()).as("the budget is exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(withheld()).as("windows were withheld").isGreaterThan(withheldBefore);
			assertThat(first.client().closed() || second.client().closed()).as("no connection torn down").isFalse();
			assertThat(first.client().resets()).isEmpty();
			assertThat(second.client().resets()).isEmpty();
			gate.countDown();
			first.awaitEnds();
			second.awaitEnds();
			assertThat(first.client().goAwayCode()).isEqualTo(-1);
			assertThat(second.client().goAwayCode()).isEqualTo(-1);
		}
		awaitReleased(route);
	}

	/**
	 * While two stalled connections hold the budget exhausted, a connection on another
	 * route uploads 1MiB on each of two streams to an upstream reading at about 4MiB/s,
	 * and gets both responses: its queue passes its allowance -- its window is withheld
	 * -- and drains below it again and again, its own drain granting the window back, so
	 * a connection whose legs drain keeps flowing.
	 */
	@Test
	void connectionOnAnotherRouteKeepsFlowingWhileTheBudgetIsExhausted() throws Exception {
		String stalledRoute = "bud.held.test";
		String flowingRoute = "bud.flowing.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(stalledRoute, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		startClient(flowingRoute, stub(H2DemuxBudgetE2ETest::answerEndedRequestsSlowly));
		try (Stalled first = stall(stalledRoute); Stalled second = stall(stalledRoute)) {
			awaitStable(() -> queuedBytes(stalledRoute));
			assertThat(budgetUsed()).as("exhausted before").isGreaterThanOrEqualTo(BUDGET);
			double withheldBefore = withheld();
			try (Stalled flowing = open(flowingRoute, 2, 1024 * 1024)) {
				flowing.awaitEnds();
				assertThat(flowing.client().connectionGranted()).isGreaterThan(Http2DemuxRelay.CONNECTION_ALLOWANCE);
			}
			// the stalled connections' shares never shrink while their upstream does not
			// read, so the budget stays exhausted, their windows stay withheld, and only
			// the flowing connection can have counted
			assertThat(withheld()).as("the flowing connection passed its allowance").isGreaterThan(withheldBefore);
			assertThat(budgetUsed()).as("still exhausted after").isGreaterThanOrEqualTo(BUDGET);
			gate.countDown();
			first.awaitEnds();
			second.awaitEnds();
		}
		awaitReleased(stalledRoute);
		awaitReleased(flowingRoute);
	}

	/**
	 * With the budget exhausted, a new stream on a connection past its allowance is
	 * refused (REFUSED_STREAM, safe to retry) and the connection stays up; the same
	 * request on a connection within its allowance reaches the upstream.
	 */
	@Test
	void newStreamIsRefusedOnlyOnAConnectionPastItsAllowance() throws Exception {
		String route = "bud.refuse.test";
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger heads = new AtomicInteger();
		startClient(route, stub(socket -> serveGated(socket, gate, heads)));
		double refusedBefore = refused("budget");
		try (Stalled first = stall(route); Stalled second = stall(route)) {
			awaitStable(() -> queuedBytes(route));
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			int extra = 1 + 2 * STREAMS;
			first.client()
				.headers(extra, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/extra"));
			assertThat(first.client().awaitTrue(() -> first.client().reset(extra) != null, Duration.ofSeconds(5)))
				.isTrue();
			assertThat(first.client().reset(extra)).isEqualTo(TestH2.REFUSED_STREAM);
			assertThat(refused("budget")).isEqualTo(refusedBefore + 1);
			assertThat(first.client().closed()).isFalse();
			int headsBefore = heads.get();
			try (H2TestClient fresh = H2TestClient.connect(dataPort)) {
				fresh.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/fresh"));
				Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> heads.get() > headsBefore);
				assertThat(fresh.reset(1)).isNull();
			}
			gate.countDown();
			first.awaitEnds();
			second.awaitEnds();
		}
		awaitReleased(route);
	}

	/**
	 * Trailers on a connection past its allowance with the budget exhausted reset their
	 * stream only: RST_STREAM ENHANCE_YOUR_CALM to the client, CANCEL to the upstream,
	 * the stream's queued data dropped. What the client still sends on the stream is void
	 * -- its DATA granted back, its trailers decoded (a later head refers to their
	 * dynamic table entry) but not relayed --, so is the reset the upstream answers with,
	 * and the other streams of the connection complete. The stream stays tracked until
	 * its reset is written to the upstream.
	 */
	@Test
	void trailersPastTheAllowanceWithTheBudgetExhaustedResetOnlyTheirStream() throws Exception {
		String route = "bud.trailers.test";
		CountDownLatch gate = new CountDownLatch(1);
		UpstreamLog log = new UpstreamLog();
		startClient(route, stub(socket -> serveGatedLogging(socket, gate, log)));
		int reset = streamId(0);
		int after = streamId(STREAMS + 1);
		int body = 4096;
		int late = 1024;
		H2TestClient client = H2TestClient.connect(dataPort, reset);
		client.headers(reset, false, head(route, "/reset"));
		try (Stalled first = open(client, route, 1, BODY, body + late); Stalled second = stall(route)) {
			first.awaitWithheld();
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			first.stop();
			long before = awaitSteady(() -> queuedBytes(route));
			assertThat(client.connectionWindow()).as("the window kept for the stream to reset")
				.isGreaterThanOrEqualTo(body + late);
			client.sendData(reset, new byte[body], 0);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> queuedBytes(route) >= before + body);
			long queued = queuedBytes(route);
			double resetsBefore = resets("budget");
			client.headers(reset, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(client.awaitTrue(() -> client.reset(reset) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(reset)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(resets("budget")).isEqualTo(resetsBefore + 1);
			assertThat(queuedBytes(route)).as("the stream's queued data dropped, a reset queued")
				.isLessThanOrEqualTo(queued - body + RST_STREAM_BYTES);
			assertThat(trackedStreams(route)).as("counted until its reset is written to the stalled upstream")
				.isEqualTo(2 * STREAMS + 1);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
			// sent before the reset arrived, as far as the relay can tell
			long granted = awaitStable(client::connectionGranted);
			client.sendData(reset, new byte[late], 0);
			client.headers(reset, true, TestH2.appendLiteralIncremental(new byte[0], "x-late", "1"));
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= granted + late, Duration.ofSeconds(5)))
				.as("the DATA dropped is granted back")
				.isTrue();
			gate.countDown();
			first.resume();
			first.awaitEnds();
			second.awaitEnds();
			client.headers(after, true, TestH2
				.appendIndexedDynamic(TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/after"), 62));
			assertThat(client.awaitEnd(after, Duration.ofSeconds(10))).isTrue();
			assertThat(log.heads.get(after)).as("decoded with the entry of the trailers dropped")
				.isEqualTo(List.of(":authority: " + route, ":method: GET", ":path: /after", "x-late: 1"));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(trackedStreams(route)).as("the reset stream no longer tracked either")
					.isZero());
			assertThat(log.resets).as("the upstream was told").isEqualTo(Map.of(reset, CANCEL));
			assertThat(log.afterReset).as("nothing of the stream relayed after its reset").isEmpty();
			assertThat(types(client.frames(reset))).as("the upstream's reset in answer is not relayed")
				.isEqualTo(List.of(TestH2.RST_STREAM));
			assertThat(client.reset(reset)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
		awaitReleased(route);
	}

	/**
	 * A stream the relay reset -- its trailers past the allowance with the budget
	 * exhausted -- whose reset still waits for the stalled upstream, the stream still
	 * tracked: a client WINDOW_UPDATE with an increment of 0 on it gets no second
	 * RST_STREAM and counts no protocol reset, and what the upstream sends on it -- a
	 * response head, DATA, an unknown frame, a reset -- never reaches the client; the
	 * upstream gets the one CANCEL.
	 */
	@Test
	void aStreamWhoseResetWaitsForTheUpstreamIsResetOnceAndRelaysNothing() throws Exception {
		String route = "bud.zero.test";
		String stalledRoute = "bud.zero.held.test";
		CountDownLatch gate = new CountDownLatch(1);
		UpstreamLog log = new UpstreamLog();
		startClient(route, stub(socket -> serveGatedLogging(socket, gate, log)));
		startClient(stalledRoute, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		int reset = streamId(0);
		int probe = streamId(1);
		H2TestClient client = H2TestClient.connect(dataPort, reset, probe);
		client.headers(reset, false, head(route, "/reset"));
		try (Stalled first = open(client, route, 1, BODY, 0); Stalled second = stall(stalledRoute)) {
			first.awaitWithheld();
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			first.stop();
			double budgetResets = resets("budget");
			double protocolResets = resets("protocol");
			client.headers(reset, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(client.awaitTrue(() -> client.reset(reset) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(reset)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(resets("budget")).isEqualTo(budgetResets + 1);
			client.frame(TestH2.WINDOW_UPDATE, 0, reset, TestH2.int32(0));
			awaitPingAck(client);
			// the upstream, no longer reading, answers on the stream; an unknown frame on
			// another stream, relayed as it is, tells the leg's inbound side read past it
			OutputStream upstream = log.awaitIdle();
			TestH2.writeFrame(upstream, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, reset, new byte[] { (byte) 0x88 });
			TestH2.writeFrame(upstream, TestH2.DATA, 0, reset, new byte[16]);
			TestH2.writeFrame(upstream, UNKNOWN_FRAME, 0, reset, new byte[4]);
			TestH2.writeFrame(upstream, TestH2.RST_STREAM, 0, reset, TestH2.int32(NO_ERROR));
			TestH2.writeFrame(upstream, UNKNOWN_FRAME, 0, probe, new byte[4]);
			assertThat(
					client.awaitTrue(() -> types(client.frames(probe)).contains(UNKNOWN_FRAME), Duration.ofSeconds(5)))
				.as("the inbound side read past the stream's frames")
				.isTrue();
			// no stream is opened meanwhile, so the count only falls: tracked now, the
			// stream was tracked when the window update and the frames were read
			assertThat(trackedStreams(route)).as("still tracked: its reset waits for the stalled upstream")
				.isEqualTo(STREAMS + 1);
			assertThat(types(client.frames(reset))).as("one reset, nothing after it")
				.containsExactly(TestH2.RST_STREAM);
			assertThat(resets("protocol")).isEqualTo(protocolResets);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			gate.countDown();
			first.resume();
			first.awaitEnds();
			second.awaitEnds();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(trackedStreams(route)).isZero());
			assertThat(log.resets).as("the upstream was told").isEqualTo(Map.of(reset, CANCEL));
			assertThat(log.afterReset).as("nothing of the stream relayed after its reset").isEmpty();
			assertThat(types(client.frames(reset))).containsExactly(TestH2.RST_STREAM);
		}
		awaitReleased(route);
		awaitReleased(stalledRoute);
	}

	/**
	 * The client gone -- its connection reset -- as the relay writes the RST_STREAM of
	 * trailers past the allowance with the budget exhausted: the write fails out of the
	 * outbound parse, the relay ends, and what it held goes back -- its bytes queued for
	 * the route (the stream's data and the CANCEL queued for the stalled upstream among
	 * them), its share of the budget, and its tracked streams.
	 */
	@Test
	void clientGoneAsTheRelayWritesItsResetReleasesWhatTheRelayHeld() throws Exception {
		String route = "bud.gone-reset.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		int reset = streamId(0);
		int body = 4096;
		HeldWrite held = HeldWrite.install(dataProxyServer(),
				new Http2DemuxRelay.WriteSeam.Decision(Http2DemuxRelay.WriteSeam.Site.RESET, reset));
		H2TestClient client = H2TestClient.connect(dataPort, reset);
		client.headers(reset, false, head(route, "/reset"));
		try (Stalled first = open(client, route, 1, BODY, body); Stalled second = stall(route)) {
			first.awaitWithheld();
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			first.stop();
			long before = awaitSteady(() -> queuedBytes(route));
			client.sendData(reset, new byte[body], 0);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> queuedBytes(route) >= before + body);
			double resetsBefore = resets("budget");
			client.headers(reset, true, TestH2.headerBlock("x-trailer=1"));
			Http2DemuxRelay relay = held.awaitDecided();
			assertThat(client.reset(reset)).as("decided before the RST_STREAM was written").isNull();
			int clientPort = client.localPort();
			client.abort();
			awaitResetTaken(clientPort);
			held.proceed();
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(relay::finished);
			assertThat(resets("budget")).as("the RST_STREAM write failed: the reset was never counted")
				.isEqualTo(resetsBefore);
			assertThat(trackedStreams(route)).as("only the other connection's streams tracked").isEqualTo(STREAMS);
			second.close();
		}
		finally {
			dataProxyServer().h2WriteSeam(Http2DemuxRelay.WriteSeam.NONE);
			held.proceed();
			gate.countDown();
		}
		awaitReleased(route);
	}

	/**
	 * A stream the relay reset while its response waits for the client's connection
	 * window: the waiting response is dropped and its window granted back to the
	 * upstream, and nothing the upstream had in flight on the stream -- DATA, a header
	 * block, an unknown frame -- reaches the client, though the header block is decoded
	 * (another stream's trailers refer to its dynamic table entry). The stream is no
	 * longer tracked, and owing no response -- the upstream never ended it --, the
	 * upstream leaving retires the leg without ending the connection.
	 */
	@Test
	void upstreamFramesOnAStreamTheRelayResetNeverReachTheClient() throws Exception {
		String stalledRoute = "bud.cancel.held.test";
		String route = "bud.cancel.test";
		CountDownLatch gate = new CountDownLatch(1);
		int reset = streamId(0);
		int probe = streamId(1);
		CancelScript script = new CancelScript(reset, probe);
		startClient(stalledRoute, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		startClient(route, stub(script::serve));
		H2TestClient client = H2TestClient.connect(dataPort, reset, probe);
		client.headers(reset, false, head(route, "/reset"));
		client.headers(probe, false, head(route, "/probe"));
		try (Stalled first = open(client, stalledRoute, 2, BODY, 0); Stalled second = stall(stalledRoute)) {
			first.awaitWithheld();
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			first.stop();
			client.withholdConnectionWindow();
			script.respond.countDown();
			assertThat(script.queued.await(10, TimeUnit.SECONDS)).as("the upstream's PING fence came back").isTrue();
			assertThat(client.awaitTrue(() -> client.dataReceived(probe) == H2TestClient.INITIAL_WINDOW,
					Duration.ofSeconds(5)))
				.as("the probe's response took the whole connection window")
				.isTrue();
			assertThat(client.awaitTrue(() -> types(client.frames(reset)).equals(List.of(TestH2.HEADERS)),
					Duration.ofSeconds(5)))
				.as("the head of the response reached the client, its DATA waits")
				.isTrue();
			assertThat(script.granted.get()).as("the waiting DATA's window is held")
				.isEqualTo(H2TestClient.INITIAL_WINDOW);
			assertThat(trackedStreams(route)).isEqualTo(2);
			double resetsBefore = resets("budget");
			client.headers(reset, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(client.awaitTrue(() -> client.reset(reset) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(reset)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(resets("budget")).isEqualTo(resetsBefore + 1);
			assertThat(script.answered.await(10, TimeUnit.SECONDS)).as("the upstream read the reset").isTrue();
			assertThat(script.resetCode.get()).isEqualTo(CANCEL);
			assertThat(client.awaitEnd(probe, Duration.ofSeconds(5))).as("the probe's trailers arrived").isTrue();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(script.granted.get())
					.as("the waiting DATA and the DATA in flight granted back")
					.isEqualTo(H2TestClient.INITIAL_WINDOW + CancelScript.WAITING + CancelScript.IN_FLIGHT));
			assertThat(types(client.frames(reset))).as("nothing after the relay's reset")
				.isEqualTo(List.of(TestH2.HEADERS, TestH2.RST_STREAM));
			assertThat(lastHeaderBlock(client.frames(probe))).as("decoded with the entry of the block dropped")
				.isEqualTo(List.of("x-dropped: 1"));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 1);
			script.leave.countDown();
			assertThat(client.awaitTrue(() -> client.reset(probe) != null, Duration.ofSeconds(5)))
				.as("the leg retired, resetting the stream the client still sends on")
				.isTrue();
			assertThat(client.reset(probe)).isEqualTo(NO_ERROR);
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			gate.countDown();
		}
		awaitReleased(stalledRoute);
		awaitReleased(route);
	}

	/**
	 * A connection whose window is withheld gets it once another connection frees budget
	 * -- here by going away -- though none of its own queued bytes drained: its upstream
	 * still does not read. Both connections are withheld, each on a route of its own so
	 * its queue is its route's; the one left waiting is the one with the smaller share of
	 * the budget, the only one whose share alone may leave the budget room: a relay
	 * granted while the budget had room is charged the DATA that follows regardless, so
	 * one share may pass the whole budget.
	 */
	@Test
	void withheldWindowIsGrantedOnceAnotherConnectionFreesTheBudget() throws Exception {
		String firstRoute = "bud.wake.first.test";
		String secondRoute = "bud.wake.second.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(firstRoute, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		startClient(secondRoute, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		try (Stalled first = stall(firstRoute); Stalled second = stall(secondRoute)) {
			awaitStable(() -> queuedBytes(firstRoute) + queuedBytes(secondRoute));
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(first.client().connectionWindow()).as("the first connection's window withheld").isZero();
			assertThat(second.client().connectionWindow()).as("the second connection's window withheld").isZero();
			assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).as("withheld windows wait")
				.isEqualTo(2);
			long firstShare = budgetShare(firstRoute);
			long secondShare = budgetShare(secondRoute);
			Stalled waiting = firstShare <= secondShare ? first : second;
			Stalled leaving = waiting == first ? second : first;
			assertThat(Math.min(firstShare, secondShare)).as("the waiting connection's share leaves room")
				.isLessThan(BUDGET);
			long granted = waiting.client().connectionGranted();
			leaving.close();
			assertThat(waiting.client()
				.awaitTrue(() -> waiting.client().connectionGranted() > granted, Duration.ofSeconds(5)))
				.as("the withheld window was granted")
				.isTrue();
			assertThat(waiting.client().closed()).isFalse();
			gate.countDown();
			waiting.awaitEnds();
		}
		awaitReleased(firstRoute);
		awaitReleased(secondRoute);
	}

	/**
	 * Clients gone without a word while their windows are withheld: their relays release
	 * every byte of the budget, and leave no waiter behind.
	 */
	@Test
	void clientsGoneWhileWithheldReleaseTheBudget() throws Exception {
		String route = "bud.gone.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		try (Stalled first = stall(route); Stalled second = stall(route)) {
			awaitStable(() -> queuedBytes(route));
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).isPositive();
			first.close();
			second.close();
			awaitReleased(route);
		}
		finally {
			gate.countDown();
		}
	}

	/**
	 * A connection opening {@link #STREAMS} streams on the route, each sending
	 * {@link #BODY} bytes as fast as its windows admit.
	 */
	private Stalled stall(String route) throws Exception {
		return open(route, STREAMS, BODY);
	}

	/**
	 * A connection opening {@code streams} streams on the route, each sending
	 * {@code body} bytes as fast as its windows admit.
	 */
	private Stalled open(String route, int streams, int body) throws Exception {
		Stalled stalled = new Stalled(H2TestClient.connect(dataPort), 0, streams, body, 0);
		stalled.open(route);
		stalled.start();
		return stalled;
	}

	/**
	 * Opens {@link #STREAMS} streams on the route over the client, the first one at the
	 * stream index {@code first}, each sending {@code body} bytes as fast as its windows
	 * admit, while leaving {@code reserve} bytes of the connection window unspent.
	 */
	private static Stalled open(H2TestClient client, String route, int first, int body, int reserve) throws Exception {
		Stalled stalled = new Stalled(client, first, STREAMS, body, reserve);
		stalled.open(route);
		stalled.start();
		return stalled;
	}

	private static byte[] head(String route, String path) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=" + path);
	}

	private static int streamId(int index) {
		return 1 + 2 * index;
	}

	/** One connection and the thread pumping its request bodies. */
	private static final class Stalled implements AutoCloseable {

		private final H2TestClient client;

		private final long[] remaining;

		private final AtomicBoolean stop = new AtomicBoolean();

		private final AtomicLong idleSince = new AtomicLong();

		/** The connection window the pump leaves unspent. */
		private final int reserve;

		private @Nullable Thread pump;

		/** The stream index of the first stream. */
		private final int first;

		Stalled(H2TestClient client, int first, int streams, int body, int reserve) {
			this.client = client;
			this.first = first;
			this.remaining = new long[streams];
			this.reserve = reserve;
			java.util.Arrays.fill(this.remaining, body);
		}

		H2TestClient client() {
			return this.client;
		}

		/** Sends the heads of the streams. */
		void open(String route) throws IOException {
			for (int i = 0; i < this.remaining.length; i++) {
				this.client.headers(id(i), false, head(route, "/" + i));
			}
		}

		private int id(int index) {
			return streamId(this.first + index);
		}

		void start() {
			this.pump = Thread.ofVirtual().name("e2e-bud-pump").start(this::pump);
		}

		private void pump() {
			try {
				while (!this.stop.get() && !this.client.closed()) {
					boolean progress = false;
					boolean done = true;
					for (int i = 0; i < this.remaining.length; i++) {
						if (this.remaining[i] == 0) {
							continue;
						}
						done = false;
						int id = id(i);
						long window = Math.min(this.client.sendable(id), this.client.connectionWindow() - this.reserve);
						int n = (int) Math.min(Math.min(this.remaining[i], window), MAX_FRAME_SIZE);
						if (n > 0) {
							this.remaining[i] -= n;
							this.client.sendData(id, new byte[n], this.remaining[i] == 0 ? TestH2.FLAG_END_STREAM : 0);
							progress = true;
						}
					}
					if (done) {
						return;
					}
					if (progress) {
						this.idleSince.set(0);
					}
					else {
						this.idleSince.compareAndSet(0, System.nanoTime());
						Thread.sleep(1);
					}
				}
			}
			catch (IOException | InterruptedException e) {
				// torn down
			}
		}

		/**
		 * Whether the pump waits for window while bodies remain: its window is withheld.
		 */
		boolean withheld() {
			long since = this.idleSince.get();
			return since != 0 && System.nanoTime() - since > TimeUnit.MILLISECONDS.toNanos(500);
		}

		/**
		 * Waits until the pump got no window for a while: the connection's own queue is
		 * stalled, not only the route's.
		 */
		void awaitWithheld() {
			Awaitility.await().atMost(Duration.ofSeconds(60)).until(this::withheld);
		}

		/** Stops the pump; the streams stay unfinished. */
		void stop() throws InterruptedException {
			this.stop.set(true);
			Thread thread = this.pump;
			if (thread != null) {
				thread.join(TimeUnit.SECONDS.toMillis(5));
			}
		}

		/** Starts the pump again after {@link #stop()}. */
		void resume() {
			this.stop.set(false);
			start();
		}

		/** Waits until every stream sent its body and got its response. */
		void awaitEnds() throws InterruptedException {
			for (int i = 0; i < this.remaining.length; i++) {
				int id = id(i);
				assertThat(this.client.awaitEnd(id, Duration.ofSeconds(30)))
					.as("stream %d completes (closed %s, resets %s)", id, this.client.closed(), this.client.resets())
					.isTrue();
			}
		}

		@Override
		public void close() throws IOException {
			this.stop.set(true);
			this.client.close();
		}

	}

	/** Waits until the value has not changed for a second; returns it. */
	private static long awaitSteady(LongSupplier value) throws InterruptedException {
		long last = value.getAsLong();
		long changedAt = System.nanoTime();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (System.nanoTime() - changedAt < TimeUnit.SECONDS.toNanos(1)) {
			assertThat(System.nanoTime()).as("the value settles").isLessThan(deadline);
			long current = value.getAsLong();
			if (current != last) {
				last = current;
				changedAt = System.nanoTime();
			}
			Thread.sleep(10);
		}
		return last;
	}

	/** Waits until the value has not grown for a second; returns its maximum. */
	private static long awaitStable(LongSupplier value) throws InterruptedException {
		long max = 0;
		long grownAt = System.nanoTime();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (System.nanoTime() - grownAt < TimeUnit.SECONDS.toNanos(1)) {
			assertThat(System.nanoTime()).as("the queue settles").isLessThan(deadline);
			long current = value.getAsLong();
			if (current > max) {
				max = current;
				grownAt = System.nanoTime();
			}
			Thread.sleep(10);
		}
		return max;
	}

	/** Waits until the route queues nothing and the budget is back to empty. */
	private void awaitReleased(String route) {
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> assertThat(queuedBytes(route)).as("queued for the route").isZero());
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> assertThat(budgetUsed()).as("budget used").isZero());
		assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).as("no waiter left").isZero();
	}

	/**
	 * Grants the connection and every stream all the window there is, reads the heads of
	 * the streams, and then nothing until the gate opens; from then on reads everything
	 * and answers every ended request.
	 */
	private static void serveGated(Socket socket, CountDownLatch gate, AtomicInteger heads) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			int seen = 0;
			TestH2.@Nullable Frame frame;
			while (gate.getCount() > 0 && seen < STREAMS && (frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS) {
					heads.incrementAndGet();
					seen++;
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(),
							TestH2.int32(Integer.MAX_VALUE - 65535));
				}
			}
			gate.await();
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * {@link #serveGated} that decodes every request head and, once the gate opened, logs
	 * the heads, the resets, and any frame on a stream after its reset; it answers a
	 * reset with one of its own.
	 */
	private static void serveGatedLogging(Socket socket, CountDownLatch gate, UpstreamLog log) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);
			Set<Integer> reset = new HashSet<>();
			int seen = 0;
			TestH2.@Nullable Frame frame;
			while (gate.getCount() > 0 && seen < STREAMS && (frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS) {
					decoder.decodeHeaders(frame.streamId(), Unpooled.wrappedBuffer(frame.payload()));
					seen++;
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(),
							TestH2.int32(Integer.MAX_VALUE - 65535));
				}
			}
			log.idle.set(out);
			gate.await();
			while ((frame = TestH2.readFrame(in)) != null) {
				int streamId = frame.streamId();
				if (reset.contains(streamId)) {
					log.afterReset.add("type=%d stream=%d".formatted(frame.type(), streamId));
				}
				if (frame.type() == TestH2.HEADERS) {
					log.heads.put(streamId,
							lines(decoder.decodeHeaders(streamId, Unpooled.wrappedBuffer(frame.payload()))));
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, streamId, TestH2.int32(Integer.MAX_VALUE - 65535));
				}
				else if (frame.type() == TestH2.RST_STREAM) {
					reset.add(streamId);
					log.resets.put(streamId, TestH2.errorCode(frame));
					// as if sent before the reset arrived
					TestH2.writeFrame(out, TestH2.RST_STREAM, 0, streamId, TestH2.int32(NO_ERROR));
				}
				boolean ended = streamId != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(streamId, "ok"));
					out.flush();
				}
			}
		}
		catch (IOException | Http2Exception e) {
			// connection ended
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * What the gated upstreams of a route saw once their gate opened, across their
	 * connections: request heads (decoded, by stream), resets (error code by stream), and
	 * frames on a stream after its reset.
	 */
	private static final class UpstreamLog {

		private final Map<Integer, List<String>> heads = new ConcurrentHashMap<>();

		private final Map<Integer, Integer> resets = new ConcurrentHashMap<>();

		private final List<String> afterReset = new CopyOnWriteArrayList<>();

		/**
		 * The output of the route's upstream once it stopped reading for the gate; one
		 * connection per route.
		 */
		private final AtomicReference<@Nullable OutputStream> idle = new AtomicReference<>();

		/** Waits until the route's upstream stopped reading for the gate. */
		OutputStream awaitIdle() {
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> this.idle.get() != null);
			return Objects.requireNonNull(this.idle.get());
		}

	}

	/**
	 * An upstream scripted around a stream the relay resets: once both request heads
	 * arrived and {@link #respond} opened, it answers the probe stream with a head and a
	 * whole connection window of DATA, then -- granted that back -- answers the stream to
	 * reset with a head and {@link #WAITING} bytes of DATA, followed by a PING whose ack
	 * ({@link #queued}) tells the relay processed them. On the relay's reset it writes
	 * what it might have had in flight on the stream ({@link #IN_FLIGHT} bytes of DATA, a
	 * header block adding a dynamic table entry, an unknown frame), none of it ending the
	 * stream, then the probe's trailers referring to that entry ({@link #answered}); it
	 * leaves once {@link #leave} opens.
	 */
	private static final class CancelScript {

		static final int WAITING = 1000;

		static final int IN_FLIGHT = 500;

		private static final byte[] FENCE = "fence-01".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

		private final int reset;

		private final int probe;

		private final CountDownLatch heads = new CountDownLatch(2);

		private final CountDownLatch respond = new CountDownLatch(1);

		private final CountDownLatch queued = new CountDownLatch(1);

		private final CountDownLatch cancelled = new CountDownLatch(1);

		private final CountDownLatch answered = new CountDownLatch(1);

		private final CountDownLatch leave = new CountDownLatch(1);

		/** The connection window the relay granted back. */
		private final AtomicLong granted = new AtomicLong();

		private final AtomicInteger resetCode = new AtomicInteger(-1);

		CancelScript(int reset, int probe) {
			this.reset = reset;
			this.probe = probe;
		}

		void serve(Socket socket) {
			try (socket) {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
					return;
				}
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
				Thread.ofVirtual().name("e2e-bud-cancel-reader").start(() -> read(in));
				this.heads.await();
				this.respond.await();
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, this.probe, new byte[] { (byte) 0x88 });
				for (int left = H2TestClient.INITIAL_WINDOW; left > 0; left -= MAX_FRAME_SIZE) {
					TestH2.writeFrame(out, TestH2.DATA, 0, this.probe, new byte[Math.min(left, MAX_FRAME_SIZE)]);
				}
				Awaitility.await()
					.atMost(Duration.ofSeconds(10))
					.until(() -> this.granted.get() >= H2TestClient.INITIAL_WINDOW);
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, this.reset, new byte[] { (byte) 0x88 });
				TestH2.writeFrame(out, TestH2.DATA, 0, this.reset, new byte[WAITING]);
				TestH2.writeFrame(out, TestH2.PING, 0, 0, FENCE);
				this.cancelled.await();
				TestH2.writeFrame(out, TestH2.DATA, 0, this.reset, new byte[IN_FLIGHT]);
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, this.reset,
						TestH2.appendLiteralIncremental(new byte[0], "x-dropped", "1"));
				TestH2.writeFrame(out, 0xfa, 0, this.reset, new byte[4]);
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, this.probe,
						TestH2.appendIndexedDynamic(new byte[0], 62));
				this.answered.countDown();
				this.leave.await();
			}
			catch (IOException e) {
				// connection ended
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private void read(InputStream in) {
			try {
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					switch (frame.type()) {
						case TestH2.HEADERS -> this.heads.countDown();
						case TestH2.WINDOW_UPDATE -> {
							if (frame.streamId() == 0) {
								this.granted.addAndGet(TestH2.increment(frame));
							}
						}
						case TestH2.PING -> {
							if ((frame.flags() & TestH2.FLAG_ACK) != 0 && Arrays.equals(frame.payload(), FENCE)) {
								this.queued.countDown();
							}
						}
						case TestH2.RST_STREAM -> {
							if (frame.streamId() == this.reset) {
								this.resetCode.set(TestH2.errorCode(frame));
								this.cancelled.countDown();
							}
						}
						default -> {
							// not tracked
						}
					}
				}
			}
			catch (IOException e) {
				// connection ended
			}
		}

	}

	/** The decoded headers as {@code name: value} lines, in order. */
	private static List<String> lines(Http2Headers headers) {
		List<String> lines = new ArrayList<>();
		headers.forEach(header -> lines.add(header.getKey() + ": " + header.getValue()));
		return lines;
	}

	/** The types of the frames, the relay's own stream window grants left out. */
	private static List<Integer> types(List<TestH2.Frame> frames) {
		return frames.stream().map(TestH2.Frame::type).filter(type -> type != TestH2.WINDOW_UPDATE).toList();
	}

	/** The last header block of the frames, decoded in order with the ones before it. */
	private static List<String> lastHeaderBlock(List<TestH2.Frame> frames) throws Http2Exception {
		DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);
		List<String> last = List.of();
		for (TestH2.Frame frame : frames) {
			if (frame.type() == TestH2.HEADERS) {
				last = lines(decoder.decodeHeaders(frame.streamId(), Unpooled.wrappedBuffer(frame.payload())));
			}
		}
		return last;
	}

	/**
	 * Reads everything at about 4MiB/s, granting it back, and answers every ended
	 * request.
	 */
	private static void answerEndedRequestsSlowly(Socket socket) {
		try (socket) {
			InputStream in = new ThrottledInputStream(socket.getInputStream());
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(frame.payload().length));
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(),
							TestH2.int32(frame.payload().length));
				}
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** Reads at most 64KiB per 16ms: about 4MiB/s. */
	private static final class ThrottledInputStream extends InputStream {

		private static final int SLICE = 64 * 1024;

		private final InputStream in;

		private int budget = SLICE;

		ThrottledInputStream(InputStream in) {
			this.in = in;
		}

		@Override
		public int read() throws IOException {
			byte[] one = new byte[1];
			int n = read(one, 0, 1);
			return n == 1 ? one[0] & 0xff : -1;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (len == 0) {
				return 0;
			}
			if (this.budget == 0) {
				try {
					Thread.sleep(16);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException(e);
				}
				this.budget = SLICE;
			}
			int n = this.in.read(b, off, Math.min(len, this.budget));
			if (n > 0) {
				this.budget -= n;
			}
			return n;
		}

	}

	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find(H2DemuxMeters.QUEUED_GAUGE_NAME)
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.sum());
	}

	private long budgetGauge() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.bytes").gauge().value());
	}

	/** The part of the route's queue charged to the budget: its relay's share. */
	private long budgetShare(String route) {
		return Math.max(0, queuedBytes(route) - Http2DemuxRelay.CONNECTION_ALLOWANCE);
	}

	private long budgetUsed() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private double withheld() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.window.withheld").counter().count();
	}

	private double refused(String reason) {
		Counter counter = Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.refused")
			.tag("reason", reason)
			.counter();
		return counter.count();
	}

	private double resets(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private long trackedStreams(String route) {
		return dataProxyServer().h2DemuxMeters().trackedStreams(route);
	}

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private static void awaitPingAck(H2TestClient client) throws Exception {
		byte[] ping = Arrays.copyOf(TestH2.int32((int) PINGS.incrementAndGet()), 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
				Duration.ofSeconds(5)))
			.as("the relay acked the PING")
			.isTrue();
	}

	/**
	 * Waits until the server side of the connection from {@code clientPort} took the
	 * client's reset. On Linux, until the kernel dropped the connection from
	 * {@code /proc/net/tcp*}: a round trip on another loopback connection does not order
	 * the two there (the connection was seen still listed past one). Elsewhere, past such
	 * a round trip.
	 */
	private void awaitResetTaken(int clientPort) throws IOException {
		if (TCP_TABLES.isEmpty()) {
			loopbackRoundTrip();
			return;
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !connectionListed(clientPort));
	}

	/** Whether the kernel lists the data plane's connection from {@code clientPort}. */
	private boolean connectionListed(int clientPort) throws IOException {
		String local = ":%04X".formatted(dataPort);
		String remote = ":%04X".formatted(clientPort);
		for (Path table : TCP_TABLES) {
			for (String line : Files.readAllLines(table)) {
				String[] fields = line.trim().split("\\s+");
				if (fields.length > 2 && fields[1].endsWith(local) && fields[2].endsWith(remote)) {
					return true;
				}
			}
		}
		return false;
	}

	/** A byte each way over a fresh loopback connection. */
	private static void loopbackRoundTrip() throws IOException {
		try (ServerSocket server = new ServerSocket()) {
			server.bind(new InetSocketAddress("127.0.0.1", 0));
			try (Socket near = new Socket("127.0.0.1", server.getLocalPort()); Socket far = server.accept()) {
				near.getOutputStream().write(1);
				assertThat(far.getInputStream().read()).isEqualTo(1);
				far.getOutputStream().write(2);
				assertThat(near.getInputStream().read()).isEqualTo(2);
			}
		}
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-bud").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-bud-accept").start(() -> {
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

}
