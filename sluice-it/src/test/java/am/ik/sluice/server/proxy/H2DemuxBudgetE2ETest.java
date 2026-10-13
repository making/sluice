package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

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

	/**
	 * Past one connection's share, below two: two stalled connections, each stopping at
	 * its share at the latest, exhaust it whatever the tunnel absorbs. Past a relay's
	 * connection window too: a relay granted while the budget had room is charged what
	 * its client then sends within that window, so a smaller budget would let one share
	 * alone pass it sooner.
	 */
	private static final long BUDGET = 3 * NettyHttp2DemuxRelay.CONNECTION_WINDOW;

	/**
	 * Streams per stalled connection: their stream windows add up far past the budget.
	 */
	private static final int STREAMS = 20;

	/**
	 * The request body of a stalled stream: no end until the test ends it, so its
	 * connection sends until its window is withheld, whatever the tunnel and the
	 * upstream's socket absorb before the relay holds anything.
	 */
	private static final long UNBOUNDED = Long.MAX_VALUE;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private static final int NO_ERROR = 0x0;

	private static final int CANCEL = 0x8;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	/** A RST_STREAM frame: its header and error code. */
	private static final int RST_STREAM_BYTES = 9 + 4;

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
	 * Two connections whose upstream does not read, twenty streams each sending without
	 * end: together they stop at the budget plus what each queues outside it -- far below
	 * what their stream windows allow -- with the budget exhausted and their windows
	 * withheld, yet no stream is reset and no connection torn down; once the upstream
	 * reads, every stream completes and the budget is released.
	 */
	@Test
	void stalledConnectionsStopAtTheBudgetAndCompleteOnceTheUpstreamReads() throws Exception {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		String route = "bud.stall.test";
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger heads = new AtomicInteger();
		startClient(route, stub(socket -> serveGated(socket, gate, heads)));
		double withheldBefore = withheld();
		try (Stalled first = stall(route, heads); Stalled second = stall(route, heads)) {
			awaitExhausted();
			long maxQueued = awaitStable(() -> queuedBytes(route));
			assertThat(maxQueued).as("queued at most: the budget plus each connection's unbudgeted bound")
				.isLessThanOrEqualTo(BUDGET + 2 * NettyHttp2DemuxRelay.UNBUDGETED_BYTES)
				.isGreaterThanOrEqualTo(BUDGET);
			assertThat(budgetUsed()).as("the budget is exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(withheld()).as("windows were withheld").isGreaterThan(withheldBefore);
			assertThat(first.client().closed() || second.client().closed()).as("no connection torn down").isFalse();
			assertThat(first.client().resets()).isEmpty();
			assertThat(second.client().resets()).isEmpty();
			gate.countDown();
			first.end();
			second.end();
			first.awaitEnds();
			second.awaitEnds();
			assertThat(first.client().goAwayCode()).isEqualTo(-1);
			assertThat(second.client().goAwayCode()).isEqualTo(-1);
		}
		awaitReleased(route);
	}

	/**
	 * While two stalled connections hold the budget exhausted, a connection on another
	 * route uploads 512KiB on each of four streams to an upstream reading at about
	 * 4MiB/s, and gets every response: its queue passes its allowance -- its window is
	 * withheld -- and drains below it again and again, its own drain granting the window
	 * back, so a connection whose legs drain keeps flowing.
	 */
	@Test
	void connectionOnAnotherRouteKeepsFlowingWhileTheBudgetIsExhausted() throws Exception {
		String stalledRoute = "bud.held.test";
		String flowingRoute = "bud.flowing.test";
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger heads = new AtomicInteger();
		startClient(stalledRoute, stub(socket -> serveGated(socket, gate, heads)));
		startClient(flowingRoute, stub(H2DemuxBudgetE2ETest::answerEndedRequestsSlowly));
		try (Stalled first = stall(stalledRoute, heads); Stalled second = stall(stalledRoute, heads)) {
			awaitExhausted();
			awaitStable(() -> queuedBytes(stalledRoute));
			assertThat(budgetUsed()).as("exhausted before").isGreaterThanOrEqualTo(BUDGET);
			double withheldBefore = withheld("budget");
			// the relay holds a stream window per stream: four take it past its
			// allowance.
			// A first exchange opens the leg and takes the upstream's SETTINGS, so the
			// uploads' streams open at once instead of waiting for the leg's concurrency
			// limit (waiting streams past the allowance are refused while withheld)
			H2TestClient client = H2TestClient.connect(dataPort);
			client.headers(1, true, TestH2.headerBlock(":authority=" + flowingRoute, ":method=GET", ":path=/warm"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).as("the leg opened").isTrue();
			try (Stalled flowing = new Stalled(client, 1, 4, 512 * 1024, 0)) {
				flowing.open(flowingRoute);
				flowing.start();
				flowing.awaitEnds();
				assertThat(flowing.client().connectionGranted())
					.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
			}
			// the stalled connections' shares never shrink while their upstream does not
			// read, so the budget stays exhausted, their windows stay withheld, and only
			// the flowing connection can have counted
			assertThat(withheld("budget")).as("the flowing connection passed its allowance")
				.isGreaterThan(withheldBefore);
			assertThat(budgetUsed()).as("still exhausted after").isGreaterThanOrEqualTo(BUDGET);
			gate.countDown();
			first.end();
			second.end();
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
		try (Stalled first = stall(route, heads); Stalled second = stall(route, heads)) {
			awaitExhausted();
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
			first.end();
			second.end();
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
		try (Stalled first = open(client, route, 1, UNBOUNDED, body + late, log.heads());
				Stalled second = stall(route, log.heads())) {
			// the first connection may stop at its share before the second exhausts the
			// budget
			first.awaitWithheld();
			awaitExhausted();
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
			// the client reset goes out at once, before the relay's action ends: the
			// leg's
			// reset and the accounting follow
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(
						() -> assertThat(queuedBytes(route)).as("the stream's queued data dropped, a reset queued")
							.isLessThanOrEqualTo(queued - body + RST_STREAM_BYTES));
			assertThat(trackedStreams(route)).as("forgotten once its client stream closed").isEqualTo(2 * STREAMS);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
			gate.countDown();
			first.end();
			second.end();
			first.awaitEnds();
			second.awaitEnds();
			client.headers(after, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/after"));
			assertThat(client.awaitEnd(after, Duration.ofSeconds(10))).isTrue();
			assertThat(log.requestHeads.get(after)).as("the next stream's head")
				.isEqualTo(List.of(":authority: " + route, ":method: GET", ":path: /after"));
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
	 * A connection whose window is withheld gets it once other connections free budget --
	 * here by going away -- though none of its own queued bytes drained: its upstream
	 * still does not read. Two connections on a route whose upstream does not read
	 * exhaust the budget, each stopping at its share or at the budget; the connection on
	 * a route of its own (its queue is its route's) is withheld as soon as it passes its
	 * allowance, so its share stays below a connection's -- what the DATA following the
	 * withholding adds included -- and below the budget: once the others left, nothing of
	 * its own holds its window back.
	 */
	@Test
	void withheldWindowIsGrantedOnceOtherConnectionsFreeTheBudget() throws Exception {
		String heldRoute = "bud.wake.held.test";
		String waitingRoute = "bud.wake.waiting.test";
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger heldHeads = new AtomicInteger();
		AtomicInteger waitingHeads = new AtomicInteger();
		startClient(heldRoute, stub(socket -> serveGated(socket, gate, heldHeads)));
		startClient(waitingRoute, stub(socket -> serveGated(socket, gate, waitingHeads)));
		try (Stalled first = stall(heldRoute, heldHeads); Stalled second = stall(heldRoute, heldHeads)) {
			awaitExhausted();
			awaitStable(() -> queuedBytes(heldRoute));
			try (Stalled waiting = stall(waitingRoute, waitingHeads)) {
				waiting.awaitWithheld();
				awaitStable(() -> queuedBytes(waitingRoute));
				assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
				assertThat(waiting.client().connectionWindow()).as("the waiting connection's window withheld").isZero();
				assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).as("withheld for the budget")
					.isPositive();
				assertThat(budgetShare(waitingRoute))
					.as("the waiting connection's share: below its own, below the budget")
					.isLessThan(Math.min(NettyHttp2DemuxRelay.CONNECTION_SHARE, BUDGET));
				long granted = waiting.client().connectionGranted();
				first.close();
				second.close();
				assertThat(waiting.client()
					.awaitTrue(() -> waiting.client().connectionGranted() > granted, Duration.ofSeconds(5)))
					.as("the withheld window was granted")
					.isTrue();
				assertThat(waiting.client().closed()).isFalse();
				gate.countDown();
				waiting.end();
				waiting.awaitEnds();
			}
		}
		finally {
			gate.countDown();
		}
		awaitReleased(heldRoute);
		awaitReleased(waitingRoute);
	}

	/**
	 * Clients gone without a word while their windows are withheld: their relays release
	 * every byte of the budget, and leave no waiter behind.
	 */
	@Test
	void clientsGoneWhileWithheldReleaseTheBudget() throws Exception {
		String route = "bud.gone.test";
		CountDownLatch gate = new CountDownLatch(1);
		AtomicInteger heads = new AtomicInteger();
		startClient(route, stub(socket -> serveGated(socket, gate, heads)));
		try (Stalled first = stall(route, heads); Stalled second = stall(route, heads)) {
			awaitExhausted();
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
	 * A connection opening {@link #STREAMS} streams on the route, each sending without
	 * end as fast as its windows admit once the route's upstream counted their heads in
	 * {@code heads}.
	 */
	private Stalled stall(String route, AtomicInteger heads) throws Exception {
		return open(H2TestClient.connect(dataPort), route, 0, UNBOUNDED, 0, heads);
	}

	/**
	 * Opens {@link #STREAMS} streams on the route over the client, the first one at the
	 * stream index {@code first}, and waits until the route's upstream counted their
	 * heads in {@code heads}; then each sends {@code body} bytes as fast as its windows
	 * admit, while leaving {@code reserve} bytes of the connection window unspent. A
	 * stream whose head has not reached the upstream may wait for the leg's first
	 * SETTINGS (concurrency one until then); waiting streams past the allowance are
	 * refused once the windows are withheld, their DATA void, so the relay would hold
	 * less than the connection sent.
	 */
	private static Stalled open(H2TestClient client, String route, int first, long body, int reserve,
			AtomicInteger heads) throws Exception {
		int before = heads.get();
		Stalled stalled = new Stalled(client, first, STREAMS, body, reserve);
		stalled.open(route);
		Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> heads.get() >= before + STREAMS);
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

		Stalled(H2TestClient client, int first, int streams, long body, int reserve) {
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
						if (this.client.reset(id) != null) {
							// nothing more to send on a stream the relay reset
							this.remaining[i] = 0;
							continue;
						}
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

		/**
		 * Stops the pump and ends every stream still sending with an empty DATA frame
		 * carrying END_STREAM, which takes no window.
		 */
		void end() throws IOException, InterruptedException {
			stop();
			for (int i = 0; i < this.remaining.length; i++) {
				int id = id(i);
				if (this.remaining[i] != 0 && this.client.reset(id) == null) {
					this.remaining[i] = 0;
					this.client.sendData(id, new byte[0], TestH2.FLAG_END_STREAM);
				}
			}
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

	/**
	 * Waits until the budget is exhausted: the stalled connections may take a while to
	 * start (a loaded machine), and the queue's plateau only counts after that.
	 */
	private void awaitExhausted() {
		Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> budgetUsed() >= BUDGET);
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
					log.heads.incrementAndGet();
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
					log.requestHeads.put(streamId,
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
	 * What the gated upstreams of a route saw, across their connections: the request
	 * heads read before the gate opened (counted), and once it opened, request heads
	 * (decoded, by stream), resets (error code by stream), and frames on a stream after
	 * its reset.
	 */
	private static final class UpstreamLog {

		private final AtomicInteger heads = new AtomicInteger();

		private final Map<Integer, List<String>> requestHeads = new ConcurrentHashMap<>();

		private final Map<Integer, Integer> resets = new ConcurrentHashMap<>();

		private final List<String> afterReset = new CopyOnWriteArrayList<>();

		/**
		 * The output of the route's upstream once it stopped reading for the gate; one
		 * connection per route.
		 */
		private final AtomicReference<@Nullable OutputStream> idle = new AtomicReference<>();

		/** The request heads read before the gate opened. */
		AtomicInteger heads() {
			return this.heads;
		}

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
		return Math.max(0, queuedBytes(route) - NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
	}

	private long budgetUsed() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	/** The times a relay withheld its windows, for its share or for the budget. */
	private double withheld() {
		return Objects.requireNonNull(this.meterRegistry)
			.find("sluice.h2demux.window.withheld")
			.counters()
			.stream()
			.mapToDouble(Counter::count)
			.sum();
	}

	/** The times a relay withheld its windows for the reason. */
	private double withheld(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.window.withheld")
			.tag("reason", reason)
			.counter()
			.count();
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
