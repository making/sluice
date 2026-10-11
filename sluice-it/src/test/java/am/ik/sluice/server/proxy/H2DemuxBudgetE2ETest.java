package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
	 * Trailers on a connection past its allowance with the budget exhausted cannot be
	 * refused without leaving the stream unfinished: the connection ends with GOAWAY
	 * ENHANCE_YOUR_CALM.
	 */
	@Test
	void trailersPastTheAllowanceWithTheBudgetExhaustedEndTheConnection() throws Exception {
		String route = "bud.trailers.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		try (Stalled first = stall(route); Stalled second = stall(route)) {
			awaitStable(() -> queuedBytes(route));
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			first.stop();
			int stream = first.unfinishedStream();
			first.client().headers(stream, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(first.client().awaitTrue(first.client()::closed, Duration.ofSeconds(5))).isTrue();
			assertThat(first.client().goAwayCode()).isEqualTo(0xb);
			gate.countDown();
			second.awaitEnds();
		}
		awaitReleased(route);
	}

	/**
	 * A connection whose window is withheld gets it once another connection frees budget
	 * -- here by going away -- though none of its own queued bytes drained: its upstream
	 * still does not read.
	 */
	@Test
	void withheldWindowIsGrantedOnceAnotherConnectionFreesTheBudget() throws Exception {
		String route = "bud.wake.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, new AtomicInteger())));
		try (Stalled first = stall(route); Stalled second = stall(route)) {
			awaitStable(() -> queuedBytes(route));
			assertThat(budgetUsed()).as("exhausted").isGreaterThanOrEqualTo(BUDGET);
			assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).as("withheld windows wait")
				.isPositive();
			Stalled waiting = first.withheld() ? first : second;
			Stalled leaving = waiting == first ? second : first;
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
		awaitReleased(route);
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
		H2TestClient client = H2TestClient.connect(dataPort);
		for (int i = 0; i < streams; i++) {
			client.headers(streamId(i), false,
					TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/" + i));
		}
		Stalled stalled = new Stalled(client, streams, body);
		stalled.start();
		return stalled;
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

		private @Nullable Thread pump;

		Stalled(H2TestClient client, int streams, int body) {
			this.client = client;
			this.remaining = new long[streams];
			java.util.Arrays.fill(this.remaining, body);
		}

		H2TestClient client() {
			return this.client;
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
						int id = streamId(i);
						int n = (int) Math.min(Math.min(this.remaining[i], this.client.sendable(id)), MAX_FRAME_SIZE);
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

		/** Stops the pump; the streams stay unfinished. */
		void stop() throws InterruptedException {
			this.stop.set(true);
			Thread thread = this.pump;
			if (thread != null) {
				thread.join(TimeUnit.SECONDS.toMillis(5));
			}
		}

		/** A stream with body left; only after {@link #stop()}. */
		int unfinishedStream() {
			for (int i = 0; i < this.remaining.length; i++) {
				if (this.remaining[i] > 0) {
					return streamId(i);
				}
			}
			throw new IllegalStateException("every stream finished");
		}

		/** Waits until every stream sent its body and got its response. */
		void awaitEnds() throws InterruptedException {
			for (int i = 0; i < this.remaining.length; i++) {
				int id = streamId(i);
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
