package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.DelayingProxy;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connection window the relays' peers hold is charged to the queue budget past RFC
 * 9113's window per hop: with many h2 connections downloading to clients that do not read
 * and uploading to upstreams that do not read, what the relays hold plus the window their
 * peers hold -- what they may still make the relays hold -- stays within the budget and
 * what the connections reserve outside it at every sample, though granted 1MiB windows
 * the peers alone would hold far more. The charged window is released as the hop holding
 * it ends: an upstream going away, a client going away, a client closing, while a reset
 * or a stream's end releases what was held of it and leaves the window the peer still
 * holds charged. In the relay's package for the relay's constants and the limits'
 * accounts.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxWindowCreditE2ETest {

	private static final String DOWNLOAD_HOST = "credit.download.test";

	private static final String UPLOAD_HOST = "credit.upload.test";

	private static final String DELAYED_HOST = "credit.delayed.test";

	/** h2 connections at once, half downloading, half uploading. */
	private static final int CONNECTIONS = 16;

	private static final long MEMORY = CONNECTIONS
			* (ConnectionLimits.connectionBytes() + ConnectionLimits.h2RelayBytes());

	private static final long BUDGET = 4L * 1024 * 1024;

	/** Streams per connection, each moving a stream window. */
	private static final int STREAMS = 4;

	private static final int BODY = NettyHttp2DemuxRelay.STREAM_WINDOW;

	/** The delayed download: four of the largest windows. */
	private static final int DELAYED_BODY = 4 * NettyHttp2DemuxRelay.CONNECTION_WINDOW;

	/** The delayed stream an upstream sends until reset. */
	private static final int ENDLESS_STREAM = 3;

	/** The delayed stream whose response fits the relay's stream window. */
	private static final int HELD_STREAM = 5;

	/** {@link #HELD_STREAM}'s response: half a stream window. */
	private static final int HELD_BODY = NettyHttp2DemuxRelay.STREAM_WINDOW / 2;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private static final int FLOOR = HopWindow.FLOOR;

	private static final Duration DELAY = Duration.ofMillis(10);

	private static final Duration TIMEOUT = Duration.ofSeconds(20);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	/** The delayed upstream's connections, the latest last. */
	private final List<WindowedH2Upstream> delayedUpstreams = new CopyOnWriteArrayList<>();

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
		registry.add("sluice.limits.connection-memory", () -> MEMORY + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClients() throws IOException {
		startClient(DOWNLOAD_HOST, stub(H2DemuxWindowCreditE2ETest::serveResponses), grpcPort);
		startClient(UPLOAD_HOST, stub(H2DemuxWindowCreditE2ETest::holdRequests), grpcPort);
		DelayingProxy tunnel = DelayingProxy.start(grpcPort, DELAY);
		this.closeables.add(tunnel);
		startClient(DELAYED_HOST, stub(this::serveDelayed), tunnel.port());
	}

	@BeforeEach
	void awaitReleased() {
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(limits().reserved()).as("reserved").isZero();
			assertThat(held()).as("held").isZero();
			assertThat(credit()).as("credit").isZero();
			assertThat(used()).as("budget used").isZero();
		});
	}

	@AfterEach
	void forgetUpstreams() {
		this.delayedUpstreams.clear();
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxWindowCreditE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Sixteen connections, eight downloading four stream windows each to a client
	 * granting no connection window, eight uploading four each to an upstream granting no
	 * stream window: what the relays hold plus the window their peers hold, sampled every
	 * millisecond until it settled, stays within the budget plus what the connections
	 * reserve; the peers' windows grew past the floor where the budget had room.
	 */
	@Test
	void manyConnectionsHoldWithTheirPeersWindowsNoMoreThanTheBudgetAndWhatTheyReserve() throws Exception {
		assertThat(limits().memory()).as("the configured memory took effect").isEqualTo(MEMORY);
		assertThat(limits().h2Budget().max()).as("the configured budget took effect").isEqualTo(BUDGET);
		long bound = BUDGET + CONNECTIONS * ConnectionLimits.h2RelayBytes();
		AtomicLong maxExposure = new AtomicLong();
		AtomicLong maxCredit = new AtomicLong();
		AtomicLong maxUsed = new AtomicLong();
		AtomicBoolean sampling = new AtomicBoolean(true);
		Thread sampler = Thread.ofVirtual().name("e2e-credit-sampler").start(() -> {
			while (sampling.get()) {
				long credit = credit();
				maxCredit.accumulateAndGet(credit, Math::max);
				maxExposure.accumulateAndGet(held() + credit, Math::max);
				maxUsed.accumulateAndGet(used(), Math::max);
				try {
					Thread.sleep(1);
				}
				catch (InterruptedException e) {
					return;
				}
			}
		});
		List<H2TestClient> downloads = new ArrayList<>();
		List<Pump> uploads = new ArrayList<>();
		try {
			for (int i = 0; i < CONNECTIONS / 2; i++) {
				downloads.add(download());
				uploads.add(upload());
			}
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(limits().reserved()).as("each connection reserved its relay")
					.isEqualTo(MEMORY));
			uploads.forEach(Pump::start);
			awaitStable(maxExposure);
			sampling.set(false);
			sampler.join();
			System.out.printf(
					"window credit: %d connections, held + credit at most %dKiB (bound %dKiB), credit at most %dKiB, budget used at most %dKiB%n",
					CONNECTIONS, maxExposure.get() >> 10, bound >> 10, maxCredit.get() >> 10, maxUsed.get() >> 10);
			assertThat(maxExposure.get()).as("held + credit: within the budget and what the connections reserve")
				.isLessThanOrEqualTo(bound);
			assertThat((long) CONNECTIONS * 2 * NettyHttp2DemuxRelay.CONNECTION_WINDOW)
				.as("what the peers would hold at the largest window: past the bound")
				.isGreaterThan(bound);
			assertThat(maxCredit.get()).as("windows grown past the floors").isGreaterThan(2L * CONNECTIONS * FLOOR);
			assertThat(maxUsed.get()).as("the budget charged").isPositive();
			for (H2TestClient download : downloads) {
				assertThat(download.closed()).as("a download held, not ended").isFalse();
			}
		}
		finally {
			sampling.set(false);
			uploads.forEach(Pump::close);
			downloads.forEach(H2DemuxWindowCreditE2ETest::close);
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(credit()).as("no credit left").isZero();
			assertThat(used()).as("the budget released").isZero();
		});
	}

	/**
	 * A download through a delayed tunnel grows its leg's window; once it ended, the
	 * window its upstream still holds stays charged -- the budget used is the credit past
	 * the two floors --, a stream the client resets releases what was held of it and no
	 * more, and the upstream going away releases its window while the client's connection
	 * goes on; the client closing releases the rest.
	 */
	@Test
	void aLegsWindowStaysChargedThroughItsStreamsAndIsReleasedWhenItsUpstreamGoes() throws Exception {
		try (H2TestClient client = boostedDownload()) {
			client.headers(ENDLESS_STREAM, true, head(DELAYED_HOST, "GET", "/endless"));
			assertThat(client.awaitTrue(() -> client.dataReceived(ENDLESS_STREAM) > DELAYED_BODY, TIMEOUT))
				.as("the endless stream flowing")
				.isTrue();
			client.frame(TestH2.RST_STREAM, 0, ENDLESS_STREAM, TestH2.int32(0x8));
			awaitChargedCreditOnly("after the reset");

			WindowedH2Upstream upstream = this.delayedUpstreams.getLast();
			upstream.abort();
			Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
				assertThat(credit()).as("the client's floor alone").isEqualTo(FLOOR);
				assertThat(used()).as("the leg's window released").isZero();
			});
			assertThat(client.closed()).as("the client's connection goes on").isFalse();
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(credit()).isZero();
			assertThat(used()).isZero();
		});
	}

	/**
	 * An upstream going away while the relay still holds a response it completed for a
	 * client that does not read: its leg's window is released with its close, before the
	 * response drains -- the budget charges only what is held past the allowance.
	 */
	@Test
	void aLegClosedWhileItsResponseIsHeldReleasesItsWindowAtOnce() throws Exception {
		try (H2TestClient client = boostedDownload()) {
			client.withholdConnectionWindow();
			client.headers(HELD_STREAM, true, head(DELAYED_HOST, "GET", "/held"));
			// the upstream sent all of it; the client took one connection window
			long least = HELD_BODY - H2TestClient.INITIAL_WINDOW - 16 * 1024;
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(held()).as("the response held").isGreaterThan(least));
			WindowedH2Upstream upstream = this.delayedUpstreams.getLast();
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.sent() >= DELAYED_BODY + HELD_BODY);
			upstream.abort();
			Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
				assertThat(credit()).as("the client's floor alone").isEqualTo(FLOOR);
				long held = held();
				assertThat(held).as("still held for the client").isGreaterThan(least);
				assertThat(used()).as("budget used: what is held past the allowance, %d held", held)
					.isEqualTo(held - NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
			});
			assertThat(client.closed()).isFalse();
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(credit()).isZero();
			assertThat(used()).isZero();
		});
	}

	/** A client going away with its leg's window grown releases it. */
	@Test
	void aClientGoingAwayReleasesItsLegsWindow() throws Exception {
		H2TestClient client = boostedDownload();
		this.closeables.add(client);
		client.abort();
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(credit()).as("no credit left").isZero();
			assertThat(used()).as("the budget released").isZero();
			assertThat(limits().reserved()).as("the connection released").isZero();
		});
	}

	/**
	 * A client downloading {@link #DELAYED_BODY} through the delayed tunnel, the stream
	 * ended and nothing held: its leg's window grew past the floor, charged.
	 */
	private H2TestClient boostedDownload() throws Exception {
		H2TestClient client = H2TestClient.connect(dataPort);
		try {
			client.frame(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024));
			client.headers(1, true, head(DELAYED_HOST, "GET", "/"));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the download ended").isTrue();
			assertThat(client.dataReceived(1)).isEqualTo(DELAYED_BODY);
			awaitChargedCreditOnly("after the download");
			assertThat(credit()).as("the leg's window grown past its floor").isGreaterThan(2L * FLOOR);
			return client;
		}
		catch (Exception | AssertionError e) {
			close(client);
			throw e;
		}
	}

	/**
	 * Waits until the relay holds nothing, then checks that the budget charges the credit
	 * past the client's and the leg's floors, no more.
	 */
	private void awaitChargedCreditOnly(String when) {
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(held()).as("held %s", when).isZero();
			long credit = credit();
			assertThat(used()).as("budget used %s: the credit %d past the floors", when, credit)
				.isEqualTo(credit - 2L * FLOOR);
		});
	}

	/** A client asking for {@link #STREAMS} downloads, granting no connection window. */
	private H2TestClient download() throws IOException {
		H2TestClient client = H2TestClient.connect(dataPort);
		client.withholdConnectionWindow();
		client.frame(TestH2.SETTINGS, 0, 0,
				WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, BODY));
		for (int i = 0; i < STREAMS; i++) {
			client.headers(streamId(i), true, head(DOWNLOAD_HOST, "GET", "/" + i));
		}
		return client;
	}

	/** A client opening {@link #STREAMS} uploads; the pump sends their bodies. */
	private Pump upload() throws IOException {
		H2TestClient client = H2TestClient.connect(dataPort);
		for (int i = 0; i < STREAMS; i++) {
			client.headers(streamId(i), false, head(UPLOAD_HOST, "POST", "/" + i));
		}
		return new Pump(client);
	}

	private static int streamId(int index) {
		return 2 * index + 1;
	}

	private static byte[] head(String host, String method, String path) {
		return TestH2.headerBlock(":authority=" + host, ":method=" + method, ":path=" + path);
	}

	/** Waits until the value has not grown for a second. */
	private static void awaitStable(AtomicLong value) throws InterruptedException {
		long max = 0;
		long grownAt = System.nanoTime();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (System.nanoTime() - grownAt < TimeUnit.SECONDS.toNanos(1)) {
			assertThat(System.nanoTime()).as("the relays settle").isLessThan(deadline);
			long current = value.get();
			if (current > max) {
				max = current;
				grownAt = System.nanoTime();
			}
			Thread.sleep(10);
		}
	}

	private ConnectionLimits limits() {
		return Objects.requireNonNull(this.limits);
	}

	private long held() {
		return gauge(H2DemuxMeters.HELD_GAUGE_NAME);
	}

	private long credit() {
		return gauge(H2DemuxMeters.CREDIT_GAUGE_NAME);
	}

	private long used() {
		return limits().h2Budget().used();
	}

	private long gauge(String name) {
		return Math.round(Objects.requireNonNull(this.meterRegistry).get(name).gauge().value());
	}

	/** One upload connection and the thread sending its bodies within the windows. */
	private static final class Pump implements AutoCloseable {

		private final H2TestClient client;

		private final long[] remaining = new long[STREAMS];

		private final AtomicBoolean stop = new AtomicBoolean();

		Pump(H2TestClient client) {
			this.client = client;
			Arrays.fill(this.remaining, BODY);
		}

		void start() {
			Thread.ofVirtual().name("e2e-credit-pump").start(this::pump);
		}

		private void pump() {
			try {
				while (!this.stop.get() && !this.client.closed()) {
					boolean progress = false;
					for (int i = 0; i < STREAMS; i++) {
						int id = streamId(i);
						long window = Math.min(this.client.sendable(id), this.client.connectionWindow());
						int n = (int) Math.min(Math.min(this.remaining[i], window), MAX_FRAME_SIZE);
						if (n > 0) {
							this.remaining[i] -= n;
							this.client.sendData(id, new byte[n], this.remaining[i] == 0 ? TestH2.FLAG_END_STREAM : 0);
							progress = true;
						}
					}
					if (!progress) {
						Thread.sleep(1);
					}
				}
			}
			catch (IOException | InterruptedException e) {
				// torn down
			}
		}

		@Override
		public void close() {
			this.stop.set(true);
			H2DemuxWindowCreditE2ETest.close(this.client);
		}

	}

	/** Answers every request with a head and a stream window of DATA, within windows. */
	private static void serveResponses(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			respond(upstream, streamId -> BODY);
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/**
	 * The delayed upstream: {@link #DELAYED_BODY} on stream 1, far more on
	 * {@link #ENDLESS_STREAM}, until reset.
	 */
	private void serveDelayed(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			this.delayedUpstreams.add(upstream);
			respond(upstream, streamId -> switch (streamId) {
				case ENDLESS_STREAM -> 64 * DELAYED_BODY;
				case HELD_STREAM -> HELD_BODY;
				default -> DELAYED_BODY;
			});
		}
		catch (IOException e) {
			// connection ended
		}
	}

	private static void respond(WindowedH2Upstream upstream, java.util.function.IntUnaryOperator body)
			throws IOException {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				int streamId = frame.streamId();
				Thread.ofVirtual().name("e2e-credit-respond").start(() -> {
					try {
						upstream.headers(streamId, false);
						upstream.sendData(streamId, new byte[body.applyAsInt(streamId)], true);
					}
					catch (IOException | InterruptedException e) {
						// the relay went away
					}
				});
			}
		}
	}

	/** Holds every request body: a stream window of 0, read and never granted. */
	private static void holdRequests(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			upstream.settings(0);
			while (upstream.next() != null) {
				// read and dropped
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	private void startClient(String host, ServerSocket upstream, int serverPort) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + serverPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-credit").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-credit-accept").start(() -> {
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
