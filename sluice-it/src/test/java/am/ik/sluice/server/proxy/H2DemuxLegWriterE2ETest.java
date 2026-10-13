package am.ik.sluice.server.proxy;

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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.Parameter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux leg writer: it sleeps while its leg is idle, writes the relay's own
 * connection-level frames ahead of queued request data, and its queue is metered once per
 * route. In the relay's package for the package-private wakeup counter.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
class H2DemuxLegWriterE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final byte[] PING_PAYLOAD = "pingpong".getBytes(StandardCharsets.US_ASCII);

	private static final int BULK_STREAMS = 20;

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

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxLegWriterE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/** An idle leg's writer is not woken while nothing is queued for it. */
	@Test
	@LegacyOnly("the legacy relay's leg writer thread and its wakeup counter: a netty hop's writer waits on its "
			+ "queue")
	void idleLegWriterSleeps() throws Exception {
		String route = route("lw.idle");
		startClient(route, stub(H2DemuxLegWriterE2ETest::answerEndedRequests));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			Thread.sleep(200);
			long before = writerWakeups(route);
			Thread.sleep(1000);
			long after = writerWakeups(route);
			assertThat(before).as("the counter counts: the request woke the writer").isGreaterThanOrEqualTo(1);
			assertThat(after - before).as("wakeups of the idle leg's writer in 1s").isLessThanOrEqualTo(2);
			assertThat(client.closed()).as("the leg is alive").isFalse();
		}
	}

	/**
	 * An upstream PING sent while the leg queue holds more than 1MiB of request data is
	 * acknowledged before that data is written.
	 */
	@Test
	@LegacyOnly("PING ordering and the tunnel byte counter: netty acks a PING as it reads it, behind what its "
			+ "writer queue holds; a later migration step")
	void upstreamPingIsAckedAheadOfQueuedData() throws Exception {
		String route = route("lw.ping");
		CountDownLatch ping = new CountDownLatch(1);
		CountDownLatch read = new CountDownLatch(1);
		AtomicLong beforeAck = new AtomicLong(-1);
		startClient(route, stub(socket -> stallPingThenRead(socket, ping, read, beforeAck)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			fillStalledLeg(client, route);
			long queued = queuedBytes(route);
			long relayed = relayedBytes(route);
			ping.countDown();
			Thread.sleep(500); // the PING reaches the relay while the leg is still
								// stalled
			assertThat(queuedBytes(route)).as("still stalled").isEqualTo(queued);
			read.countDown();
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> beforeAck.get() >= 0);
			assertThat(queued).as("queued when the PING was sent").isGreaterThan(1024 * 1024);
			assertThat(beforeAck.get() - relayed)
				.as("request bytes the upstream read before the ack, beyond those written at the ping")
				.isLessThan(512 * 1024);
		}
	}

	/**
	 * An upstream flooding PINGs while its leg is stalled ends its leg (a connection
	 * error {@code ENHANCE_YOUR_CALM}; the stalled upstream cannot read the GOAWAY): the
	 * client's streams on it are answered with a {@code 502}, and its connection goes on:
	 * a stream of another route completes.
	 */
	@Test
	@LegacyOnly("netty pauses reading an upstream that floods PINGs without reading (the hop's read pause) instead "
			+ "of ending its leg, and the tunnel byte counter is not yet on the netty engine; a per-hop rewrite")
	void upstreamPingFloodEndsItsLeg() throws Exception {
		String route = route("lw.flood");
		String otherRoute = route("lw.flood-other");
		CountDownLatch ping = new CountDownLatch(1);
		startClient(route, stub(socket -> stallThenFlood(socket, ping)));
		startClient(otherRoute, stub(H2DemuxLegWriterE2ETest::answerEndedRequests));
		double failed = Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.legs.failed").counter().count();
		Integer[] streams = new Integer[BULK_STREAMS];
		for (int i = 0; i < BULK_STREAMS; i++) {
			streams[i] = 1 + 2 * i;
		}
		int other = 1 + 2 * BULK_STREAMS;
		try (H2TestClient client = H2TestClient.connect(dataPort, streams)) {
			fillStalledLeg(client, route);
			ping.countDown();
			for (Integer streamId : streams) {
				assertThat(client.awaitEnd(streamId, Duration.ofSeconds(10))).as("stream %d answered", streamId)
					.isTrue();
				assertThat(TestH2.status(client.frames(streamId))).isEqualTo("502");
			}
			assertThat(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.legs.failed").counter().count()
					- failed)
				.as("the leg ended on the flood")
				.isEqualTo(1);
			client.headers(other, true, TestH2.headerBlock(":authority=" + otherRoute, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(other, Duration.ofSeconds(5))).as("another route's stream completes").isTrue();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Client stream WINDOW_UPDATEs queued behind a stalled leg coalesce into one frame
	 * carrying their sum.
	 */
	@Test
	@LegacyOnly("per-hop windows: the netty engine relays no client WINDOW_UPDATE to an upstream")
	void clientWindowUpdatesCoalesceWhileQueued() throws Exception {
		String route = route("lw.window");
		CountDownLatch read = new CountDownLatch(1);
		AtomicLong frames = new AtomicLong();
		AtomicLong sum = new AtomicLong();
		startClient(route, stub(socket -> stallThenCountWindowUpdates(socket, read, frames, sum)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			fillStalledLeg(client, route);
			for (int i = 0; i < 500; i++) {
				client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(1));
			}
			Thread.sleep(500); // every update reached the relay while the leg is stalled
			read.countDown();
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> sum.get() >= 500);
			Thread.sleep(200);
			assertThat(sum.get()).isEqualTo(500);
			assertThat(frames.get()).as("WINDOW_UPDATE frames carrying the 500 increments").isLessThanOrEqualTo(2);
		}
	}

	/**
	 * A long-lived connection does not accumulate the state of its finished streams.
	 */
	@Test
	void finishedStreamsAreForgotten() throws Exception {
		String route = route("lw.forget");
		startClient(route, stub(H2DemuxLegWriterE2ETest::answerEndedRequests));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			for (int i = 0; i < 300; i++) {
				int streamId = 1 + 2 * i;
				client.headers(streamId, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
				assertThat(client.awaitEnd(streamId, Duration.ofSeconds(5))).isTrue();
			}
			assertThat(proxy.h2DemuxMeters().trackedStreams(route)).as("streams tracked while the relay serves")
				.isLessThan(300);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> proxy.h2DemuxMeters().trackedStreams(route) == 0);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * The queue gauge is one per route, whatever the number of connections: no
	 * per-connection tag.
	 */
	@Test
	void queueGaugeIsOnePerRoute() throws Exception {
		String route = route("lw.gauge");
		startClient(route, stub(H2DemuxLegWriterE2ETest::answerEndedRequests));
		try (H2TestClient first = H2TestClient.connect(dataPort);
				H2TestClient second = H2TestClient.connect(dataPort)) {
			for (H2TestClient client : List.of(first, second)) {
				client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
				assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			}
			List<Gauge> gauges = new ArrayList<>(Objects.requireNonNull(this.meterRegistry)
				.find("sluice.h2demux.queued.bytes")
				.tags("route", route)
				.gauges());
			assertThat(gauges).hasSize(1);
			assertThat(gauges.get(0).getId().getTag("connection")).as("no per-connection tag").isNull();
			assertThat(first.closed() || second.closed()).as("both legs alive").isFalse();
		}
	}

	/**
	 * Opens {@code #BULK_STREAMS} streams on the route, whose upstream grants each 1MiB
	 * and stops reading, and fills them until the relay path saturates.
	 */
	private void fillStalledLeg(H2TestClient client, String route) throws Exception {
		for (int i = 0; i < BULK_STREAMS; i++) {
			client.headers(1 + 2 * i, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
		}
		Thread.sleep(500); // the grants precede the data
		for (int i = 0; i < BULK_STREAMS; i++) {
			client.fill(1 + 2 * i, Duration.ofMillis(100));
		}
		awaitOutboundPlateau(route);
	}

	/**
	 * Grants the connection and each of the first {@code #BULK_STREAMS} streams 1MiB as
	 * their HEADERS arrive, then stops reading.
	 */
	private static void grantThenStall(InputStream in, OutputStream out) throws IOException {
		if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
			throw new IOException("no preface");
		}
		TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
		TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
		int seen = 0;
		TestH2.@Nullable Frame frame;
		while (seen < BULK_STREAMS && (frame = TestH2.readFrame(in)) != null) {
			if (frame.type() == TestH2.HEADERS) {
				TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(), TestH2.int32(1024 * 1024));
				seen++;
			}
		}
	}

	/**
	 * Stalls the leg, sends a PING on {@code ping}, resumes reading on {@code read}, and
	 * records the bytes of the relayed request frames (HEADERS, CONTINUATION, DATA,
	 * stream WINDOW_UPDATE) it read before the ack.
	 */
	private static void stallPingThenRead(Socket socket, CountDownLatch ping, CountDownLatch read,
			AtomicLong beforeAck) {
		try {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			long relayed = 0;
			grantThenStall(in, out);
			ping.await();
			TestH2.writeFrame(out, TestH2.PING, 0, 0, PING_PAYLOAD);
			read.await();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0
						&& Arrays.equals(frame.payload(), PING_PAYLOAD)) {
					beforeAck.set(relayed);
					break;
				}
				boolean request = frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION
						|| frame.type() == TestH2.DATA
						|| (frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() != 0);
				if (request) {
					relayed += 9 + frame.payload().length;
				}
			}
		}
		catch (IOException | InterruptedException e) {
			// ended
		}
	}

	/**
	 * Stalls the leg, resumes reading on {@code read}, and counts the WINDOW_UPDATE
	 * frames of stream 1 and their increments.
	 */
	private static void stallThenCountWindowUpdates(Socket socket, CountDownLatch read, AtomicLong frames,
			AtomicLong sum) {
		try {
			InputStream in = socket.getInputStream();
			grantThenStall(in, socket.getOutputStream());
			read.await();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() == 1) {
					frames.incrementAndGet();
					sum.addAndGet(TestH2.increment(frame));
				}
			}
		}
		catch (IOException | InterruptedException e) {
			// ended
		}
	}

	/** Stalls the leg, then floods PINGs without reading. */
	private static void stallThenFlood(Socket socket, CountDownLatch ping) {
		try {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			grantThenStall(in, out);
			ping.await();
			for (int i = 0; i < 5000; i++) {
				TestH2.writeFrame(out, TestH2.PING, 0, 0, PING_PAYLOAD);
			}
		}
		catch (IOException | InterruptedException e) {
			// ended
		}
	}

	/** An h2 stub answering every request once it ended (END_STREAM). */
	private static void answerEndedRequests(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.@Nullable Frame frame;
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
	}

	private long writerWakeups(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().writerWakeups(route);
	}

	/**
	 * Waits until the route's relayed bytes plateau: above the per-connection bounds and
	 * unchanged for two seconds -- the whole relay path saturated.
	 */
	private void awaitOutboundPlateau(String route) {
		AtomicReference<Double> last = new AtomicReference<>(Double.valueOf(-1));
		AtomicLong stableSince = new AtomicLong(System.nanoTime());
		Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
			double count = relayedBytes(route);
			if (Double.compare(count, last.get().doubleValue()) != 0) {
				last.set(Double.valueOf(count));
				stableSince.set(System.nanoTime());
			}
			return count > 256L * 1024 && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stableSince.get()) > 2000;
		});
	}

	/** The route's relayed request bytes ({@code sluice.tunnel.bytes} outbound). */
	private long relayedBytes(String route) {
		Counter counter = Objects.requireNonNull(this.meterRegistry)
			.find("sluice.tunnel.bytes")
			.tags("direction", "inbound", "route", route)
			.counter();
		return counter == null ? 0 : Math.round(counter.count());
	}

	/** The route's leg queue gauge; zero before it exists. */
	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find("sluice.h2demux.queued.bytes")
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.max()
			.orElse(0));
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-lw").start(task))
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
			Thread.ofVirtual().name("e2e-lw-accept").start(() -> {
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

	/**
	 * The route's host for this engine's invocation: both run on one router, where the
	 * other invocation's tunnel client may still serve a host of the same name.
	 */
	private String route(String name) {
		return name + "." + String.valueOf(this.engine).toLowerCase(Locale.ROOT) + ".test";
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
