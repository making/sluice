package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
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
import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

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
 * The h2 demux towards its upstreams: a long-lived connection forgets its finished
 * streams, and the queue is metered once per route. In the relay's package for its
 * meters.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxLegWriterE2ETest {

	private static final byte[] PING_PAYLOAD = "pingpong".getBytes(StandardCharsets.US_ASCII);

	private static final int BULK_STREAMS = 20;

	/** The PINGs a flooding upstream writes at most: 17MiB, past every buffer between. */
	private static final long FLOOD_PINGS = 1024 * 1024;

	private static final int PINGS_PER_WRITE = 64;

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

	/**
	 * An upstream PING sent while the relay holds more than 1MiB of request data for the
	 * leg's streams is acknowledged ahead of that data: netty acks it as it reads it,
	 * behind no more than what the leg hop's writer queue holds then (at most
	 * {@link NettyHttp2DemuxRelay#HOP_QUEUE_BYTES}).
	 */
	@Test
	void upstreamPingIsAckedAheadOfQueuedData() throws Exception {
		String route = "lw.ping.test";
		CountDownLatch ping = new CountDownLatch(1);
		CountDownLatch read = new CountDownLatch(1);
		AtomicLong beforeAck = new AtomicLong(-1);
		startClient(route, stub(socket -> stallPingThenRead(socket, ping, read, beforeAck)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			fillStalledLeg(client, route);
			long queued = queuedBytes(route);
			long relayed = relayedBytes(route);
			ping.countDown();
			// the PING reaches the relay while the leg is still stalled
			Thread.sleep(500);
			assertThat(queuedBytes(route)).as("still stalled: netty's ack (a 17-byte frame) queued, nothing written")
				.isEqualTo(queued + 17);
			read.countDown();
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> beforeAck.get() >= 0);
			assertThat(queued).as("queued when the PING was sent").isGreaterThan(1024 * 1024);
			assertThat(beforeAck.get() - relayed)
				.as("request bytes the upstream read before the ack, beyond those written at the ping")
				.isLessThan(512 * 1024);
		}
	}

	/**
	 * An upstream flooding PINGs without reading: netty acks each as it reads it, the
	 * acks pile up in the leg's writer queue towards the upstream that does not read, and
	 * past the hop's read pause the leg's reader stops reading; the flood backs up into
	 * the upstream, and the relay holds no more than the bound of one hop's writer queue
	 * for the leg. The leg is not ended (no answer on its stream), and the client's
	 * connection goes on: a stream of another route completes.
	 */
	@Test
	void upstreamPingFloodPausesItsLegsReader() throws Exception {
		String route = "lw.flood.test";
		String otherRoute = "lw.flood-other.test";
		AtomicLong written = new AtomicLong();
		startClient(route, stub(socket -> readHeadThenFlood(socket, written)));
		startClient(otherRoute, stub(H2DemuxLegWriterE2ETest::answerEndedRequests));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			awaitSteady(written);
			assertThat(written.get()).as("PINGs written before the flood backed up").isLessThan(FLOOD_PINGS);
			assertThat(queuedBytes(route)).as("the leg's writer queue")
				.isGreaterThan(Hop.DEFAULT_READ_PAUSE_BYTES)
				.isLessThanOrEqualTo(NettyHttp2DemuxRelay.HOP_QUEUE_BYTES);
			client.headers(3, true, TestH2.headerBlock(":authority=" + otherRoute, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("another route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok");
			assertThat(client.frames(1)).as("the flooding upstream's stream").isEmpty();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * A long-lived connection does not accumulate the state of its finished streams.
	 */
	@Test
	void finishedStreamsAreForgotten() throws Exception {
		String route = "lw.forget.test";
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
		String route = "lw.gauge.test";
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
	 * Opens {@code #BULK_STREAMS} streams on the route, whose upstream grants each all
	 * the window there is and stops reading, and fills them until the relay path
	 * saturates.
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
	 * Grants the connection and each of the first {@code #BULK_STREAMS} streams all the
	 * window there is as their HEADERS arrive, then stops reading: what the relay holds
	 * for the leg waits for the tunnel, not for a stream window, however much the tunnel
	 * and the sockets absorb before the relay withholds its client's window at its share.
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
				TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(),
						TestH2.int32(Integer.MAX_VALUE - 65535));
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
	 * Sends its SETTINGS, reads until a request head arrives, then stops reading and
	 * writes up to {@link #FLOOD_PINGS} PINGs, counting those written, until the relay
	 * goes away.
	 */
	private static void readHeadThenFlood(Socket socket, AtomicLong written) {
		try {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			do {
				frame = TestH2.readFrame(in);
			}
			while (frame != null && frame.type() != TestH2.HEADERS);
			ByteArrayOutputStream batch = new ByteArrayOutputStream();
			for (int i = 0; i < PINGS_PER_WRITE; i++) {
				TestH2.writeFrame(batch, TestH2.PING, 0, 0, PING_PAYLOAD);
			}
			byte[] pings = batch.toByteArray();
			for (long i = 0; i < FLOOD_PINGS; i += PINGS_PER_WRITE) {
				out.write(pings);
				written.addAndGet(PINGS_PER_WRITE);
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/** Waits until the count is above 0 and unchanged for two seconds. */
	private static void awaitSteady(AtomicLong count) {
		AtomicLong last = new AtomicLong(-1);
		AtomicLong stableSince = new AtomicLong(System.nanoTime());
		Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).until(() -> {
			long now = count.get();
			if (now != last.get()) {
				last.set(now);
				stableSince.set(System.nanoTime());
			}
			return now > 0 && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stableSince.get()) > 2000;
		});
	}

	/**
	 * An h2 stub sending its SETTINGS first (the server preface netty's leg requires) and
	 * answering every request once it ended (END_STREAM).
	 */
	private static void answerEndedRequests(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
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

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

}
