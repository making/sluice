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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.ScriptedH2Upstream;
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
 * A client going away with streams unfinished -- its socket closed with a FIN or a reset
 * while it sends request bodies, or reset while a response waits for its window -- is no
 * protocol error: no stream counts as reset on a stream error of the client or of an
 * upstream, no leg counts as failed, and each upstream gets the streams it still has
 * cancelled ({@code CANCEL}) and, if a GOAWAY at all, one of {@code NO_ERROR}. The
 * upstreams announce a small stream window, so netty holds DATA of every stream on the
 * leg (uploads) or on the client endpoint (the download) as the client goes. In the
 * relay's package for the meters.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxClientGoneE2ETest {

	private static final Duration WITHIN = Duration.ofSeconds(5);

	private static final int NO_ERROR = 0x0;

	private static final int CANCEL = 0x8;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	/** The stream window the upstreams and the downloading client announce. */
	private static final int SMALL_WINDOW = 1024;

	/**
	 * The DATA frames of {@link #FRAME_BYTES} each stream sends before the client goes.
	 */
	private static final int FRAMES_BEFORE_GONE = 4;

	private static final int FRAME_BYTES = 1024;

	/** The one-way delay of the delayed upstream link (a 20ms round trip). */
	private static final Duration ONE_WAY_DELAY = Duration.ofMillis(10);

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
		this.closeables.forEach(H2DemuxClientGoneE2ETest::close);
	}

	/**
	 * The client closes its socket (a FIN) while it sends the bodies of two requests to
	 * one upstream.
	 */
	@Test
	void aClientClosingMidRequestIsNoProtocolError() throws Exception {
		assertUploadGoneUncounted("cg.close.test", 2, false, Duration.ZERO);
	}

	/** The client resets its socket while it sends the bodies of two requests. */
	@Test
	void aClientResettingMidRequestIsNoProtocolError() throws Exception {
		assertUploadGoneUncounted("cg.abort.test", 2, true, Duration.ZERO);
	}

	/** Ten requests in flight on one connection: one leg, ten streams cancelled. */
	@Test
	void aClientClosingWithTenRequestsInFlightIsNoProtocolError() throws Exception {
		assertUploadGoneUncounted("cg.ten.test", 10, false, Duration.ZERO);
	}

	/** The upstream behind a delayed link. */
	@Test
	void aClientClosingMidRequestOverADelayedLinkIsNoProtocolError() throws Exception {
		assertUploadGoneUncounted("cg.delayed.test", 2, false, ONE_WAY_DELAY);
	}

	/**
	 * The client resets its socket while netty holds the rest of a response for the
	 * client's stream window: the client endpoint closes with that DATA unwritten.
	 */
	@Test
	void aClientResettingMidResponseIsNoProtocolError() throws Exception {
		String route = "cg.download.test";
		int responseBytes = 4 * SMALL_WINDOW;
		ScriptedH2Upstream upstream = upstream((connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.head(frame.streamId(), false);
				connection.write(TestH2.DATA, 0, frame.streamId(), new byte[responseBytes]);
			}
		});
		startClient(route, upstream.port());
		H2DemuxMeters meters = meters();
		Counts before = counts();
		H2TestClient client = H2TestClient.connect(dataPort, 1);
		try {
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, SMALL_WINDOW));
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitTrue(() -> client.dataReceived(1) == SMALL_WINDOW, WITHIN))
				.as("the client's stream window of the response")
				.isTrue();
			Awaitility.await()
				.atMost(WITHIN)
				.alias("netty holds the rest for the client")
				.until(() -> meters.writesHeld(route) == 1);
			client.abort();
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertUpstreamCancelled(connection, new int[] { 1 });
			assertGoneUncounted(route, connection, before);
		}
		finally {
			client.close();
		}
	}

	/**
	 * A stream waiting for its upstream's concurrency limit of 2 as the client goes
	 * leaves the leg's buffer unseen by the upstream: cancelling the two open streams
	 * does not open it.
	 */
	@Test
	void aStreamWaitingAsTheClientGoesNeverReachesItsUpstream() throws Exception {
		String route = "cg.waiting.test";
		ScriptedH2Upstream upstream = upstream(WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, 2),
				(connection, frame) -> {
				});
		startClient(route, upstream.port());
		Counts before = counts();
		H2TestClient client = H2TestClient.connect(dataPort);
		try {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			awaitWindowApplied(connection);
			client.headers(3, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.headers(5, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			byte[] ping = Arrays.copyOf(TestH2.int32(5), 8);
			client.frame(TestH2.PING, 0, 0, ping);
			assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
					WITHIN))
				.as("the relay handled stream 5: it waits for the upstream's limit")
				.isTrue();
			assertThat(connection.await(f -> f.type() == TestH2.HEADERS && f.streamId() == 3, WITHIN))
				.as("stream 3 reached the upstream")
				.isNotNull();
			assertThat(connection.frames()).as("nothing of stream 5 reached the upstream yet")
				.noneMatch(f -> f.streamId() == 5);

			client.close();

			assertUpstreamCancelled(connection, new int[] { 1, 3 });
			assertThat(connection.frames()).as("nothing of stream 5 reached the upstream")
				.noneMatch(f -> f.streamId() == 5);
			assertGoneUncounted(route, connection, before);
		}
		finally {
			client.close();
		}
	}

	/**
	 * Opens {@code streams} POST streams to the route's upstream, which reads them and
	 * answers nothing, sends DATA on each until netty holds DATA of every stream for the
	 * upstream's window, then goes away -- a FIN, or a reset if {@code abort}.
	 */
	private void assertUploadGoneUncounted(String route, int streams, boolean abort, Duration delay) throws Exception {
		ScriptedH2Upstream upstream = upstream((connection, frame) -> {
		});
		startClient(route, delay.isZero() ? upstream.port() : delayed(upstream.port(), delay));
		H2DemuxMeters meters = meters();
		Counts before = counts();
		int[] streamIds = new int[streams];
		for (int i = 0; i < streams; i++) {
			streamIds[i] = 2 * i + 1;
		}
		H2TestClient client = H2TestClient.connect(dataPort);
		try {
			for (int streamId : streamIds) {
				client.headers(streamId, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			}
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			awaitWindowApplied(connection);
			for (int frame = 0; frame < FRAMES_BEFORE_GONE; frame++) {
				for (int streamId : streamIds) {
					client.sendData(streamId, new byte[FRAME_BYTES], 0);
				}
			}
			for (int streamId : streamIds) {
				assertThat(connection.await(f -> f.type() == TestH2.DATA && f.streamId() == streamId, WITHIN))
					.as("the upstream got DATA of stream %d", streamId)
					.isNotNull();
			}
			Awaitility.await()
				.atMost(WITHIN)
				.alias("netty holds DATA of every stream for the upstream")
				.until(() -> meters.writesHeld(route) == streams);
			if (abort) {
				client.abort();
			}
			else {
				client.close();
			}
			assertUpstreamCancelled(connection, streamIds);
			assertGoneUncounted(route, connection, before);
		}
		finally {
			client.close();
		}
	}

	/**
	 * Waits until the leg acknowledged the upstream's SETTINGS: DATA the relay sends from
	 * then on is held to the small window.
	 */
	private static void awaitWindowApplied(ScriptedH2Upstream.Connection connection) throws InterruptedException {
		assertThat(connection.await(f -> f.type() == TestH2.SETTINGS && (f.flags() & TestH2.FLAG_ACK) != 0, WITHIN))
			.as("the leg acknowledged the upstream's SETTINGS")
			.isNotNull();
	}

	/**
	 * Each of the upstream's streams was reset with {@code CANCEL}, then the leg closed.
	 */
	private static void assertUpstreamCancelled(ScriptedH2Upstream.Connection connection, int[] streamIds)
			throws InterruptedException {
		assertThat(connection.awaitEof(WITHIN)).as("the leg closed").isTrue();
		for (int streamId : streamIds) {
			assertThat(connection.frames()).filteredOn(f -> f.type() == TestH2.RST_STREAM && f.streamId() == streamId)
				.as("the resets of the upstream's stream %d", streamId)
				.singleElement()
				.satisfies(reset -> assertThat(TestH2.errorCode(reset)).isEqualTo(CANCEL));
		}
	}

	/**
	 * Once the relay let go of the route's streams and leg: the GOAWAYs the upstream got
	 * are {@code NO_ERROR}, and nothing counts as a stream error or a failed leg.
	 */
	private void assertGoneUncounted(String route, ScriptedH2Upstream.Connection connection, Counts before) {
		H2DemuxMeters meters = meters();
		Awaitility.await().atMost(WITHIN).until(() -> meters.legs(route) == 0 && meters.trackedStreams(route) == 0);
		assertThat(connection.frames()).filteredOn(f -> f.type() == TestH2.GOAWAY)
			.as("the GOAWAYs the upstream got")
			.allSatisfy(goAway -> assertThat(TestH2.errorCode(goAway)).isEqualTo(NO_ERROR));
		assertThat(counts().minus(before)).as("protocol resets, upstream resets, failed legs")
			.isEqualTo(new Counts(0, 0, 0));
	}

	private Counts counts() {
		return new Counts(counter("sluice.h2demux.streams.reset", "protocol"),
				counter("sluice.h2demux.streams.reset", "upstream"), counter("sluice.h2demux.legs.failed", null));
	}

	private H2DemuxMeters meters() {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters();
	}

	private double counter(String name, @Nullable String reason) {
		MeterRegistry registry = Objects.requireNonNull(this.meterRegistry);
		return reason == null ? registry.get(name).counter().count()
				: registry.get(name).tag("reason", reason).counter().count();
	}

	/** An upstream announcing {@link #SMALL_WINDOW} as its stream window. */
	private ScriptedH2Upstream upstream(BiConsumer<ScriptedH2Upstream.Connection, TestH2.Frame> handler)
			throws IOException {
		return upstream(WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, SMALL_WINDOW), handler);
	}

	private ScriptedH2Upstream upstream(byte[] settings,
			BiConsumer<ScriptedH2Upstream.Connection, TestH2.Frame> handler) throws IOException {
		ScriptedH2Upstream upstream = ScriptedH2Upstream.start(settings, handler);
		this.closeables.add(upstream);
		return upstream;
	}

	/** A TCP proxy to the port delaying every chunk by {@code delay} each way. */
	private int delayed(int port, Duration delay) throws IOException {
		DelayingProxy proxy = new DelayingProxy(port, delay);
		this.closeables.add(proxy);
		return proxy.port();
	}

	private void startClient(String host, int port) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + port).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-cg").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/** The protocol resets, the upstream resets and the failed legs counted. */
	private record Counts(double protocolResets, double upstreamResets, double legsFailed) {

		Counts minus(Counts other) {
			return new Counts(this.protocolResets - other.protocolResets, this.upstreamResets - other.upstreamResets,
					this.legsFailed - other.legsFailed);
		}

	}

	/**
	 * A TCP proxy delaying every chunk read by a fixed time in both directions, an EOF
	 * passed on as a half-close after the same delay.
	 */
	private static final class DelayingProxy implements AutoCloseable {

		private final ServerSocket server;

		private final int target;

		private final long delayNanos;

		private final List<Socket> sockets = new CopyOnWriteArrayList<>();

		DelayingProxy(int target, Duration delay) throws IOException {
			this.target = target;
			this.delayNanos = delay.toNanos();
			this.server = new ServerSocket();
			this.server.bind(new InetSocketAddress("127.0.0.1", 0));
			Thread.ofVirtual().name("e2e-delay-accept").start(this::accept);
		}

		int port() {
			return this.server.getLocalPort();
		}

		private void accept() {
			while (!this.server.isClosed()) {
				try {
					Socket from = this.server.accept();
					Socket to = new Socket("127.0.0.1", this.target);
					this.sockets.add(from);
					this.sockets.add(to);
					pipe(from, to);
					pipe(to, from);
				}
				catch (IOException e) {
					return;
				}
			}
		}

		private void pipe(Socket from, Socket to) {
			LinkedBlockingQueue<Chunk> queue = new LinkedBlockingQueue<>();
			Thread.ofVirtual().name("e2e-delay-read").start(() -> {
				byte[] buffer = new byte[16 * 1024];
				try {
					InputStream in = from.getInputStream();
					int n;
					while ((n = in.read(buffer)) >= 0) {
						queue.add(new Chunk(System.nanoTime() + this.delayNanos, Arrays.copyOf(buffer, n)));
					}
				}
				catch (IOException e) {
					// closed: ends as on EOF
				}
				queue.add(new Chunk(System.nanoTime() + this.delayNanos, null));
			});
			Thread.ofVirtual().name("e2e-delay-write").start(() -> {
				try {
					OutputStream out = to.getOutputStream();
					while (true) {
						Chunk chunk = queue.take();
						long wait;
						while ((wait = chunk.due() - System.nanoTime()) > 0) {
							LockSupport.parkNanos(wait);
						}
						byte[] bytes = chunk.bytes();
						if (bytes == null) {
							to.shutdownOutput();
							return;
						}
						out.write(bytes);
						out.flush();
					}
				}
				catch (IOException e) {
					// closed
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
		}

		@Override
		public void close() throws IOException {
			this.server.close();
			for (Socket socket : this.sockets) {
				socket.close();
			}
		}

		/** Bytes due at a time; null bytes = the EOF. */
		private record Chunk(long due, byte @Nullable [] bytes) {
		}

	}

}
