package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

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
 * The h2 demux honors the upstream's SETTINGS_MAX_CONCURRENT_STREAMS (RFC 9113 5.1.2): a
 * stream beyond it waits in the relay, over the same leg, until a stream of the leg
 * closed on the upstream; until the upstream's first SETTINGS one stream is opened. A
 * waiting stream reset by either side never reaches the upstream, is granted no stream
 * window beyond the initial one, and is refused ({@code REFUSED_STREAM}) when its
 * upstream goes away. In the relay's package for the tracked-streams meter.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
@LegacyOnly("upstream concurrency waiting: step 4 (101)")
class H2DemuxUpstreamStreamLimitE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int CANCEL = 0x8;

	/** How long a stream that must wait is watched not reaching the upstream. */
	private static final Duration HELD = Duration.ofMillis(300);

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

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
		this.closeables.forEach(H2DemuxUpstreamStreamLimitE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * An upstream announcing a limit of 1: of two concurrent streams the second waits
	 * until the first ended, then goes over the same leg; the upstream never has two
	 * streams open.
	 */
	@Test
	void streamBeyondTheUpstreamLimitWaitsForAStreamToClose() throws Exception {
		String route = "sl.wait.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("the second stream waits in the relay").isFalse();
			upstream.release(1);
			awaitOpen(upstream, 3);
			upstream.release(3);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok3");
			assertThat(upstream.peak()).as("streams open at once on the upstream").isEqualTo(1);
			assertThat(connections).as("one leg").hasSize(1);
		}
	}

	/**
	 * The limit lowered from 2 to 1 by a later SETTINGS: the two streams open go on, and
	 * a stream opened after it waits until no stream is open.
	 */
	@Test
	void limitLoweredByALaterSettingsAppliesToStreamsOpenedAfterIt() throws Exception {
		String route = "sl.lowered.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(2L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			awaitOpen(upstream, 3);
			assertThat(upstream.peak()).as("the first limit admitted two streams").isEqualTo(2);
			upstream.settings(1L);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 2);
			upstream.release(1);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			client.headers(5, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(5)).as("stream 3 still takes the only stream of the lowered limit").isFalse();
			upstream.release(3);
			awaitOpen(upstream, 5);
			upstream.release(5);
			assertThat(client.awaitTrue(() -> client.ended(3) && client.ended(5), TIMEOUT)).isTrue();
			assertThat(upstream.openWhenSeen(5)).as("streams open on the upstream when stream 5 arrived").isZero();
		}
	}

	/**
	 * Until the upstream's first SETTINGS the relay opens one stream; a SETTINGS without
	 * the limit then lifts it: the waiting stream goes while the first is still open.
	 */
	@Test
	void oneStreamIsOpenedUntilTheUpstreamSettingsArrive() throws Exception {
		String route = "sl.presettings.test";
		List<UpstreamConnection> connections = start(route, connection -> {
		});
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("the second stream waits for the upstream's SETTINGS").isFalse();
			upstream.settings(null);
			awaitOpen(upstream, 3);
			assertThat(upstream.peak()).as("no limit announced: both streams open at once").isEqualTo(2);
			upstream.release(1);
			upstream.release(3);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), TIMEOUT)).isTrue();
		}
	}

	/** A limit of 0 holds a new stream until a later SETTINGS raises it. */
	@Test
	void zeroLimitHoldsNewStreamsUntilRaised() throws Exception {
		String route = "sl.zero.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(0L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			upstream.release(1);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("a limit of 0 opens no stream").isFalse();
			upstream.settings(1L);
			awaitOpen(upstream, 3);
			upstream.release(3);
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
		}
	}

	/**
	 * A waiting stream the client resets -- its head, DATA and a window update queued --
	 * never reaches the upstream, not even as a reset; its queued bytes and its tracking
	 * go at once, and a later stream opens normally.
	 */
	@Test
	void waitingStreamResetByTheClientNeverReachesTheUpstream() throws Exception {
		String route = "sl.clientreset.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			UpstreamConnection upstream = holdOneAndQueueAnother(client, connections, route);
			client.frame(TestH2.RST_STREAM, 0, 3, TestH2.int32(CANCEL));
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0 && trackedStreams(route) == 1);
			upstream.release(1);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			client.headers(5, true, get(route));
			awaitOpen(upstream, 5);
			upstream.release(5);
			assertThat(client.awaitEnd(5, TIMEOUT)).isTrue();
			assertThat(upstream.seen(3)).as("nothing of stream 3 reached the upstream").isFalse();
		}
	}

	/**
	 * A waiting stream the relay resets itself (a client WINDOW_UPDATE of increment 0):
	 * the client gets {@code PROTOCOL_ERROR}, the upstream nothing of the stream.
	 */
	@Test
	void waitingStreamResetByTheRelayNeverReachesTheUpstream() throws Exception {
		String route = "sl.relayreset.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			UpstreamConnection upstream = holdOneAndQueueAnother(client, connections, route);
			client.frame(TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32(0));
			assertThat(client.awaitTrue(() -> client.reset(3) != null, TIMEOUT)).isTrue();
			assertThat(client.reset(3)).isEqualTo(PROTOCOL_ERROR);
			Awaitility.await().atMost(TIMEOUT).until(() -> queuedBytes(route) == 0 && trackedStreams(route) == 1);
			upstream.release(1);
			client.headers(5, true, get(route));
			awaitOpen(upstream, 5);
			upstream.release(5);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(5), TIMEOUT)).isTrue();
			assertThat(upstream.seen(3)).as("nothing of stream 3 reached the upstream").isFalse();
		}
	}

	/**
	 * A waiting stream's client gets no window beyond the initial one: payload that
	 * cannot drain is not invited. Once the stream opens, the window is granted.
	 */
	@Test
	void waitingStreamIsGrantedNoWindowBeyondTheInitialOne() throws Exception {
		String route = "sl.window.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, false, post(route));
			assertThat(client.data(3, H2TestClient.INITIAL_WINDOW, false, TIMEOUT)).isTrue();
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 3 waits").isFalse();
			assertThat(client.streamGranted(3)).as("window granted to the waiting stream").isZero();
			upstream.release(1);
			awaitOpen(upstream, 3);
			assertThat(client.awaitTrue(() -> client.streamGranted(3) > 0, TIMEOUT)).as("granted once opened").isTrue();
			client.sendData(3, new byte[0], TestH2.FLAG_END_STREAM);
			upstream.release(3);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), TIMEOUT)).isTrue();
			assertThat(upstream.upstream().received(3)).isEqualTo(H2TestClient.INITIAL_WINDOW);
		}
	}

	/**
	 * An upstream going away with no response owed but a stream waiting: the waiting
	 * stream is refused ({@code REFUSED_STREAM}, safe to retry) and the client's
	 * connection goes on -- the next stream of the route opens a new leg.
	 */
	@Test
	void upstreamGoneWithOnlyWaitingStreamsRefusesThem() throws Exception {
		String route = "sl.gone.test";
		List<UpstreamConnection> connections = start(route, connection -> {
			connection.settings(1L);
			if (connection.index() > 0) {
				connection.releaseAll();
			}
		});
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			upstream.settings(0L);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 2);
			upstream.release(1);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 3 waits for the limit of 0").isFalse();
			upstream.shutdown();
			assertThat(client.awaitTrue(() -> client.reset(3) != null, TIMEOUT)).isTrue();
			assertThat(client.reset(3)).isEqualTo(TestH2.REFUSED_STREAM);
			client.headers(5, true, get(route));
			assertThat(client.awaitEnd(5, TIMEOUT)).as("the route is served over a new leg").isTrue();
			assertThat(connections).hasSize(2);
			assertThat(client.closed()).isFalse();
			assertThat(upstream.seen(3)).isFalse();
		}
	}

	/**
	 * A client that half-closed its connection while a stream waits still gets that
	 * stream's response: the leg half-closes only once the waiting stream went and ended.
	 */
	@Test
	void halfClosedClientGetsTheResponseOfItsWaitingStream() throws Exception {
		String route = "sl.halfclose.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			client.halfClose();
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 3 waits").isFalse();
			upstream.release(1);
			awaitOpen(upstream, 3);
			upstream.release(3);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), TIMEOUT)).isTrue();
			assertThat(upstream.peak()).isEqualTo(1);
			Awaitility.await().atMost(TIMEOUT).until(upstream::eof);
		}
	}

	/**
	 * A client gone entirely while a stream waits: the relay ends, closing the leg, and
	 * the waiting stream never reaches the upstream.
	 */
	@Test
	void clientGoneWhileAStreamWaitsEndsTheRelay() throws Exception {
		String route = "sl.clientgone.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		UpstreamConnection upstream;
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 3 waits").isFalse();
		}
		Awaitility.await().atMost(TIMEOUT).until(upstream::eof);
		Awaitility.await().atMost(TIMEOUT).until(() -> trackedStreams(route) == 0);
		assertThat(upstream.seen(3)).isFalse();
	}

	/**
	 * A request ended by a DATA frame's END_STREAM, then one ended by trailers: each
	 * frees its place of the limit of 1 once its response ended, so the next stream goes.
	 */
	@Test
	void requestEndedByDataOrTrailersFreesItsPlace() throws Exception {
		String route = "sl.bodies.test";
		List<UpstreamConnection> connections = start(route, connection -> {
			connection.settings(1L);
			connection.releaseAll();
		});
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, false, post(route));
			assertThat(client.data(1, 10, true, TIMEOUT)).isTrue();
			assertThat(client.awaitEnd(1, TIMEOUT)).as("ended by DATA").isTrue();
			client.headers(3, false, post(route));
			assertThat(client.data(3, 10, false, TIMEOUT)).isTrue();
			client.headers(3, true, TestH2.headerBlock("x-trailer=1"));
			assertThat(client.awaitEnd(3, TIMEOUT)).as("ended by trailers").isTrue();
			client.headers(5, true, get(route));
			assertThat(client.awaitEnd(5, TIMEOUT)).isTrue();
			UpstreamConnection upstream = first(connections);
			assertThat(upstream.peak()).isEqualTo(1);
			assertThat(connections).hasSize(1);
		}
	}

	/**
	 * A response ended before its request: the stream stays open on the upstream until
	 * the request's END_STREAM went, and only then the waiting stream goes.
	 */
	@Test
	void streamAnsweredBeforeItsRequestEndedHoldsItsPlaceUntilThen() throws Exception {
		String route = "sl.early.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			upstream.answerEarly(1);
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			client.headers(3, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 1 is half-closed on the upstream, still open").isFalse();
			client.sendData(1, new byte[0], TestH2.FLAG_END_STREAM);
			awaitOpen(upstream, 3);
			upstream.release(3);
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(upstream.openWhenSeen(3)).isZero();
		}
	}

	/**
	 * A reset either way frees the stream's place: the client's reset goes to the
	 * upstream ahead of the waiting head, and the upstream's own reset lets the next
	 * waiting stream go.
	 */
	@Test
	void resetEitherWayFreesItsPlace() throws Exception {
		String route = "sl.resets.test";
		List<UpstreamConnection> connections = start(route, connection -> connection.settings(1L));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			UpstreamConnection upstream = first(connections);
			awaitOpen(upstream, 1);
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
			client.headers(3, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(3)).as("stream 3 waits").isFalse();
			client.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			awaitOpen(upstream, 3);
			assertThat(upstream.openWhenSeen(3)).as("stream 1's reset arrived first").isZero();
			client.headers(5, true, get(route));
			Thread.sleep(HELD.toMillis());
			assertThat(upstream.seen(5)).as("stream 5 waits").isFalse();
			upstream.reset(3);
			assertThat(client.awaitTrue(() -> client.reset(3) != null, TIMEOUT)).isTrue();
			awaitOpen(upstream, 5);
			upstream.release(5);
			assertThat(client.awaitEnd(5, TIMEOUT)).isTrue();
			assertThat(upstream.peak()).isEqualTo(1);
		}
	}

	/**
	 * Opens stream 1 (held by the upstream) and stream 3 -- head, 1000 bytes of DATA and
	 * a window update queued -- which waits behind the limit of 1.
	 */
	private UpstreamConnection holdOneAndQueueAnother(H2TestClient client, List<UpstreamConnection> connections,
			String route) throws Exception {
		client.headers(1, true, get(route));
		UpstreamConnection upstream = first(connections);
		awaitOpen(upstream, 1);
		Awaitility.await().atMost(TIMEOUT).until(() -> upstream.settingsAcks() >= 1);
		client.headers(3, false, post(route));
		assertThat(client.data(3, 1000, false, TIMEOUT)).isTrue();
		client.frame(TestH2.WINDOW_UPDATE, 0, 3, TestH2.int32(1000));
		Awaitility.await().atMost(TIMEOUT).until(() -> trackedStreams(route) == 2 && queuedBytes(route) > 0);
		Thread.sleep(HELD.toMillis());
		assertThat(upstream.seen(3)).as("stream 3 waits").isFalse();
		assertThat(queuedBytes(route)).as("stream 3's frames queued").isGreaterThan(1000);
		return upstream;
	}

	private static void awaitOpen(UpstreamConnection upstream, int streamId) {
		Awaitility.await().atMost(TIMEOUT).until(() -> upstream.seen(streamId));
	}

	private static UpstreamConnection first(List<UpstreamConnection> connections) {
		Awaitility.await().atMost(TIMEOUT).until(() -> !connections.isEmpty());
		return connections.get(0);
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
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

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	/**
	 * Registers the route with an upstream serving every connection: {@code setup} runs
	 * on it first, then its frames are read. Returns the connections, in accept order.
	 */
	private List<UpstreamConnection> start(String route, Setup setup) {
		List<UpstreamConnection> connections = new CopyOnWriteArrayList<>();
		ServerSocket server = stub(socket -> {
			try (socket) {
				WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
				if (upstream == null) {
					return;
				}
				UpstreamConnection connection;
				synchronized (connections) {
					connection = new UpstreamConnection(upstream, socket, connections.size());
					connections.add(connection);
				}
				setup.run(connection);
				connection.serve();
			}
			catch (IOException e) {
				// connection ended
			}
		});
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(route).target("tcp://127.0.0.1:" + server.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-sl").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(route).isPresent());
		return connections;
	}

	private ServerSocket stub(java.util.function.Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-sl-accept").start(() -> {
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

	/** Runs on a new upstream connection before its frames are read. */
	@FunctionalInterface
	private interface Setup {

		void run(UpstreamConnection connection) throws IOException;

	}

	/**
	 * One upstream connection: answers a stream ({@code :status 200}, body
	 * {@code ok<id>}) once its request ended and the test released it, and keeps the
	 * books of the streams open on it -- opened by the relay's HEADERS, closed by a reset
	 * either way or by the answer's END_STREAM (always after the request's).
	 */
	private static final class UpstreamConnection {

		private final WindowedH2Upstream upstream;

		private final Socket socket;

		private final int index;

		private final Set<Integer> open = new HashSet<>();

		private final Set<Integer> requestEnded = new HashSet<>();

		private final Set<Integer> released = new HashSet<>();

		private final Set<Integer> answered = new HashSet<>();

		/** Stream to the streams open on the upstream when its first frame arrived. */
		private final Map<Integer, Integer> openWhenSeen = new HashMap<>();

		private boolean releaseAll;

		private int peak;

		private int settingsAcks;

		private boolean eof;

		UpstreamConnection(WindowedH2Upstream upstream, Socket socket, int index) {
			this.upstream = upstream;
			this.socket = socket;
			this.index = index;
		}

		WindowedH2Upstream upstream() {
			return this.upstream;
		}

		int index() {
			return this.index;
		}

		/** Announces SETTINGS, with SETTINGS_MAX_CONCURRENT_STREAMS when given. */
		void settings(@Nullable Long maxConcurrentStreams) throws IOException {
			byte[] payload = maxConcurrentStreams == null ? new byte[0]
					: WindowedH2Upstream.setting(SETTINGS_MAX_CONCURRENT_STREAMS, maxConcurrentStreams.longValue());
			this.upstream.write(TestH2.SETTINGS, 0, 0, payload);
		}

		/** Answers every stream once its request ended. */
		synchronized void releaseAll() {
			this.releaseAll = true;
		}

		/** Answers the stream once its request ended -- now, if it did. */
		void release(int streamId) throws IOException {
			boolean answer;
			synchronized (this) {
				this.released.add(streamId);
				answer = takeAnswer(streamId);
			}
			if (answer) {
				answer(streamId);
			}
		}

		/**
		 * Answers the stream now, its request unfinished: the stream stays open on the
		 * upstream until the request's END_STREAM arrives.
		 */
		void answerEarly(int streamId) throws IOException {
			synchronized (this) {
				this.answered.add(streamId);
			}
			answer(streamId);
		}

		/** Resets the stream ({@code CANCEL}): it closes on the upstream. */
		void reset(int streamId) throws IOException {
			synchronized (this) {
				this.open.remove(streamId);
				this.answered.add(streamId);
			}
			this.upstream.reset(streamId, CANCEL);
		}

		/** Ends the upstream's side, then reads the relay's to its end. */
		void shutdown() throws IOException {
			this.socket.shutdownOutput();
		}

		void serve() throws IOException {
			TestH2.@Nullable Frame frame;
			while ((frame = this.upstream.next()) != null) {
				int streamId = frame.streamId();
				boolean answer;
				synchronized (this) {
					if (streamId == 0) {
						if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) != 0) {
							this.settingsAcks++;
						}
						continue;
					}
					if (!this.openWhenSeen.containsKey(streamId)) {
						this.openWhenSeen.put(streamId, this.open.size());
						if (frame.type() == TestH2.HEADERS) {
							this.open.add(streamId);
							this.peak = Math.max(this.peak, this.open.size());
						}
					}
					if (frame.type() == TestH2.RST_STREAM) {
						this.open.remove(streamId);
					}
					if ((frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
							&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						this.requestEnded.add(streamId);
						if (this.answered.contains(streamId)) {
							// answered early: the request's end closes the stream
							this.open.remove(streamId);
						}
					}
					answer = takeAnswer(streamId);
				}
				if (answer) {
					answer(streamId);
				}
			}
			synchronized (this) {
				this.eof = true;
			}
		}

		/**
		 * Whether the stream is to be answered now; it closes on the upstream with the
		 * answer, so it leaves the open streams before the answer is written.
		 */
		private boolean takeAnswer(int streamId) {
			boolean due = (this.releaseAll || this.released.contains(streamId)) && this.requestEnded.contains(streamId)
					&& this.open.contains(streamId);
			if (!due || !this.answered.add(streamId)) {
				return false;
			}
			this.open.remove(streamId);
			return true;
		}

		private void answer(int streamId) throws IOException {
			this.upstream.headers(streamId, false);
			this.upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId,
					("ok" + streamId).getBytes(StandardCharsets.US_ASCII));
		}

		/** Whether any frame of the stream arrived. */
		synchronized boolean seen(int streamId) {
			return this.openWhenSeen.containsKey(streamId);
		}

		/** The streams open on the upstream when the stream's first frame arrived. */
		synchronized int openWhenSeen(int streamId) {
			return Objects.requireNonNull(this.openWhenSeen.get(streamId));
		}

		/** The most streams ever open at once. */
		synchronized int peak() {
			return this.peak;
		}

		synchronized int settingsAcks() {
			return this.settingsAcks;
		}

		/** Whether the relay's side ended. */
		synchronized boolean eof() {
			return this.eof;
		}

	}

}
