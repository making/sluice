package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

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
import am.ik.sluice.server.route.Router;
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
 * Streams waiting for an upstream's SETTINGS_MAX_CONCURRENT_STREAMS against a queue
 * budget too small for what they hold: while the relay withholds the client's connection
 * window, the bytes of the waiting streams stay within the connection's allowance -- the
 * most recent of them refused ({@code REFUSED_STREAM}) --, so the open stream's drain
 * alone brings the queue back within it and its request completes; while the budget has
 * room, none is refused. In the relay's package for its meters and constants.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxWaitingStreamBudgetE2ETest {

	/**
	 * Room for some of what one connection's waiting streams queue past its allowance,
	 * not for all of it.
	 */
	private static final long BUDGET = 256 * 1024;

	/** Stream 1's request body: several times the client's connection window. */
	private static final int BODY = 4 * NettyHttp2DemuxRelay.CONNECTION_WINDOW;

	/**
	 * What each waiting POST sends: two of them fill the allowance, so the waiting
	 * streams past it are the third and later.
	 */
	private static final int WAITING_BODY = H2TestClient.INITIAL_WINDOW;

	/** How long the client sends on a waiting stream after its last grant. */
	private static final Duration QUIET = Duration.ofMillis(300);

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private static final Behavior LIMIT_ONE = new Behavior(1L, true);

	/** Grants nothing back: what is queued for it beyond the initial windows stays. */
	private static final Behavior NOT_READING = new Behavior(null, false);

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
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxWaitingStreamBudgetE2ETest::close);
	}

	/**
	 * Upstream limit 1: stream 1 open with an unfinished POST; stream 3 a GET and streams
	 * 5 to 15 POSTs sending 64KiB each wait behind it until they exhaust the budget. The
	 * window is withheld, and the waiting streams past the allowance -- all but 3 and 5
	 * -- are refused; stream 1's body completes, then 3 and 5 go, one at a time, and
	 * nothing of the refused streams reached the upstream.
	 */
	@Test
	void waitingStreamsExhaustingTheBudgetLeaveTheOpenStreamCompleting() throws Exception {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		String route = "ws.own.test";
		List<TestUpstream> upstreams = start(route, LIMIT_ONE);
		double withheldBefore = withheld();
		double refusedBefore = refused("waiting");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7, 9, 11, 13, 15)) {
			TestUpstream upstream = openOneAndQueueBehindIt(client, upstreams, route, 15);
			assertThat(withheld()).as("the window was withheld: the queue past the allowance, the budget exhausted")
				.isGreaterThan(withheldBefore);
			assertThat(queuedBytes(route)).as("the waiting streams hold at most the allowance")
				.isLessThanOrEqualTo(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
			completeAndDrain(client, upstream, route);
			assertThat(client.resets()).as("the waiting streams past the allowance were refused")
				.containsOnlyKeys(7, 9, 11, 13, 15)
				.allSatisfy((streamId, code) -> assertThat(code).isEqualTo(TestH2.REFUSED_STREAM));
			assertThat(refused("waiting") - refusedBefore).isEqualTo(5);
		}
	}

	/**
	 * Waiting streams holding past the allowance while the budget has room are kept; once
	 * a connection whose upstream does not read exhausts the budget, the open stream's
	 * next DATA is withheld, the waiting streams past the allowance are refused, and the
	 * open stream's body completes.
	 */
	@Test
	void waitingStreamsAreRefusedOnceAnotherConnectionExhaustsTheBudget() throws Exception {
		String route = "ws.shared.test";
		String stalledRoute = "ws.stalled.test";
		List<TestUpstream> upstreams = start(route, LIMIT_ONE);
		start(stalledRoute, NOT_READING);
		double refusedBefore = refused("waiting");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7, 9)) {
			TestUpstream upstream = openOneAndQueueBehindIt(client, upstreams, route, 9);
			assertThat(queuedBytes(route)).as("the waiting streams hold past the allowance")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
			assertThat(budgetUsed()).as("the budget has room").isLessThan(BUDGET);
			assertThat(client.resets()).as("nothing refused while the budget has room").isEmpty();
			double withheldBefore = withheld();
			try (H2TestClient stalled = H2TestClient.connect(dataPort)) {
				// eight streams: the relay holds a stream's window per stream, and these
				// hold past the budget
				for (int streamId = 1; streamId <= 15; streamId += 2) {
					stalled.headers(streamId, false, post(stalledRoute));
					stalled.fill(streamId, QUIET);
				}
				assertThat(budgetUsed()).as("the stalled connection exhausted the budget")
					.isGreaterThanOrEqualTo(BUDGET);
				assertThat(withheld()).as("the stalled connection's window was withheld").isGreaterThan(withheldBefore);
				assertThat(client.resets()).as("nothing refused before the open stream sends").isEmpty();
				completeAndDrain(client, upstream, route);
				assertThat(client.resets()).as("the waiting streams past the allowance were refused")
					.containsOnlyKeys(7, 9)
					.allSatisfy((streamId, code) -> assertThat(code).isEqualTo(TestH2.REFUSED_STREAM));
				assertThat(refused("waiting") - refusedBefore).isEqualTo(2);
			}
		}
		Awaitility.await()
			.atMost(TIMEOUT)
			.untilAsserted(() -> assertThat(budgetUsed()).as("the budget released").isZero());
	}

	/**
	 * Opens stream 1 with an unfinished POST, then queues stream 3, a GET, and streams 5
	 * to {@code last}, POSTs each sending what the windows admit of 64KiB, behind the
	 * upstream's limit of 1.
	 */
	private TestUpstream openOneAndQueueBehindIt(H2TestClient client, List<TestUpstream> upstreams, String route,
			int last) throws Exception {
		client.headers(1, false, post(route));
		TestUpstream upstream = first(upstreams);
		Awaitility.await().atMost(TIMEOUT).until(() -> upstream.seen(1) && upstream.settingsAcks() >= 1);
		client.headers(3, true, get(route));
		for (int streamId = 5; streamId <= last; streamId += 2) {
			client.headers(streamId, false, post(route));
			client.fill(streamId, WAITING_BODY, QUIET);
			// the relay reads and accounts it, and withholds past the budget
			Thread.sleep(QUIET.toMillis());
		}
		assertThat(upstream.seenStreams()).as("the streams after stream 1 wait for the limit of 1").containsExactly(1);
		return upstream;
	}

	/**
	 * Stream 1's body completes; streams 3 and 5 then go, one at a time, and no other
	 * stream reaches the upstream; nothing of the connection stays queued or tracked, and
	 * the client's half-close reaches the upstream: the leg owes no response any more.
	 */
	private void completeAndDrain(H2TestClient client, TestUpstream upstream, String route) throws Exception {
		assertThat(client.data(1, BODY, true, TIMEOUT)).as("stream 1's body is sent").isTrue();
		assertThat(client.awaitEnd(1, TIMEOUT)).as("stream 1 completes").isTrue();
		assertThat(upstream.upstream().received(1)).isEqualTo(BODY);
		client.sendData(5, new byte[0], TestH2.FLAG_END_STREAM);
		assertThat(client.awaitTrue(() -> client.ended(3) && client.ended(5), TIMEOUT))
			.as("the waiting streams kept then go")
			.isTrue();
		assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("ok5");
		assertThat(upstream.upstream().received(5)).isEqualTo(WAITING_BODY);
		assertThat(upstream.seenStreams()).as("nothing of the refused streams reached the upstream")
			.containsExactly(1, 3, 5);
		assertThat(upstream.peak()).as("streams open at once on the upstream").isEqualTo(1);
		assertThat(client.closed()).isFalse();
		Awaitility.await()
			.atMost(TIMEOUT)
			.untilAsserted(() -> assertThat(queuedBytes(route)).as("nothing left queued").isZero());
		Awaitility.await()
			.atMost(TIMEOUT)
			.untilAsserted(() -> assertThat(trackedStreams(route)).as("no stream left tracked").isZero());
		client.halfClose();
		Awaitility.await().atMost(TIMEOUT).until(upstream::eof);
	}

	private static TestUpstream first(List<TestUpstream> upstreams) {
		Awaitility.await().atMost(TIMEOUT).until(() -> !upstreams.isEmpty());
		return upstreams.get(0);
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private long budgetUsed() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private long budgetGauge() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.bytes").gauge().value());
	}

	/** The route's leg queue gauge. */
	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.get(H2DemuxMeters.QUEUED_GAUGE_NAME)
			.tag("route", route)
			.gauge()
			.value());
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private double withheld() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.window.withheld").counter().count();
	}

	private double refused(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.refused")
			.tag("reason", reason)
			.counter()
			.count();
	}

	/**
	 * Registers the route with an upstream behaving so on every connection; returns the
	 * connections, in accept order.
	 */
	private List<TestUpstream> start(String route, Behavior behavior) throws IOException {
		List<TestUpstream> upstreams = new CopyOnWriteArrayList<>();
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		this.closeables.add(server);
		Thread.ofVirtual().name("e2e-ws-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.closeables.add(socket);
					Thread.ofVirtual().name("e2e-ws-upstream").start(() -> {
						try (socket) {
							WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
							if (upstream != null) {
								TestUpstream connection = new TestUpstream(upstream, behavior);
								upstreams.add(connection);
								connection.serve();
							}
						}
						catch (IOException e) {
							// connection ended
						}
					});
				}
				catch (IOException e) {
					return;
				}
			}
		});
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(route).target("tcp://127.0.0.1:" + server.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-ws").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(route).isPresent());
		return upstreams;
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * How a test upstream behaves: the SETTINGS_MAX_CONCURRENT_STREAMS it announces, none
	 * when {@code null}; whether it grants back the DATA it reads.
	 */
	private record Behavior(@Nullable Long maxStreams, boolean reads) {
	}

	/**
	 * One upstream connection: announces its SETTINGS, grants back every DATA frame on
	 * its stream and the connection as it reads it -- unless it does not read --, answers
	 * a stream ({@code :status 200}, body {@code ok<id>}) once its request ended, and
	 * keeps the books of the streams open on it.
	 */
	private static final class TestUpstream {

		private final WindowedH2Upstream upstream;

		private final Behavior behavior;

		private final List<Integer> seen = new ArrayList<>();

		private final Set<Integer> open = new HashSet<>();

		private int peak;

		private int settingsAcks;

		private boolean eof;

		TestUpstream(WindowedH2Upstream upstream, Behavior behavior) {
			this.upstream = upstream;
			this.behavior = behavior;
		}

		WindowedH2Upstream upstream() {
			return this.upstream;
		}

		void serve() throws IOException {
			Long maxStreams = this.behavior.maxStreams();
			this.upstream.write(TestH2.SETTINGS, 0, 0, maxStreams == null ? new byte[0]
					: WindowedH2Upstream.setting(TestH2.SETTINGS_MAX_CONCURRENT_STREAMS, maxStreams));
			TestH2.@Nullable Frame frame;
			while ((frame = this.upstream.next()) != null) {
				int streamId = frame.streamId();
				synchronized (this) {
					if (streamId == 0) {
						if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) != 0) {
							this.settingsAcks++;
						}
						continue;
					}
					if (frame.type() == TestH2.HEADERS && !this.seen.contains(streamId)) {
						this.seen.add(streamId);
						this.open.add(streamId);
						this.peak = Math.max(this.peak, this.open.size());
					}
				}
				if (this.behavior.reads() && frame.type() == TestH2.DATA && frame.payload().length > 0) {
					this.upstream.grant(streamId, frame.payload().length);
					this.upstream.grant(0, frame.payload().length);
				}
				boolean ended = (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					synchronized (this) {
						// the answer closes the stream: it leaves the books first
						this.open.remove(streamId);
					}
					this.upstream.headers(streamId, false);
					this.upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId,
							("ok" + streamId).getBytes(StandardCharsets.US_ASCII));
				}
			}
			synchronized (this) {
				this.eof = true;
			}
		}

		/** Whether the relay's side ended. */
		synchronized boolean eof() {
			return this.eof;
		}

		synchronized boolean seen(int streamId) {
			return this.seen.contains(streamId);
		}

		/** The streams whose head arrived, in arrival order. */
		synchronized List<Integer> seenStreams() {
			return List.copyOf(this.seen);
		}

		/** The most streams ever open at once. */
		synchronized int peak() {
			return this.peak;
		}

		synchronized int settingsAcks() {
			return this.settingsAcks;
		}

	}

}
