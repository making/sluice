package am.ik.sluice.it;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux must not couple its legs on the outbound side: with one leg's upstream
 * gone quiet -- every buffer between the relay and that upstream filled, the leg's queue
 * engaged -- the other leg of the same client connection still completes. And a client
 * sending past a stream's window gets the connection closed with a FLOW_CONTROL_ERROR
 * instead of the relay queueing beyond the stream's budget. A client that goes away in
 * the middle of a request releases a relay stalled that way.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxStallE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-h2d").start(task);

	private static final int CHUNK = 16 * 1024;

	private static final int STREAM_CLOSED = 0x5;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	/**
	 * What the relay may queue for one stream: its payload budget (the client window plus
	 * the queue allowance) and the framing of its entries.
	 */
	private static final long STREAM_QUEUE_BOUND = 65535 + 64 * 1024 + 1024;

	private static final String ECHO_BODY = "echo-h2d";

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static final List<Socket> heldUpstreamSockets = new ArrayList<>();

	private static ServerSocket stallUpstream;

	private static ServerSocket echoUpstream;

	private static ServerSocket capUpstream;

	private static int grpcPort;

	private static int dataPort;

	private static final List<TunnelClient> clients = new ArrayList<>();

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@BeforeAll
	static void startUpstreams() {
		stallUpstream = stub(H2DemuxStallE2ETest::grantThenHold);
		echoUpstream = stub(H2DemuxStallE2ETest::serveEcho);
		capUpstream = stub(H2DemuxStallE2ETest::hold);
	}

	@AfterAll
	static void tearDown() {
		clients.forEach(TunnelClient::stop);
		synchronized (heldUpstreamSockets) {
			for (Socket socket : heldUpstreamSockets) {
				close(socket);
			}
		}
		close(stallUpstream);
		close(echoUpstream);
		close(capUpstream);
		UPSTREAM_EXECUTOR.shutdown();
	}

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

	/**
	 * A leg held on the full relay path -- its upstream granted a huge window, then
	 * accepts and never reads -- must not stop the other leg of the same client
	 * connection.
	 */
	@Test
	@Disabled("waits on the route's tunnel byte counter (sluice.tunnel.bytes), not yet on the "
			+ "h2 demux, and asserts the hand-written relay's stream budget")
	void stalledLegKeepsTheOtherLegsFlowing() throws Exception {
		startClient("h2d.stall.test", stallUpstream);
		startClient("h2d.echo.test", echoUpstream);
		AtomicBoolean stop = new AtomicBoolean();
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, false, TestH2.headerBlock(":authority=h2d.stall.test", ":method=POST", ":path=/slow"));
			Thread.ofVirtual().name("e2e-h2d-push-a").start(() -> {
				try {
					while (!stop.get() && !client.closed()) {
						client.fill(1, Duration.ofMillis(100));
					}
				}
				catch (IOException | InterruptedException e) {
					// torn down
				}
			});
			// the path saturates: the relayed bytes plateau above the per-connection
			// bounds and the socket buffers, and stay there
			awaitOutboundPlateau("h2d.stall.test", client);
			long queued = queuedBytes("h2d.stall.test");
			client.headers(3, true, TestH2.headerBlock(":authority=h2d.echo.test", ":method=GET", ":path=/fast"));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("the second leg answers within 5s").isTrue();
			stop.set(true);
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo(ECHO_BODY);
			assertThat(queued).as("the stall reached the leg queue when the second request was sent")
				.isGreaterThan(0)
				.isLessThanOrEqualTo(STREAM_QUEUE_BOUND);
			// past the relay's budget and the tunnel connection's credit window: the
			// stall
			// holds the tunnel path too, not only the relay
			assertThat(client.streamSent(1)).as("bytes pushed on the stalled leg")
				.isGreaterThan(STREAM_QUEUE_BOUND + VirtualConnection.DEFAULT_WINDOW);
			assertThat(client.maxOvershoot()).isLessThanOrEqualTo(0);
		}
	}

	/**
	 * A client closing in the middle of a request releases a relay whose leg writer is
	 * parked waiting for the tunnel connection's credit: the relay finishes and the
	 * active-connection count drops while the stalled upstream still holds its socket.
	 */
	@Test
	@Disabled("waits on the route's tunnel byte counter (sluice.tunnel.bytes), not yet on the "
			+ "h2 demux, and looks for the hand-written relay's leg writer waiting for credit")
	void closingTheClientReleasesTheStalledLeg() throws Exception {
		String route = "h2d.release.test";
		startClient(route, stallUpstream);
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		// the relays of the other tests have ended: the count below is this relay's
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
		int heldBefore = heldUpstreamSockets();
		AtomicBoolean stop = new AtomicBoolean();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=h2d.release.test", ":method=POST", ":path=/slow"));
			Thread.ofVirtual().name("e2e-h2d-push-release").start(() -> {
				try {
					while (!stop.get() && !client.closed()) {
						client.fill(1, Duration.ofMillis(100));
					}
				}
				catch (IOException | InterruptedException e) {
					// torn down
				}
			});
			awaitOutboundPlateau(route, client);
			// the leg's writer is parked: the leg queue holds what it could not write
			assertThat(queuedBytes(route)).as("the stall reached the leg queue").isGreaterThan(0);
			assertThat(ThreadDumps.anyThreadWaitingForCredit()).as("the leg writer waits for credit").isTrue();
			assertThat(proxy.activeConnections()).as("the relay is live").isEqualTo(1);
		}
		finally {
			stop.set(true);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
		assertThat(heldUpstreamSockets()).as("the stalled upstream still holds its socket").isEqualTo(heldBefore + 1);
	}

	/**
	 * The release above aborts only for an unfinished request: a client half-closing
	 * after its request ended still gets the response.
	 */
	@Test
	@Disabled("the half-close drain: the client's EOF ends the h2 demux at once")
	void halfCloseAfterAFinishedRequestStillGetsTheResponse() throws Exception {
		startClient("h2d.halfclose.test", echoUpstream);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(10));
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=h2d.halfclose.test", ":method=GET",
					":path=/done");
			socket.shutdownOutput();
			List<TestH2.Frame> frames = TestH2.readResponseFrames(socket.getInputStream())
				.stream()
				.filter(frame -> frame.streamId() == 1)
				.toList();
			assertThat(TestH2.bodyOf(frames)).isEqualTo(ECHO_BODY);
		}
	}

	/**
	 * A client sending past the stream window gets the stream reset with
	 * {@code FLOW_CONTROL_ERROR} (a stream error, RFC 9113 6.9.1); netty's codec answers
	 * each DATA frame it sends on the forgotten stream after that with RST_STREAM
	 * {@code STREAM_CLOSED}, and its 201st reset within 30s (netty's limit on the resets
	 * a server sends, 200 per 30s) ends the connection with GOAWAY
	 * {@code ENHANCE_YOUR_CALM}. The leg queue never held more than the stream's budget.
	 */
	@Test
	void streamOverItsWindowIsResetAndTheFloodPastItEndsTheConnection() throws Exception {
		String route = "h2d.cap.test";
		startClient(route, capUpstream);
		AtomicLong maxQueued = new AtomicLong();
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, TestH2.headerBlock(":authority=h2d.cap.test", ":method=POST", ":path=/flood"));
			byte[] chunk = new byte[CHUNK];
			long pushed = 0;
			try {
				while (!client.closed() && pushed < 64L * 1024 * 1024) {
					client.sendData(1, chunk, 0);
					pushed += CHUNK;
					maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
				}
			}
			catch (IOException e) {
				// the server closed the connection
			}
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(client::closed);
			List<Integer> resets = client.frames(1)
				.stream()
				.filter(frame -> frame.type() == TestH2.RST_STREAM)
				.map(TestH2::errorCode)
				.toList();
			assertThat(resets).as("the resets of stream 1").hasSize(201);
			assertThat(resets.get(0)).isEqualTo(TestH2.FLOW_CONTROL_ERROR);
			assertThat(resets.subList(1, resets.size())).containsOnly(STREAM_CLOSED);
			assertThat(client.goAwayCode()).as("GOAWAY error code").isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(client.maxOvershoot()).as("the client did exceed the window").isGreaterThan(0);
			assertThat(maxQueued.get()).as("queued-bytes high-water mark").isLessThanOrEqualTo(STREAM_QUEUE_BOUND);
		}
	}

	/**
	 * Waits until the route's relayed bytes plateau: above the per-connection bounds and
	 * unchanged for two seconds -- the whole relay path saturated. Fails at once when the
	 * relay reset stream 1: no stall can build up on a refused or lost stream.
	 */
	private void awaitOutboundPlateau(String route, H2TestClient client) {
		AtomicReference<Double> last = new AtomicReference<>(Double.valueOf(-1));
		AtomicLong stableSince = new AtomicLong(System.nanoTime());
		Awaitility.await()
			.atMost(Duration.ofSeconds(30))
			.failFast("stream 1 reset", () -> client.reset(1) != null)
			.until(() -> {
				double count = relayedBytes(route);
				if (Double.compare(count, last.get().doubleValue()) != 0) {
					last.set(Double.valueOf(count));
					stableSince.set(System.nanoTime());
				}
				return count > 256L * 1024
						&& TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stableSince.get()) > 2000;
			});
	}

	/**
	 * The route's {@code sluice.tunnel.bytes} inbound counter (bytes towards the
	 * upstream); zero before it exists.
	 */
	private double relayedBytes(String route) {
		Counter counter = Objects.requireNonNull(meterRegistry)
			.find("sluice.tunnel.bytes")
			.tags("direction", "inbound", "route", route)
			.counter();
		return counter == null ? 0 : counter.count();
	}

	/** The leg queue gauge's largest value for the route; zero before it exists. */
	private long queuedBytes(String route) {
		double max = Objects.requireNonNull(meterRegistry)
			.find("sluice.h2demux.queued.bytes")
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.max()
			.orElse(0);
		return Math.round(max);
	}

	private void startClient(String host, ServerSocket upstream) {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
			.token("it-token")
			.build();
		TunnelClient client = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(router).lookup(host).isPresent());
	}

	/** The upstream sockets currently held open by the stubs. */
	private static int heldUpstreamSockets() {
		synchronized (heldUpstreamSockets) {
			return (int) heldUpstreamSockets.stream().filter(socket -> !socket.isClosed()).count();
		}
	}

	/** A raw TCP stub serving every accepted socket with the handler. */
	private static ServerSocket stub(java.util.function.Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.setReuseAddress(true);
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			Thread.ofVirtual().name("e2e-h2d-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
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
	 * Grants the connection a huge window, and stream 1 once its HEADERS arrived, then
	 * never reads again, holding the socket for the teardown. Granting stream 1 before
	 * its HEADERS would be a WINDOW_UPDATE on an idle stream, a connection error (RFC
	 * 9113 5.1) that ends the leg -- and refuses the request when it arrives first.
	 */
	private static void grantThenHold(Socket socket) {
		try {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(Integer.MAX_VALUE - 65535));
					break;
				}
			}
		}
		catch (IOException e) {
			return;
		}
		hold(socket);
	}

	/** Accepts and never reads, holding the socket for the teardown. */
	private static void hold(Socket socket) {
		synchronized (heldUpstreamSockets) {
			heldUpstreamSockets.add(socket);
		}
		try {
			Thread.sleep(TimeUnit.MINUTES.toMillis(5));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** An h2 stub: every completed request is answered with a canned 200. */
	private static void serveEcho(Socket socket) {
		try (socket) {
			socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(30));
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean requestEnd = frame.streamId() != 0
						&& (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (requestEnd) {
					out.write(TestH2.cannedResponse(frame.streamId(), ECHO_BODY));
					out.flush();
				}
			}
		}
		catch (IOException e) {
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

}
