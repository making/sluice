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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux must not couple its legs on the outbound side: with one leg's upstream
 * gone quiet -- every buffer between the relay and that upstream filled -- the other leg
 * of the same client connection still completes. And the per-leg queue stays bounded: a
 * client ignoring the flow-control windows gets the connection closed instead of the
 * relay buffering without limit.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxStallE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-h2d").start(task);

	private static final int CHUNK = 16 * 1024;

	/**
	 * The total pushed on the stalled leg: past the per-connection bounds and socket
	 * buffers of the whole relay path (the client's 8MiB inbound bound, the server's 1MiB
	 * sender queue, the gRPC windows), yet below that path plus the 4MiB leg queue cap,
	 * so the relay survives with its queue engaged.
	 */
	private static final long PUSHED_TOTAL = 4L * 1024 * 1024;

	private static final long MAX_QUEUED_BYTES = 4L * 1024 * 1024;

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

	@BeforeAll
	static void startUpstreams() {
		stallUpstream = stub(H2DemuxStallE2ETest::hold);
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
	 * A leg held on the full relay path -- its upstream accepts and never reads -- must
	 * not stop the other leg of the same client connection.
	 */
	@Test
	void stalledLegKeepsTheOtherLegsFlowing() throws Exception {
		startClient("h2d.stall.test", stallUpstream);
		startClient("h2d.echo.test", echoUpstream);
		AtomicLong written = new AtomicLong();
		Map<Integer, List<TestH2.Frame>> seen = new ConcurrentHashMap<>();
		CountDownLatch echoDone = new CountDownLatch(1);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			Thread.ofVirtual().name("e2e-h2d-reader").start(() -> readFrames(socket, echoDone, seen));
			byte[] head = TestH2.headerBlock(":authority=h2d.stall.test", ":method=POST", ":path=/slow");
			synchronized (out) {
				out.write(TestH2.PREFACE);
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, head);
			}
			Thread pusher = Thread.ofVirtual().name("e2e-h2d-push-a").start(() -> pushData(out, 1, written));
			// the path saturates: the relayed bytes plateau above the per-connection
			// bounds and the socket buffers, and stay there
			awaitOutboundPlateau("h2d.stall.test");
			byte[] second = TestH2.headerBlock(":authority=h2d.echo.test", ":method=GET", ":path=/fast");
			synchronized (out) {
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3, second);
			}
			assertThat(echoDone.await(5, TimeUnit.SECONDS)).as("the second leg answers within 5s").isTrue();
			assertThat(TestH2.bodyOf(Objects.requireNonNull(seen.get(Integer.valueOf(3))))).isEqualTo(ECHO_BODY);
			assertThat(written.get()).as("bytes pushed on the stalled leg").isGreaterThan(2L * 1024 * 1024);
			socket.close(); // ends the reader and the pusher
		}
	}

	/**
	 * A client ignoring the stream windows pushes past the per-leg queue cap: the
	 * connection is closed, and the queued bytes never grew past the cap.
	 */
	@Test
	void legQueueOverflowClosesTheConnection() throws Exception {
		String route = "h2d.cap.test";
		startClient(route, capUpstream);
		AtomicBoolean closed = new AtomicBoolean();
		AtomicLong maxQueued = new AtomicLong();
		AtomicLong pushed = new AtomicLong();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			Thread.ofVirtual().name("e2e-h2d-cap-reader").start(() -> drainUntilClosed(socket, closed));
			byte[] head = TestH2.headerBlock(":authority=h2d.cap.test", ":method=POST", ":path=/flood");
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, head);
			Thread pusher = Thread.ofVirtual()
				.name("e2e-h2d-cap-push")
				.start(() -> pushUntilClosed(out, 1, closed, pushed, maxQueued, route));
			Awaitility.await().atMost(Duration.ofSeconds(60)).untilTrue(closed);
			assertThat(pushed.get()).as("bytes pushed past the queues").isGreaterThan(2 * 1024 * 1024);
			assertThat(maxQueued.get()).as("queued-bytes high-water mark")
				.isGreaterThan(1024 * 1024)
				.isLessThanOrEqualTo(MAX_QUEUED_BYTES);
			socket.close();
			pusher.join(TimeUnit.SECONDS.toMillis(5));
		}
	}

	/** Collects the echo stream's frames, signalling its DATA END_STREAM. */
	private static void readFrames(Socket socket, CountDownLatch echoDone, Map<Integer, List<TestH2.Frame>> seen) {
		try {
			InputStream in = socket.getInputStream();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.streamId() == 3) {
					seen.computeIfAbsent(Integer.valueOf(3), id -> new ArrayList<>()).add(frame);
					if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						echoDone.countDown();
					}
				}
			}
		}
		catch (IOException e) {
			// the socket ended
		}
	}

	/** The connection is over once the server's side closes; every read byte counts. */
	private static void drainUntilClosed(Socket socket, AtomicBoolean closed) {
		try {
			InputStream in = socket.getInputStream();
			while (in.read() != -1) {
				// the window grants must drain, or the relay's writes would stall
			}
			closed.set(true);
		}
		catch (IOException e) {
			closed.set(true); // reset counts as closed
		}
	}

	/** Pushes DATA frames on the stream until {@code #PUSHED_TOTAL}, then ends it. */
	private static void pushData(OutputStream out, int streamId, AtomicLong written) {
		try {
			byte[] chunk = new byte[CHUNK];
			for (long sent = 0; sent < PUSHED_TOTAL; sent += CHUNK) {
				int flags = sent + CHUNK >= PUSHED_TOTAL ? TestH2.FLAG_END_STREAM : 0;
				synchronized (out) {
					TestH2.writeFrame(out, TestH2.DATA, flags, streamId, chunk);
				}
				written.addAndGet(CHUNK);
			}
		}
		catch (IOException e) {
			// torn down
		}
	}

	/** Pushes DATA frames until the connection closes, sampling the leg queue gauge. */
	private void pushUntilClosed(OutputStream out, int streamId, AtomicBoolean closed, AtomicLong pushed,
			AtomicLong maxQueued, String route) {
		byte[] chunk = new byte[CHUNK];
		while (!closed.get()) {
			try {
				synchronized (out) {
					TestH2.writeFrame(out, TestH2.DATA, 0, streamId, chunk);
				}
			}
			catch (IOException e) {
				return; // the server closed the connection
			}
			pushed.addAndGet(CHUNK);
			maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
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

	/**
	 * The route's {@code sluice.tunnel.bytes} outbound counter; zero before it exists.
	 */
	private double relayedBytes(String route) {
		Counter counter = Objects.requireNonNull(meterRegistry)
			.get("sluice.tunnel.bytes")
			.tags("direction", "outbound", "route", route)
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
