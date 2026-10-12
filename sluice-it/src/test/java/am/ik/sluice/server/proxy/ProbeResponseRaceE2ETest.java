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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A probe of a half-closed HTTP/1.1 client racing the response it precedes: the probe is
 * held between its decision and its write while the upstream's response arrives, and the
 * client must read the probe before the response, never inside or after it. In the
 * relay's package for the package-private probe seam.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ProbeResponseRaceE2ETest {

	private static final Duration INTERVAL = Duration.ofMillis(200);

	private static final String HOST = "race.probe.test";

	/** What every probe writes. */
	private static final String PROBE = "HTTP/1.1 100 Continue\r\n\r\n";

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	/** Released per test: the upstream writes the first part of its response. */
	private volatile Semaphore first = new Semaphore(0);

	/** Released per test: the upstream writes the rest of its response. */
	private volatile Semaphore rest = new Semaphore(0);

	/** Set per test: the parts of the upstream's response. */
	private volatile String[] parts = { OK, "" };

	/** Counted down per test when the first probe was decided and is held. */
	private volatile CountDownLatch decided = new CountDownLatch(1);

	/** Counted down per test to let the held probe be written. */
	private volatile CountDownLatch proceed = new CountDownLatch(1);

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.half-close-probe-interval", () -> INTERVAL.toMillis() + "ms");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() throws IOException {
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		this.closeables.add(upstream);
		Thread.ofVirtual().name("e2e-race-accept").start(() -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					this.closeables.add(socket);
					UPSTREAM_EXECUTOR.execute(() -> serve(socket));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(HOST).target("http://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-race-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> router().lookup(HOST).isPresent());
	}

	@BeforeEach
	void reset() {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> dataProxyServer().activeConnections() == 0);
		this.first = new Semaphore(0);
		this.rest = new Semaphore(0);
		this.decided = new CountDownLatch(1);
		this.proceed = new CountDownLatch(1);
	}

	@AfterEach
	void release() {
		// a test failing midway leaves neither the relay nor the upstream waiting
		dataProxyServer().probeSeam(ReroutingRelay.ProbeSeam.NONE);
		this.proceed.countDown();
		this.first.release(100);
		this.rest.release(100);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(ProbeResponseRaceE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * The upstream's response -- part of its head, or all of it -- reaches the relay
	 * while the first probe is held after its decision: the response waits for the probe,
	 * which the client reads first.
	 */
	@ParameterizedTest
	@CsvSource({ "HTTP/1.1 200 OK\\r\\nContent-, Length: 2\\r\\n\\r\\nok",
			"'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok', ''" })
	void aResponseArrivingWhileAProbeIsHeldFollowsTheProbe(String before, String after) throws Exception {
		String head = unescape(before);
		this.parts = new String[] { head, unescape(after) };
		AtomicReference<@Nullable ReroutingRelay> held = new AtomicReference<>();
		AtomicBoolean once = new AtomicBoolean();
		CountDownLatch decided = this.decided;
		CountDownLatch proceed = this.proceed;
		dataProxyServer().probeSeam(relay -> {
			if (!once.compareAndSet(false, true)) {
				return;
			}
			held.set(relay);
			decided.countDown();
			try {
				proceed.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			Received received = Received.start(socket);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertThat(decided.await(5, TimeUnit.SECONDS)).as("a probe was decided").isTrue();
			ReroutingRelay relay = Objects.requireNonNull(held.get());
			assertThat(received.text()).as("decided before any of the response").isEmpty();
			assertThat(relay.sinkAwaited()).as("no write waits for the client yet").isFalse();
			this.first.release();
			// the relay read the response: it waits for the client stream behind the held
			// probe, or it was written to the client past the probe
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkAwaited() || !received.text().isEmpty());
			boolean waited = relay.sinkAwaited();
			proceed.countDown();
			assertThat(received.await(PROBE.length() + head.length())).isEqualTo(PROBE + head);
			assertThat(waited).as("the response waited behind the held probe").isTrue();
			this.rest.release();
			assertThat(received.awaitEnd()).isEqualTo(PROBE + OK);
		}
	}

	/**
	 * Reads the request, waits for {@link #first}, writes the first part of the response,
	 * waits for {@link #rest}, writes the rest, and closes at the client's end.
	 */
	private void serve(Socket socket) {
		Semaphore first = this.first;
		Semaphore rest = this.rest;
		String[] parts = this.parts;
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			if (!readHead(in)) {
				return;
			}
			first.acquire();
			write(out, parts[0]);
			rest.acquire();
			write(out, parts[1]);
			in.transferTo(OutputStream.nullOutputStream());
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static void write(OutputStream out, String text) throws IOException {
		out.write(text.getBytes(StandardCharsets.US_ASCII));
		out.flush();
	}

	/** Reads up to and including the blank line; false at EOF. */
	private static boolean readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			if (head.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) {
				return true;
			}
		}
		return false;
	}

	private static String unescape(String value) {
		return value.replace("\\r", "\r").replace("\\n", "\n");
	}

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/** What the client read so far, collected on a thread of its own until EOF. */
	private static final class Received {

		private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

		private final CountDownLatch ended = new CountDownLatch(1);

		static Received start(Socket socket) throws IOException {
			Received received = new Received();
			InputStream in = socket.getInputStream();
			Thread.ofVirtual().name("e2e-race-client-read").start(() -> {
				try {
					byte[] buffer = new byte[1024];
					int n;
					while ((n = in.read(buffer)) > 0) {
						received.append(buffer, n);
					}
				}
				catch (IOException e) {
					// closed
				}
				finally {
					received.ended.countDown();
				}
			});
			return received;
		}

		private synchronized void append(byte[] buffer, int length) {
			this.bytes.write(buffer, 0, length);
		}

		synchronized String text() {
			return this.bytes.toString(StandardCharsets.US_ASCII);
		}

		/** What was read once at least {@code length} bytes were. */
		String await(int length) {
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> text().length() >= length);
			return text();
		}

		/** What was read until EOF. */
		String awaitEnd() throws InterruptedException {
			assertThat(this.ended.await(5, TimeUnit.SECONDS)).as("the connection ended").isTrue();
			return text();
		}

	}

}
