package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.TestSockets;
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
 * The deadlines between the requests of a relayed HTTP connection: a connection idle
 * between requests -- h1 with every response relayed and nothing of a next request read,
 * h2 with no open stream -- is closed at the idle timeout and its slot released, a later
 * request head is bound by the head timeout from its first bytes, and nothing in flight
 * (a long response, an open stream, an upgraded connection) is cut. In the relay's
 * package for the deadline queue of {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class KeepAliveIdleTimeoutE2ETest {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(2);

	/** Longer than the idle timeout, so a cut tells which of the two passed. */
	private static final Duration HEAD_TIMEOUT = Duration.ofSeconds(3);

	/** The earliest an idle cut may come. */
	private static final Duration NOT_BEFORE = Duration.ofMillis(1500);

	/** The latest an idle cut may come. */
	private static final Duration WITHIN = Duration.ofSeconds(3);

	/** How long the slow responses take: past the idle timeout. */
	private static final Duration SLOW = Duration.ofSeconds(4);

	private static final String H1_HOST = "h1.idle.test";

	private static final String H1_OTHER_HOST = "h1other.idle.test";

	private static final String H2_HOST = "h2.idle.test";

	private static final String H2_SLOW_HOST = "h2slow.idle.test";

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	/** The {@link #OK} of the other h1 upstream. */
	private static final String OTHER_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

	private static final String SWITCHING = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
			+ "Connection: Upgrade\r\n\r\n";

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits connectionLimits;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.idle-timeout", () -> IDLE_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.limits.head-timeout", () -> HEAD_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClient() {
		ServerSocket h1 = upstream(socket -> serveHttp1(socket, OK));
		ServerSocket h1Other = upstream(socket -> serveHttp1(socket, OTHER_OK));
		ServerSocket h2 = upstream(socket -> serveH2(socket, Duration.ZERO));
		ServerSocket h2Slow = upstream(socket -> serveH2(socket, SLOW));
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(H1_HOST).target("http://127.0.0.1:" + h1.getLocalPort()).build())
				.upstream(Upstream.builder()
					.host(H1_OTHER_HOST)
					.target("http://127.0.0.1:" + h1Other.getLocalPort())
					.build())
				.upstream(Upstream.builder().host(H2_HOST).target("tcp://127.0.0.1:" + h2.getLocalPort()).build())
				.upstream(Upstream.builder()
					.host(H2_SLOW_HOST)
					.target("tcp://127.0.0.1:" + h2Slow.getLocalPort())
					.build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-idle-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> List.of(H1_HOST, H1_OTHER_HOST, H2_HOST, H2_SLOW_HOST)
				.stream()
				.allMatch(host -> router().lookup(host).isPresent()));
	}

	@BeforeEach
	void awaitQuiet() {
		// every connection of the previous test is gone, and so are its deadlines
		awaitAdmitted(0);
		awaitNoDeadline();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(KeepAliveIdleTimeoutE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void aKeepAliveConnectionIdleAfterItsResponseIsClosedAtTheIdleTimeout() throws Exception {
		double idle = idleClosed();
		double heads = headTimeouts();
		try (Socket socket = connect()) {
			exchange(socket, get("/"), OK);
			assertThat(admitted()).isEqualTo(1);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(idleClosed()).isEqualTo(idle + 1);
		assertThat(headTimeouts()).isEqualTo(heads);
	}

	@Test
	void aConnectionSendingItsNextRequestEverySecondStaysOpen() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			long start = System.nanoTime();
			int requests = 0;
			while (System.nanoTime() - start < Duration.ofSeconds(5).toNanos()) {
				exchange(socket, get("/"), OK);
				requests++;
				Thread.sleep(1000);
			}
			exchange(socket, get("/"), OK);
			assertThat(requests).as("requests over 5s").isGreaterThanOrEqualTo(5);
		}
		assertThat(idleClosed()).isEqualTo(idle);
	}

	@Test
	void aWebSocketRelayIdleLongerThanTheIdleTimeoutStaysOpen() throws Exception {
		double idle = idleClosed();
		double heads = headTimeouts();
		try (Socket socket = connect()) {
			exchange(socket,
					("GET /ws HTTP/1.1\r\nHost: " + H1_HOST + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
						.getBytes(StandardCharsets.US_ASCII),
					SWITCHING);
			// relayed verbatim from the upgrade on, with no verbatim idle timeout:
			// nothing
			// is scheduled for the connection
			awaitNoDeadline();
			Thread.sleep(IDLE_TIMEOUT.multipliedBy(2).plusSeconds(1).toMillis());
			exchange(socket, "ping".getBytes(StandardCharsets.US_ASCII), "ping");
		}
		assertThat(idleClosed()).isEqualTo(idle);
		assertThat(headTimeouts()).isEqualTo(heads);
	}

	@Test
	void aResponseTakingLongerThanTheIdleTimeoutIsRelayed() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			socket.setSoTimeout((int) SLOW.plusSeconds(3).toMillis());
			long start = System.nanoTime();
			exchange(socket, get("/slow"), OK);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the response")
				.isGreaterThan(SLOW.minusMillis(500));
			assertThat(idleClosed()).isEqualTo(idle);
			// idle from the end of the response on
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aNextRequestHeadDrippedIsClosedAtTheHeadTimeout() throws Exception {
		double idle = idleClosed();
		double heads = headTimeouts();
		try (Socket socket = connect()) {
			exchange(socket, get("/"), OK);
			byte[] head = get("/");
			Thread dripper = Thread.ofVirtual().name("e2e-idle-dripper").start(() -> {
				try {
					OutputStream out = socket.getOutputStream();
					// one byte every 200ms: the head takes far longer than either timeout
					for (int i = 0; i < head.length - 1; i++) {
						out.write(head[i]);
						out.flush();
						Thread.sleep(200);
					}
				}
				catch (IOException | InterruptedException e) {
					// closed by the deadline
				}
			});
			Duration closed = TestSockets.awaitClosed(socket, HEAD_TIMEOUT.plusSeconds(2));
			dripper.interrupt();
			assertThat(closed).as("cut by the head timeout, not the idle timeout")
				.isGreaterThan(HEAD_TIMEOUT.minusMillis(500))
				.isLessThan(HEAD_TIMEOUT.plusMillis(1500));
		}
		awaitAdmitted(0);
		assertThat(headTimeouts()).isEqualTo(heads + 1);
		assertThat(idleClosed()).isEqualTo(idle);
	}

	@ParameterizedTest
	@CsvSource(delimiter = '|', value = { "HEAD / HTTP/1.1|HTTP/1.1 200 OK\\r\\nContent-Length: 1000\\r\\n\\r\\n",
			"GET /304 HTTP/1.1|HTTP/1.1 304 Not Modified\\r\\nContent-Length: 5\\r\\n\\r\\n",
			"GET /204 HTTP/1.1|HTTP/1.1 204 No Content\\r\\n\\r\\n",
			"GET /advertise HTTP/1.1|HTTP/1.1 200 OK\\r\\nUpgrade: h2c\\r\\nConnection: Upgrade\\r\\nContent-Length: 2\\r\\n\\r\\nok" })
	void aConnectionIsIdleAfterABodilessOrUpgradeAdvertisingResponse(String requestLine, String response)
			throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			exchange(socket, request(requestLine), unescape(response));
			// the exchange that follows is tracked too: its end makes the connection idle
			exchange(socket, get("/"), OK);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void pipelinedRequestsAreTrackedByTheirOwnMethods() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			ByteArrayOutputStream pipelined = new ByteArrayOutputStream();
			pipelined.writeBytes(request("HEAD / HTTP/1.1"));
			pipelined.writeBytes(get("/"));
			pipelined.writeBytes(get("/"));
			// tracked by the GET's framing, the HEAD response's length would swallow both
			exchange(socket, pipelined.toByteArray(), "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n" + OK + OK);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aConnectionWhoseUpgradeWasDeclinedIsParsedOnAndIdlesOut() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			exchange(socket, request("GET /h2c HTTP/1.1", "Upgrade: h2c\r\nConnection: Upgrade, HTTP2-Settings\r\n"
					+ "HTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n"), OK);
			// parsed again: the next request reaches the upstream of its own host
			exchange(socket, request("GET / HTTP/1.1", H1_OTHER_HOST, ""), OTHER_OK);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aTlsTerminatedH2ConnectionWithNoOpenStreamIsClosedAtTheIdleTimeout() throws Exception {
		double idle = idleClosed();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "h2" });
			socket.setSSLParameters(parameters);
			socket.setSoTimeout(5000);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + H2_HOST, ":method=GET", ":path=/");
			InputStream in = socket.getInputStream();
			TestH2.@Nullable Frame frame;
			do {
				frame = TestH2.readFrame(in);
			}
			while (frame != null && !(frame.streamId() == 1 && frame.type() == TestH2.DATA
					&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0));
			assertThat(frame).as("the stream's response").isNotNull();
			long ended = System.nanoTime();
			do {
				frame = TestH2.readFrame(in);
			}
			while (frame != null && frame.type() != TestH2.GOAWAY);
			assertThat(frame).as("a GOAWAY").isNotNull();
			assertThat(TestH2.errorCode(Objects.requireNonNull(frame))).as("NO_ERROR").isZero();
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).isGreaterThan(NOT_BEFORE).isLessThan(WITHIN);
			TestSockets.awaitClosed(socket, Duration.ofSeconds(1));
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aTlsTerminatedConnectionIdleAfterItsResponseIsClosedAtTheIdleTimeout() throws Exception {
		double idle = idleClosed();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.startHandshake();
			exchange(socket, get("/"), OK);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aConnectionClosedByItsClientLeavesNoDeadlineBehind() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			exchange(socket, get("/"), OK);
			// the idle timeout runs
			Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> connectionLimits().scheduledDeadlines() == 1);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		Thread.sleep(IDLE_TIMEOUT.plusMillis(500).toMillis());
		assertThat(idleClosed()).as("no timeout passed for the ended relay").isEqualTo(idle);
	}

	@Test
	void anH2ConnectionWithNoOpenStreamIsClosedWithAGoAwayAtTheIdleTimeout() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			assertThat(get(client, 1, H2_HOST)).isTrue();
			assertThat(get(client, 3, H2_HOST)).isTrue();
			long ended = System.nanoTime();
			assertThat(client.awaitTrue(client::closed, WITHIN)).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).isGreaterThan(NOT_BEFORE);
			assertThat(client.goAwayCode()).as("GOAWAY NO_ERROR").isZero();
			assertThat(client.goAwayLastStreamId()).isEqualTo(3);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void anH2ConnectionClosedByItsClientLeavesNoDeadlineBehind() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			assertThat(get(client, 1, H2_HOST)).isTrue();
			// the idle timeout runs
			Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> connectionLimits().scheduledDeadlines() == 1);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		Thread.sleep(IDLE_TIMEOUT.plusMillis(500).toMillis());
		assertThat(idleClosed()).as("no timeout passed for the ended relay").isEqualTo(idle);
	}

	/**
	 * A connection head whose request HEADERS is on stream 0 -- routed by its authority
	 * all the same -- opens no stream: a connection error (RFC 9113 6.2), so the
	 * connection is closed at once with GOAWAY {@code PROTOCOL_ERROR} rather than left to
	 * the idle timeout, and no deadline is left behind.
	 */
	@Test
	void anH2ConnectionHeadOnStreamZeroIsClosedAtOnce() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			OutputStream out = socket.getOutputStream();
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 0,
					TestH2.headerBlock(":authority=" + H2_HOST, ":method=GET", ":path=/"));
			long sent = System.nanoTime();
			TestH2.@Nullable Frame frame;
			do {
				frame = TestH2.readFrame(socket.getInputStream());
			}
			while (frame != null && frame.type() != TestH2.GOAWAY);
			assertThat(frame).as("a GOAWAY").isNotNull();
			assertThat(TestH2.errorCode(Objects.requireNonNull(frame))).as("PROTOCOL_ERROR").isEqualTo(0x1);
			assertThat(Duration.ofNanos(System.nanoTime() - sent)).isLessThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(idleClosed()).isEqualTo(idle);
	}

	@Test
	void pingsDoNotKeepAnIdleH2ConnectionOpen() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			assertThat(get(client, 1, H2_HOST)).isTrue();
			long ended = System.nanoTime();
			while (!client.closed() && System.nanoTime() - ended < WITHIN.toNanos()) {
				client.frame(TestH2.PING, 0, 0, "idleping".getBytes(StandardCharsets.US_ASCII));
				Thread.sleep(500);
			}
			assertThat(client.pingAcks()).as("the pings were answered").isNotEmpty();
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(1))).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void anH2ConnectionOpeningAStreamEverySecondStaysOpen() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			int streamId = 1;
			while (System.nanoTime() - start < Duration.ofSeconds(5).toNanos()) {
				assertThat(get(client, streamId, H2_HOST)).isTrue();
				streamId += 2;
				Thread.sleep(1000);
			}
			assertThat(get(client, streamId, H2_HOST)).isTrue();
			assertThat(client.closed()).isFalse();
		}
		assertThat(idleClosed()).isEqualTo(idle);
	}

	@Test
	void anH2StreamOpenLongerThanTheIdleTimeoutIsRelayed() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long start = System.nanoTime();
			client.headers(1, true, TestH2.headerBlock(":authority=" + H2_SLOW_HOST, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, SLOW.plusSeconds(3))).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the response")
				.isGreaterThan(SLOW.minusMillis(500));
			assertThat(idleClosed()).isEqualTo(idle);
			long ended = System.nanoTime();
			assertThat(client.awaitTrue(client::closed, WITHIN)).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aStreamTheRelayAnswersItselfStartsTheIdleTimeoutOver() throws Exception {
		double idle = idleClosed();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			assertThat(get(client, 1, H2_HOST)).isTrue();
			Thread.sleep(1500);
			long answered = System.nanoTime();
			assertThat(get(client, 3, "nothing.idle.test")).as("answered by the relay: no route").isTrue();
			assertThat(client.awaitTrue(client::closed, WITHIN)).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - answered)).isGreaterThan(NOT_BEFORE);
			assertThat(client.goAwayLastStreamId()).isEqualTo(3);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	/**
	 * A client flooding PINGs without reading blocks the relay's write of a PING ack, and
	 * the GOAWAY at the idle timeout behind it: the transport is closed under the blocked
	 * writes after the teardown grace.
	 */
	@Test
	void anIdleH2ConnectionWhoseClientStoppedReadingIsClosedAfterTheTeardownGrace() throws Exception {
		double idle = idleClosed();
		try (Socket socket = new Socket()) {
			socket.setReceiveBufferSize(4096);
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort));
			socket.setSoTimeout(5000);
			OutputStream out = socket.getOutputStream();
			TestH2.writeClientHead(out, 1, ":authority=" + H2_HOST, ":method=GET", ":path=/");
			InputStream in = socket.getInputStream();
			TestH2.@Nullable Frame frame;
			do {
				frame = TestH2.readFrame(in);
			}
			while (frame != null && !(frame.streamId() == 1 && frame.type() == TestH2.DATA
					&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0));
			assertThat(frame).as("the stream's response").isNotNull();
			long ended = System.nanoTime();
			AtomicLong progress = new AtomicLong(System.nanoTime());
			byte[] pings = new byte[1000 * 17];
			ByteArrayOutputStream ping = new ByteArrayOutputStream();
			TestH2.writeFrame(ping, TestH2.PING, 0, 0, "flooding".getBytes(StandardCharsets.US_ASCII));
			for (int i = 0; i < 1000; i++) {
				System.arraycopy(ping.toByteArray(), 0, pings, i * 17, 17);
			}
			Thread flooder = Thread.ofVirtual().name("e2e-idle-flooder").start(() -> {
				try {
					while (true) {
						out.write(pings);
						progress.set(System.nanoTime());
					}
				}
				catch (IOException e) {
					// closed under the write
				}
			});
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> System.nanoTime() - progress.get() > Duration.ofMillis(300).toNanos());
			assertThat(Duration.ofNanos(progress.get() - ended))
				.as("the relay stopped reading, blocked on a write, before the idle timeout")
				.isLessThan(IDLE_TIMEOUT.minusMillis(500));
			assertThat(admitted()).isEqualTo(1);
			Awaitility.await()
				.atMost(IDLE_TIMEOUT.plus(ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE).plusSeconds(2))
				.until(() -> admitted() == 0);
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).as("after the idle timeout and the grace")
				.isGreaterThan(IDLE_TIMEOUT.plus(ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE).minusMillis(500));
			flooder.interrupt();
		}
		awaitNoDeadline();
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	private static Socket connect() throws IOException {
		Socket socket = new Socket("127.0.0.1", dataPort);
		socket.setSoTimeout(5000);
		return socket;
	}

	private static byte[] get(String path) {
		return request("GET " + path + " HTTP/1.1");
	}

	private static byte[] request(String requestLine) {
		return request(requestLine, H1_HOST, "");
	}

	private static byte[] request(String requestLine, String headers) {
		return request(requestLine, H1_HOST, headers);
	}

	private static byte[] request(String requestLine, String host, String headers) {
		return (requestLine + "\r\nHost: " + host + "\r\n" + headers + "\r\n").getBytes(StandardCharsets.US_ASCII);
	}

	/** CSV values carry {@code \r\n} escaped. */
	private static String unescape(String value) {
		return value.replace("\\r", "\r").replace("\\n", "\n");
	}

	/** Writes the bytes and reads exactly the expected response. */
	private static void exchange(Socket socket, byte[] request, String expected) throws IOException {
		socket.getOutputStream().write(request);
		socket.getOutputStream().flush();
		assertThat(TestSockets.readUntil(socket.getInputStream(), expected.length())).isEqualTo(expected);
	}

	/** Sends a GET on the stream; whether its answer ended it. */
	private static boolean get(H2TestClient client, int streamId, String host) throws Exception {
		client.headers(streamId, true, TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/"));
		return client.awaitEnd(streamId, Duration.ofSeconds(5));
	}

	/**
	 * The h1 upstream: {@code ok} to every request, except bodiless answers to HEAD,
	 * {@code /304} and {@code /204}, an Upgrade-advertising {@code 200} to
	 * {@code /advertise}, a {@code 200} after {@link #SLOW} to {@code /slow}, and a
	 * WebSocket upgrade echoing from then on.
	 */
	private static void serveHttp1(Socket socket, String ok) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			String head;
			while ((head = readHead(in)) != null) {
				if (head.toLowerCase(Locale.ROOT).contains("upgrade: websocket")) {
					out.write(SWITCHING.getBytes(StandardCharsets.US_ASCII));
					out.flush();
					in.transferTo(out);
					return;
				}
				String requestLine = head.substring(0, head.indexOf("\r\n"));
				String response = switch (requestLine) {
					case "HEAD / HTTP/1.1" -> "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n";
					case "GET /304 HTTP/1.1" -> "HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n";
					case "GET /204 HTTP/1.1" -> "HTTP/1.1 204 No Content\r\n\r\n";
					case "GET /advertise HTTP/1.1" ->
						"HTTP/1.1 200 OK\r\nUpgrade: h2c\r\nConnection: Upgrade\r\nContent-Length: 2\r\n\r\nok";
					case "GET /slow HTTP/1.1" -> {
						Thread.sleep(SLOW.toMillis());
						yield ok;
					}
					default -> ok;
				};
				out.write(response.getBytes(StandardCharsets.US_ASCII));
				out.flush();
			}
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** The bytes up to and including the blank line; null at EOF. */
	private static @Nullable String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			String text = head.toString(StandardCharsets.US_ASCII);
			if (text.endsWith("\r\n\r\n")) {
				return text;
			}
		}
		return null;
	}

	/** An h2 upstream answering every ended request after {@code delay}. */
	private static void serveH2(Socket socket, Duration delay) {
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
					Thread.sleep(delay.toMillis());
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private ServerSocket upstream(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-idle-accept").start(() -> {
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

	/** A self-signed keystore with SANs for localhost, via keytool. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-e2e", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=localhost", "-ext",
				"san=dns:localhost,ip:127.0.0.1", "-validity", "1", "-keystore", keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		return keystore;
	}

	private static SSLContext clientSslContext() throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(Objects.requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keyStore);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		return sslContext;
	}

	private void awaitAdmitted(int expected) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == expected);
	}

	private void awaitNoDeadline() {
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> connectionLimits().scheduledDeadlines() == 0);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double idleClosed() {
		return meterRegistry().get("sluice.data.connections.idle.closed").counter().count();
	}

	private double headTimeouts() {
		return meterRegistry().get("sluice.data.connections.rejected").tag("reason", "head-timeout").counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private ConnectionLimits connectionLimits() {
		return Objects.requireNonNull(this.connectionLimits, "connectionLimits");
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

}
