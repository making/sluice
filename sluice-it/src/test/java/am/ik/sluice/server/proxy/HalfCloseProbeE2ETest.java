package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.TestSockets;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * The probe of an HTTP/1.1 connection whose client ended its side while a response is
 * owed: TCP tells a half-close from a close only on a write, so after
 * {@code sluice.limits.half-close-probe-interval} of quiet the relay writes a
 * {@code 100 Continue}. A client gone fails the second write at the latest, which fails
 * the tunnel connection (ERROR) and releases the upstream; a client that only half-closed
 * reads the interim responses, at most four per response, and then its response. Nothing
 * is injected into a response begun, after an upgrade, or towards an HTTP/1.0 request.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HalfCloseProbeE2ETest {

	private static final Duration INTERVAL = Duration.ofMillis(500);

	/** What every probe writes. */
	private static final String PROBE = "HTTP/1.1 100 Continue\r\n\r\n";

	/** The probes one response may get. */
	private static final int MAX_PROBES = 4;

	/** Never answers: reads the request and its end, then holds until released. */
	private static final String HOLD_HOST = "hold.probe.test";

	/** Answers on the test's gate, see {@link #serveGated}. */
	private static final String GATED_HOST = "gated.probe.test";

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	private static final String NOW = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nnow";

	private static final String SWITCHING = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
			+ "Connection: Upgrade\r\n\r\n";

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private @Nullable TunnelClient client;

	/** Opened per test: lets the gated upstream answer. */
	private volatile Semaphore gate = new Semaphore(0);

	/** Opened per test: lets a holding upstream connection close. */
	private volatile Semaphore release = new Semaphore(0);

	/** Counted down per test when a holding upstream connection read its request. */
	private volatile CountDownLatch held = new CountDownLatch(1);

	/**
	 * Counted down per test when a holding upstream connection read the request's end.
	 */
	private volatile CountDownLatch ended = new CountDownLatch(1);

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private SluiceServerProperties properties;

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
		registry.add("sluice.limits.half-close-probe-interval", () -> INTERVAL.toMillis() + "ms");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClient() {
		ServerSocket hold = upstream(this::serveHolding);
		ServerSocket gated = upstream(this::serveGated);
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(HOLD_HOST).target("http://127.0.0.1:" + hold.getLocalPort()).build())
				.upstream(
						Upstream.builder().host(GATED_HOST).target("http://127.0.0.1:" + gated.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-probe-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> router().lookup(HOLD_HOST).isPresent() && router().lookup(GATED_HOST).isPresent());
	}

	@BeforeEach
	void awaitQuiet() {
		// the previous test's connections are gone on both sides
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> dataProxyServer().activeConnections() == 0 && upstreamConnections() == 0);
		this.gate = new Semaphore(0);
		this.release = new Semaphore(0);
		this.held = new CountDownLatch(1);
		this.ended = new CountDownLatch(1);
	}

	@AfterEach
	void openGates() {
		// a test failing midway leaves no upstream waiting
		this.gate.release(100);
		this.release.release(100);
		dataProxyServer().clientPipeSeam(UnaryOperator.identity());
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(HalfCloseProbeE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void theConfiguredIntervalIsInEffect() {
		assertThat(Objects.requireNonNull(this.properties).limits().halfCloseProbeInterval()).isEqualTo(INTERVAL);
	}

	/**
	 * Half-closed, then closed, while the upstream holds the request: the probe's writes
	 * fail, the relay ends and its tunnel connection is failed, which closes the upstream
	 * connection on the client side.
	 */
	@Test
	void aClientGoneAfterItsHalfCloseIsNoticedByTheProbe() throws Exception {
		AtomicReference<@Nullable RecordingPipe> pipe = recordClientPipe();
		Socket socket = new Socket("127.0.0.1", dataPort);
		try (socket) {
			socket.getOutputStream().write(get("/", HOLD_HOST, "1.1"));
			socket.getOutputStream().flush();
			assertThat(this.held.await(5, TimeUnit.SECONDS)).as("the upstream read the request").isTrue();
			socket.shutdownOutput();
			assertThat(this.ended.await(5, TimeUnit.SECONDS)).as("the half-close reached the upstream").isTrue();
			assertThat(dataProxyServer().activeConnections()).as("the relay is live").isEqualTo(1);
			assertThat(upstreamConnections()).as("so is the upstream connection").isEqualTo(1);
		}
		awaitEndedAtAProbe(pipe);
	}

	/** The same over TLS: the client's close_notify reads as the end of its side. */
	@Test
	void aTlsClientGoneAfterItsRequestIsNoticedByTheProbe() throws Exception {
		AtomicReference<@Nullable RecordingPipe> pipe = recordClientPipe();
		SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort);
		try (socket) {
			SSLParameters parameters = socket.getSSLParameters();
			parameters.setServerNames(List.of(new SNIHostName(HOLD_HOST)));
			parameters.setApplicationProtocols(new String[] { "http/1.1" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			socket.getOutputStream().write(get("/", HOLD_HOST, "1.1"));
			socket.getOutputStream().flush();
			assertThat(this.held.await(5, TimeUnit.SECONDS)).as("the upstream read the request").isTrue();
		}
		assertThat(this.ended.await(5, TimeUnit.SECONDS)).as("the close reached the upstream as a half-close").isTrue();
		awaitEndedAtAProbe(pipe);
	}

	/**
	 * Records the client pipe of the relays started from now on, the latest in the
	 * reference; {@link #openGates} resets the seam.
	 */
	private AtomicReference<@Nullable RecordingPipe> recordClientPipe() {
		AtomicReference<@Nullable RecordingPipe> recorded = new AtomicReference<>();
		dataProxyServer().clientPipeSeam(pipe -> {
			RecordingPipe recording = new RecordingPipe(pipe);
			recorded.set(recording);
			return recording;
		});
		return recorded;
	}

	/**
	 * Awaits the end of the relay and its upstream connection, and asserts the relay
	 * wrote nothing but probes to its gone client, the second write failing at the
	 * latest.
	 */
	private void awaitEndedAtAProbe(AtomicReference<@Nullable RecordingPipe> pipe) {
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> dataProxyServer().activeConnections() == 0 && upstreamConnections() == 0);
		assertThat(Objects.requireNonNull(pipe.get()).writes()).as("ended at the first probe or the second")
			.extracting(write -> new String(write.bytes(), StandardCharsets.US_ASCII))
			.hasSizeBetween(1, 2)
			.containsOnly(PROBE);
	}

	/**
	 * A client that only half-closed reads a probe after the interval, then the response.
	 */
	@Test
	void aClientThatOnlyHalfClosedGetsTheProbeThenTheResponse() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(get("/", GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			long sent = System.nanoTime();
			socket.shutdownOutput();
			String probe = TestSockets.readUntil(socket.getInputStream(), PROBE.length());
			assertThat(Duration.ofNanos(System.nanoTime() - sent)).as("not before the interval of quiet")
				.isGreaterThanOrEqualTo(INTERVAL.minusMillis(50));
			this.gate.release();
			assertThat(probe + readToEnd(socket)).isEqualTo(PROBE + OK);
		}
	}

	/**
	 * A response coming within the interval of its request gets no probe, though the
	 * connection idled longer than the interval before that request.
	 */
	@Test
	void aPromptResponseGetsNoProbe() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(get("/now", GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			assertThat(TestSockets.readUntil(socket.getInputStream(), NOW.length())).isEqualTo(NOW);
			Thread.sleep(INTERVAL.multipliedBy(2).toMillis());
			socket.getOutputStream().write(get("/now", GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertThat(readToEnd(socket)).isEqualTo(NOW);
		}
	}

	/** One response gets at most four probes, however long it stalls. */
	@Test
	void probesStopAtFourPerResponse() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(get("/", GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertThat(TestSockets.readUntil(socket.getInputStream(), PROBE.length() * MAX_PROBES))
				.isEqualTo(PROBE.repeat(MAX_PROBES));
			assertNothingFor(socket, INTERVAL.multipliedBy(3));
			this.gate.release();
			assertThat(readToEnd(socket)).isEqualTo(OK);
		}
	}

	/**
	 * A request expecting a {@code 100 Continue}, its body sent and the client's side
	 * ended: the upstream's own 100 is relayed, and the probes follow it until the final
	 * response.
	 */
	@Test
	void probesFollowTheUpstreamsOwnInterimResponse() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream()
				.write(request("POST /expect HTTP/1.1", GATED_HOST, "Expect: 100-continue\r\nContent-Length: 2\r\n"));
			socket.getOutputStream().write("hi".getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			String interims = TestSockets.readUntil(socket.getInputStream(), PROBE.length() * 2);
			this.gate.release();
			assertThat(interims + readToEnd(socket)).isEqualTo(PROBE + PROBE + OK);
		}
	}

	/**
	 * Pipelined requests: the probe goes between the first response, relayed whole, and
	 * the second, which it then precedes.
	 */
	@Test
	void aProbeBetweenPipelinedResponsesPrecedesTheNext() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			OutputStream out = socket.getOutputStream();
			out.write(get("/now", GATED_HOST, "1.1"));
			out.write(get("/", GATED_HOST, "1.1"));
			out.flush();
			socket.shutdownOutput();
			String first = TestSockets.readUntil(socket.getInputStream(), NOW.length() + PROBE.length());
			this.gate.release();
			assertThat(first + readToEnd(socket)).isEqualTo(NOW + PROBE + OK);
		}
	}

	/**
	 * A response the upstream stalls in -- within its head, within its body, or one whose
	 * framing the relay cannot follow -- takes no probe: it would corrupt the response. A
	 * pipelined request waits behind it, so a request is owed its response throughout.
	 */
	@ParameterizedTest
	@CsvSource({ "/partial-head, HTTP/1.1 200 OK\\r\\nContent-, Length: 2\\r\\n\\r\\nok",
			"/partial-body, HTTP/1.1 200 OK\\r\\nContent-Length: 4\\r\\n\\r\\nok, ok",
			"/untracked, NOT HTTP\\r\\n\\r\\n, rest" })
	void aResponseBegunTakesNoProbe(String path, String before, String after) throws Exception {
		String first = unescape(before);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(get(path, GATED_HOST, "1.1"));
			socket.getOutputStream().write(get("/now", GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertThat(TestSockets.readUntil(socket.getInputStream(), first.length())).isEqualTo(first);
			assertNothingFor(socket, INTERVAL.multipliedBy(3));
			this.gate.release();
			assertThat(readToEnd(socket)).isEqualTo(unescape(after) + NOW);
		}
	}

	/**
	 * Nor is a client gone within a response noticed: it gets no probe, and the relay
	 * waits for the upstream.
	 */
	@ParameterizedTest
	@CsvSource({ "/partial-head, HTTP/1.1 200 OK\\r\\nContent-",
			"/partial-body, HTTP/1.1 200 OK\\r\\nContent-Length: 4\\r\\n\\r\\nok" })
	void aClientGoneWithinAResponseIsNotNoticed(String path, String before) throws Exception {
		String first = unescape(before);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(get(path, GATED_HOST, "1.1"));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertThat(TestSockets.readUntil(socket.getInputStream(), first.length())).isEqualTo(first);
		}
		Thread.sleep(INTERVAL.multipliedBy(4).toMillis());
		assertThat(dataProxyServer().activeConnections()).as("the relay still waits").isEqualTo(1);
		assertThat(upstreamConnections()).isEqualTo(1);
		this.gate.release();
	}

	/** An upgraded connection is relayed verbatim: its half-close takes no probe. */
	@Test
	void anUpgradedConnectionTakesNoProbe() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream()
				.write(request("GET / HTTP/1.1", GATED_HOST, "Upgrade: websocket\r\nConnection: Upgrade\r\n"));
			socket.getOutputStream().flush();
			assertThat(TestSockets.readUntil(socket.getInputStream(), SWITCHING.length())).isEqualTo(SWITCHING);
			socket.getOutputStream().write('x');
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			assertNothingFor(socket, INTERVAL.multipliedBy(3));
			this.gate.release();
			assertThat(readToEnd(socket)).isEqualTo("x");
		}
	}

	/**
	 * An HTTP/1.0 request takes no 1xx (RFC 9110 15.2): a client gone after it is not
	 * noticed, and the relay waits for the upstream.
	 */
	@Test
	void anHttp10RequestTakesNoProbe() throws Exception {
		Socket socket = new Socket("127.0.0.1", dataPort);
		try (socket) {
			socket.getOutputStream().write(get("/", HOLD_HOST, "1.0"));
			socket.getOutputStream().flush();
			assertThat(this.held.await(5, TimeUnit.SECONDS)).isTrue();
			socket.shutdownOutput();
			assertThat(this.ended.await(5, TimeUnit.SECONDS)).isTrue();
			assertNothingFor(socket, INTERVAL.multipliedBy(3));
		}
		Thread.sleep(INTERVAL.multipliedBy(3).toMillis());
		assertThat(dataProxyServer().activeConnections()).as("the relay still waits").isEqualTo(1);
		assertThat(upstreamConnections()).isEqualTo(1);
		this.release.release();
	}

	/**
	 * While a request asking for an upgrade awaits its answer the relay reads nothing
	 * from the client, so it does not see the client's end: a client gone then is not
	 * noticed.
	 */
	@Test
	void aClientGoneWhileItsUpgradeAwaitsTheAnswerIsNotNoticed() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.getOutputStream()
				.write(request("GET / HTTP/1.1", HOLD_HOST, "Upgrade: websocket\r\nConnection: Upgrade\r\n"));
			socket.getOutputStream().flush();
			assertThat(this.held.await(5, TimeUnit.SECONDS)).isTrue();
		}
		Thread.sleep(INTERVAL.multipliedBy(4).toMillis());
		assertThat(this.ended.getCount()).as("the client's end was not relayed").isEqualTo(1);
		assertThat(dataProxyServer().activeConnections()).as("the relay still waits").isEqualTo(1);
		assertThat(upstreamConnections()).isEqualTo(1);
		this.release.release();
	}

	private static byte[] get(String path, String host, String version) {
		return request("GET " + path + " HTTP/" + version, host, "");
	}

	private static byte[] request(String requestLine, String host, String headers) {
		return (requestLine + "\r\nHost: " + host + "\r\n" + headers + "\r\n").getBytes(StandardCharsets.US_ASCII);
	}

	private static String unescape(String value) {
		return value.replace("\\r", "\r").replace("\\n", "\n");
	}

	/** Reads until EOF. */
	private static String readToEnd(Socket socket) throws IOException {
		return new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
	}

	/** Fails when a byte or the end arrives within {@code quiet}. */
	private static void assertNothingFor(Socket socket, Duration quiet) throws IOException {
		int timeout = socket.getSoTimeout();
		socket.setSoTimeout((int) quiet.toMillis());
		try {
			assertThatExceptionOfType(SocketTimeoutException.class).isThrownBy(() -> socket.getInputStream().read());
		}
		finally {
			socket.setSoTimeout(timeout);
		}
	}

	/**
	 * Reads the request and, meanwhile, the end of the client's side; holds the
	 * connection until the test releases it.
	 */
	private void serveHolding(Socket socket) {
		// the latches of the test this connection belongs to
		CountDownLatch held = this.held;
		CountDownLatch ended = this.ended;
		Semaphore release = this.release;
		try (socket; InputStream in = socket.getInputStream()) {
			if (readHead(in) == null) {
				return;
			}
			held.countDown();
			Thread.ofVirtual().name("e2e-probe-hold-read").start(() -> {
				try {
					in.transferTo(OutputStream.nullOutputStream());
					ended.countDown();
				}
				catch (IOException e) {
					// closed
				}
			});
			release.acquire();
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Answers each request: {@code /now} with {@link #NOW} at once; {@code /} with
	 * {@link #OK} once the gate opens; {@code /partial-head} and {@code /partial-body}
	 * with the first part of {@link #OK}-like responses at once and the rest once the
	 * gate opens; {@code /expect} with a {@code 100 Continue} at once and {@link #OK}
	 * once the gate opens; {@code /untracked} with a head that is no HTTP response, and
	 * more once the gate opens; an upgrade with {@link #SWITCHING}, echoing from then on
	 * once the gate opens. Closes at the client's end.
	 */
	private void serveGated(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			String head;
			while ((head = readHead(in)) != null) {
				String path = head.substring(head.indexOf(' ') + 1, head.indexOf(' ', head.indexOf(' ') + 1));
				if (head.toLowerCase(Locale.ROOT).contains("upgrade: websocket")) {
					write(out, SWITCHING);
					this.gate.acquire();
					in.transferTo(out);
					return;
				}
				switch (path) {
					case "/now" -> write(out, NOW);
					case "/partial-head" -> {
						write(out, "HTTP/1.1 200 OK\r\nContent-");
						this.gate.acquire();
						write(out, "Length: 2\r\n\r\nok");
					}
					case "/expect" -> {
						write(out, "HTTP/1.1 100 Continue\r\n\r\n");
						this.gate.acquire();
						write(out, OK);
					}
					case "/untracked" -> {
						write(out, "NOT HTTP\r\n\r\n");
						this.gate.acquire();
						write(out, "rest");
					}
					case "/partial-body" -> {
						write(out, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nok");
						this.gate.acquire();
						write(out, "ok");
					}
					default -> {
						this.gate.acquire();
						write(out, OK);
					}
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

	private static void write(OutputStream out, String text) throws IOException {
		out.write(text.getBytes(StandardCharsets.US_ASCII));
		out.flush();
	}

	/** The bytes up to and including the blank line; null at EOF. */
	private static @Nullable String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			if (head.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) {
				return head.toString(StandardCharsets.US_ASCII);
			}
		}
		return null;
	}

	private ServerSocket upstream(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-probe-accept").start(() -> {
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

	/** Upstream connections the tunnel client holds open. */
	private int upstreamConnections() {
		return (int) this.clientMeters.get("sluice.connections.active").gauge().value();
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

}
