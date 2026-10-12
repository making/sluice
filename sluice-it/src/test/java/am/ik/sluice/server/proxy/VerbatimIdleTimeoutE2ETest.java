package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
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
 * The verbatim idle timeout a route sets on the upstream it advertises, with no server
 * default: a connection relayed verbatim -- a tcp route, TLS passthrough, a hostless
 * connection (plaintext or TLS-terminated), an upgraded HTTP/1.1 connection -- that
 * relays no byte in either direction for the timeout is closed and its slot released,
 * also while the relay is blocked writing to a client that stopped reading; bytes flowing
 * in either direction keep it open; a route without one keeps its connections open. In
 * the relay's package for the deadline queue of {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class VerbatimIdleTimeoutE2ETest {

	private static final Duration TIMEOUT = Duration.ofSeconds(2);

	/** The latest a cut may come: the timeout, the teardown grace, and slack. */
	private static final Duration WITHIN = TIMEOUT.plusSeconds(4);

	/** How long a connection that must stay open is watched. */
	private static final Duration STAY = TIMEOUT.multipliedBy(3);

	/** How often a connection that must stay open relays a byte. */
	private static final Duration EVERY = Duration.ofSeconds(1);

	private static final String PASSTHROUGH_HOST = "pass.test";

	private static final String PASSTHROUGH_OFF_HOST = "pass-off.test";

	private static final String TERMINATED_HOST = "resp.test";

	private static final String UPGRADE_HOST = "ws.test";

	private static int grpcPort;

	private static int dataPort;

	private static int timedPort;

	private static int untimedPort;

	private static @Nullable Path keystorePath;

	private final CommandUpstream upstream = CommandUpstream.start();

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
		timedPort = TestPorts.freePort();
		untimedPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> timedPort + "," + untimedPort);
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClient() {
		String target = "tcp://127.0.0.1:" + this.upstream.port();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder()
					.host("timed.tcp")
					.target(target)
					.listenPort(timedPort)
					.verbatimIdleTimeout(TIMEOUT)
					.build())
				.upstream(Upstream.builder().host("untimed.tcp").target(target).listenPort(untimedPort).build())
				.upstream(Upstream.builder()
					.host(PASSTHROUGH_HOST)
					.target(target)
					.tlsPassthrough(true)
					.verbatimIdleTimeout(TIMEOUT)
					.build())
				.upstream(Upstream.builder().host(PASSTHROUGH_OFF_HOST).target(target).tlsPassthrough(true).build())
				.upstream(Upstream.builder().host(TERMINATED_HOST).target(target).verbatimIdleTimeout(TIMEOUT).build())
				.upstream(Upstream.builder().host(UPGRADE_HOST).target(target).verbatimIdleTimeout(TIMEOUT).build())
				// the catch-all route: reached by hostless plaintext heads
				.upstream(Upstream.builder().host("").target(target).verbatimIdleTimeout(TIMEOUT).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-verbatim-idle-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> router().lookupByPort(timedPort).isPresent() && router().lookupByPort(untimedPort).isPresent()
					&& List.of(PASSTHROUGH_HOST, PASSTHROUGH_OFF_HOST, TERMINATED_HOST, UPGRADE_HOST)
						.stream()
						.allMatch(host -> router().lookup(host).isPresent()));
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> reachable(timedPort) && reachable(untimedPort));
	}

	@BeforeEach
	void awaitQuiet() {
		// every connection of the previous test is gone, and so are its deadlines
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admitted() == 0);
		awaitNoDeadline();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.upstream.close();
	}

	@Test
	void theTimeoutReachesTheServerAsTheRouteSetIt() {
		assertThat(timeoutOf(router().lookupByPort(timedPort).orElseThrow())).isEqualTo(TIMEOUT);
		assertThat(timeoutOf(router().lookup(PASSTHROUGH_HOST).orElseThrow())).isEqualTo(TIMEOUT);
		assertThat(timeoutOf(router().lookup(TERMINATED_HOST).orElseThrow())).isEqualTo(TIMEOUT);
		assertThat(timeoutOf(router().lookup(UPGRADE_HOST).orElseThrow())).isEqualTo(TIMEOUT);
		// no route value and no server default: none
		assertThat(router().lookupByPort(untimedPort).orElseThrow().verbatimIdleTimeout()).isZero();
		assertThat(timeoutOf(router().lookupByPort(untimedPort).orElseThrow())).isZero();
		assertThat(timeoutOf(router().lookup(PASSTHROUGH_OFF_HOST).orElseThrow())).isZero();
	}

	@Test
	void anIdleTcpRouteConnectionIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		double closed = verbatimIdleClosed();
		int ended = this.upstream.ended();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			long start = System.nanoTime();
			send(socket, "sink\r\n");
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 1);
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
			TestSockets.awaitClosed(socket, WITHIN);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
			// released while the client still holds its end
			awaitReleased(closed);
		}
		// the tunnel connection went with it: the upstream saw its end
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.upstream.ended() > ended);
	}

	@Test
	void aTcpRouteConnectionEchoingEverySecondStaysOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			socket.setSoTimeout(5000);
			send(socket, "echo\r\n");
			for (long start = System.nanoTime(); System.nanoTime() - start < STAY.toNanos();) {
				exchange(socket, "ping");
				Thread.sleep(EVERY.toMillis());
			}
			exchange(socket, "still open");
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	@Test
	void aTcpRouteConnectionSendingOnlyTowardsItsUpstreamStaysOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			send(socket, "sink\r\n");
			long received = this.upstream.received();
			int sent = 0;
			for (long start = System.nanoTime(); System.nanoTime() - start < STAY.toNanos();) {
				send(socket, "x");
				sent++;
				Thread.sleep(EVERY.toMillis());
			}
			// open past three timeouts, the last byte less than one ago
			assertThat(verbatimIdleClosed()).isEqualTo(closed);
			assertThat(admitted()).isEqualTo(1);
			// the bytes reached the upstream, which never answered: one direction only
			int expected = sent;
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> this.upstream.received() - received >= expected);
		}
	}

	@Test
	void aTcpRouteConnectionReceivingOnlyFromItsUpstreamStaysOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			socket.setSoTimeout(5000);
			send(socket, "push\r\n");
			InputStream in = socket.getInputStream();
			int pushed = 0;
			for (long start = System.nanoTime(); System.nanoTime() - start < STAY.toNanos();) {
				assertThat(in.read()).isEqualTo('.');
				pushed++;
			}
			assertThat(pushed)
				.isGreaterThanOrEqualTo((int) (STAY.toMillis() / CommandUpstream.PUSH_EVERY.toMillis()) - 1);
			assertThat(in.read()).isEqualTo('.');
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	/**
	 * The relay blocks writing to a client that stopped reading: the expiry closes the
	 * transport under the write, which frees the slot while the client still holds its
	 * end, unread.
	 */
	@Test
	void aTcpRouteClientNotReadingIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket()) {
			socket.setReceiveBufferSize(16 * 1024);
			socket.connect(new InetSocketAddress("127.0.0.1", timedPort));
			long flooded = this.upstream.flooded();
			send(socket, "flood\r\n");
			// the flood is under way, past what the client's receive buffer holds
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> this.upstream.flooded() - flooded > 1024 * 1024);
			awaitReleased(closed);
		}
	}

	@Test
	void aTcpRouteConnectionClosedByItsClientLeavesNoDeadlineBehind() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			socket.setSoTimeout(5000);
			send(socket, "echo\r\n");
			exchange(socket, "ping");
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		awaitNoDeadline();
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	@Test
	void aTcpRouteConnectionEndedByBothSidesLeavesNoDeadlineBehind() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			send(socket, "quit\r\n");
			// the upstream's end arrives as a half-close; the client ends its side too
			assertThat(TestSockets.awaitClosed(socket, Duration.ofSeconds(5))).isLessThan(TIMEOUT);
			socket.shutdownOutput();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
			awaitNoDeadline();
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	/**
	 * The upstream ended its side, the client keeps its own open and sends nothing: the
	 * half-closed relay holds its slot until the timeout.
	 */
	@Test
	void aTcpRouteConnectionHalfClosedByItsUpstreamIsClosedAtTheTimeout() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", timedPort)) {
			send(socket, "quit\r\n");
			long start = System.nanoTime();
			assertThat(TestSockets.awaitClosed(socket, Duration.ofSeconds(5))).isLessThan(TIMEOUT);
			assertThat(admitted()).isEqualTo(1);
			awaitReleased(closed);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
		}
	}

	@Test
	void anIdleTlsPassthroughConnectionIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		double closed = verbatimIdleClosed();
		int helloes = this.upstream.helloes();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			long start = System.nanoTime();
			send(socket, clientHello(PASSTHROUGH_HOST));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.upstream.helloes() > helloes);
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
			TestSockets.awaitClosed(socket, WITHIN);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
			awaitReleased(closed);
		}
	}

	@Test
	void aTlsPassthroughConnectionEchoingEverySecondStaysOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			send(socket, clientHello(PASSTHROUGH_HOST));
			send(socket, "echo\r\n");
			for (long start = System.nanoTime(); System.nanoTime() - start < STAY.toNanos();) {
				exchange(socket, "ping");
				Thread.sleep(EVERY.toMillis());
			}
			exchange(socket, "still open");
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	@Test
	void aTlsPassthroughConnectionClosedByItsClientLeavesNoDeadlineBehind() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			send(socket, clientHello(PASSTHROUGH_HOST));
			send(socket, "echo\r\n");
			exchange(socket, "ping");
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		awaitNoDeadline();
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	/**
	 * The relay blocks writing to a TLS client that stopped reading; the slot is freed
	 * while the client still holds its end, unread. (A TLS socket's close waits for the
	 * write in progress: the transport closed under such a teardown is shown by
	 * {@code ConnectionLimitsTest}.)
	 */
	@Test
	void aTlsTerminatedHostlessClientNotReadingIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		double closed = verbatimIdleClosed();
		Socket plain = new Socket();
		plain.setReceiveBufferSize(16 * 1024);
		plain.connect(new InetSocketAddress("127.0.0.1", dataPort));
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory()
			.createSocket(plain, "127.0.0.1", dataPort, true)) {
			SSLParameters parameters = socket.getSSLParameters();
			parameters.setServerNames(List.of(new SNIHostName(TERMINATED_HOST)));
			socket.setSSLParameters(parameters);
			socket.setSoTimeout(5000);
			// the data plane terminates TLS: the handshake completes with its certificate
			socket.startHandshake();
			long flooded = this.upstream.flooded();
			// a RESP array: a hostless head, routed by the SNI host name
			send(socket, "*1\r\n$5\r\nflood\r\n");
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> this.upstream.flooded() - flooded > 1024 * 1024);
			awaitReleased(closed);
		}
	}

	@Test
	void anIdlePlaintextHostlessConnectionIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		assertThat(timeoutOf(router().lookup(null).orElseThrow())).isEqualTo(TIMEOUT);
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			long start = System.nanoTime();
			// a RESP array: a hostless head, routed to the catch-all route
			send(socket, "*1\r\n$4\r\nsink\r\n");
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 1);
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
			TestSockets.awaitClosed(socket, WITHIN);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
			awaitReleased(closed);
		}
	}

	@Test
	void anIdleUpgradedConnectionIsClosedAtTheTimeoutAndReleasesItsSlot() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			long start = System.nanoTime();
			upgrade(socket);
			assertThat(connectionLimits().scheduledDeadlines()).isEqualTo(1);
			TestSockets.awaitClosed(socket, WITHIN);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
			awaitReleased(closed);
		}
	}

	/**
	 * A request asking for an upgrade whose response the relay cannot frame is relayed
	 * verbatim from then on, under the timeout as well.
	 */
	@Test
	void anIdleConnectionWhoseUpgradeAnswerCannotBeFramedIsClosedAtTheTimeout() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			long start = System.nanoTime();
			send(socket, "GET /unframed HTTP/1.1\r\nHost: " + UPGRADE_HOST
					+ "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");
			assertThat(TestSockets.readUntil(socket.getInputStream(), CommandUpstream.UNFRAMED.length()))
				.isEqualTo(CommandUpstream.UNFRAMED);
			TestSockets.awaitClosed(socket, WITHIN);
			assertThat(since(start)).isGreaterThanOrEqualTo(TIMEOUT);
			awaitReleased(closed);
		}
	}

	@Test
	void anUpgradedConnectionEchoingEverySecondStaysOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			upgrade(socket);
			send(socket, "echo\r\n");
			for (long start = System.nanoTime(); System.nanoTime() - start < STAY.toNanos();) {
				exchange(socket, "ping");
				Thread.sleep(EVERY.toMillis());
			}
			exchange(socket, "still open");
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	/** Neither the route nor the server sets a timeout: idle connections stay open. */
	@Test
	void idleConnectionsOfRoutesWithoutATimeoutStayOpen() throws Exception {
		double closed = verbatimIdleClosed();
		try (Socket tcp = new Socket("127.0.0.1", untimedPort);
				Socket passthrough = new Socket("127.0.0.1", dataPort)) {
			tcp.setSoTimeout(5000);
			passthrough.setSoTimeout(5000);
			send(passthrough, clientHello(PASSTHROUGH_OFF_HOST));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 2);
			// nothing armed for either
			assertThat(connectionLimits().scheduledDeadlines()).isZero();
			Thread.sleep(STAY.toMillis());
			send(tcp, "echo\r\n");
			exchange(tcp, "still open");
			send(passthrough, "echo\r\n");
			exchange(passthrough, "still open");
		}
		assertThat(verbatimIdleClosed()).isEqualTo(closed);
	}

	private void upgrade(Socket socket) throws IOException {
		send(socket,
				"GET /ws HTTP/1.1\r\nHost: " + UPGRADE_HOST + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");
		assertThat(TestSockets.readUntil(socket.getInputStream(), CommandUpstream.SWITCHING.length()))
			.isEqualTo(CommandUpstream.SWITCHING);
	}

	private Duration timeoutOf(Router.Route route) {
		return connectionLimits().verbatimIdleTimeout(route);
	}

	/** The time passed since {@code start}, a {@link System#nanoTime()} reading. */
	private static Duration since(long start) {
		return Duration.ofNanos(System.nanoTime() - start);
	}

	private static void send(Socket socket, String text) throws IOException {
		send(socket, text.getBytes(StandardCharsets.US_ASCII));
	}

	private static void send(Socket socket, byte[] bytes) throws IOException {
		OutputStream out = socket.getOutputStream();
		out.write(bytes);
		out.flush();
	}

	/** Writes the text and reads it back from the echoing upstream. */
	private static void exchange(Socket socket, String text) throws IOException {
		send(socket, text);
		assertThat(TestSockets.readUntil(socket.getInputStream(), text.length())).isEqualTo(text);
	}

	/**
	 * A ClientHello naming {@code sni}, as a TLS client sends it first: the data plane
	 * routes the passthrough connection by it.
	 */
	private static byte[] clientHello(String sni) throws Exception {
		SSLEngine engine = SSLContext.getDefault().createSSLEngine(sni, 443);
		engine.setUseClientMode(true);
		SSLParameters parameters = engine.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(sni)));
		engine.setSSLParameters(parameters);
		engine.beginHandshake();
		ByteBuffer out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
		engine.wrap(ByteBuffer.allocate(0), out);
		out.flip();
		byte[] bytes = new byte[out.remaining()];
		out.get(bytes);
		return bytes;
	}

	private static boolean reachable(int port) {
		try (Socket socket = new Socket("127.0.0.1", port)) {
			return socket.isConnected();
		}
		catch (IOException e) {
			return false;
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

	/**
	 * Waits until the verbatim idle timeout closed one more connection than
	 * {@code closed} and the slots are free again.
	 */
	private void awaitReleased(double closed) {
		Awaitility.await().atMost(WITHIN).until(() -> verbatimIdleClosed() > closed);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		awaitNoDeadline();
	}

	private void awaitNoDeadline() {
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> connectionLimits().scheduledDeadlines() == 0);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double verbatimIdleClosed() {
		return meterRegistry().get("sluice.data.connections.verbatim.idle.closed").counter().count();
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

}
