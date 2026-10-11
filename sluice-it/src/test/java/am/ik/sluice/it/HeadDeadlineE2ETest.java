package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The head deadline: one deadline for the whole head phase of a data plane connection --
 * TLS handshake and request head -- not a timeout per read, so a peer dripping bytes is
 * cut as surely as a silent one. An established relay has no deadline.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HeadDeadlineE2ETest {

	private static final Duration HEAD_TIMEOUT = Duration.ofSeconds(2);

	/** The earliest a deadline cut may come; well past any per-read gap of the tests. */
	private static final Duration NOT_BEFORE = Duration.ofMillis(1500);

	private static final Duration WITHIN = Duration.ofSeconds(3);

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static @Nullable HoldingUpstream upstream;

	private @Nullable TunnelClient client;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@BeforeAll
	static void startUpstream() throws IOException {
		upstream = HoldingUpstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.head-timeout", () -> HEAD_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
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

	@BeforeEach
	void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host("ws.local").target("http://127.0.0.1:" + upstream().port()).build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-deadline-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	@AfterAll
	void tearDown() throws IOException {
		if (this.client != null) {
			this.client.stop();
		}
		upstream().close();
	}

	@Test
	void aSilentConnectionIsClosedAtTheDeadline() throws Exception {
		double timeouts = headTimeouts();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		assertThat(headTimeouts()).isEqualTo(timeouts + 1);
		awaitAdmitted(0);
	}

	@Test
	void aConnectionDrippingItsHeadIsClosedAtTheDeadline() throws Exception {
		double timeouts = headTimeouts();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			byte[] head = TestSockets.get("ws.local");
			Thread dripper = Thread.ofVirtual().name("e2e-dripper").start(() -> {
				try {
					OutputStream out = socket.getOutputStream();
					// one byte every 500ms never trips a 2s timeout per read
					for (int i = 0; i < head.length - 1; i++) {
						out.write(head[i]);
						out.flush();
						Thread.sleep(500);
					}
				}
				catch (IOException | InterruptedException e) {
					// closed by the deadline
				}
			});
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
			dripper.interrupt();
		}
		assertThat(headTimeouts()).isEqualTo(timeouts + 1);
	}

	@Test
	void aStalledTlsHandshakeIsClosedAtTheDeadline() throws Exception {
		double timeouts = headTimeouts();
		SSLEngine engine = clientSslContext().createSSLEngine();
		engine.setUseClientMode(true);
		engine.beginHandshake();
		ByteBuffer hello = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
		engine.wrap(ByteBuffer.allocate(0), hello);
		hello.flip();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			// the ClientHello and nothing after: the server waits for the client's
			// handshake messages
			socket.getOutputStream().write(hello.array(), 0, hello.limit());
			socket.getOutputStream().flush();
			Duration closed = awaitClosedReadingAll(socket);
			assertThat(closed).isGreaterThan(NOT_BEFORE).isLessThan(WITHIN);
		}
		assertThat(headTimeouts()).isEqualTo(timeouts + 1);
	}

	@Test
	void aTlsConnectionSilentAfterItsHandshakeIsClosedAtTheDeadline() throws Exception {
		double timeouts = headTimeouts();
		long start = System.nanoTime();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.startHandshake();
			TestSockets.awaitClosed(socket, WITHIN);
			// one deadline covers handshake and head together, counted from the accept
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThan(NOT_BEFORE).isLessThan(WITHIN);
		}
		assertThat(headTimeouts()).isEqualTo(timeouts + 1);
	}

	@Test
	void anEstablishedRelayIdleLongerThanTheDeadlineStaysOpen() throws Exception {
		double timeouts = headTimeouts();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			OutputStream out = socket.getOutputStream();
			out.write(("GET /ws HTTP/1.1\r\nHost: ws.local\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			assertThat(TestSockets.readUntil(in, HoldingUpstream.SWITCHING.length()))
				.isEqualTo(HoldingUpstream.SWITCHING);
			Thread.sleep(HEAD_TIMEOUT.multipliedBy(2).plusSeconds(1).toMillis());
			out.write("ping".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			assertThat(TestSockets.readUntil(in, 4)).isEqualTo("ping");
		}
		assertThat(headTimeouts()).isEqualTo(timeouts);
	}

	/** Reads (and drops) what the server sends until it closes; the time it took. */
	private static Duration awaitClosedReadingAll(Socket socket) throws IOException {
		long start = System.nanoTime();
		socket.setSoTimeout((int) WITHIN.toMillis());
		try {
			socket.getInputStream().transferTo(OutputStream.nullOutputStream());
		}
		catch (java.net.SocketException e) {
			// reset
		}
		return Duration.ofNanos(System.nanoTime() - start);
	}

	private void awaitAdmitted(int expected) {
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> meterRegistry().get("sluice.data.connections.admitted").gauge().value() == expected);
	}

	private double headTimeouts() {
		return meterRegistry().get("sluice.data.connections.rejected").tag("reason", "head-timeout").counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
