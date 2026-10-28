package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.slf4j.LoggerFactory;

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
 * An overflow connection of the data plane limit behind a PROXY protocol header (v1 and
 * v2): the header is read first like on any data port connection, the TLS handshake and
 * the head follow it, and the {@code 503} answer's access log line names the PROXY source
 * address.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConnectionLimitOverflowProxyProtocolE2ETest {

	private static final byte[] NONE = new byte[0];

	private static final Duration WITHIN = Duration.ofSeconds(4);

	private static final byte[] V2_SIGNATURE = { 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54,
			0x0A };

	private static final Pattern PROXIED_LINE = Pattern
		.compile("type=conn id=\\d+ event=reject listener=data remote=203\\.0\\.113\\.7:32100 reason=limit "
				+ "transport=h1 status=503 durationMs=\\d+");

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ConcurrentListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static @Nullable HoldingUpstream upstream;

	private @Nullable TunnelClient client;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@BeforeAll
	static void startUpstream() throws IOException {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		upstream = HoldingUpstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = TestKeystore.create();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.data-proxy-protocol", () -> "true");
		registry.add("sluice.limits.max-connections", () -> "1");
		registry.add("sluice.limits.overflow-connections", () -> "2");
		registry.add("sluice.limits.overflow-timeout", () -> "2s");
		registry.add("sluice.limits.overflow-retry-after", () -> "7s");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeEach
	void startClient() {
		ACCESS_EVENTS.list.clear();
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("held.local")
				.target("http://127.0.0.1:" + Objects.requireNonNull(upstream).port())
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-overflow-proxy-worker").start(task))
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
		Objects.requireNonNull(upstream).close();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@Test
	void aTlsClientBehindAProxyV1HeaderPastTheLimitIsAnswered503() throws Exception {
		assertAnsweredBehind("PROXY TCP4 203.0.113.7 198.51.100.7 32100 80\r\n".getBytes(StandardCharsets.US_ASCII),
				true);
	}

	@Test
	void aTlsClientBehindAProxyV2HeaderPastTheLimitIsAnswered503() throws Exception {
		assertAnsweredBehind(proxyV2(), true);
	}

	@Test
	void aPlaintextClientBehindAProxyV1HeaderPastTheLimitIsAnswered503() throws Exception {
		assertAnsweredBehind("PROXY TCP4 203.0.113.7 198.51.100.7 32100 80\r\n".getBytes(StandardCharsets.US_ASCII),
				false);
	}

	@Test
	void aMalformedProxyHeaderInTheOverflowIsClosedUnanswered() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4)
					.put(V2_SIGNATURE)
					.put((byte) 0x21)
					.put((byte) 0x31) // family 3: unsupported
					.putShort((short) 0)
					.array();
				socket.getOutputStream().write(header);
				socket.getOutputStream().flush();
				assertThat(TestSockets.awaitClosed(socket, WITHIN)).isLessThan(Duration.ofSeconds(1));
			}
			Awaitility.await().atMost(WITHIN).until(() -> overflow() == 0);
			assertThat(rejected()).isEqualTo(rejected + 1);
			assertThat(ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage))
				.noneMatch(line -> line.contains("status=503"));
		}
	}

	private void assertAnsweredBehind(byte[] header, boolean tls) throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double answered = answered();
			try (Socket raw = new Socket("127.0.0.1", dataPort)) {
				raw.setSoTimeout((int) WITHIN.toMillis());
				raw.getOutputStream().write(header);
				raw.getOutputStream().flush();
				Socket socket = raw;
				if (tls) {
					SSLSocket ssl = (SSLSocket) TestKeystore.clientContext(Objects.requireNonNull(keystorePath))
						.getSocketFactory()
						.createSocket(raw, "127.0.0.1", dataPort, true);
					SSLParameters parameters = ssl.getSSLParameters();
					parameters.setApplicationProtocols(new String[] { "http/1.1" });
					ssl.setSSLParameters(parameters);
					ssl.startHandshake();
					socket = ssl;
				}
				socket.getOutputStream().write(TestSockets.get("held.local"));
				socket.getOutputStream().flush();
				String head = readHead(socket.getInputStream());
				assertThat(head).startsWith("HTTP/1.1 503 Service Unavailable\r\n").contains("\r\nRetry-After: 7\r\n");
			}
			assertThat(rejected()).isEqualTo(rejected + 1);
			Awaitility.await().atMost(WITHIN).until(() -> answered() == answered + 1);
			Awaitility.await().atMost(WITHIN).until(() -> overflow() == 0);
			// the PROXY source, not the socket peer, on the reject line
			Awaitility.await()
				.atMost(WITHIN)
				.untilAsserted(() -> assertThat(ACCESS_EVENTS.list.stream()
					.map(ILoggingEvent::getFormattedMessage)
					.filter(line -> PROXIED_LINE.matcher(line).matches())).hasSize(1));
		}
	}

	private static String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) {
			int b = in.read();
			assertThat(b).as("EOF in the response head").isNotNegative();
			head.write(b);
		}
		return head.toString(StandardCharsets.ISO_8859_1);
	}

	/** A v2 PROXY header for the TCP4 connection 203.0.113.7:32100 to 198.51.100.7:80. */
	private static byte[] proxyV2() {
		return ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 12)
			.put(V2_SIGNATURE)
			.put((byte) 0x21) // version 2, command PROXY
			.put((byte) 0x11) // family INET, transport STREAM
			.putShort((short) 12)
			.put(new byte[] { (byte) 203, 0, (byte) 113, 7 })
			.put(new byte[] { (byte) 198, (byte) 51, 100, 7 })
			.putShort((short) 32100)
			.putShort((short) 80)
			.array();
	}

	/** An established relay holding the one slot of the limit (no PROXY header). */
	private Socket holdTheLimit() throws IOException {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> gauge("sluice.data.connections.admitted") == 0);
		Awaitility.await().atMost(WITHIN).until(() -> overflow() == 0);
		Socket held = TestSockets.establish(dataPort, NONE, "held.local");
		assertThat(gauge("sluice.data.connections.admitted")).isEqualTo(1);
		assertThat(gauge("sluice.data.connections.limit")).isEqualTo(1);
		return held;
	}

	private int overflow() {
		return (int) gauge("sluice.data.connections.overflow");
	}

	private double gauge(String name) {
		return meterRegistry().get(name).gauge().value();
	}

	private double rejected() {
		return meterRegistry().get("sluice.data.connections.rejected").tag("reason", "limit").counter().count();
	}

	private double answered() {
		return meterRegistry().get("sluice.data.connections.overflow.answered").tag("protocol", "h1").counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

}
