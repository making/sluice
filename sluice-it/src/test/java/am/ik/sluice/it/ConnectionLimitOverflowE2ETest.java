package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The overflow of the data plane connection limit: a few connections past the limit are
 * served far enough to be answered {@code 503} with {@code Retry-After} -- the TLS
 * handshake, the head -- under a short deadline, then closed; past the overflow, and for
 * what has no HTTP to answer in (TLS passthrough, a tcp route), connections are closed as
 * before. Every place of the overflow is released on every path.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConnectionLimitOverflowE2ETest {

	private static final byte[] NONE = new byte[0];

	private static final int OVERFLOW = 2;

	private static final Duration OVERFLOW_TIMEOUT = Duration.ofSeconds(2);

	private static final Duration WITHIN = Duration.ofSeconds(4);

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ConcurrentListAppender<>();

	private static final Pattern ANSWERED_LINE = Pattern.compile(
			"type=conn id=\\d+ event=reject listener=data remote=127\\.0\\.0\\.1:\\d+ reason=limit transport=(h1|h2) "
					+ "status=503 durationMs=\\d+");

	private static final Pattern CONTENT_LENGTH = Pattern.compile("(?i)\r\ncontent-length: (\\d+)\r\n");

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

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
		tcpRoutePort = TestPorts.freePort();
		keystorePath = TestKeystore.create();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.limits.max-connections", () -> "1");
		registry.add("sluice.limits.overflow-connections", () -> String.valueOf(OVERFLOW));
		registry.add("sluice.limits.overflow-timeout", () -> OVERFLOW_TIMEOUT.toSeconds() + "s");
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
		String target = "127.0.0.1:" + upstream().port();
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host("held.local").target("http://" + target).build())
			.upstream(Upstream.builder().host("pass.local").target("tcp://" + target).tlsPassthrough(true).build())
			.upstream(Upstream.builder().host("held.tcp").target("tcp://" + target).listenPort(tcpRoutePort).build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-overflow-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			try (Socket probe = TestSockets.establish(tcpRoutePort, NONE, "held.tcp")) {
				// the tcp route listener is bound
			}
		});
	}

	@AfterAll
	void tearDown() throws IOException {
		if (this.client != null) {
			this.client.stop();
		}
		upstream().close();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@Test
	void aTlsClientOfferingHttp11PastTheLimitIsAnswered503WithRetryAfter() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double answered = answered("h1");
			try (SSLSocket socket = tlsSocket("h2", "http/1.1")) {
				socket.startHandshake();
				assertThat(socket.getApplicationProtocol()).isEqualTo("http/1.1");
				socket.getOutputStream()
					.write("GET / HTTP/1.1\r\nHost: held.local\r\nAccept: text/html\r\n\r\n"
						.getBytes(StandardCharsets.US_ASCII));
				socket.getOutputStream().flush();
				Response response = readHttp1Response(socket);
				assertThat(response.head()).isEqualToNormalizingWhitespace("""
						HTTP/1.1 503 Service Unavailable
						Content-Type: text/html; charset=utf-8
						Content-Length: %d
						Cache-Control: no-store
						Retry-After: 7
						Connection: close
						""".formatted(response.body().length));
				assertThat(new String(response.body(), StandardCharsets.UTF_8)).contains("held.local");
			}
			assertThat(rejected()).isEqualTo(rejected + 1);
			awaitAnswered("h1", answered + 1);
			awaitOverflow(0);
			assertThat(admitted()).isEqualTo(1);
			assertThat(answeredLines()).singleElement().asString().contains("transport=h1");
		}
		awaitAdmitted(0);
	}

	@Test
	void aPlaintextHttp11ClientPastTheLimitIsAnswered503WithRetryAfter() throws Exception {
		try (Socket held = holdTheLimit()) {
			double answered = answered("h1");
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				socket.setSoTimeout((int) WITHIN.toMillis());
				socket.getOutputStream().write(TestSockets.get("held.local"));
				socket.getOutputStream().flush();
				Response response = readHttp1Response(socket);
				assertThat(response.head()).startsWith("HTTP/1.1 503 Service Unavailable\r\n")
					.contains("\r\nRetry-After: 7\r\n");
			}
			awaitAnswered("h1", answered + 1);
			awaitOverflow(0);
		}
	}

	@Test
	void aClientSendingABodyPastTheLimitStillReadsItsAnswer() throws Exception {
		try (Socket held = holdTheLimit()) {
			byte[] body = new byte[512 * 1024];
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				socket.setSoTimeout((int) WITHIN.toMillis());
				OutputStream out = socket.getOutputStream();
				out.write(("POST / HTTP/1.1\r\nHost: held.local\r\nAccept: text/html\r\nContent-Length: " + body.length
						+ "\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
				out.flush();
				// the body is not read for the answer: closed with it unread, the
				// connection would be reset, the answer with it
				CompletableFuture<Void> upload = CompletableFuture.runAsync(() -> {
					try {
						out.write(body);
						out.flush();
					}
					catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}, task -> Thread.ofVirtual().name("e2e-overflow-upload").start(task));
				String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
				upload.get(WITHIN.toMillis(), TimeUnit.MILLISECONDS);
				assertThat(response).startsWith("HTTP/1.1 503 Service Unavailable\r\n").endsWith("</html>\n");
			}
			awaitOverflow(0);
		}
	}

	@Test
	void aPlaintextH2ClientPastTheLimitIsAnswered503OnItsStreamThenGoaway() throws Exception {
		try (Socket held = holdTheLimit()) {
			double answered = answered("h2");
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				socket.setSoTimeout((int) WITHIN.toMillis());
				TestH2.writeClientHead(socket.getOutputStream(), 3, ":method=GET", ":scheme=http",
						":authority=held.local", ":path=/", "accept=text/html");
				List<TestH2.Frame> frames = readFramesUntilGoaway(socket);
				assertThat(TestH2.status(frames)).isEqualTo("503");
				assertThat(frames.getFirst().streamId()).isEqualTo(3);
				TestH2.Frame goaway = frames.getLast();
				assertThat(goaway.type()).isEqualTo(TestH2.GOAWAY);
				assertThat(ByteBuffer.wrap(goaway.payload()).getInt()).isEqualTo(3);
			}
			awaitAnswered("h2", answered + 1);
			awaitOverflow(0);
		}
	}

	@Test
	void aHeadThatIsNoHttpRequestIsClosedUnanswered() throws Exception {
		try (Socket held = holdTheLimit()) {
			double answered = answered("h1") + answered("h2");
			double expired = expired();
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				// in the overflow, not closed at accept
				awaitOverflow(1);
				socket.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
				socket.getOutputStream().flush();
				TestSockets.awaitClosed(socket, WITHIN);
			}
			awaitOverflow(0);
			assertThat(expired()).as("closed without its deadline").isEqualTo(expired);
			assertThat(answered("h1") + answered("h2")).isEqualTo(answered);
		}
	}

	@Test
	void anH2HeadThatCannotBeDecodedIsClosedUnanswered() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double answered = answered("h1") + answered("h2");
			double expired = expired();
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				socket.setSoTimeout((int) WITHIN.toMillis());
				OutputStream out = socket.getOutputStream();
				out.write(TestH2.PREFACE);
				TestH2.writeFrame(out, TestH2.SETTINGS, 0x0, 0, NONE);
				// an HPACK indexed field far past the static table, no dynamic entry
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
						new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f });
				// closed without a byte: no answer, not even a GOAWAY
				TestSockets.awaitClosed(socket, WITHIN);
			}
			assertThat(rejected()).isEqualTo(rejected + 1);
			awaitOverflow(0);
			assertThat(expired()).as("closed without its deadline").isEqualTo(expired);
			assertThat(answered("h1") + answered("h2")).isEqualTo(answered);
			assertThat(answeredLines()).isEmpty();
		}
	}

	@Test
	void aClientGoneBeforeItsAnswerFreesItsPlaceAtOnce() throws Exception {
		try (Socket held = holdTheLimit()) {
			double expired = expired();
			// gone before its first byte
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				awaitOverflow(1);
			}
			awaitOverflow(0);
			assertThat(expired()).as("released without its deadline").isEqualTo(expired);
			// gone in its TLS handshake
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				socket.getOutputStream().write(clientHello(null));
				socket.getOutputStream().flush();
				awaitOverflow(1);
			}
			awaitOverflow(0);
			assertThat(expired()).isEqualTo(expired);
			// gone in its head
			try (SSLSocket socket = tlsSocket("http/1.1")) {
				socket.startHandshake();
				socket.getOutputStream().write("GET / HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII));
				socket.getOutputStream().flush();
				assertThat(overflow()).isEqualTo(1);
			}
			awaitOverflow(0);
			assertThat(expired()).isEqualTo(expired);
		}
	}

	@Test
	void anH2OnlyTlsClientPastTheLimitIsAnswered503OnItsStreamThenGoaway() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double answered = answered("h2");
			try (SSLSocket socket = tlsSocket("h2")) {
				socket.startHandshake();
				assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
				OutputStream out = socket.getOutputStream();
				TestH2.writeClientHead(out, 1, ":method=GET", ":scheme=https", ":authority=held.local", ":path=/",
						"accept=text/html");
				List<TestH2.Frame> frames = readFramesUntilGoaway(socket);
				TestH2.Frame headers = frames.stream()
					.filter(frame -> frame.type() == TestH2.HEADERS)
					.findFirst()
					.orElseThrow();
				assertThat(headers.streamId()).isEqualTo(1);
				Http2Headers decoded = new DefaultHttp2HeadersDecoder(true).decodeHeaders(1,
						Unpooled.wrappedBuffer(headers.payload()));
				assertThat(String.valueOf(decoded.status())).isEqualTo("503");
				assertThat(String.valueOf(decoded.get("retry-after"))).isEqualTo("7");
				assertThat(TestH2.bodyOf(frames)).contains("held.local");
				TestH2.Frame goaway = frames.getLast();
				assertThat(goaway.type()).isEqualTo(TestH2.GOAWAY);
				// the last stream processed is the answered one; NO_ERROR
				assertThat(ByteBuffer.wrap(goaway.payload()).getInt()).isEqualTo(1);
				assertThat(TestH2.errorCode(goaway)).isZero();
			}
			assertThat(rejected()).isEqualTo(rejected + 1);
			awaitAnswered("h2", answered + 1);
			awaitOverflow(0);
			assertThat(answeredLines()).singleElement().asString().contains("transport=h2");
		}
	}

	@Test
	void pastTheOverflowConnectionsAreClosedAtAccept() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double expired = expired();
			List<Socket> silent = new ArrayList<>();
			try {
				for (int i = 0; i < OVERFLOW; i++) {
					silent.add(new Socket("127.0.0.1", dataPort));
				}
				awaitOverflow(OVERFLOW);
				try (Socket past = new Socket("127.0.0.1", dataPort)) {
					past.getOutputStream().write(TestSockets.get("held.local"));
					TestSockets.awaitClosed(past, WITHIN);
				}
				assertThat(overflow()).isEqualTo(OVERFLOW);
				// the silent ones are cut by the overflow deadline, answered nothing
				for (Socket socket : silent) {
					TestSockets.awaitClosed(socket, WITHIN);
				}
			}
			finally {
				for (Socket socket : silent) {
					socket.close();
				}
			}
			assertThat(rejected()).isEqualTo(rejected + OVERFLOW + 1);
			awaitOverflow(0);
			assertThat(expired()).as("the deadlines of the silent ones alone").isEqualTo(expired + OVERFLOW);
			assertThat(answeredLines()).isEmpty();
		}
	}

	@Test
	void aClientStallingItsHandshakeInTheOverflowIsCutAtTheDeadline() throws Exception {
		try (Socket held = holdTheLimit()) {
			double answered = answered("h1") + answered("h2");
			double expired = expired();
			long start = System.nanoTime();
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				// the ClientHello and nothing after
				socket.getOutputStream().write(clientHello(null));
				socket.getOutputStream().flush();
				awaitOverflow(1);
				awaitClosedReadingAll(socket);
				// the deadline starts at the accept, after the start
				assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(OVERFLOW_TIMEOUT);
			}
			awaitOverflow(0);
			assertThat(expired()).as("cut by its deadline").isEqualTo(expired + 1);
			assertThat(answered("h1") + answered("h2")).isEqualTo(answered);
		}
	}

	@Test
	void aClientStallingItsHeadAfterTheHandshakeIsCutAtTheDeadline() throws Exception {
		try (Socket held = holdTheLimit()) {
			double expired = expired();
			long start = System.nanoTime();
			try (SSLSocket socket = tlsSocket("http/1.1")) {
				socket.startHandshake();
				// a part of the head, never completed
				socket.getOutputStream().write("GET / HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII));
				socket.getOutputStream().flush();
				assertThat(overflow()).isEqualTo(1);
				TestSockets.awaitClosed(socket, WITHIN);
				// one deadline for the handshake and the head, counted from the accept
				assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(OVERFLOW_TIMEOUT);
			}
			awaitOverflow(0);
			assertThat(expired()).as("cut by its deadline").isEqualTo(expired + 1);
		}
	}

	@Test
	void aFailedHandshakeReleasesItsPlaceAtOnce() throws Exception {
		try (Socket held = holdTheLimit()) {
			double expired = expired();
			// the default trust store does not trust the self-signed certificate
			SSLContext untrusting = SSLContext.getInstance("TLS");
			untrusting.init(null, null, null);
			try (SSLSocket socket = (SSLSocket) untrusting.getSocketFactory().createSocket("127.0.0.1", dataPort)) {
				assertThatThrownBy(socket::startHandshake).isInstanceOf(SSLHandshakeException.class);
			}
			awaitOverflow(0);
			assertThat(expired()).as("released without its deadline").isEqualTo(expired);
		}
	}

	@Test
	void tlsPassthroughAndTcpRouteConnectionsPastTheLimitAreClosedUnanswered() throws Exception {
		try (Socket held = holdTheLimit()) {
			double rejected = rejected();
			double expired = expired();
			try (Socket passthrough = new Socket("127.0.0.1", dataPort)) {
				// in the overflow, not closed at accept
				awaitOverflow(1);
				passthrough.getOutputStream().write(clientHello("pass.local"));
				passthrough.getOutputStream().flush();
				// nothing to answer in: the server has no say in the passthrough
				// handshake
				TestSockets.awaitClosed(passthrough, WITHIN);
			}
			try (Socket tcp = new Socket("127.0.0.1", tcpRoutePort)) {
				TestSockets.awaitClosed(tcp, WITHIN);
			}
			assertThat(rejected()).isEqualTo(rejected + 2);
			awaitOverflow(0);
			assertThat(expired()).as("closed without a deadline").isEqualTo(expired);
			assertThat(answeredLines()).isEmpty();
		}
	}

	/** A TLS ClientHello naming {@code sni} as its server name, or none. */
	private static byte[] clientHello(@Nullable String sni) throws Exception {
		SSLEngine engine = sni == null
				? TestKeystore.clientContext(Objects.requireNonNull(keystorePath)).createSSLEngine()
				: TestKeystore.clientContext(Objects.requireNonNull(keystorePath)).createSSLEngine(sni, 443);
		engine.setUseClientMode(true);
		engine.beginHandshake();
		ByteBuffer hello = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
		engine.wrap(ByteBuffer.allocate(0), hello);
		return Arrays.copyOf(hello.array(), hello.position());
	}

	/** An established relay holding the one slot of the limit. */
	private Socket holdTheLimit() throws IOException {
		awaitAdmitted(0);
		awaitOverflow(0);
		Socket held = TestSockets.establish(dataPort, NONE, "held.local");
		assertThat(admitted()).isEqualTo(1);
		assertThat(gauge("sluice.data.connections.limit")).isEqualTo(1);
		return held;
	}

	private SSLSocket tlsSocket(String... protocols) throws Exception {
		SSLSocket socket = (SSLSocket) TestKeystore.clientContext(Objects.requireNonNull(keystorePath))
			.getSocketFactory()
			.createSocket("127.0.0.1", dataPort);
		socket.setSoTimeout((int) WITHIN.toMillis());
		SSLParameters parameters = socket.getSSLParameters();
		parameters.setApplicationProtocols(protocols);
		socket.setSSLParameters(parameters);
		return socket;
	}

	private record Response(String head, byte[] body) {
	}

	/**
	 * Reads the response head and the body its {@code Content-Length} announces: the
	 * server closes only after the client did, or after its linger.
	 */
	private static Response readHttp1Response(Socket socket) throws IOException {
		InputStream in = socket.getInputStream();
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) {
			int b = in.read();
			assertThat(b).as("EOF in the response head").isNotNegative();
			head.write(b);
		}
		String text = head.toString(StandardCharsets.ISO_8859_1);
		Matcher length = CONTENT_LENGTH.matcher(text);
		assertThat(length.find()).isTrue();
		byte[] body = in.readNBytes(Integer.parseInt(length.group(1)));
		return new Response(text.substring(0, text.length() - 2), body);
	}

	/** Reads the frames the server sends up to its GOAWAY, SETTINGS left out. */
	private static List<TestH2.Frame> readFramesUntilGoaway(Socket socket) throws IOException {
		List<TestH2.Frame> frames = new ArrayList<>();
		InputStream in = socket.getInputStream();
		TestH2.Frame frame;
		while ((frame = TestH2.readFrame(in)) != null) {
			if (frame.type() != TestH2.SETTINGS) {
				frames.add(frame);
			}
			if (frame.type() == TestH2.GOAWAY) {
				break;
			}
		}
		return frames;
	}

	/** Reads (and drops) what the server sends until it closes. */
	private static void awaitClosedReadingAll(Socket socket) throws IOException {
		socket.setSoTimeout((int) WITHIN.toMillis());
		try {
			socket.getInputStream().transferTo(OutputStream.nullOutputStream());
		}
		catch (SocketException e) {
			// reset
		}
	}

	private List<String> answeredLines() {
		return ACCESS_EVENTS.list.stream()
			.map(ILoggingEvent::getFormattedMessage)
			.filter(line -> ANSWERED_LINE.matcher(line).matches())
			.toList();
	}

	private void awaitAdmitted(int expected) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == expected);
	}

	/**
	 * The answer is counted once written, which may come after the client read it.
	 */
	private void awaitAnswered(String protocol, double expected) {
		Awaitility.await().atMost(WITHIN).until(() -> answered(protocol) == expected);
	}

	private void awaitOverflow(int expected) {
		Awaitility.await().atMost(WITHIN).until(() -> overflow() == expected);
	}

	private int admitted() {
		return (int) gauge("sluice.data.connections.admitted");
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

	private double expired() {
		return meterRegistry().get("sluice.data.connections.overflow.expired").counter().count();
	}

	private double answered(String protocol) {
		return meterRegistry().get("sluice.data.connections.overflow.answered")
			.tag("protocol", protocol)
			.counter()
			.count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
