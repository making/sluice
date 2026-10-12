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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;
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
import org.junit.jupiter.params.provider.ValueSource;

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
 * HTTP/1.1 input the relay used to give up parsing on: a field value with obs-text (a
 * UTF-8 header value) is routed and tracked like any other, so the connection keeps its
 * deadlines and its rerouting; a malformed request is answered 400 at a request boundary
 * and the connection closed, nothing of it reaching an upstream; and a response the relay
 * cannot frame is relayed until its upstream closes. In the relay's package for the
 * deadline queue of {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class MalformedRequestE2ETest {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(2);

	/** The earliest an idle cut may come. */
	private static final Duration NOT_BEFORE = Duration.ofMillis(1500);

	/** The latest an idle cut may come. */
	private static final Duration WITHIN = Duration.ofSeconds(3);

	/** The latest a rejected connection is closed: after the relay's linger of 2s. */
	private static final Duration CLOSED_WITHIN = Duration.ofSeconds(3);

	/** How long the slow responses take. */
	private static final Duration SLOW = Duration.ofSeconds(1);

	private static final String ECHO_HOST = "echo.malformed.test";

	private static final String OTHER_HOST = "other.malformed.test";

	private static final String REWRITE_HOST = "rewrite.malformed.test";

	private static final String ODD_HOST = "odd.malformed.test";

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	private static final String BAD_REQUEST = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

	private static final String EARLY = "HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\n\r\n";

	/** The start of a response to {@code /partial}: 3 bytes of its 10. */
	private static final String PARTIAL = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc";

	/** The body bytes the upstream sends after an unframed start, one per tick. */
	private static final int UNFRAMED_TICKS = 8;

	/** The pause between two body bytes of the unframed response. */
	private static final Duration UNFRAMED_TICK = Duration.ofMillis(500);

	/** A response with a UTF-8 field value. */
	private static final byte[] UTF8_RESPONSE = "HTTP/1.1 200 OK\r\nX-Name: éほ\r\nContent-Length: 2\r\n\r\nok"
		.getBytes(StandardCharsets.UTF_8);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static int rewritePort;

	private static @Nullable Path keystorePath;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	/** The bytes every connection of the echo upstreams received, per connection. */
	private final List<ByteArrayOutputStream> received = new CopyOnWriteArrayList<>();

	/** The records of {@link #received} whose connection ended. */
	private final Set<ByteArrayOutputStream> ended = new CopyOnWriteArraySet<>();

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
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClient() {
		ServerSocket echo = upstream(socket -> echo(socket, ""));
		ServerSocket other = upstream(socket -> echo(socket, "other:"));
		ServerSocket rewrite = upstream(socket -> echo(socket, ""));
		ServerSocket odd = upstream(MalformedRequestE2ETest::serveOdd);
		rewritePort = rewrite.getLocalPort();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(ECHO_HOST).target("http://127.0.0.1:" + echo.getLocalPort()).build())
				.upstream(
						Upstream.builder().host(OTHER_HOST).target("http://127.0.0.1:" + other.getLocalPort()).build())
				.upstream(Upstream.builder()
					.host(REWRITE_HOST)
					.target("http://127.0.0.1:" + rewritePort)
					.rewriteHost(true)
					.build())
				.upstream(Upstream.builder().host(ODD_HOST).target("http://127.0.0.1:" + odd.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-malformed-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> List.of(ECHO_HOST, OTHER_HOST, REWRITE_HOST, ODD_HOST)
				.stream()
				.allMatch(host -> router().lookup(host).isPresent()));
	}

	@BeforeEach
	void awaitQuiet() {
		// every connection of the previous test is gone, and so are its deadlines
		awaitAdmitted(0);
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> connectionLimits().scheduledDeadlines() == 0);
		this.received.clear();
		this.ended.clear();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(MalformedRequestE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void aFirstRequestHeadWithAUtf8HeaderValueIsRelayedAndAnswered() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			byte[] head = utf8Get(ECHO_HOST);
			assertThat(exchange(socket, head)).isEqualTo(echoed("", head));
			assertThat(admitted()).isEqualTo(1);
			// still parsed: the next request reaches the upstream of its own host
			byte[] next = get(OTHER_HOST);
			assertThat(exchange(socket, next)).isEqualTo(echoed("other:", next));
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void afterARequestWithAUtf8HeaderValueTheConnectionIsReroutedAndIdlesOut() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			byte[] first = get(ECHO_HOST);
			assertThat(exchange(socket, first)).isEqualTo(echoed("", first));
			byte[] utf8 = utf8Get(ECHO_HOST);
			assertThat(exchange(socket, utf8)).isEqualTo(echoed("", utf8));
			byte[] next = get(OTHER_HOST);
			assertThat(exchange(socket, next)).as("rerouted by its Host").isEqualTo(echoed("other:", next));
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@Test
	void aRewrittenHeadKeepsItsUtf8HeaderValueByteForByte() throws Exception {
		try (Socket socket = connect()) {
			byte[] head = utf8Get(REWRITE_HOST);
			byte[] rewritten = utf8Get("127.0.0.1:" + rewritePort);
			assertThat(exchange(socket, head)).isEqualTo(echoed("", rewritten));
		}
	}

	@Test
	void aLaterRequestWithAnInvalidChunkSizeIsAnswered400AndClosed() throws Exception {
		try (Socket socket = connect()) {
			byte[] first = get(ECHO_HOST);
			assertThat(exchange(socket, first)).isEqualTo(echoed("", first));
			byte[] head = ascii(
					"POST /upload HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nTransfer-Encoding: chunked\r\n\r\n");
			write(socket, concat(head, ascii("zz\r\n")));
			// more of the body after the relay answered: had it closed with these bytes
			// unread, its reset would discard the 400 before it is read
			Thread.sleep(300);
			write(socket, new byte[256 * 1024]);
			assertThat(TestSockets.readUntil(socket.getInputStream(), BAD_REQUEST.length())).isEqualTo(BAD_REQUEST);
			long answered = System.nanoTime();
			assertThat(TestSockets.awaitClosed(socket, CLOSED_WITHIN)).as("half-closed after the 400")
				.isLessThan(Duration.ofMillis(500));
			// the client keeps its side open: the relay reads it until its linger is over
			assertThat(admitted()).isEqualTo(1);
			Awaitility.await().atMost(CLOSED_WITHIN).until(() -> admitted() == 0);
			assertThat(Duration.ofNanos(System.nanoTime() - answered)).isGreaterThan(Duration.ofMillis(1500));
		}
		byte[] relayed = concat(get(ECHO_HOST),
				ascii("POST /upload HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nTransfer-Encoding: chunked\r\n\r\n"));
		// the upstream reads the head, then waits for a chunk-size line until its EOF
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> this.received.size() == 1);
		ByteArrayOutputStream record = this.received.getFirst();
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> this.ended.contains(record));
		// the head relayed before the invalid line may be dropped with the leg's teardown
		assertThat(record.toByteArray()).as("the invalid chunk-size line is not relayed")
			.satisfiesAnyOf(bytes -> assertThat(bytes).isEqualTo(relayed),
					bytes -> assertThat(bytes).isEqualTo(get(ECHO_HOST)));
	}

	@Test
	void aTlsTerminatedRequestWithAnInvalidChunkSizeIsAnswered400AndClosed() throws Exception {
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.startHandshake();
			byte[] head = ascii(
					"POST /upload HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nTransfer-Encoding: chunked\r\n\r\n");
			write(socket, concat(head, ascii("5\r\nhello\r\nzz\r\n")));
			assertThat(TestSockets.readUntil(socket.getInputStream(), BAD_REQUEST.length())).isEqualTo(BAD_REQUEST);
			TestSockets.awaitClosed(socket, CLOSED_WITHIN);
		}
		awaitAdmitted(0);
	}

	@ParameterizedTest
	@ValueSource(strings = { "POST / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nContent-Length : 5\r\n\r\nhello",
			"POST / HTTP/1.1\r\nHost: " + ECHO_HOST
					+ "\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nHost: " + OTHER_HOST + "\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nX-A: 1\r\n 2\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\nX-A: 1\r\n\r\n", "OVERSIZED" })
	void aLaterMalformedHeadIsAnswered400AndClosedUnrelayed(String malformed) throws Exception {
		byte[] bytes = "OVERSIZED".equals(malformed)
				? ascii("GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nX-Long: " + "a".repeat(64 * 1024) + "\r\n\r\n")
				: ascii(malformed);
		try (Socket socket = connect()) {
			byte[] first = get(ECHO_HOST);
			assertThat(exchange(socket, first)).isEqualTo(echoed("", first));
			write(socket, bytes);
			assertThat(TestSockets.readUntil(socket.getInputStream(), BAD_REQUEST.length())).isEqualTo(BAD_REQUEST);
			TestSockets.awaitClosed(socket, CLOSED_WITHIN);
		}
		awaitAdmitted(0);
		Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> this.received.size() == 1);
		assertThat(this.received.getFirst().toByteArray()).as("the first request only").isEqualTo(get(ECHO_HOST));
	}

	@ParameterizedTest
	@ValueSource(strings = { "GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nContent-Length: abc\r\n\r\n",
			"GET /a b HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\n\r\n" })
	void aMalformedFirstHeadIsAnswered400AndClosedUnrelayed(String malformed) throws Exception {
		try (Socket socket = connect()) {
			write(socket, ascii(malformed));
			assertThat(TestSockets.readUntil(socket.getInputStream(), BAD_REQUEST.length())).isEqualTo(BAD_REQUEST);
			TestSockets.awaitClosed(socket, Duration.ofSeconds(3));
		}
		awaitAdmitted(0);
		assertThat(this.received).as("no upstream connection").isEmpty();
	}

	@Test
	void the400FollowsTheResponseToAnEarlierPipelinedRequest() throws Exception {
		try (Socket socket = connect()) {
			byte[] slow = ascii("GET /slow HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\n\r\n");
			byte[] malformed = ascii("GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nContent-Length: -1\r\n\r\n");
			long start = System.nanoTime();
			write(socket, concat(slow, malformed));
			byte[] expected = concat(echoed("", slow), ascii(BAD_REQUEST));
			assertThat(socket.getInputStream().readNBytes(expected.length)).isEqualTo(expected);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the first response")
				.isGreaterThan(SLOW.minusMillis(200));
			TestSockets.awaitClosed(socket, CLOSED_WITHIN);
		}
		awaitAdmitted(0);
	}

	@Test
	void aClientGoneWhileAnEarlierResponseIsAwaitedReleasesItsSlot() throws Exception {
		try (Socket socket = connect()) {
			byte[] slow = ascii("GET /slow HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\n\r\n");
			byte[] malformed = ascii("GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nContent-Length: -1\r\n\r\n");
			write(socket, concat(slow, malformed));
			Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> this.received.size() == 1);
			assertThat(admitted()).isEqualTo(1);
		}
		// the relay waits for the slow response, fails to write it to the gone client
		awaitAdmitted(0);
	}

	@Test
	void anUpstreamGoneWhileAnEarlierResponseIsAwaitedClosesTheConnectionWithNo400() throws Exception {
		try (Socket socket = connect()) {
			byte[] close = ascii("GET /close HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\n\r\n");
			byte[] malformed = ascii("GET / HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nContent-Length: -1\r\n\r\n");
			write(socket, concat(close, malformed));
			TestSockets.awaitClosed(socket, CLOSED_WITHIN);
		}
		awaitAdmitted(0);
	}

	@ParameterizedTest
	@ValueSource(strings = { "/early", "/partial" })
	void aMalformedBodyWhoseResponseBeganEarlyGetsNo400(String path) throws Exception {
		try (Socket socket = connect()) {
			byte[] head = ascii(
					"POST " + path + " HTTP/1.1\r\nHost: " + ECHO_HOST + "\r\nTransfer-Encoding: chunked\r\n\r\n");
			write(socket, head);
			// the upstream answered on the head, completely or in part; the body that
			// follows is malformed
			String early = "/early".equals(path) ? EARLY : PARTIAL;
			assertThat(TestSockets.readUntil(socket.getInputStream(), early.length())).isEqualTo(early);
			write(socket, ascii("zz\r\n"));
			TestSockets.awaitClosed(socket, CLOSED_WITHIN);
		}
		awaitAdmitted(0);
	}

	@Test
	void aResponseHeadWithAUtf8ValueIsTrackedAndTheConnectionIdlesOut() throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			write(socket, ascii("GET /utf8 HTTP/1.1\r\nHost: " + ODD_HOST + "\r\n\r\n"));
			assertThat(socket.getInputStream().readNBytes(UTF8_RESPONSE.length)).isEqualTo(UTF8_RESPONSE);
			// the exchange after it is tracked too: its end makes the connection idle
			write(socket, get(ODD_HOST));
			assertThat(TestSockets.readUntil(socket.getInputStream(), OK.length())).isEqualTo(OK);
			assertThat(TestSockets.awaitClosed(socket, WITHIN)).isGreaterThan(NOT_BEFORE);
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	@ParameterizedTest
	@ValueSource(strings = { "length", "status", "chunk", "oversized" })
	void aResponseTheRelayCannotFrameIsRelayedUntilTheUpstreamCloses(String kind) throws Exception {
		double idle = idleClosed();
		try (Socket socket = connect()) {
			long start = System.nanoTime();
			write(socket, ascii("GET /unframed/" + kind + " HTTP/1.1\r\nHost: " + ODD_HOST + "\r\n\r\n"));
			String expected = unframedStart(kind) + "a".repeat(UNFRAMED_TICKS);
			assertThat(TestSockets.readUntil(socket.getInputStream(), expected.length())).isEqualTo(expected);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("streamed past the idle timeout")
				.isGreaterThan(IDLE_TIMEOUT.plusMillis(1000));
			// the upstream closed after the last byte, which ends the connection
			assertThat(TestSockets.awaitClosed(socket, Duration.ofSeconds(2))).isLessThan(Duration.ofSeconds(1));
		}
		awaitAdmitted(0);
		assertThat(idleClosed()).isEqualTo(idle);
	}

	/**
	 * The start of a response the relay cannot frame, by {@code kind}: an unreadable
	 * Content-Length, no status code, an invalid chunk-size line, a head over 64KiB.
	 */
	private static String unframedStart(String kind) {
		return switch (kind) {
			case "length" -> "HTTP/1.1 200 OK\r\nContent-Length: x\r\n\r\n";
			case "status" -> "HTTP/1.1 OK\r\nContent-Length: 1\r\n\r\n";
			case "chunk" -> "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n";
			case "oversized" -> "HTTP/1.1 200 OK\r\nX-Long: " + "a".repeat(64 * 1024) + "\r\nContent-Length: 1\r\n\r\n";
			default -> throw new IllegalArgumentException(kind);
		};
	}

	private static Socket connect() throws IOException {
		Socket socket = new Socket("127.0.0.1", dataPort);
		socket.setSoTimeout(5000);
		return socket;
	}

	private static byte[] get(String host) {
		return ascii("GET / HTTP/1.1\r\nHost: " + host + "\r\n\r\n");
	}

	/** A GET whose head carries a UTF-8 field value. */
	private static byte[] utf8Get(String host) {
		return ("GET / HTTP/1.1\r\nHost: " + host + "\r\nX-Name: éほ\r\n\r\n").getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] ascii(String text) {
		return text.getBytes(StandardCharsets.US_ASCII);
	}

	private static byte[] concat(byte[]... parts) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	/** The echo upstream's response to a request with {@code head} and no body. */
	private static byte[] echoed(String tag, byte[] head) {
		byte[] body = concat(ascii(tag), head);
		return concat(ascii("HTTP/1.1 200 OK\r\nContent-Length: " + body.length + "\r\n\r\n"), body);
	}

	private static void write(Socket socket, byte[] bytes) throws IOException {
		socket.getOutputStream().write(bytes);
		socket.getOutputStream().flush();
	}

	/** Writes the request and reads the echo upstream's response to it. */
	private static byte[] exchange(Socket socket, byte[] request) throws IOException {
		write(socket, request);
		InputStream in = socket.getInputStream();
		byte[] head = readHead(in);
		assertThat(head).as("a response head").isNotNull();
		String text = new String(Objects.requireNonNull(head), StandardCharsets.ISO_8859_1);
		int length = Integer.parseInt(text.replaceAll("(?s).*Content-Length: (\\d+).*", "$1"));
		return concat(head, in.readNBytes(length));
	}

	/**
	 * An echo upstream: answers each request, once its body arrived, with its head
	 * prefixed by {@code tag} as the body; {@code /slow} after {@link #SLOW}, and
	 * {@code /early} with {@link #EARLY} and {@code /partial} with {@link #PARTIAL} on
	 * their head, before their body, which they never complete; {@code /close} closes the
	 * connection unanswered. Records every byte received.
	 */
	private void echo(Socket socket, String tag) {
		ByteArrayOutputStream record = new ByteArrayOutputStream();
		this.received.add(record);
		try (socket; OutputStream out = socket.getOutputStream()) {
			InputStream in = new RecordingInputStream(socket.getInputStream(), record);
			byte[] head;
			while ((head = readHead(in)) != null) {
				String text = new String(head, StandardCharsets.ISO_8859_1);
				String lower = text.toLowerCase(Locale.ROOT);
				if (text.startsWith("POST /early ")) {
					out.write(ascii(EARLY));
					out.flush();
				}
				else if (text.startsWith("POST /partial ")) {
					out.write(ascii(PARTIAL));
					out.flush();
				}
				if (lower.contains("transfer-encoding: chunked")) {
					readChunked(in);
				}
				else if (lower.contains("content-length: ")) {
					in.readNBytes(Integer.parseInt(lower.replaceAll("(?s).*content-length: (\\d+).*", "$1")));
				}
				if (text.startsWith("POST /early ") || text.startsWith("POST /partial ")) {
					continue;
				}
				if (text.startsWith("GET /close ")) {
					return;
				}
				if (text.startsWith("GET /slow ")) {
					Thread.sleep(SLOW.toMillis());
				}
				out.write(echoed(tag, head));
				out.flush();
			}
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			this.ended.add(record);
		}
	}

	/**
	 * The upstream of odd responses: {@code /utf8} answers with a UTF-8 field value,
	 * {@code /unframed/<kind>} with the {@link #unframedStart(String)} of its kind, then
	 * a byte per {@link #UNFRAMED_TICK}, then closes; {@link #OK} otherwise.
	 */
	private static void serveOdd(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] head;
			while ((head = readHead(in)) != null) {
				String text = new String(head, StandardCharsets.ISO_8859_1);
				if (text.startsWith("GET /utf8 ")) {
					out.write(UTF8_RESPONSE);
					out.flush();
				}
				else if (text.startsWith("GET /unframed/")) {
					String kind = text.substring("GET /unframed/".length(), text.indexOf(" HTTP/"));
					out.write(ascii(unframedStart(kind)));
					out.flush();
					for (int i = 0; i < UNFRAMED_TICKS; i++) {
						Thread.sleep(UNFRAMED_TICK.toMillis());
						out.write('a');
						out.flush();
					}
					return;
				}
				else {
					out.write(ascii(OK));
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

	/** Reads a chunked body through its trailer section. */
	private static void readChunked(InputStream in) throws IOException {
		while (true) {
			String line = readLine(in);
			int size = Integer.parseInt(line.split(";")[0].strip(), 16);
			if (size == 0) {
				while (!readLine(in).isEmpty()) {
					// a trailer
				}
				return;
			}
			in.readNBytes(size);
			readLine(in);
		}
	}

	private static String readLine(InputStream in) throws IOException {
		ByteArrayOutputStream line = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0 && b != '\n') {
			line.write(b);
		}
		if (b < 0) {
			throw new IOException("EOF in a line");
		}
		return line.toString(StandardCharsets.ISO_8859_1).strip();
	}

	/** The bytes up to and including the blank line; null at EOF. */
	private static byte @Nullable [] readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			byte[] bytes = head.toByteArray();
			int n = bytes.length;
			if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r'
					&& bytes[n - 1] == '\n') {
				return bytes;
			}
		}
		return null;
	}

	/** Copies every byte read into a record. */
	private static final class RecordingInputStream extends InputStream {

		private final InputStream in;

		private final ByteArrayOutputStream record;

		private RecordingInputStream(InputStream in, ByteArrayOutputStream record) {
			this.in = in;
			this.record = record;
		}

		@Override
		public int read() throws IOException {
			int b = this.in.read();
			if (b >= 0) {
				this.record.write(b);
			}
			return b;
		}

		@Override
		public int read(byte[] buffer, int offset, int length) throws IOException {
			int n = this.in.read(buffer, offset, length);
			if (n > 0) {
				this.record.write(buffer, offset, n);
			}
			return n;
		}

	}

	private ServerSocket upstream(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-malformed-accept").start(() -> {
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

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double idleClosed() {
		return meterRegistry().get("sluice.data.connections.idle.closed").counter().count();
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
