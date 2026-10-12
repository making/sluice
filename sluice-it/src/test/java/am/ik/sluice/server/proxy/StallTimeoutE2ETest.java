package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

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

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.TestSockets;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.DuplexPipe;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The stall timeout of a relayed HTTP/1.1 connection: an exchange in flight whose client
 * does not read the response, or does not send the request body due -- or drips it below
 * the stall minimum --, is closed at the stall timeout and its slot released, plaintext
 * and TLS-terminated; an upstream slower than the timeout, a response trickling, an
 * upload held back by its upstream, a client awaiting a {@code 100 Continue}, and a
 * client taking one write piece per second are not cut. In the relay's package for the
 * deadline queue of {@link ConnectionLimits} and the client pipe seam of
 * {@link DataProxyServer}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class StallTimeoutE2ETest {

	private static final Duration STALL_TIMEOUT = Duration.ofSeconds(2);

	/** The latest a stall is cut: two sample intervals past the timeout, plus slack. */
	private static final Duration CUT_WITHIN = STALL_TIMEOUT.plusMillis(1500);

	/** How long the upstream keeps a client waiting: past the latest a stall is cut. */
	private static final Duration REST = Duration.ofSeconds(4);

	private static final String HOST = "stall.test";

	private static final int BIG = 16 * 1024 * 1024;

	private static final int UPLOAD = 16 * 1024 * 1024;

	/**
	 * The bytes per second the slow client takes: one write piece, and the stall minimum
	 * (16KiB by default).
	 */
	private static final int SLOW_CLIENT_RATE = ConnectionLimits.KeepAliveDeadline.WRITE_PIECE;

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	private static final String CONTINUE = "HTTP/1.1 100 Continue\r\n\r\n";

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

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.stall-timeout", () -> STALL_TIMEOUT.toSeconds() + "s");
		// the idle timeout stays out of the way of the exchanges here
		registry.add("sluice.limits.idle-timeout", () -> "30s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClient() {
		ServerSocket upstream = upstream(StallTimeoutE2ETest::serve);
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(HOST).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-stall-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> router().lookup(HOST).isPresent());
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
		this.closeables.forEach(StallTimeoutE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void aClientNotReadingABigResponseIsClosedAtTheStallTimeout() throws Exception {
		try (Socket socket = new Socket()) {
			socket.setReceiveBufferSize(16 * 1024);
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort));
			assertCutWhileNotReading(socket, Duration.ZERO);
		}
	}

	/**
	 * Over TLS the teardown closes the TLS socket, or the deadline closes the transport
	 * under it after the teardown grace: allowed on top of the stall timeout.
	 */
	@Test
	void aTlsClientNotReadingABigResponseIsClosedAtTheStallTimeout() throws Exception {
		Socket plain = new Socket();
		plain.setReceiveBufferSize(16 * 1024);
		plain.connect(new InetSocketAddress("127.0.0.1", dataPort));
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory()
			.createSocket(plain, "127.0.0.1", dataPort, true)) {
			socket.setSoTimeout(5000);
			socket.startHandshake();
			assertCutWhileNotReading(socket, ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE);
		}
	}

	@Test
	void aRequestBodyNeverSentIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			send(socket, "POST /body HTTP/1.1\r\nHost: " + HOST + "\r\nContent-Length: 10\r\n\r\n");
			assertThat(TestSockets.awaitClosed(socket, CUT_WITHIN.plusSeconds(1))).isGreaterThanOrEqualTo(STALL_TIMEOUT)
				.isLessThan(CUT_WITHIN);
		}
		awaitReleased(stalls);
	}

	/**
	 * Every byte moves the body on, yet less than the stall minimum (16KiB) per stall
	 * timeout: the client is cut as if it sent nothing.
	 */
	@Test
	void aRequestBodyDrippedAByteEveryHalfSecondIsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			send(socket, "POST /body HTTP/1.1\r\nHost: " + HOST + "\r\nContent-Length: 100\r\n\r\n");
			long start = System.nanoTime();
			AtomicLong dripped = new AtomicLong();
			Thread dripper = Thread.ofVirtual().name("e2e-stall-dripper").start(() -> drip(socket, dripped));
			Duration closed = TestSockets.awaitClosed(socket, CUT_WITHIN.plusSeconds(1));
			dripper.interrupt();
			assertThat(closed).isGreaterThanOrEqualTo(STALL_TIMEOUT).isLessThan(CUT_WITHIN);
			assertThat(dripped.get())
				.as("bytes dripped before the cut, %s after the head", Duration.ofNanos(System.nanoTime() - start))
				.isGreaterThanOrEqualTo(3);
		}
		awaitReleased(stalls);
	}

	/**
	 * A head asking for {@code 100 Continue} lets the client hold its body back only
	 * until it sends a byte of it: after that the body is due.
	 */
	@Test
	void aClientSendingPartOfTheBodyItHeldForA100IsClosedAtTheStallTimeout() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			send(socket, "POST /continue HTTP/1.1\r\nHost: " + HOST
					+ "\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\no");
			assertThat(TestSockets.awaitClosed(socket, CUT_WITHIN.plusSeconds(1))).as("cut before the 100 came")
				.isGreaterThanOrEqualTo(STALL_TIMEOUT)
				.isLessThan(CUT_WITHIN);
		}
		awaitReleased(stalls);
	}

	@Test
	void anUpstreamSlowerThanTheStallTimeoutIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			socket.setSoTimeout((int) REST.plusSeconds(3).toMillis());
			long start = System.nanoTime();
			exchange(socket, "GET /slow HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n", OK);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the response")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			assertThat(stallClosed()).isEqualTo(stalls);
		}
	}

	@Test
	void aResponseTricklingLongerThanTheStallTimeoutIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			long start = System.nanoTime();
			send(socket, "GET /trickle HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n");
			String response = readChunkedResponse(socket.getInputStream());
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the response trickled")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			assertThat(response).isEqualTo("x".repeat(trickleChunks()));
			assertThat(stallClosed()).isEqualTo(stalls);
		}
	}

	/**
	 * An upstream that reads nothing for a while holds the upload back: the relay stops
	 * reading the client meanwhile, which is not the client's stall.
	 */
	@Test
	void anUploadHeldBackByItsUpstreamIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			socket.setSoTimeout(20000);
			AtomicLong lastWrite = new AtomicLong(System.nanoTime());
			AtomicLong longestGap = new AtomicLong();
			Thread uploader = Thread.ofVirtual().name("e2e-stall-uploader").start(() -> {
				try {
					OutputStream out = socket.getOutputStream();
					out.write(("POST /upload HTTP/1.1\r\nHost: " + HOST + "\r\nContent-Length: " + UPLOAD + "\r\n\r\n")
						.getBytes(StandardCharsets.US_ASCII));
					byte[] chunk = new byte[16 * 1024];
					for (int sent = 0; sent < UPLOAD; sent += chunk.length) {
						out.write(chunk);
						long now = System.nanoTime();
						longestGap.accumulateAndGet(now - lastWrite.getAndSet(now), Math::max);
					}
				}
				catch (IOException e) {
					// cut: the assertions below tell
				}
			});
			String expected = "HTTP/1.1 200 OK\r\nContent-Length: " + String.valueOf(UPLOAD).length() + "\r\n\r\n"
					+ UPLOAD;
			assertThat(TestSockets.readUntil(socket.getInputStream(), expected.length())).isEqualTo(expected);
			uploader.join();
			assertThat(Duration.ofNanos(longestGap.get())).as("the upload was held back past the stall timeout")
				.isGreaterThan(STALL_TIMEOUT.plusSeconds(1));
			assertThat(stallClosed()).isEqualTo(stalls);
		}
	}

	@Test
	void aClientAwaitingA100ItsUpstreamDelaysIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = connect()) {
			socket.setSoTimeout((int) REST.plusSeconds(3).toMillis());
			long start = System.nanoTime();
			exchange(socket, "POST /continue HTTP/1.1\r\nHost: " + HOST
					+ "\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\n", CONTINUE);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the upstream held the 100")
				.isGreaterThanOrEqualTo(REST.minusMillis(100));
			exchange(socket, "ok", OK);
			assertThat(stallClosed()).isEqualTo(stalls);
		}
	}

	/**
	 * A client taking the response at one write piece per second -- the relay's client
	 * pipe accepts {@link #SLOW_CLIENT_RATE} bytes per second, no kernel buffers in
	 * between -- is not cut, though the relay waits on it nearly all the time: each piece
	 * completed is progress. Written at once, a 64KiB read of the tunnel would keep the
	 * relay waiting 4s, past the stall timeout.
	 */
	@Test
	void aClientTakingOneWritePieceASecondIsNotCut() throws Exception {
		double stalls = stallClosed();
		dataProxyServer().clientPipeSeam(pipe -> new RateLimitedPipe(pipe, SLOW_CLIENT_RATE));
		try (Socket socket = connect()) {
			send(socket, "GET /big HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n");
			long start = System.nanoTime();
			Duration watched = STALL_TIMEOUT.multipliedBy(3);
			long received = readFor(socket, watched);
			Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertThat(received).as("the client pipe held the response back to its rate")
				.isLessThanOrEqualTo(SLOW_CLIENT_RATE * (elapsed.toMillis() / 1000 + 1))
				.isGreaterThanOrEqualTo(SLOW_CLIENT_RATE * (watched.toSeconds() - 2));
			assertThat(admitted()).as("the connection is still relayed").isEqualTo(1);
			assertThat(stallClosed()).isEqualTo(stalls);
		}
		finally {
			dataProxyServer().clientPipeSeam(UnaryOperator.identity());
		}
	}

	/** Reads what arrives for {@code duration}; the bytes read, failing at EOF. */
	private static long readFor(Socket socket, Duration duration) throws IOException {
		long end = System.nanoTime() + duration.toNanos();
		byte[] buffer = new byte[64 * 1024];
		long total = 0;
		socket.setSoTimeout(100);
		while (System.nanoTime() < end) {
			try {
				int n = socket.getInputStream().read(buffer);
				assertThat(n).as("no EOF after %d bytes", total).isPositive();
				total += n;
			}
			catch (SocketTimeoutException e) {
				// nothing yet
			}
		}
		return total;
	}

	/**
	 * Sends {@code GET /big} and reads nothing: the relay's write blocks once the buffers
	 * on the way are full, and the stall timeout closes the connection, up to
	 * {@code grace} later.
	 */
	private void assertCutWhileNotReading(Socket socket, Duration grace) throws Exception {
		double stalls = stallClosed();
		send(socket, "GET /big HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n");
		long start = System.nanoTime();
		Awaitility.await()
			.pollInterval(Duration.ofMillis(20))
			.atMost(CUT_WITHIN.plus(grace).plusSeconds(1))
			.until(() -> admitted() == 0);
		assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(STALL_TIMEOUT)
			.isLessThan(CUT_WITHIN.plus(grace));
		assertThat(stallClosed()).isEqualTo(stalls + 1);
		assertThat(drain(socket)).as("the response was cut short").isLessThan(BIG);
		awaitNoDeadline();
	}

	/** Reads until EOF or a reset; the bytes read. */
	private static long drain(Socket socket) {
		long total = 0;
		try {
			socket.setSoTimeout(5000);
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = socket.getInputStream().read(buffer)) >= 0) {
				total += n;
			}
		}
		catch (IOException e) {
			// reset
		}
		return total;
	}

	private static void drip(Socket socket, AtomicLong dripped) {
		try {
			OutputStream out = socket.getOutputStream();
			for (int i = 0; i < 100; i++) {
				Thread.sleep(500);
				out.write('x');
				out.flush();
				dripped.incrementAndGet();
			}
		}
		catch (IOException | InterruptedException e) {
			// cut
		}
	}

	private void awaitReleased(double stalls) {
		awaitAdmitted(0);
		awaitNoDeadline();
		assertThat(stallClosed()).isEqualTo(stalls + 1);
	}

	private static Socket connect() throws IOException {
		Socket socket = new Socket("127.0.0.1", dataPort);
		socket.setSoTimeout(5000);
		return socket;
	}

	private static void send(Socket socket, String bytes) throws IOException {
		socket.getOutputStream().write(bytes.getBytes(StandardCharsets.US_ASCII));
		socket.getOutputStream().flush();
	}

	/** Writes the bytes and reads exactly the expected response. */
	private static void exchange(Socket socket, String request, String expected) throws IOException {
		send(socket, request);
		assertThat(TestSockets.readUntil(socket.getInputStream(), expected.length())).isEqualTo(expected);
	}

	/** Reads a chunked response; its body. */
	private static String readChunkedResponse(InputStream in) throws IOException {
		String head = Objects.requireNonNull(readHead(in));
		assertThat(head).startsWith("HTTP/1.1 200 OK\r\n");
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		while (true) {
			String size = readLine(in);
			int length = Integer.parseInt(size, 16);
			if (length == 0) {
				readLine(in);
				return body.toString(StandardCharsets.US_ASCII);
			}
			body.writeBytes(in.readNBytes(length));
			readLine(in);
		}
	}

	private static String readLine(InputStream in) throws IOException {
		ByteArrayOutputStream line = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0 && b != '\n') {
			if (b != '\r') {
				line.write(b);
			}
		}
		return line.toString(StandardCharsets.US_ASCII);
	}

	/** The chunks of {@code /trickle}: one per 250ms for {@link #REST}. */
	private static int trickleChunks() {
		return (int) (REST.toMillis() / 250);
	}

	/**
	 * The upstream: {@code /big} answers 16MiB, {@code /slow} answers after
	 * {@link #REST}, {@code /trickle} sends a chunk every 250ms for {@link #REST},
	 * {@code /upload} reads nothing for {@link #REST}, then its body, and answers its
	 * length, {@code /continue} sends {@code 100 Continue} after {@link #REST}, then
	 * reads the body and answers, {@code /body} reads the body and answers.
	 */
	private static void serve(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			String head;
			while ((head = readHead(in)) != null) {
				String requestLine = head.substring(0, head.indexOf("\r\n"));
				long length = contentLength(head);
				switch (requestLine) {
					case "GET /big HTTP/1.1" -> {
						out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + BIG + "\r\n\r\n")
							.getBytes(StandardCharsets.US_ASCII));
						byte[] chunk = new byte[64 * 1024];
						for (int sent = 0; sent < BIG; sent += chunk.length) {
							out.write(chunk);
						}
					}
					case "GET /slow HTTP/1.1" -> {
						Thread.sleep(REST.toMillis());
						out.write(OK.getBytes(StandardCharsets.US_ASCII));
					}
					case "GET /trickle HTTP/1.1" -> {
						out.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
							.getBytes(StandardCharsets.US_ASCII));
						out.flush();
						for (int i = 0; i < trickleChunks(); i++) {
							Thread.sleep(250);
							out.write("1\r\nx\r\n".getBytes(StandardCharsets.US_ASCII));
							out.flush();
						}
						out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
					}
					case "POST /upload HTTP/1.1" -> {
						Thread.sleep(REST.toMillis());
						long read = in.readNBytes((int) length).length;
						out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + String.valueOf(read).length() + "\r\n\r\n"
								+ read)
							.getBytes(StandardCharsets.US_ASCII));
					}
					case "POST /continue HTTP/1.1" -> {
						Thread.sleep(REST.toMillis());
						out.write(CONTINUE.getBytes(StandardCharsets.US_ASCII));
						out.flush();
						in.readNBytes((int) length);
						out.write(OK.getBytes(StandardCharsets.US_ASCII));
					}
					default -> {
						in.readNBytes((int) Math.max(0, length));
						out.write(OK.getBytes(StandardCharsets.US_ASCII));
					}
				}
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

	private static long contentLength(String head) {
		for (String line : head.split("\r\n")) {
			if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
				return Long.parseLong(line.substring("content-length:".length()).trim());
			}
		}
		return 0;
	}

	/** The bytes up to and including the blank line; null at EOF. */
	private static @Nullable String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			if (head.size() >= 4) {
				byte[] bytes = head.toByteArray();
				int n = bytes.length;
				if (bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r' && bytes[n - 1] == '\n') {
					return head.toString(StandardCharsets.US_ASCII);
				}
			}
		}
		return null;
	}

	private ServerSocket upstream(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-stall-accept").start(() -> {
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

	private double stallClosed() {
		return meterRegistry().get("sluice.data.connections.stall.closed").counter().count();
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

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * A client pipe whose writes take as long as the bytes they carry at {@code rate}
	 * bytes per second before they pass on: a client taking the bytes at that rate.
	 */
	private static final class RateLimitedPipe implements DuplexPipe {

		private final DuplexPipe pipe;

		private final OutputStream sink;

		RateLimitedPipe(DuplexPipe pipe, int rate) {
			this.pipe = pipe;
			this.sink = new FilterOutputStream(pipe.sink()) {

				@Override
				public void write(int b) throws IOException {
					write(new byte[] { (byte) b }, 0, 1);
				}

				@Override
				public void write(byte[] bytes, int offset, int length) throws IOException {
					try {
						Thread.sleep(Duration.ofNanos(length * 1_000_000_000L / rate));
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new InterruptedIOException();
					}
					this.out.write(bytes, offset, length);
				}

			};
		}

		@Override
		public InputStream source() {
			return this.pipe.source();
		}

		@Override
		public OutputStream sink() {
			return this.sink;
		}

		@Override
		public void shutdownOutput() throws Exception {
			this.pipe.shutdownOutput();
		}

		@Override
		public void close() throws Exception {
			this.pipe.close();
		}

	}

}
