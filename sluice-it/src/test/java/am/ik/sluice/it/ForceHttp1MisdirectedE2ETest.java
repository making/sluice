package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SNIHostName;
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
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end for {@code force-http1} routes on h2 connections: a stream whose route's
 * upstream is reached over HTTP/1.1 only is answered {@code 421 Misdirected Request} by
 * the relay (RFC 9110 15.5.20) and reaches no upstream, the connection going on -- so a
 * browser that coalesced the request onto an h2 connection opened for another host of the
 * same certificate retries it on a new connection carrying the host's own SNI, where ALPN
 * picks http/1.1.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ForceHttp1MisdirectedE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-misdirected").start(task);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static final String H2_HOST = "h2.local";

	private static final String H1_HOST = "h1only.local";

	private static final String H1_BODY = "hello-from-h1-upstream";

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	/** The first line of every request head the h1-only upstream received. */
	private static final List<String> H1_RECEIVED = Collections.synchronizedList(new ArrayList<>());

	private static final AtomicInteger H1_ACCEPTED = new AtomicInteger();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static ServerSocket h2Upstream;

	private static ServerSocket h1Upstream;

	private @Nullable TunnelClient client;

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private Router router;

	@BeforeAll
	static void startUpstreams() throws Exception {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		h2Upstream = listen(ForceHttp1MisdirectedE2ETest::serveH2);
		h1Upstream = listen(ForceHttp1MisdirectedE2ETest::serveH1);
	}

	private static ServerSocket listen(java.util.function.Consumer<Socket> serve) throws Exception {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					UPSTREAM_EXECUTOR.execute(() -> serve.accept(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server;
	}

	/** An h2 upstream: every completed request head is answered 200. */
	private static void serveH2(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			OutputStream out = socket.getOutputStream();
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					// :status 200 (HPACK static index 8)
					TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, frame.streamId(),
							new byte[] { (byte) 0x88 });
					TestH2.writeFrame(out, TestH2.DATA, TestH2.FLAG_END_STREAM, frame.streamId(),
							"h2".getBytes(StandardCharsets.US_ASCII));
				}
			}
		}
		catch (Exception e) {
			// connection ended
		}
	}

	/**
	 * An HTTP/1.1-only upstream: records the first line of the head it reads (an h2
	 * preface reads as {@code PRI * HTTP/2.0}) and answers it with an HTTP/1.1 200.
	 */
	private static void serveH1(Socket socket) {
		H1_ACCEPTED.incrementAndGet();
		try (socket) {
			socket.setSoTimeout(30_000);
			InputStream in = socket.getInputStream();
			ByteArrayOutputStream head = new ByteArrayOutputStream();
			int b;
			while ((b = in.read()) >= 0) {
				head.write(b);
				if (head.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) {
					break;
				}
			}
			String text = head.toString(StandardCharsets.US_ASCII);
			H1_RECEIVED.add(text.contains("\r\n") ? text.substring(0, text.indexOf("\r\n")) : text);
			byte[] body = H1_BODY.getBytes(StandardCharsets.US_ASCII);
			socket.getOutputStream()
				.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().write(body);
			socket.getOutputStream().flush();
		}
		catch (Exception e) {
			// connection ended
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	/** One certificate covering both hosts, as a wildcard covers every route host. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-e2e-misdirected", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=" + H2_HOST, "-ext",
				"san=dns:" + H2_HOST + ",dns:" + H1_HOST, "-validity", "1", "-keystore",
				keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		return keystore;
	}

	@AfterAll
	void tearDown() throws Exception {
		if (this.client != null) {
			this.client.stop();
		}
		h2Upstream.close();
		h1Upstream.close();
		UPSTREAM_EXECUTOR.shutdownNow();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@BeforeEach
	void startClient() {
		H1_RECEIVED.clear();
		H1_ACCEPTED.set(0);
		synchronized (ACCESS_EVENTS) {
			ACCESS_EVENTS.list.clear();
		}
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host(H2_HOST).target("http://127.0.0.1:" + h2Upstream.getLocalPort()).build())
			.upstream(Upstream.builder()
				.host(H1_HOST)
				.target("http://127.0.0.1:" + h1Upstream.getLocalPort())
				.forceHttp1(true)
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> this.router.lookup(H2_HOST).isPresent() && this.router.lookup(H1_HOST).isPresent());
		// the precondition the tests rely on: the route is force-http1
		assertThat(this.router.lookup(H1_HOST).orElseThrow().forceHttp1()).isTrue();
	}

	/**
	 * The production case: a browser coalesces a request for a force-http1 host onto the
	 * h2 connection it opened for another host of the same certificate. The stream gets a
	 * bodiless 421 and reaches no upstream; the connection goes on, and a new connection
	 * carrying the host's SNI negotiates http/1.1 and gets the upstream's 200.
	 */
	@Test
	void aCoalescedStreamForAForceHttp1RouteIsMisdirected() throws Exception {
		double before = misdirected();
		try (H2TestClient h2 = H2TestClient.over(tls(H2_HOST, "h2", "http/1.1"), 1, 3, 5)) {
			h2.headers(1, true, TestH2.headerBlock(":authority=" + H2_HOST, ":method=GET", ":scheme=https", ":path=/"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("200");

			h2.headers(3, true,
					TestH2.headerBlock(":authority=" + H1_HOST, ":method=GET", ":scheme=https", ":path=/coalesced"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("421");
			assertThat(h2.frames(3)).singleElement()
				.satisfies(frame -> assertThat(frame.type()).isEqualTo(TestH2.HEADERS));
			assertThat(h2.reset(3)).isNull();

			// the connection serves its own host on
			h2.headers(5, true, TestH2.headerBlock(":authority=" + H2_HOST, ":method=GET", ":scheme=https", ":path=/"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(5))).isEqualTo("200");
			assertThat(h2.goAwayCode()).isEqualTo(-1);
		}
		assertThat(H1_ACCEPTED).hasValue(0);
		assertThat(misdirected() - before).isEqualTo(1.0);
		assertThat(misdirectedLines()).singleElement()
			.isEqualTo("type=req id=N route=h1only.local method=GET path=/coalesced http=2 remote=127.0.0.1:P"
					+ " status=421 reason=force-http1");

		// the retry on a connection of its own: SNI h1only.local, ALPN picks http/1.1
		try (SSLSocket socket = tls(H1_HOST, "h2", "http/1.1")) {
			assertThat(socket.getApplicationProtocol()).isEqualTo("http/1.1");
			socket.getOutputStream()
				.write("GET /coalesced HTTP/1.1\r\nHost: h1only.local\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
			assertThat(response).startsWith("HTTP/1.1 200 OK\r\n").endsWith("\r\n\r\n" + H1_BODY);
		}
		assertThat(H1_RECEIVED).containsExactly("GET /coalesced HTTP/1.1");
	}

	/**
	 * A stream still sending its request body when misdirected is answered 421 and reset
	 * with {@code NO_ERROR} (RFC 9113 8.1: no more of the request is wanted); its later
	 * DATA is dropped and the connection goes on.
	 */
	@Test
	void aMisdirectedStreamStillSendingItsBodyIsReset() throws Exception {
		try (H2TestClient h2 = H2TestClient.over(tls(H2_HOST, "h2", "http/1.1"), 1, 3)) {
			h2.headers(1, false,
					TestH2.headerBlock(":authority=" + H1_HOST, ":method=POST", ":scheme=https", ":path=/upload"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("421");
			assertThat(h2.awaitTrue(() -> h2.reset(1) != null, TIMEOUT)).isTrue();
			assertThat(h2.reset(1)).isEqualTo(0);
			h2.sendData(1, "body".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);

			h2.headers(3, true, TestH2.headerBlock(":authority=" + H2_HOST, ":method=GET", ":scheme=https", ":path=/"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("200");
			assertThat(h2.goAwayCode()).isEqualTo(-1);
		}
		assertThat(H1_ACCEPTED).hasValue(0);
	}

	/**
	 * A client offering only h2 on a connection whose SNI resolves to a force-http1
	 * route: the handshake still settles on h2 (failing it would hide why), and the
	 * stream -- the connection's first -- is answered 421 without reaching the upstream.
	 */
	@Test
	void anH2OnlyClientOfAForceHttp1RouteIsMisdirected() throws Exception {
		double before = misdirected();
		SSLSocket socket = tls(H1_HOST, "h2");
		assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
		try (H2TestClient h2 = H2TestClient.over(socket, 1)) {
			h2.headers(1, true, TestH2.headerBlock(":authority=" + H1_HOST, ":method=GET", ":scheme=https", ":path=/"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("421");
		}
		assertThat(H1_ACCEPTED).hasValue(0);
		assertThat(misdirected() - before).isEqualTo(1.0);
	}

	/**
	 * Plaintext h2 with prior knowledge to a force-http1 route: the upstream speaks no
	 * h2, so the stream is answered 421 rather than relayed as h2 frames.
	 */
	@Test
	void anH2cPriorKnowledgeStreamOfAForceHttp1RouteIsMisdirected() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, TestH2.headerBlock(":authority=" + H1_HOST, ":method=GET", ":scheme=http", ":path=/"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("421");
		}
		assertThat(H1_ACCEPTED).hasValue(0);
		assertThat(misdirectedLines()).singleElement()
			.isEqualTo("type=req id=N route=h1only.local method=GET path=/ http=2 remote=127.0.0.1:P"
					+ " status=421 reason=force-http1");
	}

	/** A TLS connection to the data plane with the given SNI and ALPN offer. */
	private static SSLSocket tls(String sni, String... protocols) throws Exception {
		SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort);
		socket.setSoTimeout((int) TIMEOUT.toMillis());
		SSLParameters parameters = socket.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(sni)));
		parameters.setApplicationProtocols(protocols);
		socket.setSSLParameters(parameters);
		socket.startHandshake();
		return socket;
	}

	private static SSLContext clientSslContext() throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keyStore);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		return sslContext;
	}

	private double misdirected() {
		Counter counter = this.meterRegistry.find("sluice.h2demux.streams.misdirected")
			.tag("reason", "force-http1")
			.counter();
		return counter == null ? 0 : counter.count();
	}

	/** The request lines of misdirected streams, the connection id and port masked. */
	private static List<String> misdirectedLines() {
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> accessLines().stream().anyMatch(line -> line.contains(" status=421 ")));
		return accessLines().stream()
			.filter(line -> line.startsWith("type=req ") && line.contains(" status="))
			.map(line -> line.replaceAll("^type=req id=\\d+ ", "type=req id=N ")
				.replaceAll("remote=127\\.0\\.0\\.1:\\d+", "remote=127.0.0.1:P"))
			.toList();
	}

	private static List<String> accessLines() {
		synchronized (ACCESS_EVENTS) {
			return ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
		}
	}

}
