package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import com.sun.net.httpserver.HttpServer;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.LocalConnector;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end over TLS: the data plane terminates TLS using an SSL bundle (the same
 * configuration surface as production), ALPN negotiates h2 and http/1.1, and plaintext
 * HTTP/1.1 coexists on the same port.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TlsDataPlaneE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-tls-worker").start(task);

	private static final String H2_BODY = "hello-from-h2-upstream";

	private static final String H1_BODY = "hello-from-tls-upstream";

	private static final String HTTPS_H1_BODY = "hello-from-https-h1-upstream";

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static ServerSocket h2Upstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static java.net.InetSocketAddress h1UpstreamAddress;

	private static HttpServer h1Upstream;

	private @Nullable TunnelClient tlsClient;

	/** An https upstream speaking h2 and http/1.1, picked over ALPN. */
	private @Nullable SSLServerSocket httpsUpstream;

	/** An https upstream without ALPN: http/1.1 only. */
	private @Nullable SSLServerSocket noAlpnUpstream;

	/** An https upstream offering http/1.1 only over ALPN. */
	private @Nullable SSLServerSocket h1AlpnUpstream;

	/** The ALPN protocol each connection to an https upstream negotiated. */
	private final List<String> httpsNegotiated = new CopyOnWriteArrayList<>();

	/** The first line each connection to an https upstream sent after the handshake. */
	private final List<String> httpsFirstLines = new CopyOnWriteArrayList<>();

	@BeforeAll
	static void startUpstreams() throws Exception {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		// stub h2 upstream: see H2cDataPlaneE2ETest
		h2Upstream = new ServerSocket();
		h2Upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!h2Upstream.isClosed()) {
				try {
					Socket socket = h2Upstream.accept();
					UPSTREAM_EXECUTOR.execute(() -> {
						try (socket) {
							TestH2.readClientHead(socket.getInputStream());
							socket.getOutputStream().write(TestH2.cannedResponse(1, H2_BODY));
							socket.getOutputStream().flush();
							socket.getInputStream().read(new byte[1]);
						}
						catch (Exception e) {
							// connection ended
						}
					});
				}
				catch (Exception e) {
					return;
				}
			}
		});
		h1Upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		h1Upstream.createContext("/", exchange -> {
			byte[] body = H1_BODY.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		h1Upstream.start();
		h1UpstreamAddress = h1Upstream.getAddress();
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
		registry.add("sluice.route.auth.[auth.local].bearer[0]", () -> "{noop}tls-token");
	}

	/** Generates a self-signed keystore with SANs for localhost via keytool. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-e2e", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=localhost", "-ext",
				"san=dns:localhost,ip:127.0.0.1", "-validity", "1", "-keystore", keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		return keystore;
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

	private static SSLContext serverSslContext() throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keyStore, "changeit".toCharArray());
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
		return sslContext;
	}

	/**
	 * Starts an https upstream offering the given ALPN protocols (none: it ignores ALPN):
	 * a connection that negotiated h2 gets the canned h2 response, any other one is
	 * answered as HTTP/1.1 -- {@code 400} for the h2 preface, as an HTTP/1.1 server does.
	 */
	private SSLServerSocket startHttpsUpstream(String... protocols) throws Exception {
		SSLServerSocket server = (SSLServerSocket) serverSslContext().getServerSocketFactory()
			.createServerSocket(0, 50, java.net.InetAddress.getByName("127.0.0.1"));
		if (protocols.length > 0) {
			SSLParameters parameters = server.getSSLParameters();
			parameters.setApplicationProtocols(protocols);
			server.setSSLParameters(parameters);
		}
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!server.isClosed()) {
				try {
					SSLSocket socket = (SSLSocket) server.accept();
					UPSTREAM_EXECUTOR.execute(() -> this.serveHttps(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server;
	}

	private void serveHttps(SSLSocket socket) {
		try (socket) {
			socket.setSoTimeout(10_000);
			socket.startHandshake();
			String negotiated = socket.getApplicationProtocol();
			this.httpsNegotiated.add(negotiated);
			InputStream in = socket.getInputStream();
			if ("h2".equals(negotiated)) {
				this.httpsFirstLines.add(readLine(in));
				in.readNBytes(TestH2.PREFACE.length - "PRI * HTTP/2.0\r\n".length());
				TestH2.Frame frame;
				while ((frame = TestH2.readFrame(in)) != null && frame.type() != TestH2.HEADERS) {
					// up to the request head
				}
				socket.getOutputStream().write(TestH2.cannedResponse(1, H2_BODY));
				socket.getOutputStream().flush();
				in.read(new byte[1]);
				return;
			}
			String line = readLine(in);
			if (line.isEmpty()) {
				return; // closed without a request
			}
			this.httpsFirstLines.add(line);
			while (!readLine(in).isEmpty()) {
				// the rest of the head
			}
			String response = line.startsWith("PRI ")
					? "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
					: "HTTP/1.1 200 OK\r\nContent-Length: " + HTTPS_H1_BODY.length() + "\r\nConnection: close\r\n\r\n"
							+ HTTPS_H1_BODY;
			socket.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
		}
		catch (Exception e) {
			// connection ended
		}
	}

	/** One CRLF-terminated line, without the CRLF; what was read at the end of input. */
	private static String readLine(InputStream in) throws java.io.IOException {
		ByteArrayOutputStream line = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0 && b != '\n') {
			if (b != '\r') {
				line.write(b);
			}
		}
		return line.toString(StandardCharsets.US_ASCII);
	}

	@Autowired
	SessionRegistry sessions;

	@AfterAll
	void tearDown() {
		if (this.tlsClient != null) {
			this.tlsClient.stop();
		}
		try {
			h2Upstream.close();
			if (this.httpsUpstream != null) {
				this.httpsUpstream.close();
			}
			if (this.noAlpnUpstream != null) {
				this.noAlpnUpstream.close();
			}
			if (this.h1AlpnUpstream != null) {
				this.h1AlpnUpstream.close();
			}
		}
		catch (Exception e) {
			// ignore
		}
		if (h1Upstream != null) {
			h1Upstream.stop(0);
		}
		UPSTREAM_EXECUTOR.shutdown();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	private void startClient() throws Exception {
		if (this.tlsClient != null) {
			return;
		}
		SSLServerSocket https = this.startHttpsUpstream("h2", "http/1.1");
		this.httpsUpstream = https;
		SSLServerSocket noAlpn = this.startHttpsUpstream();
		this.noAlpnUpstream = noAlpn;
		SSLServerSocket h1Alpn = this.startHttpsUpstream("http/1.1");
		this.h1AlpnUpstream = h1Alpn;
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			// the https upstreams present the data plane's self-signed certificate
			.insecure(true)
			.upstream(
					Upstream.builder().host("https.local").target("https://127.0.0.1:" + https.getLocalPort()).build())
			.upstream(Upstream.builder()
				.host("noalpn.local")
				.target("https://127.0.0.1:" + noAlpn.getLocalPort())
				.build())
			.upstream(Upstream.builder()
				.host("h1alpn.local")
				.target("https://127.0.0.1:" + h1Alpn.getLocalPort())
				.build())
			// the h2 stub doubles as the target for both the h2 and the h1-over-TLS path
			.upstream(Upstream.builder()
				.host("tls.local")
				.target("http://127.0.0.1:" + h2Upstream.getLocalPort())
				.build())
			.upstream(Upstream.builder()
				.host("127.0.0.1")
				.target("http://127.0.0.1:" + h1UpstreamAddress.getPort())
				.build())
			.upstream(Upstream.builder()
				.host("h1only.local")
				.target("http://127.0.0.1:" + h1UpstreamAddress.getPort())
				.forceHttp1(true)
				.build())
			.upstream(Upstream.builder()
				.host("auth.local")
				.target("http://127.0.0.1:" + h1UpstreamAddress.getPort())
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.tlsClient = started;
		// the advertise acknowledgement turns isConnected() true, and the server sends it
		// once the routes are registered; the session is registered before that
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	/**
	 * A TLS-terminated h2 connection's relay ends once the client closed the connection,
	 * though a TLS socket cannot half-close.
	 */
	@Test
	void aTlsH2RelayEndsOnceTheClientClosed() throws Exception {
		this.startClient();
		// the connections of the other tests' pooled clients stay open: told apart by
		// path
		String path = "/close";
		String id;
		try (H2TestClient h2 = H2TestClient.over(tls("tls.local", "h2"), 1)) {
			h2.headers(1, true,
					TestH2.headerBlock(":authority=tls.local", ":method=GET", ":scheme=https", ":path=" + path));
			assertThat(h2.awaitEnd(1, Duration.ofSeconds(10))).as("answered").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo(H2_BODY);
			String request = accessLines().stream()
				.filter(line -> line.startsWith("type=req ") && line.contains(" path=" + path + " "))
				.findFirst()
				.orElseThrow(() -> new AssertionError("no request line of " + path));
			int start = request.indexOf(" id=") + " id=".length();
			id = request.substring(start, request.indexOf(' ', start));
			assertThat(accessLines()).as("open while the client is")
				.noneMatch(line -> line.startsWith("type=conn id=" + id + " event=close "));
		}
		Awaitility.await("the relay ended")
			.atMost(Duration.ofSeconds(5))
			.until(() -> accessLines().stream()
				.anyMatch(line -> line.startsWith("type=conn id=" + id + " event=close ")));
	}

	/**
	 * An https target is dialed offering the protocol the leg speaks over ALPN: an h2
	 * stream reaches the upstream over h2, an HTTP/1.1 request over http/1.1, and an
	 * upstream negotiating no h2 -- no protocol, or http/1.1 with the handshake completed
	 * -- fails the h2 leg -- the stream gets the 502 page -- before a byte of h2 reached
	 * it.
	 */
	@Test
	void httpsUpstreamIsDialedWithTheLegsProtocolOverAlpn() throws Exception {
		this.startClient();
		try (H2TestClient h2 = H2TestClient.over(tls("https.local", "h2"), 1)) {
			h2.headers(1, true,
					TestH2.headerBlock(":authority=https.local", ":method=GET", ":scheme=https", ":path=/h2"));
			assertThat(h2.awaitEnd(1, Duration.ofSeconds(10))).as("answered").isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo(H2_BODY);
		}
		try (SSLSocket socket = tls("https.local", "http/1.1")) {
			assertThat(socket.getApplicationProtocol()).isEqualTo("http/1.1");
			socket.getOutputStream()
				.write("GET /h1 HTTP/1.1\r\nHost: https.local\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			assertThat(new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8))
				.startsWith("HTTP/1.1 200")
				.endsWith(HTTPS_H1_BODY);
		}
		assertThat(this.httpsNegotiated).containsExactly("h2", "http/1.1");
		assertThat(this.httpsFirstLines).containsExactly("PRI * HTTP/2.0", "GET /h1 HTTP/1.1");
		// an upstream negotiating no protocol, and one negotiating http/1.1
		for (String[] mismatch : List.of(new String[] { "noalpn.local", "", "no protocol" },
				new String[] { "h1alpn.local", "http/1.1", "http/1.1" })) {
			String host = mismatch[0];
			this.httpsNegotiated.clear();
			this.httpsFirstLines.clear();
			ListAppender<ILoggingEvent> connectorEvents = new ListAppender<>();
			connectorEvents.start();
			Logger connectorLogger = (Logger) LoggerFactory.getLogger(LocalConnector.class);
			connectorLogger.addAppender(connectorEvents);
			try (H2TestClient h2 = H2TestClient.over(tls(host, "h2"), 1)) {
				h2.headers(1, true,
						TestH2.headerBlock(":authority=" + host, ":method=GET", ":scheme=https", ":path=/"));
				assertThat(h2.awaitEnd(1, Duration.ofSeconds(10))).as("answered").isTrue();
				assertThat(TestH2.status(h2.frames(1))).as(host).isEqualTo("502");
			}
			finally {
				connectorLogger.detachAppender(connectorEvents);
			}
			Awaitility.await("the upstream saw the handshake")
				.atMost(Duration.ofSeconds(5))
				.until(() -> this.httpsNegotiated.size() == 1);
			assertThat(this.httpsNegotiated).as(host).containsExactly(mismatch[1]);
			assertThat(this.httpsFirstLines).as("nothing of h2 reached " + host).isEmpty();
			assertThat(connectorEvents.list).as(host).anySatisfy(event -> {
				assertThat(event.getLevel()).isEqualTo(Level.WARN);
				assertThat(event.getFormattedMessage()).contains("negotiated " + mismatch[2] + " over ALPN, not h2",
						"force-http1");
			});
		}
	}

	private static List<String> accessLines() {
		return List.copyOf(ACCESS_EVENTS.list).stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	private static SSLSocket tls(String sni, String... protocols) throws Exception {
		SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort);
		socket.setSoTimeout(10_000);
		SSLParameters parameters = socket.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(sni)));
		parameters.setApplicationProtocols(protocols);
		socket.setSSLParameters(parameters);
		socket.startHandshake();
		return socket;
	}

	@Test
	void httpsH2ViaAlpn() throws Exception {
		this.startClient();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "h2" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=tls.local", ":method=GET", ":path=/");
			List<TestH2.Frame> frames = TestH2.readResponseFrames(socket.getInputStream());
			assertThat(TestH2.bodyOf(frames)).isEqualTo(H2_BODY);
		}
	}

	/** A RestTestClient bound to the data plane over TLS with the given HTTP version. */
	private static RestTestClient tlsRestClient(HttpClient.Version version) throws Exception {
		HttpClient httpClient = HttpClient.newBuilder().version(version).sslContext(clientSslContext()).build();
		return RestTestClient.bindToServer(new JdkClientHttpRequestFactory(httpClient)).build();
	}

	@Test
	void httpsHttp1_1Fallback() throws Exception {
		this.startClient();
		tlsRestClient(HttpClient.Version.HTTP_1_1).get()
			.uri("https://127.0.0.1:" + dataPort + "/")
			.exchange()
			.expectStatus()
			.isOk()
			.expectBody(String.class)
			.isEqualTo(H1_BODY);
	}

	@Test
	void unroutedHostGetsTheNoRoutePageOverH2() throws Exception {
		this.startClient();
		tlsRestClient(HttpClient.Version.HTTP_2).get()
			.uri("https://localhost:" + dataPort + "/")
			.exchange()
			.expectStatus()
			.isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
			.expectHeader()
			.valueEquals("content-type", "text/html; charset=utf-8")
			.expectBody(String.class)
			.value(body -> assertThat(body).startsWith("<!DOCTYPE html>").endsWith("</html>\n"));
	}

	@Test
	void plaintextHttp1_1StillWorksOnTheSamePort() throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			InputStream in = socket.getInputStream();
			String head = new String(in.readNBytes(12), StandardCharsets.US_ASCII);
			assertThat(head).startsWith("HTTP/1.1 200");
		}
	}

	/**
	 * A route with credentials checks the requests of TLS-terminated connections as it
	 * does plaintext ones: HTTP/1.1 and h2.
	 */
	@Test
	void aRouteWithCredentialsChecksTlsTerminatedRequests() throws Exception {
		this.startClient();
		assertThat(tlsHttp1("GET / HTTP/1.1\r\nHost: auth.local\r\nConnection: close\r\n\r\n"))
			.startsWith("HTTP/1.1 401 Unauthorized\r\n");
		assertThat(tlsHttp1(
				"GET / HTTP/1.1\r\nHost: auth.local\r\nAuthorization: Bearer tls-token\r\nConnection: close\r\n\r\n"))
			.startsWith("HTTP/1.1 200")
			.endsWith(H1_BODY);
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "h2" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).as("precondition: h2").isEqualTo("h2");
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=auth.local", ":method=GET", ":path=/");
			assertThat(TestH2.status(TestH2.readResponseFrames(socket.getInputStream()))).isEqualTo("401");
		}
	}

	/** Sends the request over a TLS-terminated HTTP/1.1 connection; what came back. */
	private static String tlsHttp1(String request) throws Exception {
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "http/1.1" });
			socket.setSSLParameters(parameters);
			socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			try {
				socket.getInputStream().transferTo(bytes);
			}
			catch (javax.net.ssl.SSLException e) {
				// the peer closed without close_notify after the response
			}
			return bytes.toString(StandardCharsets.UTF_8);
		}
	}

	/**
	 * A {@code force-http1} route negotiates http/1.1 over ALPN even when the client
	 * offers h2, so h1-only upstreams are reached over HTTP/1.1.
	 */
	@Test
	void forceHttp1RouteNegotiatesHttp1_1OverAlpn() throws Exception {
		this.startClient();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			// the dial address is the loopback IP: SNI carries the route host
			parameters.setServerNames(List.of(new javax.net.ssl.SNIHostName("h1only.local")));
			parameters.setApplicationProtocols(new String[] { "http/1.1", "h2" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).isEqualTo("http/1.1");
			socket.getOutputStream()
				.write("GET / HTTP/1.1\r\nHost: h1only.local\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			InputStream in = socket.getInputStream();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				bytes.write(buffer, 0, n);
				if (bytes.toString(StandardCharsets.UTF_8).contains(H1_BODY)) {
					break;
				}
			}
			assertThat(bytes.toString(StandardCharsets.UTF_8)).startsWith("HTTP/1.1 200").contains(H1_BODY);
		}
	}

}
