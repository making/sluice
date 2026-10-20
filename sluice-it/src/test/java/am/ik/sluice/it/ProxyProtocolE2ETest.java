package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: an SNAT front end (a local proxy, here) speaks the PROXY protocol to the
 * data plane. The header is consumed before routing / relay -- never forwarded -- and its
 * source address becomes the connection peer, admitting it to routes whose
 * {@code allowed-cidrs} list no socket address would match and standing as the
 * {@code remote} of the access log lines. The gRPC control plane (TLS) takes the header
 * too: its source becomes the tunnel session's remote address.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ProxyProtocolE2ETest {

	private static final byte[] V2_SIGNATURE = { 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54,
			0x0A };

	/**
	 * loopback, not in any route allow list: only the header peer admits the connection
	 */
	private static final byte[] PROXIED_SOURCE = { (byte) 203, 0, (byte) 113, 7 };

	private static final byte[] PROXIED_DEST = { (byte) 198, (byte) 51, 100, 7 };

	private static final String PROXIED_SOURCE_HOST = "203.0.113.7";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-proxy-worker").start(task);

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static final Duration HEAD_TIMEOUT = Duration.ofSeconds(2);

	/** Connections the echo upstream accepted: the tcp route's dials. */
	private static final AtomicInteger echoAccepts = new AtomicInteger();

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	/** A tcp route open to any peer, the loopback included. */
	private static int openTcpRoutePort;

	private static HttpServer httpUpstream;

	private static @Nullable ServerSocket echoServer;

	private @Nullable TunnelClient client;

	@Autowired
	SessionRegistry sessions;

	@BeforeAll
	static void startUpstreams() throws Exception {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		httpUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		httpUpstream.createContext("/", exchange -> {
			byte[] body = "hello-from-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		httpUpstream.start();
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		echoServer = server;
		Thread.ofVirtual().name("e2e-proxy-echo-accept").start(ProxyProtocolE2ETest::acceptLoop);
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		openTcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> tcpRoutePort + "," + openTcpRoutePort);
		registry.add("sluice.data-proxy-protocol", () -> "true");
		registry.add("sluice.control-proxy-protocol", () -> "true");
		registry.add("sluice.limits.head-timeout", () -> HEAD_TIMEOUT.toMillis() + "ms");
		Path keystore = selfSignedKeystore();
		registry.add("spring.grpc.server.ssl.bundle", () -> "grpc-control");
		registry.add("spring.ssl.bundle.jks.grpc-control.keystore.location", () -> "file:" + keystore);
		registry.add("spring.ssl.bundle.jks.grpc-control.keystore.password", () -> "changeit");
		registry.add("spring.ssl.bundle.jks.grpc-control.keystore.type", () -> "PKCS12");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	private static ServerSocket echoServer() {
		return java.util.Objects.requireNonNull(echoServer, "echo server not started");
	}

	private static void acceptLoop() {
		while (!echoServer().isClosed()) {
			try {
				Socket socket = echoServer().accept();
				echoAccepts.incrementAndGet();
				Thread.ofVirtual().name("e2e-proxy-echo-conn").start(() -> echo(socket));
			}
			catch (Exception e) {
				return;
			}
		}
	}

	/** Echoes bytes back until the peer half-closes, then closes. */
	private static void echo(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] buffer = new byte[1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
		}
		catch (Exception e) {
			// connection torn down; nothing to do
		}
	}

	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpcs://127.0.0.1:" + grpcPort)
			.insecure(true)
			.clientId("direct")
			.upstream(Upstream.builder()
				.host("proxied.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.allowedCidrs(List.of(PROXIED_SOURCE_HOST + "/24"))
				.build())
			.upstream(Upstream.builder()
				.host("open.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.build())
			.upstream(Upstream.builder()
				.host("proxied.tcp")
				.target("tcp://127.0.0.1:" + echoServer().getLocalPort())
				.listenPort(tcpRoutePort)
				.allowedCidrs(List.of(PROXIED_SOURCE_HOST + "/24"))
				.build())
			.upstream(Upstream.builder()
				.host("open.tcp")
				.target("tcp://127.0.0.1:" + echoServer().getLocalPort())
				.listenPort(openTcpRoutePort)
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
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		httpUpstream.stop(0);
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
		try {
			echoServer().close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	@Test
	void v2HeaderPeerAdmitsTheConnectionToHttpRoutes() throws Exception {
		this.startClient();
		byte[] header = proxyV2(32100);
		String response = raw(header, "GET / HTTP/1.1\r\nHost: proxied.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 200 OK");
		assertThat(bodyOf(response)).isEqualTo("hello-from-upstream");
		// both lines are written before the request is relayed
		assertThat(accessLines())
			.anySatisfy(line -> assertThat(line)
				.matches("type=conn id=\\d+ event=accept listener=data remote=203\\.0\\.113\\.7:32100"))
			.anySatisfy(line -> assertThat(line).matches(
					"type=req id=\\d+ route=proxied\\.local method=GET path=/ http=1\\.1 remote=203\\.0\\.113\\.7:32100 userAgent=-"));
	}

	@Test
	void v1HeaderPeerAdmitsTheConnectionToHttpRoutes() throws Exception {
		this.startClient();
		byte[] header = "PROXY TCP4 203.0.113.7 198.51.100.7 32101 80\r\n".getBytes(StandardCharsets.US_ASCII);
		String response = raw(header, "GET / HTTP/1.1\r\nHost: proxied.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 200 OK");
		assertThat(bodyOf(response)).isEqualTo("hello-from-upstream");
		assertThat(accessLines()).anySatisfy(line -> assertThat(line).matches(
				"type=req id=\\d+ route=proxied\\.local method=GET path=/ http=1\\.1 remote=203\\.0\\.113\\.7:32101 userAgent=-"));
	}

	@Test
	void headerlessConnectionsStillServe() throws Exception {
		this.startClient();
		Response response = exchange("GET / HTTP/1.1\r\nHost: open.local\r\nConnection: close\r\n\r\n");
		assertThat(response.statusLine()).isEqualTo("HTTP/1.1 200 OK");
		assertThat(response.body()).isEqualTo("hello-from-upstream");
	}

	@Test
	void v2HeaderIsStrippedFromTcpRoutes() throws Exception {
		this.startClient();
		byte[] header = proxyV2(32100);
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(header);
			out.write("ping".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			// the echo starts at the first byte behind the header
			assertThat(new String(in.readNBytes(4), StandardCharsets.US_ASCII)).isEqualTo("ping");
			// written before the relay starts
			assertThat(accessLines()).anySatisfy(line -> assertThat(line).matches(
					"type=conn id=\\d+ event=accept listener=tcp:" + tcpRoutePort + " remote=203\\.0\\.113\\.7:32100"));
			socket.shutdownOutput();
			assertThat(in.readAllBytes()).isEmpty();
		}
	}

	/**
	 * Detecting the header takes the connection's first byte, and a tcp route's upstream
	 * is dialed only after it: a headerless connection whose server would speak first is
	 * closed at the head deadline, never dialed. A control plane connection waits for its
	 * first byte under the same deadline, ahead of gRPC's TLS handshake timeout (10s).
	 */
	@Test
	void aSilentConnectionIsClosedAtTheHeadDeadline() throws Exception {
		this.startClient();
		int dials = echoAccepts.get();
		try (Socket route = new Socket("127.0.0.1", openTcpRoutePort);
				Socket control = new Socket("127.0.0.1", grpcPort)) {
			assertThat(TestSockets.awaitClosed(route, HEAD_TIMEOUT.multipliedBy(3)))
				.isGreaterThan(HEAD_TIMEOUT.multipliedBy(3).dividedBy(4));
			// gRPC alone would hold it until its TLS handshake timeout
			TestSockets.awaitClosed(control, HEAD_TIMEOUT.multipliedBy(3));
		}
		assertThat(echoAccepts.get()).isEqualTo(dials);
	}

	@Test
	void aMalformedHeaderFailsTheConnection() throws Exception {
		this.startClient();
		// a v2 header with an unsupported address family
		byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4)
			.put(V2_SIGNATURE)
			.put((byte) 0x21) // version 2, command PROXY
			.put((byte) 0x31) // family 3: unsupported
			.putShort((short) 0)
			.array();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(header);
			out.flush();
			assertThat(socket.getInputStream().read()).isEqualTo(-1);
		}
	}

	@Test
	void controlPlaneHeaderSourceBecomesTheTunnelSessionsRemoteAddress() throws Exception {
		this.startClient();
		try (DelayingProxy frontEnd = DelayingProxy.prefixing(grpcPort, proxyV2(32200))) {
			TunnelClient proxied = TunnelClient.builder()
				.properties(SluiceClientProperties.builder()
					.serverUrl("grpcs://127.0.0.1:" + frontEnd.port())
					.insecure(true)
					.clientId("proxied")
					.upstream(Upstream.builder()
						.host("control.local")
						.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
						.build())
					.token("it-token")
					.build())
				.taskExecutor(TASK_EXECUTOR)
				.meterRegistry(new SimpleMeterRegistry())
				.build();
			proxied.start();
			try {
				Awaitility.await()
					.atMost(Duration.ofSeconds(10))
					.until(() -> proxied.isConnected() && this.sessions.find("proxied").isPresent());
				assertThat(this.sessions.find("proxied").orElseThrow().remoteAddress()).isEqualTo("203.0.113.7:32200");
				// the key of the per-address tunnel limits
				assertThat(this.sessions.find("proxied").orElseThrow().sourceAddress()).isEqualTo("203.0.113.7");
				// a headerless connection on the same listener keeps its socket peer
				assertThat(this.sessions.find("direct").orElseThrow().remoteAddress()).matches("127\\.0\\.0\\.1:\\d+");
				// the TLS stream behind the header carries the tunnel
				String response = raw(new byte[0],
						"GET / HTTP/1.1\r\nHost: control.local\r\nConnection: close\r\n\r\n");
				assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 200 OK");
				assertThat(bodyOf(response)).isEqualTo("hello-from-upstream");
			}
			finally {
				proxied.stop();
			}
		}
	}

	/** A self-signed localhost key pair for the control plane's TLS. */
	private static Path selfSignedKeystore() {
		try {
			Path keystore = Files.createTempFile("sluice-proxy-protocol", ".p12");
			Files.delete(keystore);
			keystore.toFile().deleteOnExit();
			String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
			Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "grpc", "-keyalg", "RSA", "-keysize",
					"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=localhost", "-ext",
					"san=dns:localhost,ip:127.0.0.1", "-validity", "1", "-keystore",
					keystore.toAbsolutePath().toString())
				.redirectErrorStream(true)
				.start();
			if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
				throw new IllegalStateException("keytool failed: "
						+ new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
			}
			return keystore.toAbsolutePath();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** A v2 PROXY header for a TCP4 connection from 203.0.113.7 at the source port. */
	private static byte[] proxyV2(int sourcePort) {
		return ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 12)
			.put(V2_SIGNATURE)
			.put((byte) 0x21) // version 2, command PROXY
			.put((byte) 0x11) // family INET, transport STREAM
			.putShort((short) 12)
			.put(PROXIED_SOURCE)
			.put(PROXIED_DEST)
			.putShort((short) sourcePort)
			.putShort((short) 80)
			.array();
	}

	private record Response(String statusLine, String body) {

	}

	private Response exchange(String request) throws Exception {
		String response = raw(new byte[0], request);
		int bodyStart = response.indexOf("\r\n\r\n") + 4;
		return new Response(response.substring(0, response.indexOf("\r\n")),
				response.substring(bodyStart, response.length()));
	}

	private static String raw(byte[] header, String request) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(header);
			out.write(request.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			InputStream in = socket.getInputStream();
			byte[] buffer = new byte[8192];
			int n;
			while ((n = in.read(buffer)) > 0) {
				bytes.write(buffer, 0, n);
			}
			return bytes.toString(StandardCharsets.UTF_8);
		}
	}

	private static List<String> accessLines() {
		// the appender appends under its own monitor
		synchronized (ACCESS_EVENTS) {
			return ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
		}
	}

	private static String bodyOf(String response) {
		int bodyStart = response.indexOf("\r\n\r\n") + 4;
		return response.substring(bodyStart);
	}

}
