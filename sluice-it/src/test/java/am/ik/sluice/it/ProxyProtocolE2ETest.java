package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: an SNAT front end (a local proxy, here) speaks the PROXY protocol to the
 * data plane. The header is consumed before routing / relay -- never forwarded -- and its
 * source address becomes the connection peer, admitting it to routes whose
 * {@code allowed-cidrs} list no socket address would match.
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

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static HttpServer httpUpstream;

	private static @Nullable ServerSocket echoServer;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstreams() throws Exception {
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
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.proxy-protocol", () -> "true");
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
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
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
		byte[] header = proxyV2();
		String response = raw(header, "GET / HTTP/1.1\r\nHost: proxied.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 200 OK");
		assertThat(bodyOf(response)).isEqualTo("hello-from-upstream");
	}

	@Test
	void v1HeaderPeerAdmitsTheConnectionToHttpRoutes() throws Exception {
		this.startClient();
		byte[] header = "PROXY TCP4 203.0.113.7 198.51.100.7 32100 80\r\n".getBytes(StandardCharsets.US_ASCII);
		String response = raw(header, "GET / HTTP/1.1\r\nHost: proxied.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 200 OK");
		assertThat(bodyOf(response)).isEqualTo("hello-from-upstream");
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
		byte[] header = proxyV2();
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(header);
			out.write("ping".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			// the echo starts at the first byte behind the header
			assertThat(new String(in.readNBytes(4), StandardCharsets.US_ASCII)).isEqualTo("ping");
			socket.shutdownOutput();
			assertThat(in.readAllBytes()).isEmpty();
		}
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

	/** A v2 PROXY header for the TCP4 proxied connection. */
	private static byte[] proxyV2() {
		return ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 12)
			.put(V2_SIGNATURE)
			.put((byte) 0x21) // version 2, command PROXY
			.put((byte) 0x11) // family INET, transport STREAM
			.putShort((short) 12)
			.put(PROXIED_SOURCE)
			.put(PROXIED_DEST)
			.putShort((short) 32100)
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

	private static String bodyOf(String response) {
		int bodyStart = response.indexOf("\r\n\r\n") + 4;
		return response.substring(bodyStart);
	}

}
