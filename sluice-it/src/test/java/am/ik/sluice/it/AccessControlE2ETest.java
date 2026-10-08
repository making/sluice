package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;
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
 * End to end: data plane IP access control. The server-wide allow list admits the
 * loopback; a route's {@code allowed-cidrs} replaces it, so the restricted routes answer
 * 403 (http) or close (tcp) from a non-listed address while the open routes serve.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class AccessControlE2ETest {

	private static final int GOAWAY = 0x7;

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-acl-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int lockedRoutePort;

	private static int openRoutePort;

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
		Thread.ofVirtual().name("e2e-acl-echo-accept").start(AccessControlE2ETest::acceptLoop);
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		lockedRoutePort = TestPorts.freePort();
		openRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.access-control.allow-cidrs", () -> "127.0.0.0/8");
		registry.add("sluice.tcp-port-range", () -> lockedRoutePort + "," + openRoutePort);
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
				Thread.ofVirtual().name("e2e-acl-echo-conn").start(() -> echo(socket));
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
				.host("open.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.build())
			.upstream(Upstream.builder()
				.host("locked.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.allowedCidrs(List.of("10.0.0.0/8"))
				.build())
			.upstream(Upstream.builder()
				.host("locked.tcp")
				.target("tcp://127.0.0.1:" + echoServer().getLocalPort())
				.listenPort(lockedRoutePort)
				.allowedCidrs(List.of("10.0.0.0/8"))
				.build())
			.upstream(Upstream.builder()
				.host("open.tcp")
				.target("tcp://127.0.0.1:" + echoServer().getLocalPort())
				.listenPort(openRoutePort)
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
	void openHttpRouteServesFromAnAddressTheGlobalAllowListAdmits() throws Exception {
		this.startClient();
		Response response = exchange("GET / HTTP/1.1\r\nHost: open.local\r\nConnection: close\r\n\r\n");
		assertThat(response.statusLine).isEqualTo("HTTP/1.1 200 OK");
		assertThat(response.body).isEqualTo("hello-from-upstream");
	}

	@Test
	void routeAllowedCidrsReplaceTheGlobalAllowList() throws Exception {
		this.startClient();
		// the loopback is in the global allow list, but the route admits 10/8 only
		String response = raw("GET / HTTP/1.1\r\nHost: locked.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 403 Forbidden");
		assertThat(response).startsWith("HTTP/1.1 403 Forbidden\r\nContent-Type: text/html; charset=utf-8\r\n");
		assertThat(response).endsWith("</html>\n");
	}

	@Test
	void h2cForbiddenCarriesTheStatusAndGoaway() throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":method=GET", ":scheme=http", ":path=/",
					":authority=locked.local");
			InputStream in = socket.getInputStream();
			List<TestH2.Frame> frames = TestH2.readResponseFrames(in);
			TestH2.Frame goaway = java.util.Objects.requireNonNull(TestH2.readFrame(in));

			TestH2.Frame headersFrame = frames.getFirst();
			assertThat(headersFrame.type()).isEqualTo(TestH2.HEADERS);
			Http2Headers headers = new DefaultHttp2HeadersDecoder(true).decodeHeaders(1,
					Unpooled.wrappedBuffer(headersFrame.payload()));
			assertThat(headers.status()).hasToString("403");
			assertThat(TestH2.bodyOf(frames)).startsWith("<!DOCTYPE html>").endsWith("</html>\n");
			assertThat(goaway.type()).isEqualTo(GOAWAY);
		}
	}

	@Test
	void lockedTcpRouteClosesTheConnectionWithoutRelaying() throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", lockedRoutePort)) {
			socket.setSoTimeout(10_000);
			assertThat(socket.getInputStream().readAllBytes()).isEmpty();
		}
	}

	@Test
	void openTcpRouteEchoesFromAnAddressTheGlobalAllowListAdmits() throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", openRoutePort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write("ping".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			assertThat(new String(in.readNBytes(4), StandardCharsets.US_ASCII)).isEqualTo("ping");
			socket.shutdownOutput();
			assertThat(in.readAllBytes()).isEmpty();
		}
	}

	private record Response(String statusLine, String body) {
	}

	private Response exchange(String request) throws Exception {
		String response = raw(request);
		int bodyStart = response.indexOf("\r\n\r\n") + 4;
		return new Response(response.substring(0, response.indexOf("\r\n")),
				response.substring(bodyStart, response.length()));
	}

	private static String raw(String request) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
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

}
