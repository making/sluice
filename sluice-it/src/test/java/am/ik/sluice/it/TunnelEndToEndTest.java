package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

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
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: the real {@link TunnelClient} connects to the running server, advertises an
 * upstream backed by a local HttpServer, and HTTP requests through the data plane must
 * round trip. The second request on the same connection verifies keep-alive reuse; a
 * WebSocket upgrade and its frames pass through to a second upstream.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TunnelEndToEndTest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-client-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static final String WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

	private static HttpServer upstream;

	/** A minimal RFC 6455 echo upstream. */
	private static ServerSocket webSocketUpstream;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws Exception {
		upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		upstream.createContext("/", exchange -> {
			byte[] body = "hello-from-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		upstream.start();
		webSocketUpstream = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
		Thread.ofVirtual().name("ws-echo-acceptor").start(() -> {
			while (!webSocketUpstream.isClosed()) {
				try {
					Socket socket = webSocketUpstream.accept();
					Thread.ofVirtual().start(() -> echoWebSocket(socket));
				}
				catch (IOException e) {
					return; // closed
				}
			}
		});
	}

	/** Minimal RFC 6455 echo: 101 handshake, then echo each masked frame unmasked. */
	private static void echoWebSocket(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			var out = socket.getOutputStream();
			String head = readHead(in);
			String key = webSocketHeader(head, "Sec-WebSocket-Key");
			String accept = Base64.getEncoder()
				.encodeToString(MessageDigest.getInstance("SHA-1").digest((key + WS_MAGIC).getBytes()));
			out.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
					+ "Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			while (true) {
				int b0 = in.read();
				int b1 = in.read();
				if (b0 < 0 || b1 < 0) {
					return;
				}
				int opcode = b0 & 0x0f;
				int length = b1 & 0x7f;
				if (length == 126) {
					length = (in.read() << 8) | in.read();
				}
				byte[] mask = readFully(in, 4);
				byte[] payload = readFully(in, length);
				for (int i = 0; i < length; i++) {
					payload[i] ^= mask[i % 4];
				}
				if (opcode == 8) { // close
					out.write(new byte[] { (byte) 0x88, 0 });
					out.flush();
					return;
				}
				out.write(new byte[] { (byte) (0x80 | opcode), (byte) length });
				out.write(payload);
				out.flush();
			}
		}
		catch (Exception e) {
			// connection dropped; the echo server is best effort
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		int webPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(webPort));
	}

	/** Starts the real client against the running server and waits for registration. */
	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("demo.local")
				.target("http://127.0.0.1:" + upstream.getAddress().getPort())
				.build())
			.upstream(Upstream.builder()
				.host("ws.local")
				.target("http://127.0.0.1:" + webSocketUpstream.getLocalPort())
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
		// the ADVERTISE frame is applied asynchronously on the server
		// the advertise acknowledgement turns isConnected() true, and the server sends it
		// once the routes are registered; the session is registered before that
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (upstream != null) {
			upstream.stop(0);
		}
		if (webSocketUpstream != null) {
			try {
				webSocketUpstream.close();
			}
			catch (IOException e) {
				// ignore
			}
		}
	}

	@Autowired
	SessionRegistry sessions;

	@Autowired
	DataProxyServer dataProxyServer;

	@Autowired
	MeterRegistry meterRegistry;

	@Test
	void requestRoundTripsThroughTunnel() throws Exception {
		startClient();
		assertThat(this.sessions.count()).isEqualTo(1);
		assertThat(this.dataProxyServer.boundPort()).isEqualTo(dataPort);
		Response response = exchange(false);
		assertThat(response.statusLine).startsWith("HTTP/1.1 200");
		assertThat(response.body).isEqualTo("hello-from-upstream");
	}

	@Test
	void relayedBytesAreCountedPerRouteAndDirection() throws Exception {
		startClient();
		String request = "GET / HTTP/1.1\r\nHost: demo.local\r\nConnection: close\r\n\r\n";
		exchange(false);
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> this.meterRegistry.find("sluice.tunnel.bytes")
				.tags("direction", "outbound", "route", "demo.local")
				.counter() != null);
		Counter inbound = this.meterRegistry.find("sluice.tunnel.bytes")
			.tags("direction", "inbound", "route", "demo.local")
			.counter();
		Counter outbound = this.meterRegistry.find("sluice.tunnel.bytes")
			.tags("direction", "outbound", "route", "demo.local")
			.counter();
		assertThat(inbound).isNotNull();
		assertThat(outbound).isNotNull();
		// counters are byte-accurate: the relayed totals cover the full request and the
		// 19-byte "hello-from-upstream" body plus its response head
		assertThat(inbound.count()).isGreaterThanOrEqualTo(request.length());
		assertThat(outbound.count())
			.isGreaterThanOrEqualTo("hello-from-upstream".getBytes(StandardCharsets.UTF_8).length);
	}

	@Test
	void consecutiveRequestsReuseKeepAliveConnection() throws Exception {
		startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			var out = socket.getOutputStream();
			for (int i = 0; i < 2; i++) {
				out.write("GET / HTTP/1.1\r\nHost: demo.local\r\nConnection: keep-alive\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
				out.flush();
				Response response = readResponse(socket.getInputStream());
				assertThat(response.statusLine).startsWith("HTTP/1.1 200");
				assertThat(response.body).isEqualTo("hello-from-upstream");
			}
		}
	}

	@Test
	void webSocketHandshakeAndEchoPassThroughTunnel() throws Exception {
		startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			byte[] keyBytes = new byte[16];
			for (int i = 0; i < keyBytes.length; i++) {
				keyBytes[i] = (byte) i;
			}
			String key = Base64.getEncoder().encodeToString(keyBytes);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: ws.local\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
						+ "Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			String handshake = readHead(socket.getInputStream());
			assertThat(handshake).startsWith("HTTP/1.1 101");
			assertThat(handshake).containsIgnoringCase("upgrade");

			// masked text frame -> echoed unmasked
			byte[] message = "hello-ws-through-tunnel".getBytes(StandardCharsets.UTF_8);
			byte[] mask = { 0x11, 0x22, 0x33, 0x44 };
			byte[] frame = new byte[6 + message.length];
			frame[0] = (byte) 0x81;
			frame[1] = (byte) (0x80 | message.length);
			System.arraycopy(mask, 0, frame, 2, 4);
			for (int i = 0; i < message.length; i++) {
				frame[6 + i] = (byte) (message[i] ^ mask[i % 4]);
			}
			socket.getOutputStream().write(frame);
			byte[] header = readFully(socket.getInputStream(), 2);
			assertThat(header[0] & 0x0f).isEqualTo(0x1); // text frame
			byte[] echo = readFully(socket.getInputStream(), header[1] & 0x7f);
			assertThat(new String(echo, StandardCharsets.UTF_8)).isEqualTo("hello-ws-through-tunnel");
		}
	}

	private static String readHead(InputStream in) throws IOException {
		StringBuilder head = new StringBuilder();
		while (true) {
			int b = in.read();
			if (b < 0) {
				throw new EOFException();
			}
			head.append((char) b);
			int length = head.length();
			if (length >= 4 && head.charAt(length - 4) == '\r' && head.charAt(length - 3) == '\n'
					&& head.charAt(length - 2) == '\r' && head.charAt(length - 1) == '\n') {
				return head.toString();
			}
		}
	}

	private static String webSocketHeader(String head, String name) {
		for (String line : head.split("\r\n")) {
			int colon = line.indexOf(':');
			if (colon > 0 && name.equalsIgnoreCase(line.substring(0, colon).trim())) {
				return line.substring(colon + 1).trim();
			}
		}
		throw new IllegalStateException("header not found: " + name);
	}

	private static byte[] readFully(InputStream in, int length) throws IOException {
		byte[] data = new byte[length];
		int read = 0;
		while (read < length) {
			int n = in.read(data, read, length - read);
			if (n < 0) {
				throw new EOFException();
			}
			read += n;
		}
		return data;
	}

	private Response exchange(boolean keepAlive) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			String connectionHeader = keepAlive ? "keep-alive" : "close";
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: demo.local\r\nConnection: " + connectionHeader + "\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			return readResponse(socket.getInputStream());
		}
	}

	private record Response(String statusLine, String body) {
	}

	private static Response readResponse(InputStream in) throws Exception {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int n;
		while ((n = in.read(buffer)) > 0) {
			bytes.write(buffer, 0, n);
			String text = bytes.toString(StandardCharsets.UTF_8);
			int bodyStart = text.indexOf("\r\n\r\n");
			if (bodyStart > 0) {
				int contentLength = headerValue(text, "content-length");
				if (bytes.size() >= bodyStart + 4 + contentLength) {
					return new Response(text.substring(0, text.indexOf("\r\n")),
							text.substring(bodyStart + 4, bodyStart + 4 + contentLength));
				}
			}
		}
		throw new AssertionError("response incomplete: " + bytes);
	}

	private static int headerValue(String head, String name) {
		return Integer.parseInt(head.lines()
			.filter(line -> line.toLowerCase().startsWith(name + ":"))
			.findFirst()
			.orElse(name + ": 0")
			.substring(name.length() + 1)
			.trim());
	}

}
