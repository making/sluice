package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
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
 * End to end WebSocket tunneling: a minimal RFC6455 echo server sits behind the tunnel
 * and the real {@link TunnelClient} fronts it; the handshake (101 Switching Protocols)
 * and subsequent bidirectional frames must pass through the data plane untouched.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class WebSocketTunnelE2ETest {

	private static final String WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("ws-client-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static ServerSocket upstream;

	private static @Nullable Thread acceptor;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() {
		try {
			upstream = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		Runnable echoLoop = () -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					Thread.ofVirtual().start(() -> echoWebSocket(socket));
				}
				catch (Exception e) {
					return; // closed
				}
			}
		};
		acceptor = Thread.ofPlatform().name("ws-echo-acceptor").start(echoLoop);
	}

	/** Minimal RFC6455 echo: 101 handshake, then echo each masked frame unmasked. */
	private static void echoWebSocket(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			var out = socket.getOutputStream();
			String head = readHead(in);
			String key = headerValue(head, "Sec-WebSocket-Key");
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
				byte[] mask = new byte[4];
				fill(in, mask);
				byte[] payload = new byte[length];
				fill(in, payload);
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

	private static String readHead(InputStream in) throws Exception {
		StringBuilder head = new StringBuilder();
		while (true) {
			int b = in.read();
			if (b < 0) {
				throw new java.io.EOFException();
			}
			head.append((char) b);
			int length = head.length();
			if (length >= 4 && head.charAt(length - 4) == '\r' && head.charAt(length - 3) == '\n'
					&& head.charAt(length - 2) == '\r' && head.charAt(length - 1) == '\n') {
				return head.toString();
			}
		}
	}

	private static String headerValue(String head, String name) {
		for (String line : head.split("\r\n")) {
			int colon = line.indexOf(':');
			if (colon > 0 && name.equalsIgnoreCase(line.substring(0, colon).trim())) {
				return line.substring(colon + 1).trim();
			}
		}
		throw new IllegalStateException("header not found: " + name);
	}

	private static void fill(InputStream in, byte[] buffer) throws Exception {
		int read = 0;
		while (read < buffer.length) {
			int n = in.read(buffer, read, buffer.length - read);
			if (n < 0) {
				throw new java.io.EOFException();
			}
			read += n;
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		int webPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(webPort));
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** Starts the real client against the running server and waits for registration. */
	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream("ws.local", "http://127.0.0.1:" + upstream.getLocalPort())
			.token("it-token")
			.build();
		TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
		started.start();
		this.client = started;
		// the ADVERTISE frame is applied asynchronously on the server
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (acceptor != null) {
			acceptor.interrupt();
		}
		if (upstream != null && !upstream.isClosed()) {
			try {
				upstream.close();
			}
			catch (Exception e) {
				// ignore
			}
		}
	}

	@Autowired
	SessionRegistry sessions;

	@Autowired
	DataProxyServer dataProxyServer;

	@Test
	void webSocketHandshakeAndEchoPassThroughTunnel() throws Exception {
		startClient();
		assertThat(this.sessions.count()).isEqualTo(1);
		assertThat(this.dataProxyServer.boundPort()).isEqualTo(dataPort);
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

	private byte[] readFully(InputStream in, int length) throws Exception {
		byte[] data = new byte[length];
		fill(in, data);
		return data;
	}

}
