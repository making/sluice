package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
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
import am.ik.sluice.server.proxy.ConnectionHeadParser;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end verification of the per-upstream {@code preserve-host} option: the default
 * relays the request head verbatim (the upstream sees the public host), while
 * {@code preserve-host=false} rewrites the Host header (HTTP/1.1) or the
 * {@code :authority} (h2) to the upstream target's host[:port].
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class PreserveHostE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-preserve-host").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer http1Upstream;

	private static ServerSocket h2Upstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final java.util.Map<String, TunnelClient> clients = new java.util.concurrent.ConcurrentHashMap<>();

	@BeforeAll
	static void startUpstreams() throws Exception {
		// h1 upstream echoing the received Host header back as the body
		http1Upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		http1Upstream.createContext("/", exchange -> {
			String host = exchange.getRequestHeaders().getFirst("Host");
			byte[] body = ("Host=" + host).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		http1Upstream.start();
		// h2 upstream echoing the received :authority back as the body
		h2Upstream = new ServerSocket();
		h2Upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!h2Upstream.isClosed()) {
				try {
					Socket socket = h2Upstream.accept();
					UPSTREAM_EXECUTOR.execute(() -> serveH2(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
	}

	/**
	 * Answers every request head arriving on the socket, one canned response per parsed
	 * head, so several requests can share a single connection.
	 */
	private static void serveH2(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			byte[] chunk = new byte[1024];
			boolean prefaceSeen = false;
			while (true) {
				int n = in.read(chunk);
				if (n < 0) {
					return;
				}
				buffer.write(chunk, 0, n);
				byte[] bytes = buffer.toByteArray();
				if (!prefaceSeen && !ConnectionHeadParser.isHttp2(bytes)) {
					return;
				}
				// later requests arrive without the preface; the parser expects it, and
				// the
				// re-encoded heads carry no dynamic table references so a fresh decoder
				// per
				// request parses them
				InputStream stream = prefaceSeen ? new java.io.ByteArrayInputStream(concat(TestH2.PREFACE, bytes))
						: new java.io.ByteArrayInputStream(bytes);
				ConnectionHeadParser.Head head = new ConnectionHeadParser(64 * 1024).parse(stream).orElse(null);
				if (head == null || head.request() == null || head.host() == null) {
					continue;
				}
				prefaceSeen = true;
				int streamId = head.h2() == null ? 1 : head.h2().streamId();
				buffer.reset();
				socket.getOutputStream().write(TestH2.cannedResponse(streamId, head.host()));
				socket.getOutputStream().flush();
			}
		}
		catch (Exception e) {
			// connection ended
		}
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] out = new byte[a.length + b.length];
		System.arraycopy(a, 0, out, 0, a.length);
		System.arraycopy(b, 0, out, a.length, b.length);
		return out;
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	private void startClient(String host, String target, boolean preserveHost) {
		clients.computeIfAbsent(host, h -> {
			SluiceClientProperties properties = SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(h).target(target).preserveHost(preserveHost).build())
				.token("it-token")
				.build();
			TunnelClient started = TunnelClient.builder()
				.properties(properties)
				.taskExecutor(TASK_EXECUTOR)
				.meterRegistry(new SimpleMeterRegistry())
				.build();
			started.start();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessionsCount() > 0);
			return started;
		});
	}

	private final @Nullable SessionRegistry sessions;

	PreserveHostE2ETest(@Autowired SessionRegistry sessions) {
		this.sessions = sessions;
	}

	private int sessionsCount() {
		SessionRegistry registry = this.sessions;
		return registry == null ? 0 : registry.count();
	}

	private String get(String host) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			InputStream in = socket.getInputStream();
			int n;
			while ((n = in.read(buffer)) > 0) {
				bytes.write(buffer, 0, n);
			}
			String response = bytes.toString(StandardCharsets.UTF_8);
			return response.substring(response.indexOf("\r\n\r\n") + 4);
		}
	}

	@AfterAll
	void tearDown() {
		clients.values().forEach(TunnelClient::stop);
		try {
			h2Upstream.close();
		}
		catch (Exception e) {
			// ignore
		}
		if (http1Upstream != null) {
			http1Upstream.stop(0);
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void defaultPreservesThePublicHost() throws Exception {
		String host = "keep.local";
		startClient(host, "http://127.0.0.1:" + http1Upstream.getAddress().getPort(), true);
		assertThat(get(host)).isEqualTo("Host=keep.local");
	}

	@Test
	void disabledPreserveHostRewritesHttp1Host() throws Exception {
		int port = http1Upstream.getAddress().getPort();
		String host = "rewrite.local";
		startClient(host, "http://127.0.0.1:" + port, false);
		assertThat(get(host)).isEqualTo("Host=127.0.0.1:" + port);
	}

	@Test
	void disabledPreserveHostRewritesH2Authority() throws Exception {
		int port = h2Upstream.getLocalPort();
		String host = "h2.rewrite.local";
		startClient(host, "http://127.0.0.1:" + port, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + host, ":method=GET", ":path=/");
			assertThat(TestH2.bodyOf(TestH2.readResponseFrames(socket.getInputStream())))
				.isEqualTo("127.0.0.1:" + port);
		}
	}

	@Test
	void disabledPreserveHostRewritesHttp1HostOnKeepAliveConnection() throws Exception {
		int port = http1Upstream.getAddress().getPort();
		String host = "keepalive.rewrite.local";
		startClient(host, "http://127.0.0.1:" + port, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			String first = exchange(socket, host);
			String second = exchange(socket, host);
			assertThat(first).isEqualTo("Host=127.0.0.1:" + port);
			assertThat(second).isEqualTo("Host=127.0.0.1:" + port);
		}
	}

	@Test
	void disabledPreserveHostRewritesH2AuthorityOnSecondRequestOfSameConnection() throws Exception {
		int port = h2Upstream.getLocalPort();
		String host = "h2.keepalive.local";
		startClient(host, "http://127.0.0.1:" + port, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + host, ":method=GET", ":path=/");
			assertThat(TestH2.bodyOf(TestH2.readResponseFrames(socket.getInputStream())))
				.isEqualTo("127.0.0.1:" + port);
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/"));
			assertThat(TestH2.bodyOf(TestH2.readResponseFrames(socket.getInputStream())))
				.isEqualTo("127.0.0.1:" + port);
		}
	}

	/**
	 * Sends one keep-alive request on the open socket and returns the response body.
	 */
	private static String exchange(Socket socket, String host) throws Exception {
		socket.getOutputStream()
			.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
		socket.getOutputStream().flush();
		InputStream in = socket.getInputStream();
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int current;
		while ((current = in.read()) >= 0) {
			head.write(current);
			if (endsWithBlankLine(head)) {
				break;
			}
		}
		String[] lines = head.toString(StandardCharsets.UTF_8).split("\r\n");
		int contentLength = 0;
		for (String line : lines) {
			int colon = line.indexOf(':');
			if (colon > 0 && "content-length".equalsIgnoreCase(line.substring(0, colon))) {
				contentLength = Integer.parseInt(line.substring(colon + 1).trim());
			}
		}
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		for (int i = 0; i < contentLength; i++) {
			body.write(in.read());
		}
		return body.toString(StandardCharsets.UTF_8);
	}

	private static boolean endsWithBlankLine(ByteArrayOutputStream stream) {
		byte[] bytes = stream.toByteArray();
		return bytes.length >= 4 && bytes[bytes.length - 4] == '\r' && bytes[bytes.length - 3] == '\n'
				&& bytes[bytes.length - 2] == '\r' && bytes[bytes.length - 1] == '\n';
	}

}
