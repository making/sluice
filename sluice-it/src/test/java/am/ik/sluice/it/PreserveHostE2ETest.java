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

	private static void serveH2(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			byte[] chunk = new byte[1024];
			String authority = null;
			while (authority == null) {
				int n = in.read(chunk);
				if (n < 0) {
					return;
				}
				buffer.write(chunk, 0, n);
				byte[] bytes = buffer.toByteArray();
				authority = authorityOf(bytes);
				if (authority == null && !ConnectionHeadParser.isHttp2(bytes)) {
					return;
				}
			}
			socket.getOutputStream().write(TestH2.cannedResponse(1, authority));
			socket.getOutputStream().flush();
			in.read(new byte[1]); // keep the relay open until the client hangs up
		}
		catch (Exception e) {
			// connection ended
		}
	}

	/**
	 * Parses the buffered bytes with the production head parser; null while partial.
	 */
	private static @Nullable String authorityOf(byte[] bytes) {
		if (!ConnectionHeadParser.isHttp2(bytes)) {
			return null;
		}
		return new ConnectionHeadParser(64 * 1024).parse(new java.io.ByteArrayInputStream(bytes))
			.map(ConnectionHeadParser.Head::host)
			.orElse(null);
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("server.port", () -> String.valueOf(freePort()));
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private void startClient(String host, String target, boolean preserveHost) {
		clients.computeIfAbsent(host, h -> {
			SluiceClientProperties properties = SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(h, target, preserveHost)
				.build();
			TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
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

}
