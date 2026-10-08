package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
 * End to end: access control through a proxying load balancer. A local TCP proxy plays
 * the LB: it rewrites the request head to carry {@code X-Forwarded-For: 198.51.100.7}
 * (appending to a header the client sent) and relays to the data plane as a trusted peer.
 * The route keyed to 198.51.100.0/24 answers through the LB -- the loopback peer alone
 * would not be admitted -- and a headerless direct connection is judged by its peer.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class AccessControlForwardedE2ETest {

	/** The trusted proxy peer: every LB connection and direct test connection. */
	private static final String TRUSTED_PROXY = "127.0.0.1";

	/** The client network behind the LB, as the LB reports it. */
	private static final String FORWARDED_CLIENT = "198.51.100.7";

	private static final String FORWARDED_HEADER = "X-Forwarded-For: " + FORWARDED_CLIENT + "\r\n";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-xff-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int lbPort;

	private static HttpServer httpUpstream;

	private static @Nullable LoadBalancer loadBalancer;

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
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		lbPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.access-control.trusted-proxy-cidrs", () -> TRUSTED_PROXY + "/32");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	private void startClient() throws Exception {
		if (this.client != null) {
			return;
		}
		startLoadBalancer();
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("forwarded.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.allowedCidrs(List.of("198.51.100.0/24"))
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
	static void tearDown() throws Exception {
		if (loadBalancer != null) {
			loadBalancer.close();
		}
		httpUpstream.stop(0);
	}

	@Test
	void theTrustedProxiesForwardedEntryIsJudgedInsteadOfThePeer() throws Exception {
		this.startClient();
		// the client behind the LB sends no header; the entry the LB appended decides
		Response response = this
			.throughLoadBalancer("GET / HTTP/1.1\r\nHost: forwarded.local\r\nConnection: close\r\n\r\n");
		assertThat(response.statusLine).isEqualTo("HTTP/1.1 200 OK");
		assertThat(response.body).isEqualTo("hello-from-upstream");
	}

	@Test
	void theRightmostEntryBeatsTheClientSuppliedOne() throws Exception {
		this.startClient();
		// the client spoofs an entry outside the route's list; the LB's appended entry
		// wins
		Response spoofed = this.throughLoadBalancer(
				"GET / HTTP/1.1\r\nHost: forwarded.local\r\nX-Forwarded-For: 203.0.113.9\r\nConnection: close\r\n\r\n");
		assertThat(spoofed.statusLine).isEqualTo("HTTP/1.1 200 OK");
	}

	@Test
	void aHeaderlessDirectConnectionIsJudgedByItsPeer() throws Exception {
		this.startClient();
		// no trusted proxy appended anything: the loopback peer is outside the route's
		// list
		Response direct = new Response(raw("GET / HTTP/1.1\r\nHost: forwarded.local\r\nConnection: close\r\n\r\n"));
		assertThat(direct.statusLine).isEqualTo("HTTP/1.1 403 Forbidden");
	}

	private Response throughLoadBalancer(String request) throws Exception {
		return new Response(rawThrough(request));
	}

	/**
	 * Sends the request through the LB to the data plane and reads the response to EOF.
	 */
	private static String rawThrough(String request) throws Exception {
		try (Socket lb = new Socket("127.0.0.1", lbPort)) {
			lb.setSoTimeout(10_000);
			OutputStream out = lb.getOutputStream();
			out.write(request.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			return readAll(lb.getInputStream());
		}
	}

	private static String raw(String request) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(request.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			return readAll(socket.getInputStream());
		}
	}

	private static String readAll(InputStream in) throws Exception {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int n;
		while ((n = in.read(buffer)) > 0) {
			bytes.write(buffer, 0, n);
		}
		return bytes.toString(StandardCharsets.UTF_8);
	}

	private record Response(String statusLine, String body) {

		private Response(String response) {
			this(response.substring(0, response.indexOf("\r\n")), response.substring(response.indexOf("\r\n\r\n") + 4));
		}

	}

	/**
	 * The stand-in load balancer: a TCP proxy to the data plane whose every relayed
	 * connection claims the same client address. The request head is rewritten before it
	 * is forwarded, the bytes already read with it are preserved.
	 */
	private static final class LoadBalancer implements AutoCloseable {

		private final ServerSocket server;

		private final Thread accepter;

		private LoadBalancer(int port, int dataPort, String forwardedClient) throws Exception {
			this.server = new ServerSocket();
			this.server.setReuseAddress(true);
			this.server.bind(new InetSocketAddress("127.0.0.1", port), 128);
			this.accepter = Thread.ofVirtual().name("e2e-xff-lb-accept").start(() -> {
				while (!this.server.isClosed()) {
					try {
						Socket client = this.server.accept();
						client.setTcpNoDelay(true);
						Thread.ofVirtual()
							.name("e2e-xff-lb-conn")
							.start(() -> this.relay(client, dataPort, forwardedClient));
					}
					catch (Exception e) {
						return;
					}
				}
			});
		}

		private void relay(Socket client, int dataPort, String forwardedClient) {
			try (client; Socket upstream = new Socket()) {
				// the head is complete before the upstream socket exists: it is rewritten
				// in one piece, the bytes beyond it are forwarded untouched
				HeadAndRest head = this.readHead(client);
				upstream.connect(new InetSocketAddress("127.0.0.1", dataPort));
				upstream.setTcpNoDelay(true);
				OutputStream out = upstream.getOutputStream();
				out.write(head.forwarded(forwardedClient));
				out.flush();
				InputStream upstreamIn = upstream.getInputStream();
				InputStream clientIn = client.getInputStream();
				OutputStream clientOut = client.getOutputStream();
				Thread.ofVirtual().name("e2e-xff-lb-copy").start(() -> copy(clientIn, out));
				copy(upstreamIn, clientOut);
			}
			catch (Exception e) {
				// connection torn down; nothing to do
			}
			finally {
				closeQuietly(client);
			}
		}

		/** Reads through the blank line that ends the request head. */
		private HeadAndRest readHead(Socket client) throws Exception {
			InputStream in = client.getInputStream();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			int b;
			while ((b = in.read()) >= 0) {
				bytes.write(b);
				byte[] array = bytes.toByteArray();
				int length = array.length;
				if (length >= 4 && array[length - 4] == '\r' && array[length - 3] == '\n' && array[length - 2] == '\r'
						&& array[length - 1] == '\n') {
					break;
				}
			}
			return new HeadAndRest(bytes.toByteArray());
		}

		private static void copy(InputStream in, OutputStream out) {
			try {
				byte[] buffer = new byte[8192];
				int n;
				while ((n = in.read(buffer)) > 0) {
					out.write(buffer, 0, n);
					out.flush();
				}
			}
			catch (Exception e) {
				// the other direction tore the connection down
			}
		}

		private static void closeQuietly(Socket socket) {
			try {
				socket.close();
			}
			catch (Exception e) {
				// ignore
			}
		}

		@Override
		public void close() throws Exception {
			this.server.close();
			this.accepter.join();
		}

	}

	/** The bytes read up to and including the head's blank line, verbatim. */
	private record HeadAndRest(byte[] bytes) {

		/**
		 * The head with the LB's claim appended to the client's header, if it sent one.
		 */
		byte[] forwarded(String client) {
			String text = new String(this.bytes, StandardCharsets.US_ASCII);
			int headEnd = text.indexOf("\r\n\r\n");
			String rest = text.substring(headEnd);
			List<String> lines = new ArrayList<>(List.of(text.substring(0, headEnd).split("\r\n", -1)));
			boolean appended = false;
			for (int i = 1; i < lines.size(); i++) {
				String line = lines.get(i);
				if (line.toLowerCase(java.util.Locale.ROOT).startsWith("x-forwarded-for:")) {
					String existing = line.substring(line.indexOf(':') + 1).trim();
					lines.set(i, "X-Forwarded-For: " + existing + ", " + client);
					appended = true;
					break;
				}
			}
			if (!appended) {
				lines.add(FORWARDED_HEADER.trim());
			}
			return (String.join("\r\n", lines) + rest).getBytes(StandardCharsets.US_ASCII);
		}

	}

	private static void startLoadBalancer() throws Exception {
		loadBalancer = new LoadBalancer(lbPort, dataPort, FORWARDED_CLIENT);
	}

}
