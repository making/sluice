package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLServerSocket;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.filter.UpstreamFilter;
import am.ik.sluice.client.filter.UpstreamFilterFactory;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An upstream's exchanges changed on the tunnel client, through a real server: a custom
 * filter and a plugin defined by name, used per route with their settings per route,
 * routes sharing a target among them, and the usage built-in. What a keep-alive
 * connection carries stays framed for the server's relay to follow.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@SuppressWarnings("NullAway")
class UpstreamFilterE2ETest {

	private static final Duration TIMEOUT = Duration.ofSeconds(20);

	private static final int LARGE = 8 * 1024 * 1024;

	private static final String PLUGIN = Path.of("../sluice-client/src/test/resources/wasm/test_plugin.wasm")
		.toAbsolutePath()
		.toUri()
		.toString();

	private static final String LLM_JSON = "{\"model\":\"m\",\"usage\":{\"input_tokens\":12,\"output_tokens\":34}}";

	private static final String LLM_START = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n";

	private static final String LLM_END = "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

	private static int grpcPort;

	private static int dataPort;

	/**
	 * A custom filter a custom build would define as a bean: the path picks the behavior,
	 * its route's {@code mark} is told in a header.
	 */
	static final class RewritingFactory implements UpstreamFilterFactory {

		@Override
		public String name() {
			return "rewriting";
		}

		@Override
		public UpstreamFilter create(Context context) {
			String mark = context.settings().args().getOrDefault("mark", "none");
			return (request, requestHeaders) -> {
				requestHeaders.remove("accept-encoding");
				return (status, headers, bodiless) -> onResponse(request, status, headers, mark);
			};
		}

		private static UpstreamFilter.@Nullable BodyHandler onResponse(UpstreamFilter.Request request, int status,
				UpstreamFilter.Headers headers, String mark) {
			headers.remove("x-upstream-private");
			headers.set("x-filter-saw", request.method() + " " + request.target() + " " + status + " " + mark);
			if (request.target().startsWith("/boom")) {
				throw new IllegalStateException("the filter failed");
			}
			if (request.target().startsWith("/verbatim")) {
				return null;
			}
			if (request.target().startsWith("/whole")) {
				return new Whole();
			}
			return new UpstreamFilter.BodyRewriter() {

				@Override
				public byte[] onData(byte[] bytes, int offset, int length) {
					// twice the length, upper case
					byte[] out = new byte[length * 2];
					for (int i = 0; i < length; i++) {
						byte b = bytes[offset + i];
						out[2 * i] = (byte) Character.toUpperCase((char) (b & 0xff));
						out[2 * i + 1] = '.';
					}
					return out;
				}

				@Override
				public byte[] onEnd() {
					return "<end>".getBytes(StandardCharsets.US_ASCII);
				}

			};
		}

		/** What needs the whole body: a change across the upstream's chunks. */
		private static final class Whole implements UpstreamFilter.BodyRewriter {

			private final StringBuilder held = new StringBuilder();

			@Override
			public byte[] onData(byte[] bytes, int offset, int length) {
				this.held.append(new String(bytes, offset, length, StandardCharsets.ISO_8859_1));
				return new byte[0];
			}

			@Override
			public byte[] onEnd() {
				return this.held.toString()
					.replace("hello world", "replaced across the chunks")
					.getBytes(StandardCharsets.ISO_8859_1);
			}

		}

	}

	/** An HTTP/1.1 upstream by script: the request's path picks the response. */
	static final class ScriptedUpstream implements AutoCloseable {

		final ServerSocket server;

		final List<String> heads = new CopyOnWriteArrayList<>();

		final CountDownLatch streamGate = new CountDownLatch(1);

		final CountDownLatch usageGate = new CountDownLatch(1);

		ScriptedUpstream(ServerSocket server) throws IOException {
			this.server = server;
			if (!server.isBound()) {
				server.bind(new InetSocketAddress("127.0.0.1", 0));
			}
			Thread.ofVirtual().start(() -> {
				while (!this.server.isClosed()) {
					try {
						Socket socket = this.server.accept();
						Thread.ofVirtual().start(() -> serve(socket));
					}
					catch (IOException e) {
						return;
					}
				}
			});
		}

		int port() {
			return this.server.getLocalPort();
		}

		private void serve(Socket socket) {
			try (socket) {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				serve(in, out);
			}
			catch (Exception e) {
				// the connection ended
			}
		}

		private void serve(InputStream in, OutputStream out) throws Exception {
			while (true) {
				StringBuilder read = new StringBuilder();
				int b;
				while (read.indexOf("\r\n\r\n") < 0 && (b = in.read()) >= 0) {
					read.append((char) b);
				}
				if (read.indexOf("\r\n\r\n") < 0) {
					return;
				}
				String head = read.toString();
				this.heads.add(head);
				String target = head.split(" ")[1];
				String path = target.split("\\?")[0];
				boolean headMethod = head.startsWith("HEAD ");
				String lower = head.toLowerCase(Locale.ROOT);
				boolean close = lower.contains("connection: close");
				String last = close ? "Connection: close\r\n" : "";
				switch (path) {
					case "/length" -> write(out, "HTTP/1.1 200 OK\r\nX-Upstream-Private: 1\r\n" + last
							+ "Content-Length: 5\r\n\r\n" + (headMethod ? "" : "abcde"));
					case "/chunked", "/whole" -> {
						write(out, "HTTP/1.1 200 OK\r\nX-Upstr");
						Thread.sleep(20);
						write(out, "eam-Private: 1\r\n" + last + "Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n");
						Thread.sleep(20);
						write(out, "6\r\n world\r\n0\r\n\r\n");
					}
					case "/stream" -> {
						write(out, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n");
						this.streamGate.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
						write(out, "3\r\ntwo\r\n0\r\n\r\n");
					}
					case "/not-modified" -> write(out, "HTTP/1.1 304 Not Modified\r\nETag: \"a\"\r\n" + last + "\r\n");
					case "/interim" -> {
						write(out, "HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n");
						write(out, "HTTP/1.1 200 OK\r\n" + last + "Content-Length: 1\r\n\r\nz");
					}
					case "/verbatim", "/boom" ->
						write(out, "HTTP/1.1 200 OK\r\n" + last + "Content-Length: 4\r\n\r\nsame");
					case "/unframed" -> {
						write(out, "HTTP/1.1 200 OK\r\n\r\nto the end");
						return;
					}
					case "/large" -> {
						write(out, "HTTP/1.1 200 OK\r\n" + last + "Content-Length: " + LARGE + "\r\n\r\n");
						byte[] piece = new byte[64 * 1024];
						Arrays.fill(piece, (byte) 'x');
						for (int sent = 0; sent < LARGE; sent += piece.length) {
							out.write(piece);
						}
						out.flush();
					}
					case "/ws" -> {
						write(out, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n");
						byte[] echo = new byte[1024];
						int n;
						while ((n = in.read(echo)) > 0) {
							out.write(echo, 0, n);
							out.flush();
						}
						return;
					}
					case "/close" -> {
						return;
					}
					case "/cut" -> {
						write(out, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc");
						return;
					}
					case "/request-replace" -> {
						int at = lower.indexOf("content-length: ");
						int length = Integer.parseInt(lower.substring(at + 16, lower.indexOf('\r', at)).trim());
						byte[] body = in.readNBytes(length);
						write(out, "HTTP/1.1 200 OK\r\n" + last + "Content-Length: " + length + "\r\n\r\n"
								+ new String(body, StandardCharsets.ISO_8859_1));
					}
					case "/v1/messages" -> {
						if (target.endsWith("?json")) {
							write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
									+ LLM_JSON.length() + "\r\n" + last + "\r\n" + LLM_JSON);
						}
						else {
							write(out, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n" + last
									+ "Transfer-Encoding: chunked\r\n\r\n" + chunk(LLM_START));
							this.usageGate.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
							if (target.endsWith("?leave")) {
								// keeps streaming until the server ends the connection
								while (true) {
									write(out, chunk("event: ping\ndata: {\"type\":\"ping\"}\n\n"));
									Thread.sleep(20);
								}
							}
							write(out, chunk(LLM_END) + "0\r\n\r\n");
						}
					}
					default -> write(out, "HTTP/1.1 200 OK\r\n" + last + "Content-Length: 0\r\n\r\n");
				}
				if (close) {
					return;
				}
			}
		}

		@Override
		public void close() throws IOException {
			this.server.close();
		}

	}

	private static String chunk(String text) {
		return Integer.toHexString(text.length()) + "\r\n" + text + "\r\n";
	}

	private static void write(OutputStream out, String text) throws IOException {
		out.write(text.getBytes(StandardCharsets.ISO_8859_1));
		out.flush();
	}

	private final SessionRegistry sessions;

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private final List<TunnelClient> clients = new ArrayList<>();

	private ScriptedUpstream upstream;

	private ScriptedUpstream tlsUpstream;

	private HeaderEchoUpstream echo;

	UpstreamFilterE2ETest(@Autowired SessionRegistry sessions) {
		this.sessions = sessions;
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

	private static FilterConfig rewriting(String mark) {
		return FilterConfig.builder().name("rewriting").args(Map.of("mark", mark)).build();
	}

	@BeforeAll
	void start() throws Exception {
		this.upstream = new ScriptedUpstream(new ServerSocket());
		Path keystore = TestKeystore.create();
		SSLServerSocket tls = (SSLServerSocket) TestKeystore.serverContext(keystore)
			.getServerSocketFactory()
			.createServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
		this.tlsUpstream = new ScriptedUpstream(tls);
		this.echo = new HeaderEchoUpstream();
		String target = "http://127.0.0.1:" + this.upstream.port();
		String echoTarget = "http://127.0.0.1:" + this.echo.port();
		FilterConfig plugin = FilterConfig.builder().name("plugin").build();
		int before = this.sessions.count();
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.insecure(true)
				.token("it-token")
				.filter("plugin", FilterConfig.builder().plugin(PLUGIN).configuration("defined").vms(1).build())
				.upstream(Upstream.builder()
					.host("h1.test")
					.target(target)
					.filters(List.of(FilterConfig.builder()
						.name("rewriting")
						.args(Map.of("mark", "h1"))
						.failClosed(true)
						.build()))
					.build())
				// the same target, each route with its filters
				.upstream(Upstream.builder().host("plain.test").target(target).build())
				.upstream(Upstream.builder()
					.host("other.test")
					.target(target)
					.filters(List.of(rewriting("other")))
					.build())
				.upstream(Upstream.builder().host("plugin.test").target(target).filters(List.of(plugin)).build())
				.upstream(Upstream.builder()
					.host("configured.test")
					.target(target)
					.filters(List.of(FilterConfig.builder().name("plugin").configuration("of the route").build()))
					.build())
				.upstream(Upstream.builder()
					.host("llm.test")
					.target(target)
					.filters(List.of(FilterConfig.builder().name("llm-usage").build()))
					.build())
				.upstream(Upstream.builder()
					.host("tls.test")
					.target("https://127.0.0.1:" + this.tlsUpstream.port())
					.filters(List.of(rewriting("tls")))
					.build())
				.upstream(Upstream.builder().host("h2.test").target(echoTarget).build())
				.upstream(Upstream.builder()
					.host("forced.test")
					.target(echoTarget)
					.filters(List.of(rewriting("h2")))
					.build())
				// whatever finds no route of its own: a connection with no head to read
				.upstream(Upstream.builder().target(target).filters(List.of(rewriting("any"))).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().start(task))
			.meterRegistry(this.meters)
			.filterFactories(List.of(new RewritingFactory()))
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> this.sessions.count() > before);
	}

	@AfterAll
	void stop() throws Exception {
		this.clients.forEach(TunnelClient::stop);
		this.upstream.close();
		this.tlsUpstream.close();
		this.echo.close();
	}

	private static String request(String host, String method, String path, boolean close) {
		return method + " " + path + " HTTP/1.1\r\nHost: " + host + "\r\nAccept-Encoding: gzip\r\n"
				+ (close ? "Connection: close\r\n" : "") + "\r\n";
	}

	private static String request(String method, String path, boolean close) {
		return request("h1.test", method, path, close);
	}

	private static String exchange(String requests) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			socket.getOutputStream().write(requests.getBytes(StandardCharsets.ISO_8859_1));
			socket.getOutputStream().flush();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
		}
	}

	/** The chunks and heads on one keep-alive connection stay framed, in order. */
	@Test
	void aPipelineOfEveryFramingIsRewrittenAndStaysFramed() throws Exception {
		String response = exchange(request("GET", "/length", false) + request("HEAD", "/length", false)
				+ request("GET", "/chunked", false) + request("GET", "/not-modified", false)
				+ request("GET", "/interim", false) + request("GET", "/verbatim", false)
				+ request("GET", "/whole", false) + request("GET", "/length", true));

		String lengthBody = "a\r\nA.B.C.D.E.\r\n5\r\n<end>\r\n0\r\n\r\n";
		assertThat(response).doesNotContain("X-Upstream-Private").doesNotContain("Content-Length: 5");
		assertThat(response.indexOf("x-filter-saw: GET /length 200 h1")).isGreaterThan(0);
		assertThat(response)
			.contains("x-filter-saw: HEAD /length 200 h1\r\nTransfer-Encoding: chunked\r\n\r\nHTTP/1.1 ");
		// the upstream's chunks arrive split: each came out as its own chunk or more
		assertThat(response).contains("a\r\nH.E.L.L.O.\r\n").contains("<end>\r\n0\r\n\r\nHTTP/1.1 304");
		assertThat(response)
			.contains("HTTP/1.1 304 Not Modified\r\nETag: \"a\"\r\nx-filter-saw: GET /not-modified 304 h1\r\n\r\n");
		assertThat(response).contains("HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK");
		assertThat(response).contains("Content-Length: 4\r\nx-filter-saw: GET /verbatim 200 h1\r\n\r\nsame");
		assertThat(response).contains("1a\r\nreplaced across the chunks\r\n0\r\n\r\n");
		assertThat(response).endsWith(lengthBody);
		assertThat(this.upstream.heads).filteredOn(head -> head.contains("Host: h1.test"))
			.noneMatch(head -> head.toLowerCase(Locale.ROOT).contains("accept-encoding"));
	}

	/** The rewrite streams: a chunk comes out before the upstream sent the next. */
	@Test
	void aBodyIsRewrittenAsItStreams() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			socket.getOutputStream().write(request("GET", "/stream", true).getBytes(StandardCharsets.ISO_8859_1));
			socket.getOutputStream().flush();
			InputStream in = socket.getInputStream();
			StringBuilder read = new StringBuilder();
			while (read.indexOf("O.N.E.") < 0) {
				read.append((char) in.read());
			}
			assertThat(read.toString()).doesNotContain("T.W.O.");
			this.upstream.streamGate.countDown();
			String rest = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
			assertThat(rest).contains("T.W.O.").contains("<end>");
		}
	}

	/** 8MiB doubled in length through the tunnel's flow control, nothing lost. */
	@Test
	void aLargeBodyChangingLengthPassesTheFlowControl() throws Exception {
		String response = exchange(request("GET", "/large", true));

		String body = response.substring(response.indexOf("\r\n\r\n") + 4);
		assertThat(body.chars().filter(c -> c == '.').count()).isEqualTo(LARGE);
		assertThat(body.chars().filter(c -> c == 'X').count()).isEqualTo(LARGE);
		assertThat(body).endsWith("<end>\r\n0\r\n\r\n");
	}

	/** What the relay answers of an upstream's end stays as it was. */
	@Test
	void anUpstreamsEndsAreAnsweredAsWithoutTheFilter() throws Exception {
		assertThat(exchange(request("GET", "/close", false))).startsWith("HTTP/1.1 502 ");
		// a filter failing closed on a head is the same to the client as a lost upstream
		assertThat(exchange(request("GET", "/boom", false))).startsWith("HTTP/1.1 502 ");
		// a cut body stays cut: no last chunk is made up
		assertThat(exchange(request("GET", "/cut", false))).contains("A.B.C.").doesNotContain("0\r\n\r\n");
		// a body the upstream ends by closing does end
		assertThat(exchange(request("GET", "/unframed", false))).endsWith("<end>\r\n0\r\n\r\n");
		// failing open, on another route, the response goes on and the failure is counted
		assertThat(exchange(request("other.test", "GET", "/boom", true))).startsWith("HTTP/1.1 200 ").endsWith("same");
		assertThat(this.meters.get("sluice.client.filter.failures").tag("route", "other.test").counter().count())
			.isEqualTo(1);
	}

	@Test
	void anUpgradedConnectionIsRelayedAsItIs() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			OutputStream out = socket.getOutputStream();
			write(out, "GET /ws HTTP/1.1\r\nHost: h1.test\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n");
			InputStream in = socket.getInputStream();
			StringBuilder head = new StringBuilder();
			while (head.indexOf("\r\n\r\n") < 0) {
				head.append((char) in.read());
			}
			assertThat(head.toString()).startsWith("HTTP/1.1 101 ");
			write(out, "GET /length HTTP/1.1\r\n\r\n");
			assertThat(new String(in.readNBytes(24), StandardCharsets.US_ASCII))
				.isEqualTo("GET /length HTTP/1.1\r\n\r\n");
		}
		// h2 past a 101 could not be read: the upstream gets an ordinary request
		assertThat(exchange("GET /verbatim HTTP/1.1\r\nHost: h1.test\r\nConnection: Upgrade, HTTP2-Settings, close\r\n"
				+ "Upgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n\r\n"))
			.startsWith("HTTP/1.1 200 ")
			.endsWith("same");
		assertThat(this.upstream.heads).filteredOn(head -> head.startsWith("GET /verbatim") && head.contains("close"))
			.isNotEmpty()
			.noneMatch(head -> head.toLowerCase(Locale.ROOT).contains("h2c"));
	}

	/** Each route of a target has its filters and settings, or none. */
	@Test
	void routesSharingATargetKeepTheirFiltersAndSettings() throws Exception {
		assertThat(exchange(request("plain.test", "GET", "/length", true))).isEqualTo(
				"HTTP/1.1 200 OK\r\nX-Upstream-Private: 1\r\nConnection: close\r\nContent-Length: 5\r\n\r\nabcde");
		assertThat(exchange(request("other.test", "GET", "/verbatim", true)))
			.contains("x-filter-saw: GET /verbatim 200 other\r\n");
		// from one route to another on one connection: each request by its own route
		assertThat(exchange(request("other.test", "GET", "/verbatim", false)
				+ request("h1.test", "GET", "/verbatim", false) + request("plain.test", "GET", "/verbatim", true)))
			.contains("x-filter-saw: GET /verbatim 200 other\r\n")
			.contains("x-filter-saw: GET /verbatim 200 h1\r\n")
			.endsWith("Connection: close\r\nContent-Length: 4\r\n\r\nsame");
	}

	/** An h2 connection would pass unfiltered: the filtered route refuses it. */
	@Test
	void h2IsRefusedOnAFilteredRouteOnly() throws Exception {
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, TestH2.headerBlock(":method=GET", ":scheme=http", ":path=/", ":authority=h2.test"));
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo(HeaderEchoUpstream.GREETING);

			client.headers(3, true,
					TestH2.headerBlock(":method=GET", ":scheme=http", ":path=/", ":authority=forced.test"));
			assertThat(client.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).doesNotContain(HeaderEchoUpstream.GREETING);
		}
	}

	/** Bytes the server relays verbatim could not be read: they reach no upstream. */
	@Test
	void whatAFilteredRouteCannotReadIsRefused() throws Exception {
		int before = this.upstream.heads.size();
		assertThat(exchange("*1\r\n$4\r\nPING\r\n")).isEmpty();
		assertThat(this.upstream.heads).hasSize(before);
		assertThat(this.meters.get("sluice.client.filter.refused").tag("route", "*").counter().count()).isEqualTo(1);
	}

	/** The same filter over TLS to its upstream. */
	@Test
	void anHttpsUpstreamIsFilteredAlike() throws Exception {
		assertThat(exchange(request("tls.test", "GET", "/length", false) + request("tls.test", "GET", "/length", true)))
			.isEqualTo(("HTTP/1.1 200 OK\r\nx-filter-saw: GET /length 200 tls\r\nTransfer-Encoding: chunked\r\n\r\n"
					+ "a\r\nA.B.C.D.E.\r\n5\r\n<end>\r\n0\r\n\r\n")
					+ ("HTTP/1.1 200 OK\r\nConnection: close\r\nx-filter-saw: GET /length 200 tls\r\n"
							+ "Transfer-Encoding: chunked\r\n\r\na\r\nA.B.C.D.E.\r\n5\r\n<end>\r\n0\r\n\r\n"));
	}

	@Test
	void aPluginServesEachRouteWithItsSettings() throws Exception {
		assertThat(exchange(request("plugin.test", "GET", "/properties", true)))
			.isEqualTo("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\nx-route: plugin.test\r\n"
					+ "x-code: 200\r\nx-configuration: defined\r\n\r\n");
		assertThat(exchange(request("configured.test", "GET", "/properties", true)))
			.isEqualTo("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\nx-route: configured.test\r\n"
					+ "x-code: 200\r\nx-configuration: of the route\r\n\r\n");
		// its answer ends the connection, with nothing reaching the upstream
		int before = this.upstream.heads.size();
		assertThat(exchange(request("plugin.test", "GET", "/local", false)))
			.isEqualTo("HTTP/1.1 403 Forbidden\r\nx-plugin: local\r\nContent-Length: 7\r\nConnection: close\r\n\r\n"
					+ "denied\n");
		assertThat(this.upstream.heads).hasSize(before);
		// a request body replaced whole: the upstream reads a length
		String post = "POST /request-replace HTTP/1.1\r\nHost: plugin.test\r\nConnection: close\r\n"
				+ "Content-Length: 5\r\n\r\nabcde";
		assertThat(exchange(post)).endsWith("Content-Length: 5\r\n\r\nABCDE");
	}

	private double tokens(String type) {
		var counter = this.meters.find("sluice.client.filter.llm.tokens").tag("type", type).counter();
		return counter == null ? 0 : counter.count();
	}

	private double requests(String outcome) {
		return this.meters.find("sluice.client.filter.llm.requests")
			.tag("outcome", outcome)
			.counters()
			.stream()
			.mapToDouble(c -> c.count())
			.sum();
	}

	private static String post(String query, boolean close) {
		return "POST /v1/messages" + query
				+ " HTTP/1.1\r\nHost: llm.test\r\nAccept-Encoding: gzip\r\nContent-Length: 0\r\n"
				+ (close ? "Connection: close\r\n" : "") + "\r\n";
	}

	/**
	 * The usage is counted as the responses pass byte for byte, a stream as the upstream
	 * sends it, and a client leaving a stream is counted as cut.
	 */
	@Test
	void usageIsCountedAndAClientLeavingAStreamCut() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			write(out, post("?json", false));
			StringBuilder read = new StringBuilder();
			while (read.indexOf(LLM_JSON) < 0) {
				read.append((char) in.read());
			}
			write(out, post("?stream", true));
			while (read.indexOf(LLM_START) < 0) {
				read.append((char) in.read());
			}
			assertThat(read.toString()).doesNotContain("message_delta");
			this.upstream.usageGate.countDown();
			assertThat(new String(in.readAllBytes(), StandardCharsets.ISO_8859_1))
				.isEqualTo("\r\n" + chunk(LLM_END) + "0\r\n\r\n");
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> assertThat(requests("complete")).isEqualTo(2));
		assertThat(tokens("input")).isEqualTo(12 + 5);
		assertThat(tokens("output")).isEqualTo(34 + 9);
		assertThat(this.upstream.heads).filteredOn(head -> head.contains("Host: llm.test"))
			.noneMatch(head -> head.toLowerCase(Locale.ROOT).contains("accept-encoding"));

		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TIMEOUT.toMillis());
			write(socket.getOutputStream(), post("?leave", false));
			InputStream in = socket.getInputStream();
			StringBuilder read = new StringBuilder();
			while (read.indexOf("ping") < 0) {
				read.append((char) in.read());
			}
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> assertThat(requests("cut")).isEqualTo(1));
	}

}
