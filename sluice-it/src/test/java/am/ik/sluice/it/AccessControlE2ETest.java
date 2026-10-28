package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.TunnelSession;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: data plane IP access control and per-route credentials. The server-wide
 * allow list admits the loopback; a route's {@code allowed-cidrs} replaces it, so the
 * restricted routes answer 403 (http) or close (tcp) from a non-listed address while the
 * open routes serve. A route with credentials ({@code sluice.route.auth}) answers a
 * request without them 401 with its challenge, over HTTP/1.1 and h2c, on the connection's
 * first request as on a later one, and relays one with them, the credential header
 * removed unless the route forwards it -- static secrets and the access tokens of an OIDC
 * provider ({@link MockOidcServer}) alike; a TLS passthrough connection to such a route
 * is closed unrelayed.
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

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ConcurrentListAppender<>();

	private static final ListAppender<ILoggingEvent> SESSION_EVENTS = new ConcurrentListAppender<>();

	private static final String OIDC_AUDIENCE = "api://oidc.local";

	private static HeaderEchoUpstream httpUpstream;

	/** The provider of the {@code oidc.local} route, up before the server starts. */
	private static @Nullable MockOidcServer oidcProvider;

	private static @Nullable ServerSocket echoServer;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstreams() throws Exception {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		SESSION_EVENTS.start();
		((Logger) LoggerFactory.getLogger(TunnelSession.class)).addAppender(SESSION_EVENTS);
		httpUpstream = new HeaderEchoUpstream();
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
		// every refusal line is asserted
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.route.auth.[basic.local].basic.alice", () -> "{noop}wonderland");
		// the access control answers first: 403, not 401
		registry.add("sluice.route.auth.[locked.local].bearer[0]", () -> "{noop}locked-token");
		registry.add("sluice.route.auth.[bearer.local].bearer[0]",
				() -> "{bcrypt}" + new BCryptPasswordEncoder(4).encode("bearer-token"));
		registry.add("sluice.route.auth.[token.local].api-token[0]", () -> "{sha256}" + sha256Hex("api-token"));
		registry.add("sluice.route.auth.[forward.local].bearer[0]", () -> "{noop}forward-token");
		registry.add("sluice.route.auth.[forward.local].forward-credentials", () -> "true");
		registry.add("sluice.route.auth.[passthrough.local].bearer[0]", () -> "{noop}passthrough-token");
		MockOidcServer provider = new MockOidcServer("console-client");
		oidcProvider = provider;
		registry.add("sluice.route.auth.[oidc.local].oidc.issuer-uri", provider::issuer);
		registry.add("sluice.route.auth.[oidc.local].oidc.audiences[0]", () -> OIDC_AUDIENCE);
	}

	private static MockOidcServer oidcProvider() {
		return java.util.Objects.requireNonNull(oidcProvider, "oidc provider not started");
	}

	@AfterAll
	static void detachAppenders() {
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
		((Logger) LoggerFactory.getLogger(TunnelSession.class)).detachAppender(SESSION_EVENTS);
		oidcProvider().close();
	}

	private static String sha256Hex(String value) {
		try {
			return HexFormat.of()
				.formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
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
		String http = "http://127.0.0.1:" + httpUpstream.port();
		SluiceClientProperties.Builder builder = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host("open.local").target(http).build())
			.upstream(Upstream.builder().host("locked.local").target(http).allowedCidrs(List.of("10.0.0.0/8")).build());
		for (String host : List.of("basic.local", "bearer.local", "token.local", "forward.local", "oidc.local")) {
			builder.upstream(Upstream.builder().host(host).target(http).build());
		}
		SluiceClientProperties properties = builder
			.upstream(Upstream.builder()
				.host("passthrough.local")
				.target("tcp://127.0.0.1:" + echoServer().getLocalPort())
				.tlsPassthrough(true)
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
		try {
			httpUpstream.close();
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
		assertThat(response.body).isEqualTo(HeaderEchoUpstream.GREETING);
	}

	@Test
	void routeAllowedCidrsReplaceTheGlobalAllowList() throws Exception {
		this.startClient();
		// the loopback is in the global allow list, but the route admits 10/8 only
		String response = raw("GET / HTTP/1.1\r\nHost: locked.local\r\nAccept: text/html\r\nConnection: close\r\n\r\n");
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
					":authority=locked.local", "accept=text/html");
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

	/**
	 * A route with credentials: its scheme's name, the header carrying a good and a wrong
	 * credential, and the challenge of its 401 without a credential and with the wrong
	 * one.
	 */
	record Scheme(String host, String header, String good, String wrong, String challenge, String refusal) {

		@Override
		public String toString() {
			return this.host;
		}

	}

	static List<Scheme> schemes() {
		String basic = "Basic realm=\"basic.local\", charset=\"UTF-8\"";
		return List.of(
				new Scheme("basic.local", "authorization", "Basic " + base64("alice:wonderland"),
						"Basic " + base64("alice:mad-hatter"), basic, basic),
				new Scheme("bearer.local", "authorization", "Bearer bearer-token", "Bearer other-token",
						"Bearer realm=\"bearer.local\"", "Bearer realm=\"bearer.local\", error=\"invalid_token\""),
				new Scheme("token.local", "x-api-token", "api-token", "other-token",
						"X-API-Token realm=\"token.local\"", "X-API-Token realm=\"token.local\""),
				new Scheme("oidc.local", "authorization", "Bearer " + oidcProvider().accessToken(OIDC_AUDIENCE),
						"Bearer " + oidcProvider().accessToken("api://elsewhere"), "Bearer realm=\"oidc.local\"",
						"Bearer realm=\"oidc.local\", error=\"invalid_token\""));
	}

	private static String base64(String value) {
		return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}

	@ParameterizedTest
	@MethodSource("schemes")
	void http1RequestsWithoutTheRoutesCredentialAre401AndWithItRelayedWithoutIt(Scheme scheme) throws Exception {
		this.startClient();
		ACCESS_EVENTS.list.clear();

		String refused = raw("GET /first HTTP/1.1\r\nHost: " + scheme.host() + "\r\nAccept: text/html\r\n\r\n");
		assertThat(refused).as("the first request").startsWith("HTTP/1.1 401 Unauthorized\r\n");
		assertThat(refused).contains("\r\nWWW-Authenticate: " + scheme.challenge() + "\r\n").endsWith("</html>\n");

		Response passed = exchange("GET / HTTP/1.1\r\nHost: " + scheme.host() + "\r\n" + scheme.header() + ": "
				+ scheme.good() + "\r\nConnection: close\r\n\r\n");
		assertThat(passed.statusLine).isEqualTo("HTTP/1.1 200 OK");
		assertThat(passed.body).as("the credential header removed").isEqualTo(HeaderEchoUpstream.GREETING);

		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			// pipelined: the refusal waits for the response before it, and the refused
			// request's body is read, not left to reset the connection under the 401
			byte[] body = new byte[32 * 1024];
			out.write(("GET / HTTP/1.1\r\nHost: open.local\r\n\r\nPOST /later HTTP/1.1\r\nHost: " + scheme.host()
					+ "\r\n" + scheme.header() + ": " + scheme.wrong() + "\r\nContent-Length: " + body.length
					+ "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.write(body);
			out.flush();
			assertThat(readResponse(in)).as("the keep-alive connection's first response, whole")
				.startsWith("HTTP/1.1 200 OK\r\n")
				.endsWith(HeaderEchoUpstream.GREETING);
			assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).as("the later request's, then closed")
				.startsWith("HTTP/1.1 401 Unauthorized\r\n")
				.contains("\r\nWWW-Authenticate: " + scheme.refusal() + "\r\n");
		}

		assertThat(accessLines())
			.anySatisfy(line -> assertThat(line).contains("route=" + scheme.host())
				.contains("path=/first")
				.endsWith("status=401 reason=auth-missing userAgent=-"))
			.anySatisfy(line -> assertThat(line).contains("route=" + scheme.host())
				.contains("path=/later")
				.endsWith("status=401 reason=auth-invalid userAgent=-"));
	}

	@ParameterizedTest
	@MethodSource("schemes")
	void h2cStreamsWithoutTheRoutesCredentialAre401AndWithItRelayedWithoutIt(Scheme scheme) throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			// the connection's first stream, routed with the connection
			TestH2.writeClientHead(out, 1, ":method=GET", ":scheme=http", ":path=/", ":authority=" + scheme.host());
			List<TestH2.Frame> seed = drainStream(in, 1);
			assertThat(headersOf(seed).status()).hasToString("401");
			assertThat(headersOf(seed).getAll("www-authenticate")).map(CharSequence::toString)
				.containsExactly(scheme.challenge());

			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":method=GET", ":scheme=http", ":path=/", ":authority=" + scheme.host(),
							scheme.header() + "=" + scheme.good()));
			List<TestH2.Frame> passed = drainStream(in, 3);
			assertThat(TestH2.status(passed)).as("the connection goes on").isEqualTo("200");
			assertThat(TestH2.bodyOf(passed)).as("the credential header removed")
				.isEqualTo(HeaderEchoUpstream.GREETING);

			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 5,
					TestH2.headerBlock(":method=GET", ":scheme=http", ":path=/", ":authority=open.local"));
			assertThat(TestH2.status(drainStream(in, 5))).isEqualTo("200");
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 7,
					TestH2.headerBlock(":method=GET", ":scheme=http", ":path=/", ":authority=" + scheme.host(),
							scheme.header() + "=" + scheme.wrong()));
			Http2Headers refused = headersOf(drainStream(in, 7));
			assertThat(refused.status()).as("a later stream").hasToString("401");
			assertThat(refused.getAll("www-authenticate")).map(CharSequence::toString)
				.containsExactly(scheme.refusal());
		}
	}

	@Test
	void aRouteForwardingItsCredentialsRelaysTheHeader() throws Exception {
		this.startClient();
		Response response = exchange(
				"GET / HTTP/1.1\r\nHost: forward.local\r\nAuthorization: Bearer forward-token\r\nConnection: close\r\n\r\n");
		assertThat(response.statusLine).isEqualTo("HTTP/1.1 200 OK");
		assertThat(response.body).isEqualTo(HeaderEchoUpstream.GREETING + " authorization=Bearer forward-token");
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":method=GET", ":scheme=http", ":path=/",
					":authority=forward.local", "authorization=Bearer forward-token");
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 1)))
				.isEqualTo(HeaderEchoUpstream.GREETING + " authorization=Bearer forward-token");
		}
	}

	@Test
	void aTlsPassthroughConnectionToARouteWithCredentialsIsClosedUnrelayed() throws Exception {
		this.startClient();
		assertThat(SESSION_EVENTS.list).as("warned at the advertise")
			.anySatisfy(event -> assertThat(event.getFormattedMessage())
				.contains("host=[passthrough.local] is TLS passthrough on a route requiring credentials"));
		ACCESS_EVENTS.list.clear();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write(clientHello("passthrough.local"));
			socket.getOutputStream().flush();
			// relayed, the echo upstream would send the ClientHello back
			assertThat(socket.getInputStream().readAllBytes()).isEmpty();
		}
		assertThat(accessLines()).anySatisfy(line -> assertThat(line).contains("route=passthrough.local")
			.endsWith("status=401 reason=auth-unreadable userAgent=-"));
	}

	/** A TLS ClientHello naming {@code host}. */
	private static byte[] clientHello(String host) throws Exception {
		SSLEngine engine = SSLContext.getDefault().createSSLEngine(host, 443);
		engine.setUseClientMode(true);
		SSLParameters parameters = engine.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(host)));
		engine.setSSLParameters(parameters);
		ByteBuffer hello = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
		engine.beginHandshake();
		engine.wrap(ByteBuffer.allocate(0), hello);
		hello.flip();
		byte[] bytes = new byte[hello.remaining()];
		hello.get(bytes);
		return bytes;
	}

	private static List<String> accessLines() {
		return ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	private static Http2Headers headersOf(List<TestH2.Frame> frames) throws Exception {
		TestH2.Frame head = frames.stream().filter(frame -> frame.type() == TestH2.HEADERS).findFirst().orElseThrow();
		return new DefaultHttp2HeadersDecoder(true).decodeHeaders(head.streamId(),
				Unpooled.wrappedBuffer(head.payload()));
	}

	/**
	 * The frames of the stream up to its END_STREAM; other streams' frames are skipped.
	 */
	private static List<TestH2.Frame> drainStream(InputStream in, int streamId) throws Exception {
		List<TestH2.Frame> frames = new ArrayList<>();
		TestH2.@Nullable Frame frame;
		while ((frame = TestH2.readFrame(in)) != null) {
			if (frame.streamId() != streamId) {
				continue;
			}
			frames.add(frame);
			if ((frame.type() == TestH2.DATA || frame.type() == TestH2.HEADERS)
					&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
				return frames;
			}
		}
		throw new AssertionError("the connection ended before stream %d did".formatted(streamId));
	}

	/** One HTTP/1.1 response whose body has a Content-Length. */
	private static String readResponse(InputStream in) throws Exception {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) {
			int b = in.read();
			if (b < 0) {
				throw new AssertionError("EOF in the response head: " + head);
			}
			head.write(b);
		}
		String text = head.toString(StandardCharsets.ISO_8859_1);
		int length = text.lines()
			.filter(line -> line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
			.map(line -> Integer.parseInt(line.substring(line.indexOf(':') + 1).trim()))
			.findFirst()
			.orElse(0);
		return text + new String(in.readNBytes(length), StandardCharsets.UTF_8);
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
