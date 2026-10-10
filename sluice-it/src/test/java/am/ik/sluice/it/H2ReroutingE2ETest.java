package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end for HTTP/2 stream demultiplexing: streams of one prior-knowledge h2
 * connection whose {@code :authority} resolves to another client's upstream are answered
 * by that upstream (the Chrome connection-coalescing case), each with a {@code type=req}
 * access log line of its own.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2ReroutingE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-h2r").start(task);

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	/** Frames the stubs received, as {@code name:type:stream}; tests clear it. */
	private static final List<String> RECEIVED = java.util.Collections.synchronizedList(new ArrayList<>());

	private static int grpcPort;

	private static int dataPort;

	private static ServerSocket alphaUpstream;

	private static ServerSocket betaUpstream;

	private final Map<String, TunnelClient> clients = new ConcurrentHashMap<>();

	@Nullable
	@Autowired
	private Router router;

	@Autowired
	private SessionRegistry sessions;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@BeforeAll
	static void startUpstreams() {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		alphaUpstream = echoStub("alpha");
		betaUpstream = echoStub("beta");
	}

	/**
	 * An h2 stub upstream: every completed request (END_STREAM) is answered with a canned
	 * 200 whose body is {@code <name>:<authority>} -- the name tells the upstream apart.
	 */
	private static ServerSocket echoStub(String name) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0));
			UPSTREAM_EXECUTOR.execute(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						UPSTREAM_EXECUTOR.execute(() -> serve(socket, name));
					}
					catch (Exception e) {
						return;
					}
				}
			});
			return server;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static void serve(Socket socket, String name) {
		try (socket) {
			System.err.println("STUB[" + name + "] accepted");
			socket.setSoTimeout(30_000);
			InputStream in = socket.getInputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			Map<Integer, String> authorities = new java.util.concurrent.ConcurrentHashMap<>();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				RECEIVED.add(name + ":" + frame.type() + ":" + frame.streamId());
				if (frame.type() == TestH2.HEADERS) {
					authorities.put(frame.streamId(), authorityOf(frame.payload()));
					if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						respond(socket, name, frame.streamId(), authorities.get(frame.streamId()));
					}
				}
				else if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					System.err.println("STUB[" + name + "] respond st=" + frame.streamId());
					respond(socket, name, frame.streamId(), authorities.get(frame.streamId()));
				}
			}
		}
		catch (Exception e) {
			// connection ended
		}
	}

	private static void respond(Socket socket, String name, int streamId, @Nullable String authority) {
		try {
			socket.getOutputStream().write(TestH2.cannedResponse(streamId, name + ":" + authority));
			socket.getOutputStream().flush();
		}
		catch (Exception e) {
			// connection ended
		}
	}

	@Test
	void relaysRequestTrailersOnTheStreamsRoute() throws Exception {
		startClient("trl.a.test", alphaUpstream, false);
		RECEIVED.clear();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			// the request ends with trailers: HEADERS, DATA, then a trailers HEADERS
			var out = socket.getOutputStream();
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1,
					TestH2.headerBlock(":authority=trl.a.test", ":method=GET", ":path=/x"));
			TestH2.writeFrame(out, TestH2.DATA, 0, 1, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 1,
					TestH2.headerBlock("x-trailer=1"));
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 1))).startsWith("alpha:");
		}
		// the head and the trailers both reached the stream's upstream
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> RECEIVED.stream().filter(r -> r.equals("alpha:1:1")).count() == 2);
	}

	@Test
	void relaysClientResetOnTheStreamsRoute() throws Exception {
		startClient("rst.a.test", alphaUpstream, false);
		DataProxyServer proxy = Objects.requireNonNull(dataProxyServer);
		RECEIVED.clear();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=rst.a.test", ":method=POST", ":path=/x");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.RST_STREAM, 0, 1, new byte[] { 0, 0, 0, 8 });
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> RECEIVED.contains("alpha:3:1")); // RST_STREAM
																											// relayed
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
	}

	private static String authorityOf(byte[] block) {
		try {
			var decoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder(true);
			io.netty.handler.codec.http2.Http2Headers headers = decoder.decodeHeaders(1,
					io.netty.buffer.Unpooled.wrappedBuffer(block));
			Object authority = headers.authority();
			return authority == null ? "?" : authority.toString();
		}
		catch (Exception e) {
			return "?";
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	private void startClient(String host, ServerSocket upstream, boolean rewriteHost) {
		clients.computeIfAbsent(host, h -> {
			SluiceClientProperties properties = SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder()
					.host(h)
					.target("http://127.0.0.1:" + upstream.getLocalPort())
					.rewriteHost(rewriteHost)
					.build())
				.token("it-token")
				.build();
			TunnelClient started = TunnelClient.builder()
				.properties(properties)
				.taskExecutor(TASK_EXECUTOR)
				.meterRegistry(new SimpleMeterRegistry())
				.build();
			started.start();
			Router routes = Objects.requireNonNull(router);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> routes.lookup(h).isPresent());
			return started;
		});
	}

	/**
	 * Reads frames of the given stream until its DATA END_STREAM, skipping connection
	 * housekeeping and other streams.
	 */
	private static List<TestH2.Frame> drainStream(InputStream in, int streamId) throws Exception {
		return Objects.requireNonNull(drainStreams(in, streamId).get(Integer.valueOf(streamId)));
	}

	/**
	 * Reads frames until every given stream saw its DATA END_STREAM. Responses of
	 * concurrent streams interleave, so the frames are returned per stream; no complete
	 * response is left unconsumed.
	 */
	private static Map<Integer, List<TestH2.Frame>> drainStreams(InputStream in, int... streamIds) throws Exception {
		Map<Integer, List<TestH2.Frame>> frames = new LinkedHashMap<>();
		Set<Integer> pending = new LinkedHashSet<>();
		for (int streamId : streamIds) {
			frames.put(streamId, new ArrayList<>());
			pending.add(streamId);
		}
		TestH2.@Nullable Frame frame;
		while (!pending.isEmpty() && (frame = TestH2.readFrame(in)) != null) {
			if (frame.type() == TestH2.SETTINGS) {
				continue;
			}
			List<TestH2.Frame> ofStream = frames.get(Integer.valueOf(frame.streamId()));
			if (ofStream != null) {
				ofStream.add(frame);
			}
			if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
				pending.remove(Integer.valueOf(frame.streamId()));
			}
		}
		return frames;
	}

	/** The {@code :status} of the first HEADERS frame of the collected response. */
	private static int statusOf(List<TestH2.Frame> frames) throws Exception {
		for (TestH2.Frame frame : frames) {
			if (frame.type() == TestH2.HEADERS) {
				var decoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder(true);
				var headers = decoder.decodeHeaders(frame.streamId(),
						io.netty.buffer.Unpooled.wrappedBuffer(frame.payload()));
				return Integer.parseInt(Objects.requireNonNull(headers.status()).toString());
			}
		}
		return -1;
	}

	@AfterAll
	void tearDown() {
		clients.values().forEach(TunnelClient::stop);
		try {
			alphaUpstream.close();
			betaUpstream.close();
		}
		catch (Exception e) {
			// ignore
		}
		UPSTREAM_EXECUTOR.shutdown();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@Test
	void routesEachStreamToTheUpstreamOfItsAuthority() throws Exception {
		startClient("h2r.a.test", alphaUpstream, false);
		startClient("h2r.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=h2r.a.test", ":method=GET", ":path=/one");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=h2r.b.test", ":method=GET", ":path=/two"));
			Map<Integer, List<TestH2.Frame>> frames = drainStreams(socket.getInputStream(), 1, 3);
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(1)))).isEqualTo("alpha:h2r.a.test");
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(3)))).isEqualTo("beta:h2r.b.test");
		}
	}

	@Test
	void routesConcurrentStreamsInEitherOrder() throws Exception {
		startClient("ord.a.test", alphaUpstream, false);
		startClient("ord.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			// both heads are written before any response is read: the streams overlap
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=ord.b.test", ":method=GET", ":path=/x");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=ord.a.test", ":method=GET", ":path=/y"));
			Map<Integer, List<TestH2.Frame>> frames = drainStreams(socket.getInputStream(), 1, 3);
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(1)))).isEqualTo("beta:ord.b.test");
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(3)))).isEqualTo("alpha:ord.a.test");
		}
	}

	@Test
	void rewriteHostAppliesToTheRouteOfEveryStream() throws Exception {
		startClient("rw2.a.test", alphaUpstream, true);
		startClient("rw2.b.test", betaUpstream, true);
		int alphaPort = alphaUpstream.getLocalPort();
		int betaPort = betaUpstream.getLocalPort();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=rw2.a.test", ":method=GET", ":path=/x");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=rw2.b.test", ":method=GET", ":path=/y"));
			Map<Integer, List<TestH2.Frame>> frames = drainStreams(socket.getInputStream(), 1, 3);
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(1)))).isEqualTo("alpha:127.0.0.1:" + alphaPort);
			assertThat(TestH2.bodyOf(Objects.requireNonNull(frames.get(3)))).isEqualTo("beta:127.0.0.1:" + betaPort);
		}
	}

	@Test
	void answersNoRouteForAnUnroutableLaterStream() throws Exception {
		startClient("gone2.a.test", alphaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=gone2.a.test", ":method=GET", ":path=/x");
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 1))).isEqualTo("alpha:gone2.a.test");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=nowhere.test", ":method=GET", ":path=/x"));
			assertThat(statusOf(drainStream(socket.getInputStream(), 3))).isEqualTo(503);
		}
	}

	@Test
	void answersNoRouteWhenTheStreamsRouteVanishes() throws Exception {
		startClient("van2.a.test", alphaUpstream, false);
		startClient("van2.b.test", betaUpstream, false);
		TunnelClient beta = Objects.requireNonNull(clients.get("van2.b.test"));
		Router routes = Objects.requireNonNull(router);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=van2.a.test", ":method=GET", ":path=/x");
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 1))).startsWith("alpha:");
			beta.stop();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> routes.lookup("van2.b.test").isEmpty());
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=van2.b.test", ":method=GET", ":path=/x"));
			assertThat(statusOf(drainStream(socket.getInputStream(), 3))).isEqualTo(503);
		}
		finally {
			clients.remove("van2.b.test");
		}
	}

	@Test
	void closingTheConnectionReleasesTheRelay() throws Exception {
		startClient("rel2.a.test", alphaUpstream, false);
		startClient("rel2.b.test", betaUpstream, false);
		DataProxyServer proxy = Objects.requireNonNull(dataProxyServer);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=rel2.a.test", ":method=GET", ":path=/x");
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 1))).startsWith("alpha:");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=rel2.b.test", ":method=GET", ":path=/y"));
			assertThat(TestH2.bodyOf(drainStream(socket.getInputStream(), 3))).startsWith("beta:");
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
	}

	@Test
	void logsOneRequestLinePerStreamWithItsRoute() throws Exception {
		startClient("log2.a.test", alphaUpstream, false);
		startClient("log2.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=log2.a.test", ":method=GET", ":path=/one");
			TestH2.writeFrame(socket.getOutputStream(), TestH2.HEADERS,
					TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3,
					TestH2.headerBlock(":authority=log2.b.test", ":method=GET", ":path=/two"));
			drainStreams(socket.getInputStream(), 1, 3);
		}
		List<String> events = ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> events.stream()
				.filter(m -> m.startsWith("type=req") && m.contains("route=log2.b.test "))
				.anyMatch(m -> m.contains("path=/two")));
		assertThat(events).anyMatch(m -> m.startsWith("type=req") && m.contains("route=log2.a.test ")
				&& m.contains("path=/one") && m.contains("http=2"));
	}

	@Test
	void grantsConnectionWindowWhileRelayingRequestData() throws Exception {
		startClient("win.a.test", alphaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=win.a.test", ":method=POST", ":path=/x");
			byte[] body = "request-body".getBytes(java.nio.charset.StandardCharsets.UTF_8);
			TestH2.writeFrame(socket.getOutputStream(), TestH2.DATA, TestH2.FLAG_END_STREAM, 1, body);
			// the relay must release the connection-level window it consumed by granting
			// the client's WINDOW_UPDATE stream 0 (a request-direction window grant)
			boolean granted = false;
			List<TestH2.Frame> frames = new ArrayList<>();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(socket.getInputStream())) != null) {
				if (frame.type() == TestH2.SETTINGS) {
					continue;
				}
				if (frame.type() == 0x8 && frame.streamId() == 0 && frame.payload().length == 4) {
					granted = true;
				}
				if (frame.streamId() == 1) {
					frames.add(frame);
					if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						if (granted) {
							break;
						}
					}
				}
			}
			assertThat(granted).as("connection-level WINDOW_UPDATE observed").isTrue();
			assertThat(TestH2.bodyOf(frames)).isEqualTo("alpha:win.a.test");
		}
	}

}
