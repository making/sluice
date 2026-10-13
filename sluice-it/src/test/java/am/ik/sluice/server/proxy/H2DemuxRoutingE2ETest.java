package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Exception;
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
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Routing of an h2 connection's streams: the connection's first stream takes the route
 * resolved with the connection head without a second lookup (with round-robin load
 * balancing a second lookup would move every first stream to the other upstream), streams
 * keep their ids on their leg, and a head up to the 64KiB header list limit is relayed
 * whole.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxRoutingE2ETest {

	/** Served by two clients, round-robin. */
	private static final String SHARED_HOST = "shared.h2routing.test";

	private static final String FIRST_HOST = "first.h2routing.test";

	private static final String SECOND_HOST = "second.h2routing.test";

	/** Its upstream sends a GOAWAY after each response, keeping the connection open. */
	private static final String GOAWAY_HOST = "goaway.h2routing.test";

	/** The connections the GOAWAY upstream accepted. */
	private static final AtomicInteger GOAWAY_CONNECTIONS = new AtomicInteger();

	/** The connections each named upstream accepted. */
	private static final Map<String, AtomicInteger> CONNECTIONS = new ConcurrentHashMap<>();

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	/** The decoded request heads, by {@code <upstream>:<stream id>}. */
	private static final Map<String, Http2Headers> DECODED = new ConcurrentHashMap<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.http-load-balance", () -> "round-robin");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClients() {
		startClient(SHARED_HOST, upstream("a"));
		startClient(SHARED_HOST, upstream("b"));
		startClient(FIRST_HOST, upstream("first"));
		startClient(SECOND_HOST, upstream("second"));
		startClient(GOAWAY_HOST, goingAwayUpstream());
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> router().lookup(FIRST_HOST).isPresent() && router().lookup(SECOND_HOST).isPresent()
					&& router().lookup(GOAWAY_HOST).isPresent()
					&& router().resolve(SHARED_HOST).map(group -> group.candidates().size()).orElse(0) == 2);
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxRoutingE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Each connection's only stream goes to the upstream the connection's lookup chose:
	 * round-robin alternates them, so four connections reach both.
	 */
	@Test
	void theFirstStreamTakesTheRouteResolvedWithTheConnection() throws Exception {
		Set<String> answered = new HashSet<>();
		for (int i = 0; i < 4; i++) {
			try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
				h2.headers(1, true, get(SHARED_HOST, "/rr"));
				assertThat(h2.awaitEnd(1, TIMEOUT)).as("connection %d answered", i).isTrue();
				answered.add(TestH2.bodyOf(h2.frames(1)));
			}
		}
		assertThat(answered).containsExactlyInAnyOrder("a", "b");
	}

	/**
	 * Streams keep their ids on the leg of their route, each leg a subsequence; one leg
	 * per route.
	 */
	@Test
	void streamsKeepTheirIdsOnTheirLegs() throws Exception {
		int first = connections("first");
		int second = connections("second");
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			h2.headers(1, true, get(FIRST_HOST, "/ids-1"));
			h2.headers(3, true, get(SECOND_HOST, "/ids-3"));
			h2.headers(5, true, get(SECOND_HOST, "/ids-5"));
			h2.headers(7, true, get(FIRST_HOST, "/ids-7"));
			for (int streamId : new int[] { 1, 3, 5, 7 }) {
				assertThat(h2.awaitEnd(streamId, TIMEOUT)).as("stream %d answered", streamId).isTrue();
			}
			assertThat(TestH2.bodyOf(h2.frames(3))).isEqualTo("second");
			assertThat(TestH2.bodyOf(h2.frames(7))).isEqualTo("first");
		}
		assertThat(pathsOf("first")).containsEntry(1, "/ids-1").containsEntry(7, "/ids-7");
		assertThat(pathsOf("second")).containsEntry(3, "/ids-3").containsEntry(5, "/ids-5");
		assertThat(connections("first") - first).as("legs of the first route").isEqualTo(1);
		assertThat(connections("second") - second).as("legs of the second route").isEqualTo(1);
	}

	private static int connections(String upstream) {
		AtomicInteger count = CONNECTIONS.get(upstream);
		return count == null ? 0 : count.get();
	}

	/**
	 * A head whose header list is just under the 64KiB limit -- eight times netty's
	 * default -- reaches the upstream whole, a later stream of the connection after it.
	 */
	@Test
	void aHeadUpToTheHeaderListLimitIsRelayed() throws Exception {
		String big = "x".repeat(60 * 1024);
		Http2Headers headers = new DefaultHttp2Headers().method("GET")
			.scheme("http")
			.authority(FIRST_HOST)
			.path("/big")
			.add("x-big", big);
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, true, get(FIRST_HOST, "/before-big"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, encode(headers));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("answered").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(3))).isEqualTo("first");
			assertThat(h2.goAwayCode()).as("no GOAWAY").isEqualTo(-1);
		}
		Http2Headers received = DECODED.values()
			.stream()
			.filter(head -> "/big".contentEquals(Objects.requireNonNullElse(head.path(), "")))
			.reduce((first, last) -> last)
			.orElseThrow();
		assertThat(String.valueOf(received.get("x-big"))).isEqualTo(big);
	}

	/**
	 * A leg whose upstream sent a GOAWAY takes no new stream: the route's next stream
	 * opens a new leg, a new upstream connection, and is answered.
	 */
	@Test
	void aLegWhoseUpstreamWentAwayTakesNoNewStream() throws Exception {
		int connections = GOAWAY_CONNECTIONS.get();
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, true, get(GOAWAY_HOST, "/goaway-1"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("stream 1 answered").isTrue();
			h2.headers(3, true, get(GOAWAY_HOST, "/goaway-3"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("stream 3 answered").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(3))).isEqualTo("goaway");
			assertThat(h2.resets()).isEmpty();
		}
		assertThat(GOAWAY_CONNECTIONS.get() - connections).as("upstream connections").isEqualTo(2);
	}

	/** Response trailers reach the client and end its stream. */
	@Test
	void responseTrailersEndTheClientsStream() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, get(FIRST_HOST, "/trailers"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("ended").isTrue();
			List<TestH2.Frame> heads = h2.frames(1).stream().filter(frame -> frame.type() == TestH2.HEADERS).toList();
			assertThat(heads).hasSize(2);
			assertThat(TestH2.status(heads.subList(0, 1))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo("first");
			Http2Headers trailers = decode(new DefaultHttp2HeadersDecoder(true), 1, heads.get(1).payload());
			assertThat(String.valueOf(trailers.get("grpc-status"))).isEqualTo("0");
			assertThat(heads.get(1).flags() & TestH2.FLAG_END_STREAM).isNotZero();
		}
	}

	/** An interim (1xx) head reaches the client ahead of the final one. */
	@Test
	void anInterimHeadPrecedesTheFinalOne() throws Exception {
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, get(FIRST_HOST, "/interim"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("ended").isTrue();
			List<TestH2.Frame> heads = h2.frames(1).stream().filter(frame -> frame.type() == TestH2.HEADERS).toList();
			assertThat(heads).hasSize(2);
			assertThat(TestH2.status(heads.subList(0, 1))).isEqualTo("103");
			assertThat(TestH2.status(heads.subList(1, 2))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo("first");
		}
	}

	/** The stream ids and paths the upstream received heads on. */
	private static Map<Integer, String> pathsOf(String upstream) {
		Map<Integer, String> paths = new HashMap<>();
		DECODED.forEach((key, head) -> {
			if (key.startsWith(upstream + ":")) {
				paths.put(Integer.parseInt(key.substring(upstream.length() + 1)), String.valueOf(head.path()));
			}
		});
		return paths;
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.token("it-token")
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-h2routing-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
	}

	/**
	 * An h2 upstream answering every ended request with its name; it decodes every head,
	 * CONTINUATION frames joined, with one decoder per connection.
	 */
	/**
	 * An h2 upstream answering each ended request, then sending a GOAWAY naming that
	 * stream the last; it keeps the connection open.
	 */
	private ServerSocket goingAwayUpstream() {
		return stub(socket -> {
			try (socket) {
				WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
				if (upstream == null) {
					return;
				}
				GOAWAY_CONNECTIONS.incrementAndGet();
				upstream.settings(null);
				TestH2.@Nullable Frame frame;
				while ((frame = upstream.next()) != null) {
					if (frame.type() == TestH2.HEADERS && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						upstream.headers(frame.streamId(), false);
						upstream.sendData(frame.streamId(), "goaway".getBytes(StandardCharsets.UTF_8), true);
						byte[] payload = new byte[8];
						System.arraycopy(TestH2.int32(frame.streamId()), 0, payload, 0, 4);
						upstream.write(TestH2.GOAWAY, 0, 0, payload);
					}
				}
			}
			catch (Exception e) {
				// connection ended
			}
		});
	}

	private ServerSocket upstream(String name) {
		return stub(socket -> {
			try (socket) {
				WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
				if (upstream == null) {
					return;
				}
				CONNECTIONS.computeIfAbsent(name, key -> new AtomicInteger()).incrementAndGet();
				upstream.settings(null);
				DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true, 256 * 1024);
				ByteArrayOutputStream block = new ByteArrayOutputStream();
				boolean headEnds = false;
				TestH2.@Nullable Frame frame;
				while ((frame = upstream.next()) != null) {
					int streamId = frame.streamId();
					if (frame.type() == TestH2.HEADERS) {
						block.reset();
						headEnds = (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
					}
					if (frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION) {
						block.writeBytes(frame.payload());
						if ((frame.flags() & TestH2.FLAG_END_HEADERS) != 0) {
							DECODED.put(name + ":" + streamId, decode(decoder, streamId, block.toByteArray()));
							if (headEnds) {
								respond(upstream, streamId, name, DECODED.get(name + ":" + streamId));
							}
						}
					}
					else if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						respond(upstream, streamId, name, DECODED.get(name + ":" + streamId));
					}
				}
			}
			catch (Exception e) {
				// connection ended
			}
		});
	}

	/**
	 * Answers with a 200 head and the body; the path {@code /interim} gets a {@code 103}
	 * head first, {@code /trailers} ends with trailers instead of the body's END_STREAM.
	 */
	private static void respond(WindowedH2Upstream upstream, int streamId, String body, @Nullable Http2Headers head)
			throws IOException, InterruptedException, Http2Exception {
		String path = head == null ? "" : String.valueOf(head.path());
		if (path.equals("/interim")) {
			upstream.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId,
					encode(new DefaultHttp2Headers().status("103").add("link", "</style.css>; rel=preload")));
		}
		upstream.headers(streamId, false);
		boolean trailers = path.equals("/trailers");
		upstream.sendData(streamId, body.getBytes(StandardCharsets.UTF_8), !trailers);
		if (trailers) {
			upstream.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, streamId,
					encode(new DefaultHttp2Headers().add("grpc-status", "0")));
		}
	}

	private static Http2Headers decode(DefaultHttp2HeadersDecoder decoder, int streamId, byte[] block)
			throws Http2Exception {
		return decoder.decodeHeaders(streamId, Unpooled.wrappedBuffer(block));
	}

	/** A header block of literals with proper HPACK lengths (TestH2's fit 127 bytes). */
	private static byte[] encode(Http2Headers headers) throws Http2Exception {
		ByteBuf out = Unpooled.buffer();
		try {
			DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder((name, value) -> true);
			encoder.configuration().maxHeaderListSize(256 * 1024);
			encoder.encodeHeaders(1, headers, out);
			return ByteBufUtil.getBytes(out);
		}
		finally {
			out.release();
		}
	}

	private static byte[] get(String authority, String path) {
		return TestH2.headerBlock(":authority=" + authority, ":method=GET", ":scheme=http", ":path=" + path);
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-h2routing-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> handler.accept(socket));
					}
					catch (IOException e) {
						return;
					}
				}
			});
			return server;
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

}
