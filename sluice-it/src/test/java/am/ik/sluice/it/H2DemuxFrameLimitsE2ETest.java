package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.proxy.H2EngineTest;
import am.ik.sluice.server.proxy.LegacyOnly;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.Parameter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux buffers at most one default-sized frame and one header block of the
 * decoder's limit per peer: a larger frame or block from the client ends the connection
 * with a GOAWAY, one from an upstream ends its leg with a GOAWAY to the upstream, the
 * client's connection going on -- before the rest arrives, so nothing beyond the limit is
 * ever buffered.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
class H2DemuxFrameLimitsE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int FRAME_SIZE_ERROR = 0x6;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

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

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxFrameLimitsE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * A client frame announcing more than the default max frame size: GOAWAY
	 * FRAME_SIZE_ERROR at its header, while most of its payload never comes.
	 */
	@Test
	void clientFrameAboveTheMaxFrameSizeEndsTheConnection() throws Exception {
		String route = route("lim.frame");
		AtomicBoolean routed = new AtomicBoolean();
		startClient(route, stub((in, out) -> routed.set(true)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			Awaitility.await().atMost(Duration.ofSeconds(5)).untilTrue(routed);
			client.raw(frameHeader(MAX_FRAME_SIZE + 1, TestH2.DATA, 0, 1));
			client.raw(new byte[100]);
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay closed the connection")
				.isTrue();
			assertThat(client.goAwayCode()).isEqualTo(FRAME_SIZE_ERROR);
		}
	}

	/**
	 * A client frame of exactly the max frame size is taken: the limit is not off by one.
	 */
	@Test
	void clientFrameOfTheMaxFrameSizeIsRelayed() throws Exception {
		String route = route("lim.exact");
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, stub((in, out) -> recordFrames(in, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.sendData(1, new byte[MAX_FRAME_SIZE], 0);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> received.stream()
					.filter(frame -> frame.type() == TestH2.DATA)
					.mapToInt(frame -> frame.payload().length)
					.sum() == MAX_FRAME_SIZE);
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * HEADERS without END_HEADERS followed by CONTINUATION frames past the decoder's
	 * header list limit (a CONTINUATION flood): GOAWAY ENHANCE_YOUR_CALM.
	 */
	@Test
	@LegacyOnly("netty ends the connection past 80KiB of header block (its GOAWAY threshold over the 64KiB list limit): per-hop rewrite, step 8 (105)")
	void clientHeaderBlockAboveTheListLimitEndsTheConnection() throws Exception {
		String route = route("lim.block");
		AtomicBoolean routed = new AtomicBoolean();
		startClient(route, stub((in, out) -> routed.set(true)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			Awaitility.await().atMost(Duration.ofSeconds(5)).untilTrue(routed);
			client.frame(TestH2.HEADERS, 0, 3, new byte[MAX_FRAME_SIZE]);
			for (int i = 0; i < 4 && !client.closed(); i++) {
				client.frame(TestH2.CONTINUATION, 0, 3, new byte[MAX_FRAME_SIZE]);
			}
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay closed the connection")
				.isTrue();
			assertThat(client.goAwayCode()).isEqualTo(ENHANCE_YOUR_CALM);
		}
	}

	/**
	 * An upstream frame announcing more than the default max frame size ends its leg with
	 * a GOAWAY {@code FRAME_SIZE_ERROR} (RFC 9113 4.2).
	 */
	@Test
	void upstreamFrameAboveTheMaxFrameSizeEndsItsLeg() throws Exception {
		assertUpstreamEndsItsLeg(route("lim.up-frame"), FRAME_SIZE_ERROR, out -> {
			out.write(frameHeader(MAX_FRAME_SIZE + 1, TestH2.DATA, 0, 1));
			out.write(new byte[100]);
		});
	}

	/**
	 * An upstream header block past the header list limit ends its leg with a GOAWAY: the
	 * legacy relay's {@code ENHANCE_YOUR_CALM} past 64KiB, netty's {@code PROTOCOL_ERROR}
	 * past 80KiB (its GOAWAY threshold over the list limit), so the upstream sends 96KiB.
	 */
	@Test
	void upstreamHeaderBlockAboveTheListLimitEndsItsLeg() throws Exception {
		assertUpstreamEndsItsLeg(route("lim.up-block"),
				this.engine == H2DemuxEngine.NETTY ? PROTOCOL_ERROR : ENHANCE_YOUR_CALM, out -> {
					TestH2.writeFrame(out, TestH2.HEADERS, 0, 1, new byte[MAX_FRAME_SIZE]);
					for (int i = 0; i < 5; i++) {
						TestH2.writeFrame(out, TestH2.CONTINUATION, 0, 1, new byte[MAX_FRAME_SIZE]);
					}
				});
	}

	/**
	 * An upstream starting a header block while another is unfinished ends its leg with a
	 * GOAWAY {@code PROTOCOL_ERROR} (RFC 9113 6.10): one unfinished block per leg is all
	 * the relay holds.
	 */
	@Test
	void upstreamInterleavingHeaderBlocksEndsItsLeg() throws Exception {
		assertUpstreamEndsItsLeg(route("lim.up-interleave"), PROTOCOL_ERROR, out -> {
			TestH2.writeFrame(out, TestH2.HEADERS, 0, 1, new byte[] { (byte) 0x88 });
			TestH2.writeFrame(out, TestH2.HEADERS, 0, 3, new byte[] { (byte) 0x88 });
		});
	}

	/**
	 * Opens streams 1 and 3; once both reached the upstream, it writes the frames: the
	 * upstream gets a GOAWAY of the code, the client a {@code 502} on both streams (no
	 * response head reached it) and no GOAWAY, and a stream of another route completes.
	 */
	private void assertUpstreamEndsItsLeg(String route, int code, Writer frames) throws Exception {
		AtomicBoolean answered = new AtomicBoolean();
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, stub((in, out) -> {
			try {
				// the server preface first: the relay opens one stream until it arrived
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
				int heads = 0;
				TestH2.@Nullable Frame frame;
				while (heads < 2 && (frame = TestH2.readFrame(in)) != null) {
					if (frame.type() == TestH2.HEADERS) {
						heads++;
					}
				}
				frames.write(out);
				out.flush();
				answered.set(true);
				// held open: only the frames end the leg
				recordFrames(in, received);
			}
			catch (IOException e) {
				// ended
			}
		}));
		String otherRoute = "other." + route;
		ScriptedH2Upstream other = ScriptedH2Upstream.start(new byte[0], ScriptedH2Upstream.answering("other"));
		this.closeables.add(other);
		startClient(otherRoute, other.port());
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.headers(3, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			Awaitility.await().atMost(Duration.ofSeconds(5)).untilTrue(answered);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(5)))
				.as("both streams answered")
				.isTrue();
			assertThat(TestH2.status(client.frames(1))).isEqualTo("502");
			assertThat(TestH2.status(client.frames(3))).isEqualTo("502");
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> received.stream().anyMatch(frame -> frame.type() == TestH2.GOAWAY));
			TestH2.Frame goAway = received.stream()
				.filter(frame -> frame.type() == TestH2.GOAWAY)
				.findFirst()
				.orElseThrow();
			assertThat(TestH2.errorCode(goAway)).isEqualTo(code);
			client.headers(5, true, TestH2.headerBlock(":authority=" + otherRoute, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(5, Duration.ofSeconds(5))).as("another route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("other");
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
		}
	}

	/** Writes frames on the upstream's socket. */
	@FunctionalInterface
	private interface Writer {

		void write(OutputStream out) throws IOException;

	}

	private static void recordFrames(InputStream in, List<TestH2.Frame> received) {
		try {
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				received.add(frame);
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	private static byte[] frameHeader(int length, int type, int flags, int streamId) {
		return new byte[] { (byte) (length >>> 16), (byte) (length >>> 8), (byte) length, (byte) type, (byte) flags,
				(byte) (streamId >>> 24 & 0x7f), (byte) (streamId >>> 16), (byte) (streamId >>> 8), (byte) streamId };
	}

	/**
	 * The route {@code name} of this invocation: the runs on both engines share the
	 * router, and a route registered by the other run's client would take the stream.
	 */
	private String route(String name) {
		return name + "." + String.valueOf(this.engine).toLowerCase(Locale.ROOT) + ".test";
	}

	private void startClient(String host, ServerSocket upstream) {
		startClient(host, upstream.getLocalPort());
	}

	private void startClient(String host, int port) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + port).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-lim").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/**
	 * A raw TCP stub: reads the preface of every accepted socket, then hands its streams
	 * to the handler.
	 */
	private ServerSocket stub(BiConsumer<InputStream, OutputStream> handler) {
		Consumer<Socket> serve = socket -> {
			try (socket) {
				InputStream in = socket.getInputStream();
				if (in.readNBytes(TestH2.PREFACE.length).length == TestH2.PREFACE.length) {
					handler.accept(in, socket.getOutputStream());
					// held until the test ends
					while (in.read() != -1) {
						// drain
					}
				}
			}
			catch (IOException e) {
				// ended
			}
		};
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-lim-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> serve.accept(socket));
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

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

}
