package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stream reset on one hop while frames of the stream are on their way to the peer: the
 * peer reads those frames before the RST_STREAM or not at all, never after it (RFC 9113
 * 5.1) -- whether the reset and the frames come out of one read of the relay (a DATA
 * frame the relay holds for the peer's window, queued in netty's flow controller, and a
 * reset written in the same action), or the two hops' threads race into the relay (each
 * enters it only under the relay's one lock).
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxResetOrderE2ETest {

	private static final int STREAMS = 30;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int STREAM_CLOSED = 0x5;

	private static final int CANCEL = 0x8;

	private static final AtomicLong PINGS = new AtomicLong();

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
		this.closeables.forEach(H2DemuxResetOrderE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * A client write carrying a stream's head, a DATA frame and then what ends the stream
	 * for the relay -- a RST_STREAM, or a DATA frame after the stream's end, which
	 * netty's codec resets with {@code STREAM_CLOSED} -- makes the relay queue the DATA
	 * for the upstream and reset the upstream stream ({@code CANCEL}) in one action: the
	 * upstream gets the DATA before the RST_STREAM or not at all.
	 */
	@ParameterizedTest(name = "{0}")
	@MethodSource("clientEnds")
	void aStreamTheClientEndsBehindItsDataSendsNothingAfterItsResetToTheUpstream(ClientEnd end) throws Exception {
		String route = "ro.up." + end.tag() + ".test";
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, stub(socket -> recordAndAnswerGets(socket, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			// the leg's first stream: the upstream's SETTINGS lift the one-stream limit
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			for (int i = 0; i < STREAMS; i++) {
				client.raw(end.bytes(3 + 2 * i, route));
			}
			int sentinel = 3 + 2 * STREAMS;
			client.headers(sentinel, true, get(route));
			assertThat(client.awaitEnd(sentinel, Duration.ofSeconds(5))).isTrue();
			for (int i = 0; i < STREAMS; i++) {
				int streamId = 3 + 2 * i;
				Awaitility.await()
					.atMost(Duration.ofSeconds(5))
					.untilAsserted(() -> assertThat(resetsOf(received, streamId)).as("stream %d", streamId)
						.containsExactly(CANCEL));
			}
			if (end.clientReset() < 0) {
				assertThat(client.resets()).as("the relay's answers to the client").isEmpty();
			}
			else {
				assertThat(client.resets().values()).as("the relay's answers to the client")
					.containsOnly(end.clientReset());
			}
			assertThat(client.closed()).isFalse();
		}
		assertThat(framesAfterTheirReset(received)).as("frames after their stream's reset").isEmpty();
	}

	static Stream<ClientEnd> clientEnds() {
		return Stream.of(
				new ClientEnd("rst", -1,
						(streamId, route) -> concat(
								frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, post(route)),
								frame(TestH2.DATA, 0, streamId, new byte[16]),
								frame(TestH2.RST_STREAM, 0, streamId, TestH2.int32(CANCEL)))),
				new ClientEnd("closed", STREAM_CLOSED,
						(streamId, route) -> concat(
								frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, post(route)),
								frame(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, new byte[16]),
								frame(TestH2.DATA, 0, streamId, new byte[1]))));
	}

	/**
	 * An upstream write carrying a response head, a DATA frame and a RST_STREAM on the
	 * stream makes the relay queue the DATA for the client and relay the reset in one
	 * action: the client gets the DATA before the RST_STREAM or not at all.
	 */
	@Test
	void aStreamTheUpstreamResetsBehindItsDataSendsNothingAfterItsResetToTheClient() throws Exception {
		String route = "ro.client.test";
		startClient(route, stub(H2DemuxResetOrderE2ETest::answerWithDataAndReset));
		Integer[] streams = new Integer[STREAMS];
		for (int i = 0; i < STREAMS; i++) {
			streams[i] = 1 + 2 * i;
		}
		try (H2TestClient client = H2TestClient.connect(dataPort, streams)) {
			for (int streamId : streams) {
				client.headers(streamId, true, get(route));
			}
			for (int streamId : streams) {
				assertThat(client.awaitTrue(() -> client.reset(streamId) != null, Duration.ofSeconds(5)))
					.as("stream %d reset", streamId)
					.isTrue();
			}
			awaitPingAck(client);
			assertThat(client.resets().values()).containsOnly(CANCEL);
			assertThat(client.closed()).isFalse();
			Map<Integer, List<Integer>> after = new TreeMap<>();
			for (int streamId : streams) {
				List<Integer> types = typesAfterTheReset(client.frames(streamId));
				if (!types.isEmpty()) {
					after.put(streamId, types);
				}
			}
			assertThat(after).as("frames after their stream's reset").isEmpty();
		}
	}

	/**
	 * The two hops' threads racing on a stream: the upstream streams DATA on every stream
	 * while the client resets each -- a WINDOW_UPDATE of 0, reset by netty's codec with
	 * {@code PROTOCOL_ERROR} -- once its first DATA arrived. Whichever thread enters the
	 * relay first, the client reads nothing of the stream after its RST_STREAM, and the
	 * upstream gets the {@code CANCEL}.
	 */
	@Test
	void aResetRacingTheUpstreamsDataLeavesNothingAfterTheResetOnEitherHop() throws Exception {
		String route = "ro.race.test";
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, stub(socket -> streamOnEveryStream(socket, received)));
		Integer[] streams = new Integer[STREAMS];
		for (int i = 0; i < STREAMS; i++) {
			streams[i] = 1 + 2 * i;
		}
		try (H2TestClient client = H2TestClient.connect(dataPort, streams)) {
			client.frame(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
			for (int streamId : streams) {
				client.headers(streamId, true, get(route));
			}
			for (int streamId : streams) {
				assertThat(client.awaitTrue(() -> client.dataReceived(streamId) > 0, Duration.ofSeconds(10)))
					.as("stream %d streams", streamId)
					.isTrue();
				client.frame(TestH2.WINDOW_UPDATE, 0, streamId, TestH2.int32(0));
			}
			for (int streamId : streams) {
				assertThat(client.awaitTrue(() -> client.reset(streamId) != null, Duration.ofSeconds(5)))
					.as("stream %d reset", streamId)
					.isTrue();
				// the relay's CANCEL first: the upstream's DATA in flight after it is
				// ignored
				Awaitility.await()
					.atMost(Duration.ofSeconds(5))
					.untilAsserted(() -> assertThat(resetsOf(received, streamId)).as("stream %d", streamId)
						.first()
						.isEqualTo(CANCEL));
			}
			awaitPingAck(client);
			assertThat(client.resets().values()).containsOnly(PROTOCOL_ERROR);
			assertThat(client.closed()).isFalse();
			Map<Integer, List<Integer>> after = new TreeMap<>();
			for (int streamId : streams) {
				List<Integer> types = typesAfterTheReset(client.frames(streamId));
				if (!types.isEmpty()) {
					after.put(streamId, types);
				}
			}
			assertThat(after).as("frames after their stream's reset").isEmpty();
		}
		assertThat(framesAfterTheirReset(received)).as("frames after their stream's reset, upstream").isEmpty();
	}

	/** The error codes of the RST_STREAM frames the upstream received on the stream. */
	private static List<Integer> resetsOf(List<TestH2.Frame> received, int streamId) {
		return received.stream()
			.filter(frame -> frame.type() == TestH2.RST_STREAM && frame.streamId() == streamId)
			.map(TestH2::errorCode)
			.toList();
	}

	/**
	 * By stream, the types of the frames received after the stream's first RST_STREAM,
	 * further RST_STREAM frames aside: those answer frames the other side sent on the
	 * closed stream (RFC 9113 5.1).
	 */
	private static Map<Integer, List<Integer>> framesAfterTheirReset(List<TestH2.Frame> received) {
		Map<Integer, List<TestH2.Frame>> byStream = new TreeMap<>();
		for (TestH2.Frame frame : received) {
			if (frame.streamId() != 0) {
				byStream.computeIfAbsent(frame.streamId(), id -> new ArrayList<>()).add(frame);
			}
		}
		Map<Integer, List<Integer>> after = new TreeMap<>();
		byStream.forEach((streamId, frames) -> {
			List<Integer> types = typesAfterTheReset(frames);
			if (!types.isEmpty()) {
				after.put(streamId, types);
			}
		});
		return after;
	}

	private static List<Integer> typesAfterTheReset(List<TestH2.Frame> frames) {
		List<Integer> types = new ArrayList<>();
		boolean reset = false;
		for (TestH2.Frame frame : frames) {
			if (reset && frame.type() != TestH2.RST_STREAM) {
				types.add(frame.type());
			}
			reset |= frame.type() == TestH2.RST_STREAM;
		}
		return types;
	}

	/**
	 * Pings the relay and waits for the ack: the relay handled every frame sent before.
	 */
	private static void awaitPingAck(H2TestClient client) throws Exception {
		byte[] ping = Arrays.copyOf(TestH2.int32((int) PINGS.incrementAndGet()), 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
				Duration.ofSeconds(5)))
			.as("the relay acked the PING")
			.isTrue();
	}

	/** Records every frame, answers every GET with a head ending the stream. */
	private static void recordAndAnswerGets(Socket socket, List<TestH2.Frame> received) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				received.add(frame);
				if (frame.type() == TestH2.HEADERS && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM,
							frame.streamId(), new byte[] { (byte) 0x88 });
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/**
	 * Answers every request with a head, 100 bytes of DATA and a RST_STREAM
	 * {@code CANCEL}, in one write.
	 */
	private static void answerWithDataAndReset(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS) {
					int streamId = frame.streamId();
					out.write(
							concat(frame(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, new byte[] { (byte) 0x88 }),
									frame(TestH2.DATA, 0, streamId, new byte[100]),
									frame(TestH2.RST_STREAM, 0, streamId, TestH2.int32(CANCEL))));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/**
	 * Answers every request with a head and DATA within the relay's windows until the
	 * stream is reset; records every frame received.
	 */
	private static void streamOnEveryStream(Socket socket, List<TestH2.Frame> received) {
		try {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			try (upstream) {
				upstream.settings(null);
				TestH2.@Nullable Frame frame;
				while ((frame = upstream.next()) != null) {
					received.add(frame);
					if (frame.type() == TestH2.HEADERS) {
						int streamId = frame.streamId();
						upstream.headers(streamId, false);
						Thread.ofVirtual().start(() -> {
							try {
								upstream.sendData(streamId, new byte[4 * 1024 * 1024], true);
							}
							catch (IOException e) {
								// the relay went away
							}
							catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
						});
					}
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			TestH2.writeFrame(out, type, flags, streamId, payload);
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return out.toByteArray();
	}

	private static byte[] concat(byte[]... parts) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-ro").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-ro-accept").start(() -> {
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

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * Frames ending a stream behind its DATA, and the reset the client gets (-1: none).
	 */
	record ClientEnd(String tag, int clientReset, Bytes bytes) {

		byte[] bytes(int streamId, String route) {
			return this.bytes.of(streamId, route);
		}

		@Override
		public String toString() {
			return this.tag;
		}

	}

	@FunctionalInterface
	interface Bytes {

		byte[] of(int streamId, String route);

	}

}
