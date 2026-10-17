package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.ReferenceCountUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux honors the upstream's flow control (RFC 9113 6.9): DATA towards an
 * upstream stays within the upstream's stream and connection windows -- initial value
 * from its SETTINGS, changed mid-stream, replenished by its WINDOW_UPDATEs -- while a
 * stream without window holds back nothing else of its leg. After the client's TCP
 * half-close the relay keeps the leg open, granting the upstream's connection window,
 * until the leg's responses ended. An upstream breaking the flow-control rules ends its
 * leg (a connection error) or resets its stream (a stream error), never the client's
 * connection. In the relay's package for the tracked-streams meter.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxUpstreamFlowControlE2ETest {

	private static final int KIB = 1024;

	private static final int CANCEL = 0x8;

	private static final int NO_ERROR = 0x0;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int FLOW_CONTROL_ERROR = 0x3;

	private static final int FRAME_SIZE_ERROR = 0x6;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	/** The registry of the client app started for each host: no registry is shared. */
	private final Map<String, MeterRegistry> clientMeters = new ConcurrentHashMap<>();

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

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
		this.closeables.forEach(H2DemuxUpstreamFlowControlE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * An upstream announcing a 16KiB initial stream window, reading promptly and granting
	 * what it read every 20ms, never receives more than its window on the stream, and a
	 * 1MiB request completes.
	 */
	@Test
	void smallUpstreamStreamWindowIsNeverExceeded() throws Exception {
		String route = "uf.small.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(16 * KIB);
			Thread granter = Thread.ofVirtual().start(() -> grantPeriodically(upstream, true));
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				answerIfEnded(upstream, frame);
			}
			granter.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			assertThat(client.data(1, 1024 * KIB, true, Duration.ofSeconds(10))).isTrue();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(20))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo(String.valueOf(1024 * KIB));
		}
		WindowedH2Upstream upstream = seen.get();
		assertThat(upstream.windowSettingAcked()).as("the relay acknowledged the 16KiB initial window").isTrue();
		assertThat(upstream.minStreamWindowAfterAck()).as("the 16KiB window held the relay back")
			.isLessThanOrEqualTo(0);
		assertThat(upstream.maxExcess()).as("DATA beyond a window").isLessThanOrEqualTo(0);
	}

	/**
	 * An upstream with ample stream windows, granting its connection window every 20ms,
	 * never receives more than that window over two concurrent streams; both requests
	 * complete.
	 */
	@Test
	void upstreamConnectionWindowIsNeverExceeded() throws Exception {
		String route = "uf.connection.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(1024 * KIB);
			Thread granter = Thread.ofVirtual().start(() -> grantPeriodically(upstream, false));
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				answerIfEnded(upstream, frame);
			}
			granter.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			client.headers(3, false, post(route));
			Thread other = Thread.ofVirtual().start(() -> send(client, 3, 200 * KIB));
			assertThat(client.data(1, 200 * KIB, true, Duration.ofSeconds(10))).isTrue();
			other.join(TimeUnit.SECONDS.toMillis(20));
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(20))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo(String.valueOf(200 * KIB));
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo(String.valueOf(200 * KIB));
		}
		WindowedH2Upstream upstream = seen.get();
		assertThat(upstream.minConnectionWindow()).as("the connection window held the relay back")
			.isLessThanOrEqualTo(0);
		assertThat(upstream.maxExcess()).as("DATA beyond a window").isLessThanOrEqualTo(0);
	}

	/**
	 * An upstream with a 2MiB initial stream window that only grants its connection
	 * window gets a 1MiB request in full: the client's stream window is regenerated as
	 * the relay writes the stream's data, not from the upstream's stream grants.
	 */
	@Test
	void largeUpstreamStreamWindowNeedsNoStreamGrants() throws Exception {
		String route = "uf.large.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(2048 * KIB);
			grantAndAnswer(upstream, false);
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			assertThat(client.data(1, 1024 * KIB, true, Duration.ofSeconds(10))).as("sent %d", client.streamSent(1))
				.isTrue();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(20))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo(String.valueOf(1024 * KIB));
		}
		assertThat(seen.get().maxExcess()).isLessThanOrEqualTo(0);
	}

	/**
	 * A stream whose upstream window is exhausted waits without holding back another
	 * stream of the same leg: the other request, larger than the initial window,
	 * completes while the first stream's data stays queued.
	 */
	@Test
	void streamWithoutWindowDoesNotHoldBackItsLeg() throws Exception {
		String route = "uf.blocked.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					upstream.grant(0, frame.payload().length);
					if (frame.streamId() == 3) {
						upstream.grant(3, frame.payload().length);
					}
				}
				answerIfEnded(upstream, frame);
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, false, post(route));
			client.fill(1, Duration.ofMillis(300));
			client.headers(3, false, post(route));
			assertThat(client.data(3, 200 * KIB, true, Duration.ofSeconds(10))).isTrue();
			assertThat(client.awaitEnd(3, Duration.ofSeconds(10))).as("the other stream of the leg completes").isTrue();
			long queued = queuedBytes(route);
			WindowedH2Upstream upstream = seen.get();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> upstream.received(1) >= WindowedH2Upstream.DEFAULT_WINDOW);
			Thread.sleep(300);
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo(String.valueOf(200 * KIB));
			assertThat(queued).as("the first stream's data waits in the leg queue").isGreaterThan(0);
			assertThat(upstream.received(1)).as("the first stream got exactly its window")
				.isEqualTo(WindowedH2Upstream.DEFAULT_WINDOW);
			assertThat(upstream.maxExcess()).isLessThanOrEqualTo(0);
		}
	}

	/**
	 * Streams of one leg take turns on the upstream's connection window: a small request
	 * sent while a bulk one keeps its stream's data pending completes long before the
	 * bulk one.
	 */
	@Test
	void streamsOfALegTakeTurnsOnTheConnectionWindow() throws Exception {
		String route = "uf.turns.test";
		AtomicLong bulkAtSmallEnd = new AtomicLong(-1);
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(4096 * KIB);
			Thread granter = Thread.ofVirtual().start(() -> grantPeriodically(upstream, false));
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.streamId() == 3 && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					bulkAtSmallEnd.set(upstream.received(1));
				}
				answerIfEnded(upstream, frame);
			}
			granter.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			Thread bulk = Thread.ofVirtual().start(() -> send(client, 1, 2048 * KIB));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> seen.get() != null && seen.get().received(1) >= 256 * KIB);
			client.headers(3, false, post(route));
			assertThat(client.data(3, 64 * KIB, true, Duration.ofSeconds(10))).isTrue();
			bulk.join(TimeUnit.SECONDS.toMillis(30));
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(30))).isTrue();
		}
		assertThat(bulkAtSmallEnd.get()).as("bulk bytes the upstream had when the small request ended")
			.isGreaterThan(0)
			.isLessThan(1024 * KIB);
		assertThat(seen.get().maxExcess()).isLessThanOrEqualTo(0);
	}

	/**
	 * Full duplex on one stream: the upstream echoes the request, granting request window
	 * only for what it could echo within the client's window. The client's stream window
	 * updates must overtake the stream's request data waiting for upstream window, or
	 * neither side moves.
	 */
	@Test
	void clientWindowUpdatesOvertakeDataWaitingForUpstreamWindow() throws Exception {
		String route = "uf.echo.test";
		int length = 512 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, H2DemuxUpstreamFlowControlE2ETest::echo));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, post(route));
			Thread.ofVirtual().start(() -> send(client, 1, length));
			long granted = 0;
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (!client.ended(1) && System.nanoTime() < deadline) {
				long delta = client.dataReceived() - granted;
				if (delta >= 16 * KIB) {
					client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32((int) delta));
					granted += delta;
				}
				Thread.sleep(1);
			}
			assertThat(client.ended(1)).as("echoed %d of %d", client.dataReceived(), length).isTrue();
			assertThat(client.dataReceived()).isEqualTo(length);
		}
		assertThat(seen.get().maxExcess()).isLessThanOrEqualTo(0);
	}

	/**
	 * An upstream lowering its initial stream window to 0 mid-stream drives the window
	 * negative (RFC 9113 6.9.2): the relay sends nothing on the stream, nor on a stream
	 * opened after the change, until WINDOW_UPDATEs make the window positive.
	 */
	@Test
	void upstreamLoweringItsInitialWindowStopsTheStreams() throws Exception {
		String route = "uf.lowered.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			Thread granter = Thread.ofVirtual().start(() -> topUpAfterTheAck(upstream));
			boolean lowered = false;
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (!lowered && upstream.received(1) >= 32 * KIB) {
					upstream.settings(0);
					lowered = true;
				}
				answerIfEnded(upstream, frame);
			}
			granter.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			Thread first = Thread.ofVirtual().start(() -> send(client, 1, 256 * KIB));
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> seen.get() != null);
			WindowedH2Upstream upstream = seen.get();
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(upstream::windowSettingAcked);
			client.headers(3, false, post(route));
			assertThat(client.data(3, 1000, true, Duration.ofSeconds(10))).isTrue();
			first.join(TimeUnit.SECONDS.toMillis(20));
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(20))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo(String.valueOf(256 * KIB));
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("1000");
			assertThat(upstream.minStreamWindowAfterAck()).as("the window went negative").isLessThan(0);
			assertThat(upstream.maxExcess()).as("DATA beyond a window").isLessThanOrEqualTo(0);
		}
	}

	/**
	 * An upstream connection-level WINDOW_UPDATE past 2^31-1 ends its leg (a connection
	 * error {@code FLOW_CONTROL_ERROR}, RFC 9113 6.9.1).
	 */
	@Test
	void connectionWindowOverflowEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.overflow-connection.test", FLOW_CONTROL_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE)));
	}

	/**
	 * An upstream stream-level WINDOW_UPDATE past 2^31-1 resets the stream (a stream
	 * error {@code FLOW_CONTROL_ERROR}, RFC 9113 6.9.1).
	 */
	@Test
	void streamWindowOverflowResetsTheStream() throws Exception {
		assertOffenceResetsTheStream("uf.overflow-stream.test", FLOW_CONTROL_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(Integer.MAX_VALUE)));
	}

	/**
	 * An upstream connection-level WINDOW_UPDATE of a length other than 4 ends its leg (a
	 * connection error {@code FRAME_SIZE_ERROR}, RFC 9113 6.9).
	 */
	@Test
	void connectionWindowUpdateOfAWrongLengthEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.length-connection.test", FRAME_SIZE_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 0, new byte[] { 0, 0, 1 }));
	}

	/**
	 * An upstream stream-level WINDOW_UPDATE of a length other than 4 ends its leg
	 * whatever the stream.
	 */
	@Test
	void streamWindowUpdateOfAWrongLengthEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.length-stream.test", FRAME_SIZE_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 1, new byte[] { 0, 0, 0, 1, 0 }));
	}

	/**
	 * An upstream connection-level WINDOW_UPDATE with an increment of 0 ends its leg (a
	 * connection error {@code PROTOCOL_ERROR}, RFC 9113 6.9).
	 */
	@Test
	void zeroConnectionWindowIncrementEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.zero-connection.test", PROTOCOL_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0)));
	}

	/**
	 * An upstream stream-level WINDOW_UPDATE with an increment of 0 on an open stream
	 * resets the stream (a stream error {@code PROTOCOL_ERROR}, RFC 9113 6.9).
	 */
	@Test
	void zeroStreamWindowIncrementResetsTheStream() throws Exception {
		assertOffenceResetsTheStream("uf.zero-stream.test", PROTOCOL_ERROR,
				upstream -> upstream.write(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0)));
	}

	/**
	 * An upstream WINDOW_UPDATE with an increment of 0 on a stream the relay no longer
	 * tracks -- its request and response ended -- is ignored: the connection serves the
	 * next stream.
	 */
	@Test
	void zeroIncrementOnAnUntrackedStreamKeepsTheConnection() throws Exception {
		String route = "uf.zero-untracked.test";
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					answer(upstream, frame.streamId(), "ok");
					// the relay reads the response's end, which untracks the stream,
					// first
					upstream.write(TestH2.WINDOW_UPDATE, 0, frame.streamId(), TestH2.int32(0));
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok");
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * An upstream SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1 ends its leg (a connection
	 * error {@code FLOW_CONTROL_ERROR}, RFC 9113 6.5.2).
	 */
	@Test
	void initialWindowAboveTheMaximumEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.overflow-setting.test", FLOW_CONTROL_ERROR,
				upstream -> upstream.write(TestH2.SETTINGS, 0, 0,
						WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, 0x80000000L)));
	}

	/**
	 * An initial window change pushing an open stream's window past 2^31-1 ends its leg
	 * (a connection error {@code FLOW_CONTROL_ERROR}, RFC 9113 6.9.2).
	 */
	@Test
	void initialWindowChangeOverflowingAStreamEndsTheLeg() throws Exception {
		assertOffenceEndsTheLeg("uf.overflow-delta.test", FLOW_CONTROL_ERROR, upstream -> {
			upstream.write(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(1));
			upstream.write(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
		});
	}

	/**
	 * The boundary of the four above: windows brought to exactly 2^31-1 are legal, and
	 * the request is answered.
	 */
	@Test
	void windowsAtTheMaximumKeepTheConnection() throws Exception {
		String route = "uf.maximum.test";
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					upstream.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
					upstream.write(TestH2.SETTINGS, 0, 0, WindowedH2Upstream
						.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
					upstream.headers(frame.streamId(), false);
					upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, frame.streamId(),
							"ok".getBytes(StandardCharsets.US_ASCII));
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			Thread.sleep(500);
			assertThat(client.closed()).isFalse();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("ok");
		}
	}

	/**
	 * After the client half-closes, a 1MiB response from an upstream that keeps the
	 * connection and honors its connection window reaches the client: the relay grants
	 * that window until the response ended, then half-closes the upstream, and the relay
	 * ends once the upstream closes.
	 */
	@Test
	void largeResponseReachesAHalfClosedClient() throws Exception {
		String route = "uf.halfclose.test";
		int length = 1024 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch eof = new CountDownLatch(1);
		AtomicBoolean responded = new AtomicBoolean();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame = upstream.next();
			while (frame != null && frame.type() != TestH2.HEADERS) {
				frame = upstream.next();
			}
			if (frame == null) {
				return;
			}
			int streamId = frame.streamId();
			Thread responder = Thread.ofVirtual().start(() -> {
				try {
					upstream.headers(streamId, false);
					upstream.sendData(streamId, new byte[length], true);
					responded.set(true);
				}
				catch (IOException | InterruptedException e) {
					// the relay went away
				}
			});
			while (upstream.next() != null) {
				// keeps reading the relay's grants until its half-close
			}
			// the relay's half-close: the response must be out by then
			responder.join(TimeUnit.SECONDS.toMillis(1));
			if (responded.get()) {
				eof.countDown();
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			// the client's stream and connection windows cover the response: it can grant
			// nothing once half-closed
			client.frame(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, 4 * length));
			client.frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(4 * length));
			client.headers(1, true, get(route));
			client.halfClose();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(20))).as("received %d", client.dataReceived()).isTrue();
			assertThat(client.dataReceived()).isEqualTo(length);
			assertThat(eof.await(5, TimeUnit.SECONDS)).as("the upstream saw the half-close after its response")
				.isTrue();
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay ended").isTrue();
		}
		assertThat(seen.get().connectionGrantsReceived()).as("the response needed the relay's connection grants")
			.isGreaterThanOrEqualTo(length - WindowedH2Upstream.DEFAULT_WINDOW);
	}

	/**
	 * A Netty h2 server in its default configuration closes its connection on reading
	 * EOF. The relay defers the half-close of the leg until the response ended, so a
	 * client half-closing after its request still gets a response sent 500ms later.
	 */
	@Test
	void nettyUpstreamAnswersAHalfClosedClient() throws Exception {
		String route = "uf.netty.test";
		AtomicReference<@Nullable Boolean> halfClosure = new AtomicReference<>();
		int port = nettyUpstream(halfClosure);
		startClient(route, port);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(10));
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + route, ":method=GET", ":scheme=http",
					":path=/late");
			socket.shutdownOutput();
			List<TestH2.Frame> frames = new ArrayList<>();
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(socket.getInputStream())) != null) {
				if (frame.streamId() == 1) {
					frames.add(frame);
					if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						break;
					}
				}
			}
			assertThat(halfClosure.get()).as("the Netty upstream closes on EOF").isFalse();
			assertThat(TestH2.bodyOf(frames)).isEqualTo("late");
		}
	}

	/**
	 * A client gone after half-closing while its response is owed: the relay probes it
	 * with a PING once a second passed without a write to it, the write fails on the
	 * client's reset, and the relay ends, closing the upstream connection -- which never
	 * answers.
	 */
	@Test
	void aHalfClosedClientGoneIsNoticedByTheProbe() throws Exception {
		String route = "uf.probe.test";
		CountDownLatch eof = new CountDownLatch(1);
		CountDownLatch head = new CountDownLatch(1);
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					head.countDown();
				}
			}
			eof.countDown();
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> proxy.activeConnections() == 0);
		AtomicReference<@Nullable RecordingPipe> pipe = recordClientPipe(proxy);
		try {
			try (Socket socket = new Socket("127.0.0.1", dataPort)) {
				TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + route, ":method=GET",
						":scheme=http", ":path=/");
				socket.shutdownOutput();
				assertThat(head.await(5, TimeUnit.SECONDS)).as("the request reached the upstream").isTrue();
			}
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
		}
		finally {
			proxy.clientPipeSeam(UnaryOperator.identity());
		}
		List<WrittenFrame> written = framesWritten(pipe);
		List<WrittenFrame> probes = written.stream().filter(WrittenFrame::probe).toList();
		// the first write fails on the client's reset, or the second
		assertThat(probes).as("ended at a probe").hasSizeBetween(1, 2);
		assertThat(written.subList(written.indexOf(probes.getFirst()), written.size())).as("nothing but probes after")
			.allMatch(WrittenFrame::probe);
		WrittenFrame before = written.get(written.indexOf(probes.getFirst()) - 1);
		assertThat(Duration.ofNanos(probes.getFirst().nanoTime() - before.nanoTime()))
			.as("after the probe interval without a write")
			.isGreaterThanOrEqualTo(Duration.ofSeconds(1));
		assertThat(eof.await(5, TimeUnit.SECONDS)).as("the upstream connection was closed").isTrue();
	}

	/** A frame the relay wrote to its client, when the write carrying its start began. */
	private record WrittenFrame(TestH2.Frame frame, long nanoTime) {

		boolean probe() {
			return this.frame.type() == TestH2.PING && (this.frame.flags() & TestH2.FLAG_ACK) == 0
					&& ByteBuffer.wrap(this.frame.payload()).getLong() == NettyHttp2DemuxRelay.PROBE_PAYLOAD;
		}

	}

	/**
	 * Records the client pipe of the relays started from now on, the latest in the
	 * reference; the caller resets the seam.
	 */
	private static AtomicReference<@Nullable RecordingPipe> recordClientPipe(DataProxyServer proxy) {
		AtomicReference<@Nullable RecordingPipe> recorded = new AtomicReference<>();
		proxy.clientPipeSeam(pipe -> {
			RecordingPipe recording = new RecordingPipe(pipe);
			recorded.set(recording);
			return recording;
		});
		return recorded;
	}

	/** The frames the relay wrote to its client, in order. */
	private static List<WrittenFrame> framesWritten(AtomicReference<@Nullable RecordingPipe> recorded)
			throws IOException {
		List<RecordingPipe.Write> writes = Objects.requireNonNull(recorded.get(), "no relay started").writes();
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		List<Long> ends = new ArrayList<>();
		for (RecordingPipe.Write write : writes) {
			bytes.writeBytes(write.bytes());
			ends.add((long) bytes.size());
		}
		ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
		List<WrittenFrame> frames = new ArrayList<>();
		while (true) {
			long start = bytes.size() - in.available();
			TestH2.@Nullable Frame frame = TestH2.readFrame(in);
			if (frame == null) {
				return frames;
			}
			int write = 0;
			while (ends.get(write) <= start) {
				write++;
			}
			frames.add(new WrittenFrame(frame, writes.get(write).nanoTime()));
		}
	}

	/**
	 * A client ending its side while a request of its is unfinished -- it can never
	 * complete now -- ends the relay at once, the upstream connection with it.
	 */
	@Test
	void aClientEndingItsSideWithARequestUnfinishedEndsTheRelayAtOnce() throws Exception {
		String route = "uf.unfinished.test";
		CountDownLatch eof = new CountDownLatch(1);
		CountDownLatch head = new CountDownLatch(1);
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					head.countDown();
				}
			}
			eof.countDown();
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		AtomicReference<@Nullable RecordingPipe> pipe = recordClientPipe(proxy);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			assertThat(head.await(5, TimeUnit.SECONDS)).as("the request reached the upstream").isTrue();
			client.halfClose();
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay ended").isTrue();
			assertThat(eof.await(5, TimeUnit.SECONDS)).as("the upstream connection was closed").isTrue();
		}
		finally {
			proxy.clientPipeSeam(UnaryOperator.identity());
		}
		assertThat(framesWritten(pipe)).as("ended without a probe").noneMatch(WrittenFrame::probe);
	}

	/**
	 * An upstream answering early and closing its connection while the stream's request
	 * data waits for window: the relay drops that data, resets the stream with NO_ERROR
	 * (the response is complete), releases the stream, and serves the next request on a
	 * new leg.
	 */
	@Test
	void upstreamClosingAfterAnEarlyResponseRetiresItsLeg() throws Exception {
		String route = "uf.retire.test";
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					answer(upstream, frame.streamId(), "early");
				}
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					upstream.grant(0, frame.payload().length);
				}
				if (upstream.received(1) >= WindowedH2Upstream.DEFAULT_WINDOW) {
					upstream.close();
					return;
				}
			}
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			client.fill(1, Duration.ofMillis(300));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).as("stream 1 reset")
				.isTrue();
			// the relay's reset first: what the client still sends after it is ignored
			// within the window it held, answered with STREAM_CLOSED past it
			assertThat(client.frames(1).stream().filter(frame -> frame.type() == TestH2.RST_STREAM).findFirst())
				.hasValueSatisfying(reset -> assertThat(TestH2.errorCode(reset)).isEqualTo(NO_ERROR));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> queuedBytes(route) == 0 && proxy.h2DemuxMeters().trackedStreams(route) == 0);
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("the next request is served").isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("early");
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * After an early response, the client ends its request and half-closes while the
	 * relay holds the rest of it for an upstream that granted nothing beyond its initial
	 * windows: the upstream connection stays open, and once the upstream grants, all of
	 * the request, END_STREAM included, reaches it before the half-close.
	 */
	@Test
	void queuedRequestDataIsWrittenBeforeTheHalfClose() throws Exception {
		String route = "uf.drain.test";
		// past the upstream's initial windows, within them and what the relay grants the
		// client back of a stream window (in halves: 32KiB of the 64KiB written)
		int length = 90 * KIB;
		AtomicLong receivedAtEof = new AtomicLong(-1);
		AtomicBoolean endStream = new AtomicBoolean();
		CountDownLatch release = new CountDownLatch(1);
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			Thread.ofVirtual().start(() -> {
				try {
					release.await();
					upstream.grant(0, 1 << 20);
					upstream.grant(1, 1 << 20);
				}
				catch (IOException | InterruptedException e) {
					// the relay went away
				}
			});
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					answer(upstream, frame.streamId(), "early");
				}
				if (frame.type() == TestH2.DATA) {
					endStream.compareAndSet(false, (frame.flags() & TestH2.FLAG_END_STREAM) != 0);
				}
			}
			receivedAtEof.set(upstream.received(1));
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, post(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertThat(client.data(1, length, true, Duration.ofSeconds(10))).isTrue();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> queuedBytes(route) > 0);
			client.halfClose();
			Thread.sleep(500);
			assertThat(receivedAtEof.get()).as("the upstream connection open while the relay holds its bytes")
				.isEqualTo(-1);
			release.countDown();
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> receivedAtEof.get() >= 0);
			assertThat(receivedAtEof.get()).as("request bytes before the half-close").isEqualTo(length);
			assertThat(endStream.get()).as("END_STREAM before the half-close").isTrue();
		}
	}

	/**
	 * A client reset of a stream whose data waits for upstream window reaches the
	 * upstream at once, and the stream's queued data is released.
	 */
	@Test
	void clientResetOvertakesDataWaitingForWindow() throws Exception {
		String route = "uf.client-reset.test";
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch reset = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					upstream.grant(0, frame.payload().length);
				}
				if (frame.type() == TestH2.RST_STREAM && frame.streamId() == 1) {
					reset.countDown();
				}
			}
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, post(route));
			client.fill(1, Duration.ofMillis(300));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> seen.get() != null && seen.get().received(1) >= WindowedH2Upstream.DEFAULT_WINDOW);
			assertThat(queuedBytes(route)).as("data waits for window").isGreaterThan(0);
			client.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			assertThat(reset.await(5, TimeUnit.SECONDS)).as("the reset reached the upstream").isTrue();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> queuedBytes(route) == 0 && proxy.h2DemuxMeters().trackedStreams(route) == 0);
		}
	}

	/**
	 * An upstream reset of a stream whose data waits for window releases the stream's
	 * queued data and the stream, while another stream of the leg stays open.
	 */
	@Test
	void upstreamResetReleasesDataWaitingForWindow() throws Exception {
		String route = "uf.upstream-reset.test";
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			boolean reset = false;
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					upstream.grant(0, frame.payload().length);
				}
				if (!reset && upstream.received(1) >= WindowedH2Upstream.DEFAULT_WINDOW) {
					Thread.sleep(200); // the client filled what the relay grants
					upstream.write(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
					reset = true;
				}
			}
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			// stream 3 keeps a response owed on the leg: the reset of stream 1 is not the
			// leg's last open stream
			client.headers(1, false, post(route));
			client.headers(3, false, post(route));
			client.fill(1, Duration.ofMillis(100));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(1)).isEqualTo(CANCEL);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> queuedBytes(route) == 0 && proxy.h2DemuxMeters().trackedStreams(route) == 1);
		}
	}

	/**
	 * Request trailers queued behind data that waits for upstream window follow that
	 * data: the upstream sees the head, all the data, then the trailers.
	 */
	@Test
	void trailersFollowDataWaitingForWindow() throws Exception {
		String route = "uf.trailers.test";
		List<String> sequence = new CopyOnWriteArrayList<>();
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(16 * KIB);
			Thread granter = Thread.ofVirtual().start(() -> grantPeriodically(upstream, true));
			int heads = 0;
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.streamId() != 1) {
					continue;
				}
				if (frame.type() == TestH2.HEADERS) {
					sequence.add(heads++ == 0 ? "HEADERS" : "TRAILERS");
				}
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					sequence.add("DATA:" + frame.payload().length);
				}
				answerIfEnded(upstream, frame);
			}
			granter.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			assertThat(client.data(1, 100 * KIB, false, Duration.ofSeconds(10))).isTrue();
			// the relay parsed the data, and some of it waits for window
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> queuedBytes(route) > 0);
			client.headers(1, true, TestH2.headerBlock("x-checksum=1"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
		}
		assertThat(summary(sequence)).isEqualTo("HEADERS DATA:" + 100 * KIB + " TRAILERS");
	}

	/**
	 * A client gone entirely (not half-closed) while a response is owed: the relay
	 * notices on writing to it and ends, though the upstream neither answers nor closes.
	 */
	@Test
	void clientGoneWhileAResponseIsOwedEndsTheRelay() throws Exception {
		String route = "uf.gone.test";
		CountDownLatch eof = new CountDownLatch(1);
		CountDownLatch held = new CountDownLatch(1);
		this.closeables.add(held::countDown);
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			while (upstream.next() != null) {
				// never answers
			}
			eof.countDown();
			held.await(); // keeps the connection
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> proxy.activeConnections() == 0);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=" + route, ":method=GET", ":path=/");
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> seen.get() != null);
			assertThat(proxy.activeConnections()).as("the relay is live").isEqualTo(1);
			// everything the relay sent is read: the close is a FIN, not a reset the
			// relay
			// would notice on reading
			socket.setSoTimeout(300);
			try {
				socket.getInputStream().transferTo(OutputStream.nullOutputStream());
			}
			catch (SocketTimeoutException e) {
				// drained
			}
			assertThat(socket.getInputStream().available()).isZero();
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
		assertThat(eof.await(5, TimeUnit.SECONDS)).as("the upstream connection was closed").isTrue();
	}

	/**
	 * The upstream answered in full -- the relay holds what the client's withheld window
	 * keeps back -- and, the client having half-closed, got the demux's half-close at
	 * once, its connection having no stream left; the client then goes: the relay ends
	 * with an ERROR after the CLOSE the client app already took, and the app must still
	 * release the upstream connection (the CLOSE ended its reader, so only the
	 * remote-failure registration of the relay can).
	 */
	@Test
	void clientGoneAfterTheUpstreamsHalfCloseReleasesTheUpstreamConnection() throws Exception {
		String route = "uf.gone.closed.test";
		// past the client's window by less than the relay holds of a stream for it (the
		// relay grants a stream window back in halves)
		int length = 80 * KIB;
		CountDownLatch eof = new CountDownLatch(1);
		CountDownLatch held = new CountDownLatch(1);
		this.closeables.add(held::countDown);
		// answers in full within the relay's windows, then holds the socket silently past
		// the half-close: it writes nothing a relay writing to the remote would trip on
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					int streamId = frame.streamId();
					upstream.headers(streamId, false);
					Thread.ofVirtual().start(() -> {
						try {
							upstream.sendData(streamId, new byte[length], true);
						}
						catch (IOException | InterruptedException e) {
							// the relay went away
						}
					});
				}
			}
			eof.countDown();
			held.await();
		}));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> proxy.activeConnections() == 0);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			// the stream window covers the response: only the connection window holds it
			client.frame(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, 1024 * KIB));
			client.headers(1, true, get(route));
			// the demux half-closes the legs once the client is done sending
			client.halfClose();
			assertThat(client.awaitTrue(() -> client.dataReceived(1) == H2TestClient.INITIAL_WINDOW,
					Duration.ofSeconds(10)))
				.as("the client's window spent")
				.isTrue();
			assertThat(eof.await(10, TimeUnit.SECONDS)).as("the upstream got the half-close").isTrue();
			assertThat(client.ended(1)).as("the rest of the response waits for the client").isFalse();
			assertThat(proxy.activeConnections()).as("the relay is live").isEqualTo(1);
			assertThat(clientActiveConnections(route)).as("the client holds the upstream connection").isEqualTo(1);
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.untilAsserted(() -> assertThat(clientActiveConnections(route)).as("the client released the leg").isZero());
	}

	private int clientActiveConnections(String host) {
		return (int) Objects.requireNonNull(this.clientMeters.get(host))
			.get("sluice.connections.active")
			.gauge()
			.value();
	}

	/**
	 * The upstream commits the offence on reading stream 1's head, before answering: it
	 * gets a GOAWAY of the code, and the relay's EOF; the client gets a {@code 502} on
	 * stream 1 and no GOAWAY, and a stream of another route completes after.
	 */
	private void assertOffenceEndsTheLeg(String route, int code, Offence offence) throws Exception {
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		CountDownLatch eof = new CountDownLatch(1);
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			// its SETTINGS first, as RFC 9113 3.4 has it: netty's client endpoint takes
			// any other first frame for a connection error of its own
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				received.add(frame);
				if (frame.type() == TestH2.HEADERS) {
					offence.commit(upstream);
				}
			}
			eof.countDown();
		}));
		String otherRoute = "other." + route;
		startClient(otherRoute, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			grantAndAnswer(upstream, false);
		}));
		double failed = counter("sluice.h2demux.legs.failed", null);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).as("stream 1 answered").isTrue();
			assertThat(TestH2.status(client.frames(1))).isEqualTo("502");
			assertThat(eof.await(5, TimeUnit.SECONDS)).as("the relay half-closed the upstream").isTrue();
			List<TestH2.Frame> goAways = received.stream().filter(frame -> frame.type() == TestH2.GOAWAY).toList();
			assertThat(goAways).as("one GOAWAY to the upstream").hasSize(1);
			assertThat(TestH2.errorCode(goAways.get(0))).isEqualTo(code);
			client.headers(3, true, get(otherRoute));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("another route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("0");
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
			assertThat(counter("sluice.h2demux.legs.failed", null) - failed).as("the leg failed").isEqualTo(1);
		}
	}

	/**
	 * The upstream commits the offence on reading a head, before answering: on stream 1,
	 * it gets RST_STREAM with the code and no GOAWAY, the client gets a {@code 502}, and
	 * stream 3 of the same route completes over the same upstream connection.
	 */
	private void assertOffenceResetsTheStream(String route, int code, Offence offence) throws Exception {
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		AtomicInteger connections = new AtomicInteger();
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			connections.incrementAndGet();
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				received.add(frame);
				if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					offence.commit(upstream);
				}
				else if (frame.type() == TestH2.HEADERS) {
					answer(upstream, frame.streamId(), "next");
				}
			}
		}));
		double resets = counter("sluice.h2demux.streams.reset", "upstream");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).as("stream 1 answered").isTrue();
			assertThat(TestH2.status(client.frames(1))).isEqualTo("502");
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> received.stream().anyMatch(frame -> frame.type() == TestH2.RST_STREAM));
			TestH2.Frame reset = received.stream()
				.filter(frame -> frame.type() == TestH2.RST_STREAM)
				.findFirst()
				.orElseThrow();
			assertThat(reset.streamId()).isEqualTo(1);
			assertThat(TestH2.errorCode(reset)).isEqualTo(code);
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("the leg goes on").isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("next");
			assertThat(connections.get()).as("over the same upstream connection").isEqualTo(1);
			assertThat(received).noneMatch(frame -> frame.type() == TestH2.GOAWAY);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(counter("sluice.h2demux.streams.reset", "upstream") - resets).as("counted once").isEqualTo(1);
		}
	}

	private double counter(String name, @Nullable String reason) {
		MeterRegistry registry = Objects.requireNonNull(this.meterRegistry);
		return reason == null ? registry.get(name).counter().count()
				: registry.get(name).tag("reason", reason).counter().count();
	}

	/** What an upstream does wrong. */
	@FunctionalInterface
	private interface Offence {

		void commit(WindowedH2Upstream upstream) throws IOException;

	}

	/** A test upstream's connection handler. */
	@FunctionalInterface
	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws IOException, InterruptedException;

	}

	/**
	 * Grants the connection, and the stream when {@code streams}, every DATA payload as
	 * it is read; answers every ended request with its byte count.
	 */
	private static void grantAndAnswer(WindowedH2Upstream upstream, boolean streams)
			throws IOException, InterruptedException {
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
				upstream.grant(0, frame.payload().length);
				if (streams) {
					upstream.grant(frame.streamId(), frame.payload().length);
				}
			}
			answerIfEnded(upstream, frame);
		}
	}

	/**
	 * Every 20ms, grants the connection, and every stream when {@code streams}, what the
	 * upstream read since: it reads ahead of its grants, so the bytes in flight show in
	 * its books.
	 */
	private static void grantPeriodically(WindowedH2Upstream upstream, boolean streams) {
		try {
			Map<Integer, Long> granted = new HashMap<>();
			while (true) {
				Thread.sleep(20);
				long connection = 0;
				for (Map.Entry<Integer, Long> entry : upstream.received().entrySet()) {
					long read = entry.getValue() - granted.getOrDefault(entry.getKey(), 0L);
					if (read > 0) {
						if (streams) {
							upstream.grant(entry.getKey(), (int) read);
						}
						granted.put(entry.getKey(), entry.getValue());
						connection += read;
					}
				}
				if (connection > 0) {
					upstream.grant(0, (int) connection);
				}
			}
		}
		catch (IOException | InterruptedException e) {
			// done
		}
	}

	/** Answers a request that ended with the frame with its DATA byte count. */
	private static void answerIfEnded(WindowedH2Upstream upstream, TestH2.Frame frame) throws IOException {
		boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
				&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
		if (ended) {
			answer(upstream, frame.streamId(), String.valueOf(upstream.received(frame.streamId())));
		}
	}

	/** A small response, far within the windows: written directly. */
	private static void answer(WindowedH2Upstream upstream, int streamId, String body) throws IOException {
		upstream.headers(streamId, false);
		upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, body.getBytes(StandardCharsets.US_ASCII));
	}

	/**
	 * Echoes stream 1 within the client's window, granting the request window (16KiB
	 * initially) only for the bytes echoed.
	 */
	private static void echo(WindowedH2Upstream upstream) throws IOException, InterruptedException {
		upstream.settings(16 * KIB);
		ArrayDeque<byte[]> pending = new ArrayDeque<>();
		AtomicBoolean ended = new AtomicBoolean();
		Thread echoer = Thread.ofVirtual().start(() -> {
			try {
				upstream.headers(1, false);
				while (true) {
					byte[] chunk;
					synchronized (pending) {
						while (pending.isEmpty() && !ended.get()) {
							pending.wait();
						}
						chunk = pending.pollFirst();
					}
					if (chunk == null) {
						upstream.sendData(1, new byte[0], true);
						return;
					}
					upstream.sendData(1, chunk, false);
					upstream.grant(1, chunk.length);
					upstream.grant(0, chunk.length);
				}
			}
			catch (IOException | InterruptedException e) {
				// the relay went away
			}
		});
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.DATA && frame.streamId() == 1) {
				synchronized (pending) {
					if (frame.payload().length > 0) {
						pending.addLast(frame.payload());
					}
					if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						ended.set(true);
					}
					pending.notifyAll();
				}
			}
		}
		echoer.interrupt();
	}

	/**
	 * Keeps the connection window topped up to 64KiB; once a changed initial window
	 * applied, waits 300ms, then keeps every stream's window topped up to 16KiB.
	 */
	private static void topUpAfterTheAck(WindowedH2Upstream upstream) {
		try {
			long ackedAt = 0;
			while (true) {
				long connection = upstream.connectionWindow();
				if (connection < WindowedH2Upstream.DEFAULT_WINDOW) {
					upstream.grant(0, (int) (WindowedH2Upstream.DEFAULT_WINDOW - connection));
				}
				if (ackedAt == 0 && upstream.windowSettingAcked()) {
					ackedAt = System.nanoTime();
				}
				if (ackedAt != 0 && System.nanoTime() - ackedAt > TimeUnit.MILLISECONDS.toNanos(300)) {
					for (Map.Entry<Integer, Long> entry : upstream.streamWindows().entrySet()) {
						if (entry.getValue() < 16 * KIB) {
							upstream.grant(entry.getKey(), (int) (16 * KIB - entry.getValue()));
						}
					}
				}
				Thread.sleep(10);
			}
		}
		catch (IOException | InterruptedException e) {
			// done
		}
	}

	/** The sequence with consecutive DATA entries summed up. */
	private static String summary(List<String> sequence) {
		List<String> out = new ArrayList<>();
		long data = -1;
		for (String entry : sequence) {
			if (entry.startsWith("DATA:")) {
				data = Math.max(data, 0) + Long.parseLong(entry.substring(5));
				continue;
			}
			if (data >= 0) {
				out.add("DATA:" + data);
				data = -1;
			}
			out.add(entry);
		}
		if (data >= 0) {
			out.add("DATA:" + data);
		}
		return String.join(" ", out);
	}

	private static void send(H2TestClient client, int streamId, int length) {
		try {
			client.data(streamId, length, true, Duration.ofSeconds(20));
		}
		catch (IOException | InterruptedException e) {
			// torn down
		}
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	/**
	 * A Netty h2c server in its default configuration answering every request 500ms after
	 * it ended; records the half-closure option of the accepted channel.
	 */
	private int nettyUpstream(AtomicReference<@Nullable Boolean> halfClosure) throws InterruptedException {
		EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
		this.closeables.add(group::shutdownGracefully);
		Channel channel = new ServerBootstrap().group(group)
			.channel(NioServerSocketChannel.class)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) {
					halfClosure.set(ch.config().getOption(ChannelOption.ALLOW_HALF_CLOSURE));
					ch.pipeline()
						.addLast(Http2FrameCodecBuilder.forServer().build(),
								new Http2MultiplexHandler(new ChannelInitializer<Http2StreamChannel>() {
									@Override
									protected void initChannel(Http2StreamChannel stream) {
										stream.pipeline().addLast(new LateResponder());
									}
								}));
				}
			})
			.bind("127.0.0.1", 0)
			.sync()
			.channel();
		this.closeables.add(channel::close);
		return ((InetSocketAddress) channel.localAddress()).getPort();
	}

	/** Answers an ended request 500ms later. */
	private static final class LateResponder extends ChannelInboundHandlerAdapter {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			try {
				if (msg instanceof Http2HeadersFrame headers && headers.isEndStream()) {
					ctx.executor().schedule(() -> {
						ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("200")));
						ctx.writeAndFlush(new DefaultHttp2DataFrame(
								Unpooled.copiedBuffer("late".getBytes(StandardCharsets.US_ASCII)), true));
					}, 500, TimeUnit.MILLISECONDS);
				}
			}
			finally {
				ReferenceCountUtil.release(msg);
			}
		}

	}

	/** The route's leg queue gauge; zero before it exists. */
	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find("sluice.h2demux.queued.bytes")
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.max()
			.orElse(0));
	}

	private void startClient(String host, ServerSocket upstream) {
		startClient(host, upstream.getLocalPort());
	}

	private void startClient(String host, int port) {
		MeterRegistry meters = new SimpleMeterRegistry();
		this.clientMeters.put(host, meters);
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + port).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-uf").start(task))
			.meterRegistry(meters)
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/**
	 * A test upstream serving every accepted connection with the handler; the first
	 * connection is published to {@code first}.
	 */
	private ServerSocket upstream(AtomicReference<WindowedH2Upstream> first, Handler handler) {
		return stub(socket -> {
			try (socket) {
				WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
				if (upstream == null) {
					return;
				}
				synchronized (first) {
					if (first.get() == null) {
						first.set(upstream);
					}
				}
				handler.serve(upstream);
			}
			catch (IOException | InterruptedException e) {
				// connection ended
			}
		});
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-uf-accept").start(() -> {
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

}
