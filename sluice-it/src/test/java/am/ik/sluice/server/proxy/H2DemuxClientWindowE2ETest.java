package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.Parameter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux honors the client's connection window on the response path (RFC 9113
 * 6.9.1): what every leg relays to the client stays within the window the client granted,
 * shared fairly by the streams of every route, and an upstream is held back by its own
 * connection window while its data waits. A leg whose data waits keeps reading its
 * upstream: pings are acked, window updates credited, heads of other streams relayed. In
 * the relay's package for the data proxy's connection count. A client WINDOW_UPDATE RFC
 * 9113 6.9 makes an error ends the connection or resets its stream.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
class H2DemuxClientWindowE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final int INTERNAL_ERROR = 0x2;

	private static final int KIB = 1024;

	private static final int WINDOW = WindowedH2Upstream.DEFAULT_WINDOW;

	/**
	 * A response the relay holds whole while the client grants no connection window:
	 * within the client's window plus what the relay holds -- the legacy engine one
	 * connection window, the netty engine one stream window, half of it granted back once
	 * the client took the rest (netty's window update ratio).
	 */
	private static final int HELD_RESPONSE = 90 * KIB;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int FRAME_SIZE_ERROR = 0x6;

	private static final int CANCEL = 0x8;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final AtomicLong PINGS = new AtomicLong();

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
		this.closeables.forEach(H2DemuxClientWindowE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * A client granting its connection window 16KiB at a time, one grant per 10ms for
	 * bytes it received, with stream windows of 1MiB, receives 512KiB on each of two
	 * streams of two routes: never more than it granted, both complete, and the streams
	 * take turns -- when the first ends, the other has a quarter of its body at least.
	 */
	@Test
	void sparseConnectionWindowIsNeverExceededAcrossTwoRoutes() throws Exception {
		String first = route("cw.sparse-a");
		String second = route("cw.sparse-b");
		int length = 512 * KIB;
		startClient(first, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, length)));
		startClient(second, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, length)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.paceConnectionGrants(16 * KIB, Duration.ofMillis(10));
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(first));
			client.headers(3, true, get(second));
			assertThat(client.awaitTrue(() -> client.ended(1) || client.ended(3), Duration.ofSeconds(30))).isTrue();
			long other = client.ended(1) ? client.dataReceived(3) : client.dataReceived(1);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(30))).isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
			assertThat(TestH2.contentOf(client.frames(3))).isEqualTo(pattern(length));
			assertThat(client.maxReceiveExcess()).as("DATA beyond the connection window granted")
				.isLessThanOrEqualTo(0);
			assertThat(other).as("the other stream's progress when the first ended").isGreaterThanOrEqualTo(length / 4);
		}
	}

	/**
	 * A Netty h2 client in its default configuration -- 65535 stream and connection
	 * windows, each replenished once half of it is consumed -- whose application reads
	 * one frame per 5ms gets two 512KiB responses of two routes on one connection: the
	 * sum of its stream windows is twice its connection window. A relay sending past the
	 * connection window gets a GOAWAY {@code FLOW_CONTROL_ERROR} from it instead.
	 */
	@Test
	void nettyClientWithDefaultFlowControlGetsTwoLargeResponses() throws Exception {
		String first = route("cw.netty-a");
		String second = route("cw.netty-b");
		int length = 512 * KIB;
		startClient(first, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, length)));
		startClient(second, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, length)));
		EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
		try {
			AtomicReference<@Nullable Long> goAway = new AtomicReference<>();
			Channel channel = new Bootstrap().group(group)
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) {
						ch.pipeline()
							.addLast(Http2FrameCodecBuilder.forClient().build(),
									new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()),
									new ChannelInboundHandlerAdapter() {
										@Override
										public void channelRead(ChannelHandlerContext ctx, Object msg) {
											if (msg instanceof Http2GoAwayFrame frame) {
												goAway.set(frame.errorCode());
											}
											ReferenceCountUtil.release(msg);
										}
									});
					}
				})
				.connect("127.0.0.1", dataPort)
				.sync()
				.channel();
			// closed even when the test fails: the class's later tests count the
			// relay's connections
			try {
				CompletableFuture<Long> firstBody = nettyGet(channel, first);
				CompletableFuture<Long> secondBody = nettyGet(channel, second);
				assertThat(firstBody.get(30, TimeUnit.SECONDS)).isEqualTo(length);
				assertThat(secondBody.get(30, TimeUnit.SECONDS)).isEqualTo(length);
				assertThat(goAway.get()).as("the client's GOAWAY").isNull();
			}
			finally {
				channel.close().sync();
			}
		}
		finally {
			group.shutdownGracefully();
		}
	}

	/**
	 * An upstream PING sent while its leg's data waits for the client's connection window
	 * is acked; the upstream is held back by its own connection window meanwhile, and the
	 * response completes once the client grants.
	 */
	@Test
	@NettyOnly("per-hop windows: what the relay holds of a response")
	void upstreamPingIsAckedWhileItsLegWaitsForTheClientWindow() throws Exception {
		String route = route("cw.ping");
		int length = 256 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch acked = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					respondAsync(upstream, frame.streamId(), length);
				}
				else if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0) {
					acked.countDown();
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			awaitWaiting(client, upstream);
			upstream.write(TestH2.PING, 0, 0, "upstream".getBytes(StandardCharsets.US_ASCII));
			assertThat(acked.await(5, TimeUnit.SECONDS)).as("the relay acked the upstream's PING").isTrue();
			assertThat(client.dataReceived()).as("the data still waits").isEqualTo(WINDOW);
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
		}
	}

	/**
	 * While a stream's response waits for the client's connection window, another stream
	 * of the same leg uploads 256KiB -- which needs the upstream's window updates, read
	 * behind the waiting data -- and gets its head-only response.
	 */
	@Test
	@NettyOnly("per-hop windows: what the relay holds of a response")
	void legKeepsRelayingWhileAResponseWaitsForTheClientWindow() throws Exception {
		String route = route("cw.upload");
		int length = 256 * KIB;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				int streamId = frame.streamId();
				if (frame.type() == TestH2.HEADERS && streamId == 1) {
					respondAsync(upstream, streamId, length);
				}
				else if (frame.type() == TestH2.DATA) {
					if (frame.payload().length > 0) {
						upstream.grant(streamId, frame.payload().length);
						upstream.grant(0, frame.payload().length);
					}
					if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						upstream.headers(streamId, true);
					}
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			awaitWaiting(client, upstream);
			client.headers(3, false, post(route));
			assertThat(client.data(3, length, true, Duration.ofSeconds(10))).as("the upload got the upstream's window")
				.isTrue();
			assertThat(client.awaitEnd(3, Duration.ofSeconds(10))).as("the head-only response arrived").isTrue();
			assertThat(upstream.received(3)).isEqualTo(length);
			assertThat(client.dataReceived()).as("stream 1 still waits").isEqualTo(WINDOW);
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
		}
	}

	/**
	 * A client WINDOW_UPDATE taking the connection window to 2^31-1 is fine; one past it
	 * ends the connection with GOAWAY {@code FLOW_CONTROL_ERROR} (RFC 9113 6.9.1).
	 */
	@Test
	void connectionWindowPastTheMaximumEndsTheConnection() throws Exception {
		String route = route("cw.overflow");
		startClient(route, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, 10)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
			// the window is 65535 - 10 now: this takes it to 2^31-1 exactly
			client.frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - WINDOW + 10));
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(10))).as("the window at the maximum keeps the connection")
				.isTrue();
			// 10 more were spent and are granted back, then one byte past the maximum
			client.frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(11));
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).isTrue();
			assertThat(client.goAwayCode()).isEqualTo(TestH2.FLOW_CONTROL_ERROR);
		}
	}

	/**
	 * A client WINDOW_UPDATE of a length other than 4 ends the connection with GOAWAY
	 * {@code FRAME_SIZE_ERROR}, on stream 0 as on an open stream (RFC 9113 6.9).
	 */
	@ParameterizedTest
	@CsvSource({ "0, 3", "0, 5", "1, 3", "1, 5" })
	void windowUpdateOfAWrongLengthEndsTheConnection(int streamId, int length) throws Exception {
		String route = "cw.wu-length-%d-%d.test".formatted(streamId, length);
		try (H2TestClient client = openHeldStream(route)) {
			byte[] payload = new byte[length];
			payload[length - 1] = 1;
			client.frame(TestH2.WINDOW_UPDATE, 0, streamId, payload);
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay closed the connection")
				.isTrue();
			assertThat(client.goAwayCode()).isEqualTo(FRAME_SIZE_ERROR);
		}
	}

	/**
	 * A client connection-level WINDOW_UPDATE with an increment of 0 ends the connection
	 * with GOAWAY {@code PROTOCOL_ERROR} (RFC 9113 6.9).
	 */
	@Test
	void zeroConnectionWindowIncrementEndsTheConnection() throws Exception {
		try (H2TestClient client = openHeldStream(route("cw.wu-zero-connection"))) {
			client.frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			assertThat(client.awaitTrue(client::closed, Duration.ofSeconds(5))).as("the relay closed the connection")
				.isTrue();
			assertThat(client.goAwayCode()).isEqualTo(PROTOCOL_ERROR);
		}
	}

	/**
	 * A client stream-level WINDOW_UPDATE with an increment of 0 on an open stream resets
	 * that stream only (RFC 9113 6.9): RST_STREAM {@code PROTOCOL_ERROR} to the client,
	 * {@code CANCEL} to the upstream, counted once with reason {@code protocol} -- a
	 * second one, once the reset stream is no longer tracked, changes nothing --, and the
	 * connection's other stream completes.
	 */
	@Test
	@LegacyOnly("a frame on a stream netty forgot is answered by netty's codec with a reset of its own, and the "
			+ "protocol reset meter is not yet on the netty engine")
	void zeroStreamWindowIncrementResetsOnlyThatStream() throws Exception {
		String route = route("cw.wu-zero-stream");
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				received.add(frame);
				if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					upstream.headers(frame.streamId(), false);
					upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, frame.streamId(),
							"ok".getBytes(StandardCharsets.US_ASCII));
				}
			}
		}));
		double protocolResets = resets("protocol");
		double budgetResets = resets("budget");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			client.headers(3, false, post(route));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> received.stream().filter(frame -> frame.type() == TestH2.HEADERS).count() == 2);
			assertThat(trackedStreams(route)).as("both streams open").isEqualTo(2);
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(1)).isEqualTo(PROTOCOL_ERROR);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(upstreamResets(received)).containsExactly(List.of(1, CANCEL)));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 1);
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0));
			awaitPingAck(client);
			List<TestH2.Frame> frames = client.frames(1);
			assertThat(frames).filteredOn(frame -> frame.type() == TestH2.RST_STREAM).as("one reset").hasSize(1);
			assertThat(frames.get(frames.size() - 1).type()).as("nothing follows the reset")
				.isEqualTo(TestH2.RST_STREAM);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
			assertThat(resets("budget") - budgetResets).isZero();
			client.sendData(3, "x".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("ok");
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(upstreamResets(received)).as("the upstream got no more resets").hasSize(1);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 0);
		}
	}

	/**
	 * A client WINDOW_UPDATE with an increment of 0 on a stream the relay no longer
	 * tracks -- its request and response ended -- is ignored (RFC 9113 5.1): no reset,
	 * the connection serves the next stream.
	 */
	@Test
	@LegacyOnly("a frame on a stream netty forgot is answered by netty's codec with a reset of its own; a per-hop "
			+ "rewrite")
	void zeroIncrementOnAnUntrackedStreamIsIgnored() throws Exception {
		String route = route("cw.wu-zero-untracked");
		startClient(route, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, 10)));
		double protocolResets = resets("protocol");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 0);
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0));
			awaitPingAck(client);
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
			assertThat(client.resets()).isEmpty();
			assertThat(client.closed()).isFalse();
			assertThat(resets("protocol") - protocolResets).isZero();
		}
	}

	/**
	 * A client WINDOW_UPDATE with an increment of 0 on a stream closed both ways -- the
	 * client ended its request, the response ended -- but still tracked, its request DATA
	 * held by the upstream's stream window of 0, is ignored (RFC 9113 5.1): no reset
	 * either way, and the DATA reaches the upstream once it grants the window.
	 */
	@Test
	@LegacyOnly("a frame on a stream netty closed is answered by netty's codec with a reset of its own; a per-hop "
			+ "rewrite. The netty engine's request data outliving its client stream: "
			+ "NettyH2DemuxBudgetE2ETest#theRequestsRestOutlivesItsClientStreamClosingOnAnEarlyResponse")
	void zeroIncrementOnAStreamClosedBothWaysIsIgnored() throws Exception {
		String route = route("cw.wu-zero-closed");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(0);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				received.add(frame);
				if (frame.type() == TestH2.HEADERS) {
					upstream.headers(frame.streamId(), true);
				}
			}
		}));
		double protocolResets = resets("protocol");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, false, post(route));
			// the upstream's SETTINGS precede its response on the leg: applied by now
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.sendData(1, "abc".getBytes(StandardCharsets.US_ASCII), TestH2.FLAG_END_STREAM);
			awaitPingAck(client);
			assertThat(trackedStreams(route)).as("tracked while its DATA waits for the upstream's window").isEqualTo(1);
			client.frame(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0));
			awaitPingAck(client);
			assertThat(trackedStreams(route)).isEqualTo(1);
			assertThat(client.resets()).isEmpty();
			assertThat(resets("protocol") - protocolResets).isZero();
			WindowedH2Upstream upstream = awaitUpstream(seen);
			assertThat(upstream.received(1)).as("nothing reached the upstream yet").isZero();
			upstream.grant(1, 3);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> upstream.received(1) == 3);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 0);
			assertThat(upstreamResets(received)).isEmpty();
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * A client resetting a stream whose response waits for its connection window: the
	 * waiting data is dropped and its window given back to the upstream, so another
	 * stream of the same leg gets its 128KiB response; nothing of the reset stream
	 * follows.
	 */
	@Test
	@NettyOnly("per-hop windows: what the relay holds of a response")
	void clientResetReturnsTheWaitingDataToTheUpstreamWindow() throws Exception {
		String route = route("cw.client-reset");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					respondAsync(upstream, frame.streamId(), frame.streamId() == 1 ? 256 * KIB : 128 * KIB);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			awaitWaiting(client, upstream);
			client.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			client.headers(3, true, get(route));
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(3, Duration.ofSeconds(10))).as("received %d", client.dataReceived(3)).isTrue();
			assertThat(TestH2.contentOf(client.frames(3))).isEqualTo(pattern(128 * KIB));
			assertThat(client.dataReceived(1)).as("nothing of the reset stream followed").isEqualTo(WINDOW);
		}
	}

	/**
	 * An upstream resetting a stream whose response waits for the client's connection
	 * window: the client gets the reset and no data of the stream after it, and the
	 * waiting data's window goes back to the upstream for another stream's 128KiB.
	 */
	@Test
	@NettyOnly("per-hop windows: what the relay holds of a response")
	void upstreamResetDropsTheWaitingData() throws Exception {
		String route = route("cw.upstream-reset");
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					respondAsync(upstream, frame.streamId(), frame.streamId() == 1 ? 256 * KIB : 128 * KIB);
				}
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			awaitWaiting(client, upstream);
			upstream.reset(1, CANCEL);
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(5))).isTrue();
			client.headers(3, true, get(route));
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(3, Duration.ofSeconds(10))).as("received %d", client.dataReceived(3)).isTrue();
			assertThat(TestH2.contentOf(client.frames(3))).isEqualTo(pattern(128 * KIB));
			List<TestH2.Frame> frames = client.frames(1);
			assertThat(frames.get(frames.size() - 1).type()).as("the reset is the stream's last frame")
				.isEqualTo(TestH2.RST_STREAM);
			assertThat(client.dataReceived(1)).isEqualTo(WINDOW);
		}
	}

	/**
	 * An upstream closing its connection right after its response: the part of it still
	 * waiting for the client's connection window reaches the client once granted.
	 */
	@Test
	void upstreamClosingAfterItsResponseStillDeliversTheWaitingData() throws Exception {
		assertWaitingDataOutlivesTheUpstream(route("cw.upstream-close"), false);
	}

	/**
	 * An upstream resetting its connection (TCP RST) right after its response: the
	 * failure reads as the upstream's end, so with no response owed the waiting part of
	 * the response still reaches the client.
	 */
	@Test
	void upstreamResettingAfterItsResponseStillDeliversTheWaitingData() throws Exception {
		assertWaitingDataOutlivesTheUpstream(route("cw.upstream-rst"), true);
	}

	/**
	 * While a leg whose upstream left drains its waiting data, a new stream of its route
	 * goes to a new leg and completes; the waiting data still follows once granted.
	 */
	@Test
	void newStreamOfARouteWhoseLegDrainsGoesToANewLeg() throws Exception {
		String route = route("cw.drain-new-leg");
		int length = HELD_RESPONSE;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch closed = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			CompletableFuture<TestH2.Frame> head = readInBackground(upstream);
			int streamId = head.get(10, TimeUnit.SECONDS).streamId();
			upstream.headers(streamId, false);
			upstream.sendData(streamId, pattern(length), true);
			if (streamId == 1) {
				upstream.close();
				closed.countDown();
			}
			else {
				upstream.awaitEof();
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			assertThat(closed.await(10, TimeUnit.SECONDS)).as("the first upstream answered and closed").isTrue();
			Thread.sleep(300);
			assertThat(client.dataReceived()).as("stream 1's rest waits").isEqualTo(WINDOW);
			client.headers(3, true, get(route));
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitTrue(() -> client.ended(1) && client.ended(3), Duration.ofSeconds(10)))
				.as("resets %s", client.resets())
				.isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
			assertThat(TestH2.contentOf(client.frames(3))).isEqualTo(pattern(length));
		}
	}

	private void assertWaitingDataOutlivesTheUpstream(String route, boolean reset) throws Exception {
		int length = HELD_RESPONSE;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch closed = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			try {
				TestH2.Frame frame = readInBackground(upstream).get(10, TimeUnit.SECONDS);
				upstream.headers(frame.streamId(), false);
				upstream.sendData(frame.streamId(), pattern(length), true);
			}
			finally {
				if (reset) {
					// a reset drops what the peer's stack holds unread: the response
					// reaches
					// the relay first
					Thread.sleep(300);
					upstream.abort();
				}
				else {
					upstream.close();
				}
				closed.countDown();
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			assertThat(closed.await(10, TimeUnit.SECONDS)).as("the upstream sent its response and closed").isTrue();
			assertThat(awaitUpstream(seen).sent()).isEqualTo(length);
			// the relay reads the upstream's end meanwhile
			Thread.sleep(300);
			assertThat(client.dataReceived()).as("the rest waits").isEqualTo(WINDOW);
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).as("received %d", client.dataReceived()).isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(length));
		}
	}

	/**
	 * A client gone entirely while a response waits for its connection window, the
	 * upstream done with it: the relay notices on probing the client and ends.
	 */
	@Test
	void clientGoneWhileAResponseWaitsForItsWindowEndsTheRelay() throws Exception {
		String route = route("cw.gone");
		int length = HELD_RESPONSE;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> respondToEach(upstream, length)));
		DataProxyServer proxy = Objects.requireNonNull(this.dataProxyServer);
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> proxy.activeConnections() == 0);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			WindowedH2Upstream upstream = awaitUpstream(seen);
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> upstream.sent() == length);
			assertThat(client.awaitTrue(() -> client.dataReceived() == WINDOW, Duration.ofSeconds(5))).isTrue();
			assertThat(proxy.activeConnections()).as("the relay is live").isEqualTo(1);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
	}

	/**
	 * An upstream sending past the connection window the relay granted it ends its leg (a
	 * connection error {@code FLOW_CONTROL_ERROR}, RFC 9113 6.9.1) rather than having the
	 * relay hold what the client has no window for: the upstream gets a GOAWAY, the
	 * client RST_STREAM {@code INTERNAL_ERROR} on the stream whose response began, never
	 * more than its window, and its connection goes on: a stream of another route
	 * completes.
	 */
	@Test
	@NettyOnly("per-hop windows: the upstream passes its stream window first")
	void upstreamIgnoringTheRelayWindowEndsItsLeg() throws Exception {
		String route = route("cw.ignoring");
		String otherRoute = route("cw.ignoring-other");
		startClient(otherRoute, upstream(new AtomicReference<>(), upstream -> respondToEach(upstream, 10)));
		List<TestH2.Frame> received = new CopyOnWriteArrayList<>();
		startClient(route, stub(socket -> {
			try (socket) {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				TestH2.readClientHead(in);
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, new byte[] { (byte) 0x88 });
				for (int i = 0; i < 13; i++) {
					TestH2.writeFrame(out, TestH2.DATA, 0, 1, new byte[16 * KIB]);
				}
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					received.add(frame);
				}
			}
			catch (IOException e) {
				// the relay went away
			}
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, Duration.ofSeconds(10))).as("stream 1 reset")
				.isTrue();
			assertThat(client.reset(1)).isEqualTo(INTERNAL_ERROR);
			// the relay refills its connection window as DATA arrives (the stream windows
			// bound what it holds), so the upstream passes the stream's window first: a
			// stream error, the stream reset towards the upstream
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> received.stream().anyMatch(frame -> frame.type() == TestH2.RST_STREAM));
			TestH2.Frame offence = received.stream()
				.filter(frame -> frame.type() == TestH2.RST_STREAM)
				.findFirst()
				.orElseThrow();
			assertThat(TestH2.errorCode(offence)).isEqualTo(TestH2.FLOW_CONTROL_ERROR);
			assertThat(client.maxReceiveExcess()).isLessThanOrEqualTo(0);
			client.grantConnectionWindowOnReceipt();
			client.headers(3, true, get(otherRoute));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).as("another route's stream completes").isTrue();
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Padded DATA frames of an upstream reach a client granting its connection window
	 * 7000 bytes per 5ms for bytes it received, so frames are split: never more than
	 * granted, the content intact, and the padding delivered too -- the client receives
	 * exactly the flow-controlled bytes the upstream sent, which keeps both sides'
	 * windows in step.
	 */
	@Test
	@NettyOnly("per-hop windows: padding is not relayed")
	void paddedFramesSplitAcrossTheClientWindowKeepTheAccounting() throws Exception {
		String route = route("cw.padded");
		int frames = 12;
		int content = 9000;
		int padLength = 200;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			int streamId = readInBackground(upstream).get(10, TimeUnit.SECONDS).streamId();
			upstream.headers(streamId, false);
			byte[] body = pattern(frames * content);
			for (int i = 0; i < frames; i++) {
				upstream.sendPadded(streamId, Arrays.copyOfRange(body, i * content, (i + 1) * content), padLength,
						i == frames - 1);
			}
			upstream.awaitEof();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.paceConnectionGrants(7000, Duration.ofMillis(5));
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).as("received %d", client.dataReceived()).isTrue();
			assertThat(TestH2.contentOf(client.frames(1))).isEqualTo(pattern(frames * content));
			// per hop: the content is relayed, the padding is not
			assertThat(client.dataReceived()).as("the flow-controlled bytes received")
				.isEqualTo((long) frames * content);
			assertThat(client.maxReceiveExcess()).isLessThanOrEqualTo(0);
		}
	}

	/**
	 * Trailers of a stream whose data waits for the client's connection window follow the
	 * data: the client sees them last, once it granted.
	 */
	@Test
	void trailersFollowTheWaitingData() throws Exception {
		String route = route("cw.trailers");
		int length = HELD_RESPONSE;
		AtomicReference<WindowedH2Upstream> seen = new AtomicReference<>();
		CountDownLatch sent = new CountDownLatch(1);
		startClient(route, upstream(seen, upstream -> {
			upstream.settings(null);
			int streamId = readInBackground(upstream).get(10, TimeUnit.SECONDS).streamId();
			upstream.headers(streamId, false);
			upstream.sendData(streamId, pattern(length), false);
			upstream.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, streamId,
					TestH2.headerBlock("grpc-status=0"));
			sent.countDown();
			upstream.awaitEof();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			assertThat(sent.await(10, TimeUnit.SECONDS)).isTrue();
			Thread.sleep(300);
			assertThat(client.dataReceived()).isEqualTo(WINDOW);
			assertThat(client.ended(1)).as("the trailers wait behind the data").isFalse();
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
			List<TestH2.Frame> frames = client.frames(1);
			assertThat(frames.get(frames.size() - 1).type()).isEqualTo(TestH2.HEADERS);
			assertThat(TestH2.contentOf(frames)).isEqualTo(pattern(length));
		}
	}

	/**
	 * Trailers waiting behind data are bounded per leg: once the trailers waiting would
	 * pass a header block's worth, the leg stops reading its upstream -- a PING sent
	 * after them is acked only once the client granted -- and every stream completes.
	 */
	@Test
	@LegacyOnly("the legacy relay's per-leg allowance of trailers waiting for the client's window: the netty "
			+ "engine holds them behind their stream's data, accounted against the queue budget")
	void trailersPastTheAllowancePauseTheLegUntilTheClientGrants() throws Exception {
		String route = route("cw.big-trailers");
		int length = 10 * KIB;
		int[] trailed = { 3, 5, 7 };
		CountDownLatch sent = new CountDownLatch(1);
		CountDownLatch acked = new CountDownLatch(1);
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			CountDownLatch heads = new CountDownLatch(1 + trailed.length);
			Thread responder = Thread.ofVirtual().start(() -> {
				try {
					heads.await();
					// stream 1 takes the client's whole window: the rest waits
					upstream.headers(1, false);
					upstream.sendData(1, pattern(WINDOW), true);
					DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();
					for (int streamId : trailed) {
						upstream.headers(streamId, false);
						upstream.sendData(streamId, pattern(length), false);
					}
					for (int streamId : trailed) {
						writeBlock(upstream, streamId, bigTrailers(encoder));
					}
					upstream.write(TestH2.PING, 0, 0, "trailers".getBytes(StandardCharsets.US_ASCII));
					sent.countDown();
				}
				catch (Exception e) {
					// the relay went away
				}
			});
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					heads.countDown();
				}
				else if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0) {
					acked.countDown();
				}
			}
			responder.interrupt();
		}));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			client.withholdConnectionWindow();
			streamWindow(client, 1024 * KIB);
			client.headers(1, true, get(route));
			for (int streamId : trailed) {
				client.headers(streamId, true, get(route));
			}
			assertThat(sent.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			assertThat(acked.await(500, TimeUnit.MILLISECONDS)).as("the leg's reading paused").isFalse();
			assertThat(client.dataReceived()).as("the trailed streams' data waits").isEqualTo(WINDOW);
			client.grantConnectionWindowOnReceipt();
			assertThat(acked.await(5, TimeUnit.SECONDS)).as("the PING acked once the trailers left").isTrue();
			for (int streamId : trailed) {
				assertThat(client.awaitEnd(streamId, Duration.ofSeconds(10))).isTrue();
				List<TestH2.Frame> frames = client.frames(streamId);
				assertThat(TestH2.contentOf(frames)).isEqualTo(pattern(length));
				assertThat(frames.get(frames.size() - 1).type()).isIn(TestH2.HEADERS, TestH2.CONTINUATION);
			}
		}
	}

	/** Trailers of about 50KiB: one header with a long value, encoded by the upstream. */
	private static byte[] bigTrailers(DefaultHttp2HeadersEncoder encoder) throws Exception {
		Http2Headers trailers = new DefaultHttp2Headers();
		trailers.add("x-big", "v".repeat(50 * KIB));
		ByteBuf out = Unpooled.buffer();
		try {
			encoder.encodeHeaders(1, trailers, out);
			byte[] block = new byte[out.readableBytes()];
			out.readBytes(block);
			return block;
		}
		finally {
			out.release();
		}
	}

	/** Writes a header block ending the stream as HEADERS plus CONTINUATION frames. */
	private static void writeBlock(WindowedH2Upstream upstream, int streamId, byte[] block) throws IOException {
		ByteArrayOutputStream frames = new ByteArrayOutputStream();
		int offset = 0;
		boolean first = true;
		while (offset < block.length) {
			int length = Math.min(16 * KIB, block.length - offset);
			boolean last = offset + length == block.length;
			int flags = (first ? TestH2.FLAG_END_STREAM : 0) | (last ? TestH2.FLAG_END_HEADERS : 0);
			TestH2.writeFrame(frames, first ? TestH2.HEADERS : TestH2.CONTINUATION, flags, streamId,
					Arrays.copyOfRange(block, offset, offset + length));
			first = false;
			offset += length;
		}
		// one write: the frames of a block never interleave with others
		upstream.raw(frames.toByteArray());
	}

	private CompletableFuture<Long> nettyGet(Channel channel, String route) throws InterruptedException {
		CompletableFuture<Long> body = new CompletableFuture<>();
		AtomicLong received = new AtomicLong();
		// an application reading one frame per 5ms: the connection window comes back as
		// it reads
		Http2StreamChannel stream = new Http2StreamChannelBootstrap(channel).option(ChannelOption.AUTO_READ, false)
			.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator().maxMessagesPerRead(1))
			.handler(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) {
					try {
						boolean end = false;
						if (msg instanceof Http2DataFrame data) {
							received.addAndGet(data.content().readableBytes());
							end = data.isEndStream();
						}
						else if (msg instanceof Http2HeadersFrame headers) {
							end = headers.isEndStream();
						}
						if (end) {
							body.complete(received.get());
						}
					}
					finally {
						ReferenceCountUtil.release(msg);
					}
				}

				@Override
				public void channelInactive(ChannelHandlerContext ctx) {
					body.completeExceptionally(new IOException("stream closed after %d".formatted(received.get())));
				}
			})
			.open()
			.sync()
			.getNow();
		stream.writeAndFlush(new DefaultHttp2HeadersFrame(
				new DefaultHttp2Headers().method("GET").scheme("http").path("/").authority(route), true));
		// the flush: netty before 4.2.19 leaves the WINDOW_UPDATE a read() writes for
		// the bytes read before unflushed while an earlier read() still waits for a
		// frame, so the client withholds window it granted (netty/netty#17600)
		ScheduledFuture<?> reads = stream.eventLoop().scheduleAtFixedRate(() -> {
			stream.read();
			stream.flush();
		}, 5, 5, TimeUnit.MILLISECONDS);
		body.whenComplete((length, failure) -> reads.cancel(false));
		return body;
	}

	/**
	 * Answers every request head with a body of {@code length}, each on its own thread.
	 */
	private static void respondToEach(WindowedH2Upstream upstream, int length) throws IOException {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				respondAsync(upstream, frame.streamId(), length);
			}
		}
	}

	private static void respondAsync(WindowedH2Upstream upstream, int streamId, int length) {
		Thread.ofVirtual().start(() -> {
			try {
				upstream.headers(streamId, false);
				upstream.sendData(streamId, pattern(length), true);
			}
			catch (IOException | InterruptedException e) {
				// the relay went away
			}
		});
	}

	/**
	 * Reads the relay's frames on a thread of its own until EOF; the future completes
	 * with the first request head, or fails once the relay went away first.
	 */
	private static CompletableFuture<TestH2.Frame> readInBackground(WindowedH2Upstream upstream) {
		CompletableFuture<TestH2.Frame> head = new CompletableFuture<>();
		Thread.ofVirtual().start(() -> {
			try {
				TestH2.@Nullable Frame frame;
				while ((frame = upstream.next()) != null) {
					if (frame.type() == TestH2.HEADERS) {
						head.complete(frame);
					}
				}
			}
			catch (IOException e) {
				// the relay went away
			}
			finally {
				head.completeExceptionally(new IOException("the relay went away"));
			}
		});
		return head;
	}

	/**
	 * Waits until the client received its whole connection window and the upstream sent
	 * what the relay holds on top: half a stream window or more, at most one, whose
	 * consumed bytes it grants back only once half of it was.
	 */
	private static void awaitWaiting(H2TestClient client, WindowedH2Upstream upstream) throws InterruptedException {
		assertThat(client.awaitTrue(() -> client.dataReceived() == WINDOW && upstream.sent() > WINDOW + WINDOW / 2,
				Duration.ofSeconds(10)))
			.as("received %d, the upstream sent %d", client.dataReceived(), upstream.sent())
			.isTrue();
		// the upstream sends on the relay's last grant on a thread of its own: wait until
		// it stopped for half a second
		long sent = upstream.sent();
		long since = System.nanoTime();
		long deadline = since + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() - since < TimeUnit.MILLISECONDS.toNanos(500)) {
			assertThat(System.nanoTime()).as("the upstream stops").isLessThan(deadline);
			Thread.sleep(20);
			if (upstream.sent() != sent) {
				sent = upstream.sent();
				since = System.nanoTime();
			}
		}
		assertThat(client.dataReceived()).as("the client got no more than its window").isEqualTo(WINDOW);
		assertThat(sent).as("the upstream got no more than the relay's window on top").isLessThanOrEqualTo(2L * WINDOW);
	}

	/**
	 * Connects and opens stream 1 of the route with a request whose end never comes; the
	 * upstream holds it. Returns once the relay tracks the stream.
	 */
	private H2TestClient openHeldStream(String route) throws IOException {
		startClient(route, upstream(new AtomicReference<>(), upstream -> {
			upstream.settings(null);
			while (upstream.next() != null) {
				// held
			}
		}));
		H2TestClient client = H2TestClient.connect(dataPort, 1);
		client.headers(1, false, post(route));
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> trackedStreams(route) == 1);
		return client;
	}

	/**
	 * Pings the relay and waits for the ack: the outbound parse handled every frame sent
	 * before.
	 */
	private static void awaitPingAck(H2TestClient client) throws Exception {
		byte[] payload = TestH2.int32((int) PINGS.incrementAndGet());
		byte[] ping = Arrays.copyOf(payload, 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
				Duration.ofSeconds(5)))
			.as("the relay acked the PING")
			.isTrue();
	}

	/** The RST_STREAM frames the upstream received: stream id and error code. */
	private static List<List<Integer>> upstreamResets(List<TestH2.Frame> received) {
		return received.stream()
			.filter(frame -> frame.type() == TestH2.RST_STREAM)
			.map(frame -> List.of(frame.streamId(), TestH2.errorCode(frame)))
			.toList();
	}

	private double resets(String reason) {
		return Objects.requireNonNull(this.dataProxyServer)
			.h2DemuxMeters()
			.registry()
			.get("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private static WindowedH2Upstream awaitUpstream(AtomicReference<WindowedH2Upstream> seen) {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> seen.get() != null);
		return seen.get();
	}

	private static void streamWindow(H2TestClient client, int window) throws IOException {
		client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, window));
	}

	/** A body whose bytes tell their offset, so loss or reordering shows. */
	private static byte[] pattern(int length) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) (i % 251);
		}
		return bytes;
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-cw").start(task))
			.meterRegistry(new SimpleMeterRegistry())
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
			catch (Exception e) {
				// connection ended
			}
		});
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-cw-accept").start(() -> {
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

	/**
	 * The route's host for this engine's invocation: both run on one router, where the
	 * other invocation's tunnel client may still serve a host of the same name.
	 */
	private String route(String name) {
		return name + "." + String.valueOf(this.engine).toLowerCase(Locale.ROOT) + ".test";
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws Exception;

	}

}
