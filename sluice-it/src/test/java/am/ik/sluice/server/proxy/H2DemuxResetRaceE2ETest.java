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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.Parameter;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux resetting a stream while a frame of the stream is on its way to the client
 * -- a response DATA frame, or a stream window grant of the relay's own: one side is held
 * between its decision and its write (the relay's write seam), with the client stream's
 * lock held, while the other side runs, and the client sees the frame before the
 * RST_STREAM or not at all, never after it (RFC 9113 5.1). The relay's own reset is a
 * WINDOW_UPDATE with an increment of 0 (RFC 9113 6.9) from the client, decided by the
 * outbound parse, or from the upstream, decided by the leg's inbound side. In the relay's
 * package for the package-private write seam.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
@LegacyOnly("write-seam races, replaced by tests of the lock boundary: not yet on the netty engine")
class H2DemuxResetRaceE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final int STREAM = 1;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int CANCEL = 0x8;

	private static final int INTERNAL_ERROR = 0x2;

	/** The stream window the relay extends a stream by once its head is written. */
	private static final int HEAD_GRANT = Http2DemuxRelay.STREAM_BUDGET - H2TestClient.INITIAL_WINDOW;

	private static final AtomicLong PINGS = new AtomicLong();

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	/** Scripted upstreams to let go once a test ends. */
	private final List<CountDownLatch> releases = new CopyOnWriteArrayList<>();

	/** Held writes to let go once a test ends. */
	private final List<HeldWrite> held = new CopyOnWriteArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

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
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void release() {
		// a test failing midway leaves neither a relay side nor an upstream waiting
		dataProxyServer().h2WriteSeam(Http2DemuxRelay.WriteSeam.NONE);
		this.held.forEach(HeldWrite::proceed);
		this.held.clear();
		this.releases.forEach(CountDownLatch::countDown);
		this.releases.clear();
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxResetRaceE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * The leg's inbound side found a response DATA frame relayable and is held before
	 * writing it when the client's WINDOW_UPDATE resets the stream: the reset waits for
	 * the client stream behind the held frame, and the client reads the frame, then the
	 * RST_STREAM.
	 */
	@Test
	void aResponseFrameHeldPastItsCheckIsWrittenBeforeTheReset() throws Exception {
		String route = "race.data-first.test";
		RaceScript script = script(route);
		HeldWrite held = hold(Http2DemuxRelay.WriteSeam.Site.RESPONSE_DATA);
		double protocolResets = resets("protocol");
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			awaitResponseHead(client);
			script.sendData.countDown();
			Http2DemuxRelay relay = held.awaitDecided();
			assertThat(types(client.frames(STREAM))).as("decided before the frame was written")
				.containsExactly(TestH2.HEADERS);
			assertThat(relay.sinkWaiters()).as("no other write waits for the client yet").isZero();
			client.frame(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			// the outbound parse read the window update: it waits for the client stream
			// behind the held frame, or it wrote its RST_STREAM past the frame
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkWaiters() > 0 || client.reset(STREAM) != null);
			boolean waited = relay.sinkWaiters() > 0;
			held.proceed();
			assertThat(client.awaitTrue(() -> client.reset(STREAM) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(script.fenced.await(5, TimeUnit.SECONDS)).as("the inbound side wrote the frame").isTrue();
			// whatever the relay wrote before acking the upstream's PING reached the
			// client
			awaitPingAck(client);
			assertThat(types(client.frames(STREAM))).as("the frame, then the reset")
				.containsExactly(TestH2.HEADERS, TestH2.DATA, TestH2.RST_STREAM);
			assertThat(waited).as("the reset waited behind the held frame").isTrue();
			assertThat(client.reset(STREAM)).isEqualTo(PROTOCOL_ERROR);
			assertThat(resets("protocol") - protocolResets).isEqualTo(1);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * The relay marked the stream reset on the client's WINDOW_UPDATE and is held before
	 * writing its RST_STREAM when the upstream's DATA frame on the stream arrives: the
	 * leg's inbound side waits for the client stream behind the held reset, and drops the
	 * frame once it is let go -- the client reads the RST_STREAM and nothing after it.
	 * The leg writer writes the reset's CANCEL meanwhile, so the stream may no longer be
	 * tracked when the frame is read; a frame read while it still is, its CANCEL held by
	 * a stalled upstream, is {@code H2DemuxBudgetE2ETest}'s.
	 */
	@Test
	void aResponseFrameReadWhileTheResetIsHeldNeverReachesTheClient() throws Exception {
		String route = "race.reset-first.test";
		RaceScript script = script(route);
		HeldWrite held = hold(Http2DemuxRelay.WriteSeam.Site.RESET);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			awaitResponseHead(client);
			client.frame(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			Http2DemuxRelay relay = held.awaitDecided();
			assertThat(client.reset(STREAM)).as("decided before the RST_STREAM was written").isNull();
			assertThat(relay.sinkWaiters()).as("no other write waits for the client yet").isZero();
			script.sendData.countDown();
			// the inbound side read the frame: it waits for the client stream behind the
			// held reset, or it is past the frame (the PING behind it was acked)
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkWaiters() > 0 || script.fenced.getCount() == 0);
			boolean waited = relay.sinkWaiters() > 0;
			held.proceed();
			assertThat(script.fenced.await(5, TimeUnit.SECONDS)).as("the inbound side read past the frame").isTrue();
			// whatever the relay wrote before acking the upstream's PING reached the
			// client
			awaitPingAck(client);
			assertThat(types(client.frames(STREAM))).as("the reset, nothing after it")
				.containsExactly(TestH2.HEADERS, TestH2.RST_STREAM);
			assertThat(waited).as("the frame waited behind the held reset").isTrue();
			assertThat(client.reset(STREAM)).isEqualTo(PROTOCOL_ERROR);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * The leg writer wrote the stream's head and is held before writing the stream window
	 * grant it computed when the client's WINDOW_UPDATE resets the stream: the reset
	 * waits for the client stream behind the held grant, and the client reads the grant,
	 * then the RST_STREAM.
	 */
	@Test
	void aStreamGrantHeldPastItsCheckIsWrittenBeforeTheReset() throws Exception {
		String route = "race.drain-grant.test";
		GrantScript script = grantScript(route, false);
		HeldWrite held = hold(Http2DemuxRelay.WriteSeam.Site.DRAIN_GRANT);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			Http2DemuxRelay relay = held.awaitDecided();
			assertThat(client.frames(STREAM)).as("decided before the grant was written").isEmpty();
			assertThat(relay.sinkWaiters()).as("no other write waits for the client yet").isZero();
			client.frame(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkWaiters() > 0 || client.reset(STREAM) != null);
			boolean waited = relay.sinkWaiters() > 0;
			held.proceed();
			// the leg writer writes the reset's CANCEL to the upstream after the grant
			assertThat(script.cancelled.await(5, TimeUnit.SECONDS)).as("the upstream read the CANCEL").isTrue();
			awaitPingAck(client);
			assertThat(client.frames(STREAM)).extracting(TestH2.Frame::type)
				.as("the grant, then the reset")
				.containsExactly(TestH2.WINDOW_UPDATE, TestH2.RST_STREAM);
			assertThat(client.streamGranted(STREAM)).isEqualTo(HEAD_GRANT);
			assertThat(waited).as("the reset waited behind the held grant").isTrue();
			assertThat(client.reset(STREAM)).isEqualTo(PROTOCOL_ERROR);
		}
	}

	/**
	 * A stream window grant the leg writer computed as it drained the stream's DATA waits
	 * for the client stream behind the reset of the stream, itself waiting behind a write
	 * held on another route's stream: once the held write goes on, the RST_STREAM is
	 * written and the grant dropped -- no WINDOW_UPDATE follows the reset (RFC 9113 5.1).
	 */
	@Test
	void aStreamGrantWaitingBehindTheResetIsDropped() throws Exception {
		String route = "race.grant-dropped.test";
		String otherRoute = "race.grant-holder.test";
		int other = STREAM + 2;
		GrantScript script = grantScript(route, true);
		grantScript(otherRoute, false);
		HeldWrite held = HeldWrite.install(dataProxyServer(),
				new Http2DemuxRelay.WriteSeam.Decision(Http2DemuxRelay.WriteSeam.Site.DRAIN_GRANT, other));
		this.held.add(held);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			// the response head follows the upstream's SETTINGS: its window of 0 applied
			assertThat(client.awaitTrue(() -> types(client.frames(STREAM)).equals(List.of(TestH2.HEADERS))
					&& client.streamGranted(STREAM) == HEAD_GRANT, Duration.ofSeconds(5)))
				.as("the response head and the grant for the written head arrived")
				.isTrue();
			long granted = client.connectionGranted();
			client.sendData(STREAM, new byte[1000], 0);
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= granted + 1000, Duration.ofSeconds(5)))
				.as("the outbound parse is done with the DATA")
				.isTrue();
			assertThat(script.dataReceived.get()).as("the DATA waits for the upstream's window").isZero();
			client.headers(other, false, post(otherRoute));
			Http2DemuxRelay relay = held.awaitDecided();
			client.frame(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> relay.sinkWaiters() == 1);
			// the upstream opens its window: the leg writer drains the DATA, computes the
			// grant, and waits for the client stream behind the reset
			script.send(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(1000));
			assertThat(script.awaitData(1000)).as("the DATA drained").isTrue();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> relay.sinkWaiters() == 2);
			List<TestH2.Frame> before = client.frames(STREAM);
			assertThat(client.reset(STREAM)).as("the reset waits too").isNull();
			held.proceed();
			assertThat(script.cancelled.await(5, TimeUnit.SECONDS)).as("the upstream read the CANCEL").isTrue();
			awaitPingAck(client);
			List<TestH2.Frame> frames = client.frames(STREAM);
			assertThat(frames.subList(before.size(), frames.size())).extracting(TestH2.Frame::type)
				.as("the reset, no grant after it")
				.containsExactly(TestH2.RST_STREAM);
			assertThat(client.streamGranted(STREAM)).as("the drain's grant dropped").isEqualTo(HEAD_GRANT);
			assertThat(client.reset(STREAM)).isEqualTo(PROTOCOL_ERROR);
		}
	}

	/**
	 * The outbound parse computed the stream window grant of the padding of a client DATA
	 * frame and is held before writing it when the upstream resets the stream: the
	 * relayed reset waits for the client stream behind the held grant, and the client
	 * reads the grant, then the RST_STREAM. The upstream's window of 0 keeps the leg
	 * writer from draining the DATA, and granting.
	 */
	@Test
	void aStreamGrantHeldPastItsCheckIsWrittenBeforeAnUpstreamReset() throws Exception {
		String route = "race.data-grant.test";
		GrantScript script = grantScript(route, true);
		HeldWrite held = hold(Http2DemuxRelay.WriteSeam.Site.DATA_GRANT);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			// the response head follows the upstream's SETTINGS: its window of 0 applied
			assertThat(client.awaitTrue(() -> types(client.frames(STREAM)).equals(List.of(TestH2.HEADERS))
					&& client.streamGranted(STREAM) == HEAD_GRANT, Duration.ofSeconds(5)))
				.as("the response head and the grant for the written head arrived")
				.isTrue();
			int padding = 100;
			byte[] padded = new byte[1 + 10 + padding];
			padded[0] = (byte) padding;
			client.sendData(STREAM, padded, TestH2.FLAG_PADDED);
			Http2DemuxRelay relay = held.awaitDecided();
			List<TestH2.Frame> before = client.frames(STREAM);
			assertThat(relay.sinkWaiters()).as("no other write waits for the client yet").isZero();
			script.send(TestH2.RST_STREAM, 0, STREAM, TestH2.int32(CANCEL));
			script.send(TestH2.PING, 0, 0, GrantScript.FENCE);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkWaiters() > 0 || client.reset(STREAM) != null);
			boolean waited = relay.sinkWaiters() > 0;
			held.proceed();
			assertThat(script.fenced.await(5, TimeUnit.SECONDS)).as("the inbound side read past the reset").isTrue();
			awaitPingAck(client);
			List<TestH2.Frame> frames = client.frames(STREAM);
			assertThat(frames.subList(before.size(), frames.size())).extracting(TestH2.Frame::type)
				.as("the grant, then the reset")
				.containsExactly(TestH2.WINDOW_UPDATE, TestH2.RST_STREAM);
			assertThat(client.streamGranted(STREAM)).as("the padding given back").isEqualTo(HEAD_GRANT + 1 + padding);
			assertThat(waited).as("the reset waited behind the held grant").isTrue();
			assertThat(client.reset(STREAM)).isEqualTo(CANCEL);
			assertThat(script.dataReceived.get()).as("the DATA never drained").isZero();
		}
	}

	/**
	 * The upstream's stream error, decided by the leg's inbound side: the outbound parse
	 * computed the stream window grant of the padding of a client DATA frame and is held
	 * before writing it when the upstream sends a WINDOW_UPDATE of 0 on the stream (RFC
	 * 9113 6.9): the reset waits for the client stream behind the held grant, and the
	 * client reads the grant, then the RST_STREAM {@code INTERNAL_ERROR} (a response head
	 * was relayed); the upstream reads RST_STREAM {@code PROTOCOL_ERROR}.
	 */
	@Test
	void aStreamGrantHeldPastItsCheckIsWrittenBeforeAnUpstreamStreamError() throws Exception {
		String route = "race.upstream-error-grant.test";
		GrantScript script = grantScript(route, true);
		HeldWrite held = hold(Http2DemuxRelay.WriteSeam.Site.DATA_GRANT);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			assertThat(client.awaitTrue(() -> types(client.frames(STREAM)).equals(List.of(TestH2.HEADERS))
					&& client.streamGranted(STREAM) == HEAD_GRANT, Duration.ofSeconds(5)))
				.as("the response head and the grant for the written head arrived")
				.isTrue();
			int padding = 100;
			byte[] padded = new byte[1 + 10 + padding];
			padded[0] = (byte) padding;
			client.sendData(STREAM, padded, TestH2.FLAG_PADDED);
			Http2DemuxRelay relay = held.awaitDecided();
			List<TestH2.Frame> before = client.frames(STREAM);
			assertThat(relay.sinkWaiters()).as("no other write waits for the client yet").isZero();
			script.send(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.pollInterval(Duration.ofMillis(1))
				.until(() -> relay.sinkWaiters() > 0 || client.reset(STREAM) != null);
			boolean waited = relay.sinkWaiters() > 0;
			held.proceed();
			assertThat(script.cancelled.await(5, TimeUnit.SECONDS)).as("the upstream read the reset").isTrue();
			awaitPingAck(client);
			List<TestH2.Frame> frames = client.frames(STREAM);
			assertThat(frames.subList(before.size(), frames.size())).extracting(TestH2.Frame::type)
				.as("the grant, then the reset")
				.containsExactly(TestH2.WINDOW_UPDATE, TestH2.RST_STREAM);
			assertThat(client.streamGranted(STREAM)).as("the padding given back").isEqualTo(HEAD_GRANT + 1 + padding);
			assertThat(waited).as("the reset waited behind the held grant").isTrue();
			assertThat(client.reset(STREAM)).isEqualTo(INTERNAL_ERROR);
			assertThat(script.resetCode.get()).isEqualTo(PROTOCOL_ERROR);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * The upstream's stream error waits for the client stream, not marked yet, behind a
	 * write held on another route's stream, and so does the outbound parse with the
	 * stream window grant it computed for the padding of a client DATA frame: once the
	 * held write goes on, the RST_STREAM is written and the grant dropped -- no
	 * WINDOW_UPDATE follows the reset (RFC 9113 5.1).
	 */
	@Test
	void aStreamGrantWaitingBehindAnUpstreamStreamErrorIsDropped() throws Exception {
		String route = "race.upstream-error-dropped.test";
		String otherRoute = "race.upstream-error-holder.test";
		int other = STREAM + 2;
		GrantScript script = grantScript(route, true);
		grantScript(otherRoute, false);
		HeldWrite held = HeldWrite.install(dataProxyServer(),
				new Http2DemuxRelay.WriteSeam.Decision(Http2DemuxRelay.WriteSeam.Site.DRAIN_GRANT, other));
		this.held.add(held);
		try (H2TestClient client = H2TestClient.connect(dataPort, STREAM)) {
			client.headers(STREAM, false, post(route));
			assertThat(client.awaitTrue(() -> types(client.frames(STREAM)).equals(List.of(TestH2.HEADERS))
					&& client.streamGranted(STREAM) == HEAD_GRANT, Duration.ofSeconds(5)))
				.as("the response head and the grant for the written head arrived")
				.isTrue();
			client.headers(other, false, post(otherRoute));
			Http2DemuxRelay relay = held.awaitDecided();
			script.send(TestH2.WINDOW_UPDATE, 0, STREAM, TestH2.int32(0));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> relay.sinkWaiters() == 1);
			byte[] padded = new byte[1 + 10 + 100];
			padded[0] = (byte) 100;
			client.sendData(STREAM, padded, TestH2.FLAG_PADDED);
			// the outbound parse computed the padding's grant, and waits behind the reset
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> relay.sinkWaiters() == 2);
			List<TestH2.Frame> before = client.frames(STREAM);
			assertThat(client.reset(STREAM)).as("the reset waits too").isNull();
			held.proceed();
			assertThat(script.cancelled.await(5, TimeUnit.SECONDS)).as("the upstream read the reset").isTrue();
			awaitPingAck(client);
			List<TestH2.Frame> frames = client.frames(STREAM);
			assertThat(frames.subList(before.size(), frames.size())).extracting(TestH2.Frame::type)
				.as("the reset, no grant after it")
				.containsExactly(TestH2.RST_STREAM);
			assertThat(client.streamGranted(STREAM)).as("the padding's grant dropped").isEqualTo(HEAD_GRANT);
			assertThat(client.reset(STREAM)).isEqualTo(INTERNAL_ERROR);
			assertThat(script.resetCode.get()).isEqualTo(PROTOCOL_ERROR);
		}
	}

	/** Starts a grant-scripted upstream for the route. */
	private GrantScript grantScript(String route, boolean respond) {
		GrantScript script = new GrantScript(respond);
		this.releases.add(script.leave);
		startClient(route, stub(script::serve));
		return script;
	}

	/** Starts a scripted upstream for the route. */
	private RaceScript script(String route) {
		RaceScript script = new RaceScript();
		this.releases.add(script.sendData);
		this.releases.add(script.leave);
		startClient(route, stub(script::serve));
		return script;
	}

	/**
	 * Holds the first write decided at the site on {@link #STREAM} of any relay started
	 * from now on.
	 */
	private HeldWrite hold(Http2DemuxRelay.WriteSeam.Site site) {
		HeldWrite held = HeldWrite.install(dataProxyServer(), new Http2DemuxRelay.WriteSeam.Decision(site, STREAM));
		this.held.add(held);
		return held;
	}

	/**
	 * An upstream answering {@link #STREAM} with a response head once its request head
	 * arrived, then -- once {@link #sendData} opens -- with a DATA frame not ending the
	 * stream followed by a PING whose ack ({@link #fenced}) tells the relay's inbound
	 * side read past the frame; it leaves once {@link #leave} opens.
	 */
	private static final class RaceScript {

		private static final byte[] FENCE = "fence-01".getBytes(StandardCharsets.US_ASCII);

		private final CountDownLatch head = new CountDownLatch(1);

		private final CountDownLatch sendData = new CountDownLatch(1);

		private final CountDownLatch fenced = new CountDownLatch(1);

		private final CountDownLatch leave = new CountDownLatch(1);

		void serve(Socket socket) {
			try (socket) {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
					return;
				}
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
				Thread.ofVirtual().name("e2e-race-h2-reader").start(() -> read(in));
				this.head.await();
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, STREAM, new byte[] { (byte) 0x88 });
				this.sendData.await();
				TestH2.writeFrame(out, TestH2.DATA, 0, STREAM, "hello".getBytes(StandardCharsets.US_ASCII));
				TestH2.writeFrame(out, TestH2.PING, 0, 0, FENCE);
				this.leave.await();
			}
			catch (IOException e) {
				// connection ended
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private void read(InputStream in) {
			try {
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					if (frame.type() == TestH2.HEADERS && frame.streamId() == STREAM) {
						this.head.countDown();
					}
					else if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0
							&& Arrays.equals(frame.payload(), FENCE)) {
						this.fenced.countDown();
					}
				}
			}
			catch (IOException e) {
				// connection ended
			}
		}

	}

	/**
	 * An upstream announcing a stream window of 0, reading everything, and answering the
	 * head of {@link #STREAM} with a response head when told to; it writes what the test
	 * {@link #send sends}, and leaves once {@link #leave} opens. {@link #cancelled} opens
	 * on the relay's RST_STREAM of {@link #STREAM}, {@link #fenced} on the ack of a PING
	 * carrying {@link #FENCE}.
	 */
	private static final class GrantScript {

		static final byte[] FENCE = "fence-02".getBytes(StandardCharsets.US_ASCII);

		/** SETTINGS_INITIAL_WINDOW_SIZE of 0. */
		private static final byte[] ZERO_WINDOW = { 0, 0x4, 0, 0, 0, 0 };

		private final boolean respond;

		private final BlockingQueue<byte[]> writes = new LinkedBlockingQueue<>();

		private final AtomicLong dataReceived = new AtomicLong();

		private final CountDownLatch cancelled = new CountDownLatch(1);

		/** The error code of the relay's RST_STREAM of {@link #STREAM}; -1 before it. */
		private final AtomicLong resetCode = new AtomicLong(-1);

		private final CountDownLatch fenced = new CountDownLatch(1);

		private final CountDownLatch leave = new CountDownLatch(1);

		GrantScript(boolean respond) {
			this.respond = respond;
		}

		/** Has the upstream write the frame. */
		void send(int type, int flags, int streamId, byte[] payload) throws IOException {
			ByteArrayOutputStream frame = new ByteArrayOutputStream();
			TestH2.writeFrame(frame, type, flags, streamId, payload);
			this.writes.add(frame.toByteArray());
		}

		/** Waits until the upstream received the DATA payload bytes. */
		boolean awaitData(long bytes) throws InterruptedException {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (this.dataReceived.get() < bytes) {
				if (System.nanoTime() > deadline) {
					return false;
				}
				Thread.sleep(1);
			}
			return true;
		}

		void serve(Socket socket) {
			try (socket) {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
					return;
				}
				TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, ZERO_WINDOW);
				Thread.ofVirtual().name("e2e-race-h2-reader").start(() -> read(in));
				while (this.leave.getCount() > 0) {
					byte[] bytes = this.writes.poll(10, TimeUnit.MILLISECONDS);
					if (bytes != null) {
						out.write(bytes);
						out.flush();
					}
				}
			}
			catch (IOException e) {
				// connection ended
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private void read(InputStream in) {
			try {
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					if (frame.type() == TestH2.HEADERS && frame.streamId() == STREAM && this.respond) {
						send(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, STREAM, new byte[] { (byte) 0x88 });
					}
					else if (frame.type() == TestH2.DATA) {
						this.dataReceived.addAndGet(frame.payload().length);
					}
					else if (frame.type() == TestH2.RST_STREAM && frame.streamId() == STREAM) {
						this.resetCode.compareAndSet(-1, TestH2.errorCode(frame));
						this.cancelled.countDown();
					}
					else if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0
							&& Arrays.equals(frame.payload(), FENCE)) {
						this.fenced.countDown();
					}
				}
			}
			catch (IOException e) {
				// connection ended
			}
		}

	}

	private static void awaitResponseHead(H2TestClient client) throws InterruptedException {
		assertThat(client.awaitTrue(() -> types(client.frames(STREAM)).equals(List.of(TestH2.HEADERS)),
				Duration.ofSeconds(5)))
			.as("the response head reached the client")
			.isTrue();
	}

	private static void awaitPingAck(H2TestClient client) throws Exception {
		byte[] ping = Arrays.copyOf(TestH2.int32((int) PINGS.incrementAndGet()), 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)),
				Duration.ofSeconds(5)))
			.as("the relay acked the PING")
			.isTrue();
	}

	/** The types of the frames, the relay's own stream window grants left out. */
	private static List<Integer> types(List<TestH2.Frame> frames) {
		return frames.stream().map(TestH2.Frame::type).filter(type -> type != TestH2.WINDOW_UPDATE).toList();
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private double resets(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-race-h2").start(task))
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
			Thread.ofVirtual().name("e2e-race-h2-accept").start(() -> {
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
