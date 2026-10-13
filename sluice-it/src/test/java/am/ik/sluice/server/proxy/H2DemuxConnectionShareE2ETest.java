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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one connection may take of the queue budget the h2 demux relays share while the
 * budget has room: past its share its windows are withheld, so another connection keeps
 * its own; its header blocks are admitted up to twice the share, and its waiting streams
 * are refused past its allowance as under an exhausted budget. In the relay's package for
 * the relay's constants and the budget's waiters.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxConnectionShareE2ETest {

	/**
	 * Far below what the uploads of one connection would hold without a share (twenty
	 * stream windows), far above what the connections of a test hold with it.
	 */
	private static final long BUDGET = 12L * 1024 * 1024;

	/** Upload streams per connection: their stream windows add up past the budget. */
	private static final int STREAMS = 20;

	/** The stream window the relay announces: each upload fills one. */
	private static final int BODY = NettyHttp2DemuxRelay.STREAM_WINDOW;

	/**
	 * What each of the two uploads of the connection that keeps its windows sends: both
	 * held, past its allowance, within its share.
	 */
	private static final int OTHER_BODY = 768 * 1024;

	/** The padding of a large request head, within the 64KiB header list limit. */
	private static final int PAD = 56 * 1024;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	/** What a client reading slowly takes at a time. */
	private static final int GRANT_STEP = 256 * 1024;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

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

	@Nullable
	@Autowired
	private ConnectionLimits limits;

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
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeEach
	void awaitBudgetEmpty() {
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> assertThat(budgetUsed()).as("budget used").isZero());
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxConnectionShareE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * A connection uploading twenty stream windows to an upstream that does not read
	 * stops at its share -- its window withheld while the budget has room -- and another
	 * connection, past its allowance, keeps its windows: its uploads, which its upstream
	 * holds (a stream window of 0), go out whole and nothing more is withheld. Once the
	 * first upstream reads, its uploads complete; the other connection goes away; every
	 * byte of the budget is released.
	 */
	@Test
	void aConnectionPastItsShareIsWithheldWhileAnotherKeepsItsWindows() throws Exception {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		String heldRoute = "share.held.test";
		String otherRoute = "share.other.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(heldRoute, stub(socket -> serveGated(socket, gate, STREAMS, 0)));
		startClient(otherRoute, stub(H2DemuxConnectionShareE2ETest::holdRequests));
		double shareBefore = withheld("share");
		double budgetBefore = withheld("budget");
		try (Pump held = pump(heldRoute, 0, STREAMS)) {
			held.awaitWithheld();
			long heldQueued = awaitStable(() -> queuedBytes(heldRoute));
			assertThat(budgetUsed()).as("the budget has room").isLessThan(BUDGET);
			assertThat(held.client().connectionWindow()).as("its window withheld").isZero();
			assertThat(withheld("share")).as("withheld for its share").isEqualTo(shareBefore + 1);
			assertThat(withheld("budget")).as("not for the budget").isEqualTo(budgetBefore);
			assertThat(heldQueued).as("held: its allowance and share, plus what its client still sent")
				.isGreaterThanOrEqualTo(
						NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + NettyHttp2DemuxRelay.CONNECTION_SHARE)
				.isLessThanOrEqualTo(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + NettyHttp2DemuxRelay.CONNECTION_SHARE
						+ NettyHttp2DemuxRelay.WITHHELD_ARRIVALS);
			try (Pump other = pump(otherRoute, 0, 2, OTHER_BODY)) {
				other.awaitSent();
				long otherQueued = awaitStable(() -> queuedBytes(otherRoute));
				assertThat(otherQueued)
					.as("the other connection holds its uploads: past its allowance, within its share")
					// but what reached the upstream within its initial window, before its
					// SETTINGS
					.isGreaterThanOrEqualTo(2L * (OTHER_BODY - WindowedH2Upstream.DEFAULT_WINDOW))
					.isLessThan(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + NettyHttp2DemuxRelay.CONNECTION_SHARE);
				assertThat(budgetUsed()).as("the budget still has room").isLessThan(BUDGET);
				assertThat(withheld("share")).as("the other connection kept its windows").isEqualTo(shareBefore + 1);
				assertThat(withheld("budget")).isEqualTo(budgetBefore);
				gate.countDown();
				held.awaitEnds();
			}
		}
		finally {
			gate.countDown();
		}
		awaitReleased(heldRoute);
		awaitReleased(otherRoute);
	}

	/**
	 * A client that stops reading -- it grants 24 streams 1MiB each but no connection
	 * window -- while its upstream answers each with 384KiB: the relay holds the
	 * responses until its share is reached, then withholds the upstream connection's
	 * window too, so the upstream stops at the window it holds, the budget having room.
	 * The client taking some of it leaves the relay below its share, though past its
	 * allowance: the window is granted back and the upstream sends again. Once the client
	 * reads, every response completes and the budget is released.
	 */
	@Test
	void aClientNotReadingStopsItsUpstreamAtItsShare() throws Exception {
		String route = "share.download.test";
		int streams = 24;
		int length = 384 * 1024;
		AtomicReference<@Nullable WindowedH2Upstream> upstream = new AtomicReference<>();
		startClient(route, stub(socket -> serveResponses(socket, length, upstream)));
		double shareBefore = withheld("share");
		Integer[] ids = new Integer[streams];
		for (int i = 0; i < streams; i++) {
			ids[i] = streamId(i);
		}
		try (H2TestClient client = H2TestClient.connect(dataPort, ids)) {
			client.withholdConnectionWindow();
			client.frame(TestH2.SETTINGS, 0, 0,
					WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, BODY));
			for (int i = 0; i < streams; i++) {
				client.headers(streamId(i), true, head(route, "GET", "/" + i));
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> withheld("share") > shareBefore);
			long held = awaitStable(() -> queuedBytes(route));
			assertThat(client.dataReceived()).as("the client got its connection window only")
				.isEqualTo(WindowedH2Upstream.DEFAULT_WINDOW);
			assertThat(budgetUsed()).as("the budget has room").isLessThan(BUDGET);
			assertThat(withheld("share")).as("withheld for its share").isEqualTo(shareBefore + 1);
			assertThat(held).as("held: its allowance and share, plus what the upstream connection still sent")
				.isGreaterThanOrEqualTo(
						NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + NettyHttp2DemuxRelay.CONNECTION_SHARE)
				.isLessThanOrEqualTo(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + NettyHttp2DemuxRelay.CONNECTION_SHARE
						+ NettyHttp2DemuxRelay.RESPONSE_QUEUE_BYTES);
			WindowedH2Upstream leg = Objects.requireNonNull(upstream.get());
			long sent = leg.sent();
			// the client takes 256KiB at a time until the upstream sends again; a DATA
			// write netty holds counts whole until it completed, so the relay's account
			// drops below its share later than what it holds does
			long taken = 0;
			while (leg.sent() == sent && taken < sent) {
				taken += GRANT_STEP;
				long expected = WindowedH2Upstream.DEFAULT_WINDOW + taken;
				client.frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(GRANT_STEP));
				Awaitility.await().atMost(TIMEOUT).until(() -> client.dataReceived() >= expected);
				Thread.sleep(100);
			}
			assertThat(leg.sent()).as("the upstream sent again").isGreaterThan(sent);
			assertThat(sent - WindowedH2Upstream.DEFAULT_WINDOW - taken)
				.as("what the relay held at least when it granted the window back: past the allowance")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
			client.grantConnectionWindowOnReceipt();
			for (int i = 0; i < streams; i++) {
				assertThat(client.awaitEnd(streamId(i), TIMEOUT)).as("stream %d completes", streamId(i)).isTrue();
				assertThat(TestH2.contentOf(client.frames(streamId(i)))).hasSize(length);
			}
			assertThat(client.closed()).isFalse();
		}
		awaitReleased(route);
	}

	/**
	 * Past its share, a connection's header blocks are still admitted while its share
	 * stays within twice the share: a new stream is relayed, trailers end their stream.
	 * Heads held for an upstream that does not read take it past twice the share: the
	 * next new stream is refused (REFUSED_STREAM) and the next trailers reset their
	 * stream (ENHANCE_YOUR_CALM), the budget having room throughout and the connection
	 * staying up. Once the upstream reads, every stream admitted completes.
	 */
	@Test
	void headerBlocksPastTheShareAreAdmittedUpToTwiceTheShare() throws Exception {
		String route = "share.blocks.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, STREAMS + 2, 0)));
		int admittedTrailers = streamId(STREAMS);
		int resetTrailers = streamId(STREAMS + 1);
		double refusedBefore = refused("share");
		double resetsBefore = resets("share");
		try (Pump pump = new Pump(H2TestClient.connect(dataPort), 0, STREAMS, BODY)) {
			H2TestClient client = pump.client();
			pump.open(route);
			client.headers(admittedTrailers, false, head(route, "POST", "/admitted"));
			client.headers(resetTrailers, false, head(route, "POST", "/reset"));
			pump.start();
			pump.awaitWithheld();
			awaitStable(() -> queuedBytes(route));
			assertThat(budgetUsed()).as("the budget has room").isLessThan(BUDGET);
			assertThat(budgetShare(route)).as("past its share, within twice it")
				.isGreaterThanOrEqualTo(NettyHttp2DemuxRelay.CONNECTION_SHARE)
				.isLessThan(NettyHttp2DemuxRelay.BLOCK_SHARE - PAD);
			int next = streamId(STREAMS + 2);
			int admitted = next;
			long tracked = trackedStreams(route);
			client.headers(admitted, true, head(route, "GET", "/new"));
			client.headers(admittedTrailers, true, TestH2.headerBlock("x-trailer=1"));
			awaitPingAck(client, 1);
			assertThat(client.resets()).as("a new stream and trailers admitted").isEmpty();
			assertThat(trackedStreams(route)).as("the new stream relayed").isEqualTo(tracked + 1);
			int refused = -1;
			for (int i = 1; i < 80 && refused == -1; i++) {
				int id = next + 2 * i;
				client.headers(id, true, padded(route, "/pile" + i));
				awaitPingAck(client, 1 + i);
				if (client.reset(id) != null) {
					refused = id;
				}
			}
			assertThat(refused).as("a head past twice the share refused").isPositive();
			assertThat(client.reset(refused)).isEqualTo(TestH2.REFUSED_STREAM);
			assertThat(client.resets()).as("only that stream").hasSize(1);
			assertThat(refused("share")).isEqualTo(refusedBefore + 1);
			// the route's gauge follows the relay's action
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(budgetShare(route)).as("its share at the refusal")
					.isGreaterThan(NettyHttp2DemuxRelay.BLOCK_SHARE - PAD - 4096)
					.isLessThanOrEqualTo(NettyHttp2DemuxRelay.BLOCK_SHARE));
			assertThat(budgetUsed()).as("the budget still has room").isLessThan(BUDGET);
			client.headers(resetTrailers, true, padded(TestH2.headerBlock("x-trailer=1")));
			assertThat(client.awaitTrue(() -> client.reset(resetTrailers) != null, TIMEOUT)).isTrue();
			assertThat(client.reset(resetTrailers)).isEqualTo(ENHANCE_YOUR_CALM);
			assertThat(resets("share")).isEqualTo(resetsBefore + 1);
			assertThat(client.closed()).isFalse();
			gate.countDown();
			pump.awaitEnds();
			for (int id = next; id < refused; id += 2) {
				assertThat(client.awaitEnd(id, TIMEOUT)).as("stream %d admitted completes", id).isTrue();
			}
			assertThat(client.awaitEnd(admittedTrailers, TIMEOUT)).as("the trailers' stream completes").isTrue();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
		finally {
			gate.countDown();
		}
		awaitReleased(route);
	}

	/**
	 * Past its share, a connection keeps the streams waiting for its upstream's
	 * concurrency limit within its allowance, as under an exhausted budget: of three
	 * large heads waiting, the most recent is refused (REFUSED_STREAM), the others are
	 * relayed once the upstream's streams close.
	 */
	@Test
	void waitingStreamsPastTheShareAreKeptWithinTheAllowance() throws Exception {
		String route = "share.waiting.test";
		CountDownLatch gate = new CountDownLatch(1);
		startClient(route, stub(socket -> serveGated(socket, gate, STREAMS, STREAMS)));
		double refusedBefore = refused("waiting");
		try (Pump pump = pump(route, 0, STREAMS)) {
			H2TestClient client = pump.client();
			pump.awaitWithheld();
			assertThat(budgetUsed()).as("the budget has room").isLessThan(BUDGET);
			assertThat(budgetShare(route)).as("past its share")
				.isGreaterThanOrEqualTo(NettyHttp2DemuxRelay.CONNECTION_SHARE);
			int first = streamId(STREAMS);
			int last = streamId(STREAMS + 2);
			for (int id = first; id <= last; id += 2) {
				client.headers(id, true, padded(route, "/wait" + id));
			}
			assertThat(client.awaitTrue(() -> client.reset(last) != null, TIMEOUT)).as("the most recent refused")
				.isTrue();
			assertThat(client.reset(last)).isEqualTo(TestH2.REFUSED_STREAM);
			awaitPingAck(client, 1);
			assertThat(client.resets()).as("only the most recent").hasSize(1);
			assertThat(refused("waiting")).isEqualTo(refusedBefore + 1);
			gate.countDown();
			pump.awaitEnds();
			for (int id = first; id < last; id += 2) {
				assertThat(client.awaitEnd(id, TIMEOUT)).as("waiting stream %d completes", id).isTrue();
			}
		}
		finally {
			gate.countDown();
		}
		awaitReleased(route);
	}

	private Pump pump(String route, int first, int streams) throws IOException {
		return pump(route, first, streams, BODY);
	}

	private Pump pump(String route, int first, int streams, int body) throws IOException {
		Pump pump = new Pump(H2TestClient.connect(dataPort), first, streams, body);
		pump.open(route);
		pump.start();
		return pump;
	}

	private static int streamId(int index) {
		return 1 + 2 * index;
	}

	private static byte[] head(String route, String method, String path) {
		return TestH2.headerBlock(":authority=" + route, ":method=" + method, ":path=" + path);
	}

	/** A GET head padded with {@link #PAD} bytes, as HPACK counts them. */
	private static byte[] padded(String route, String path) {
		return padded(head(route, "GET", path));
	}

	/** The header block padded with {@link #PAD} bytes, as HPACK counts them. */
	private static byte[] padded(byte[] headers) {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		block.writeBytes(headers);
		// literal without indexing, no huffman, lengths as HPACK integers (RFC 7541 5.1)
		block.write(0x00);
		writeLength(block, 5);
		block.writeBytes("x-pad".getBytes(StandardCharsets.US_ASCII));
		writeLength(block, PAD);
		block.writeBytes("p".repeat(PAD).getBytes(StandardCharsets.US_ASCII));
		return block.toByteArray();
	}

	private static void writeLength(ByteArrayOutputStream out, int length) {
		if (length < 127) {
			out.write(length);
			return;
		}
		out.write(127);
		int rest = length - 127;
		while (rest >= 128) {
			out.write((rest & 0x7f) | 0x80);
			rest >>>= 7;
		}
		out.write(rest);
	}

	/**
	 * Pings the relay with payload {@code n} and waits for the ack: the relay handled
	 * every frame sent before.
	 */
	private static void awaitPingAck(H2TestClient client, long n) throws Exception {
		byte[] ping = new byte[8];
		long rest = n;
		for (int i = 7; i >= 0; i--) {
			ping[i] = (byte) rest;
			rest >>>= 8;
		}
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(
				client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)), TIMEOUT))
			.as("the relay acked the PING")
			.isTrue();
	}

	/** One connection and the thread pumping its request bodies. */
	private static final class Pump implements AutoCloseable {

		private final H2TestClient client;

		private final int first;

		private final long[] remaining;

		private final AtomicBoolean stop = new AtomicBoolean();

		private final AtomicBoolean sent = new AtomicBoolean();

		private final AtomicLong idleSince = new AtomicLong();

		Pump(H2TestClient client, int first, int streams, int body) {
			this.client = client;
			this.first = first;
			this.remaining = new long[streams];
			Arrays.fill(this.remaining, body);
		}

		H2TestClient client() {
			return this.client;
		}

		void open(String route) throws IOException {
			for (int i = 0; i < this.remaining.length; i++) {
				this.client.headers(id(i), false, head(route, "POST", "/" + i));
			}
		}

		private int id(int index) {
			return streamId(this.first + index);
		}

		void start() {
			Thread.ofVirtual().name("e2e-share-pump").start(this::pump);
		}

		private void pump() {
			try {
				while (!this.stop.get() && !this.client.closed()) {
					boolean progress = false;
					boolean done = true;
					for (int i = 0; i < this.remaining.length; i++) {
						if (this.remaining[i] == 0) {
							continue;
						}
						done = false;
						int id = id(i);
						long window = Math.min(this.client.sendable(id), this.client.connectionWindow());
						int n = (int) Math.min(Math.min(this.remaining[i], window), MAX_FRAME_SIZE);
						if (n > 0) {
							this.remaining[i] -= n;
							this.client.sendData(id, new byte[n], this.remaining[i] == 0 ? TestH2.FLAG_END_STREAM : 0);
							progress = true;
						}
					}
					if (done) {
						this.sent.set(true);
						return;
					}
					if (progress) {
						this.idleSince.set(0);
					}
					else {
						this.idleSince.compareAndSet(0, System.nanoTime());
						Thread.sleep(1);
					}
				}
			}
			catch (IOException | InterruptedException e) {
				// torn down
			}
		}

		/** Waits until the pump got no window for half a second, bodies remaining. */
		void awaitWithheld() {
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
				long since = this.idleSince.get();
				return !this.sent.get() && since != 0 && System.nanoTime() - since > TimeUnit.MILLISECONDS.toNanos(500);
			});
		}

		/** Waits until every body went out: the windows admitted all of them. */
		void awaitSent() {
			Awaitility.await().atMost(TIMEOUT).until(this.sent::get);
		}

		/** Waits until every stream sent its body and got its response. */
		void awaitEnds() throws InterruptedException {
			for (int i = 0; i < this.remaining.length; i++) {
				int id = id(i);
				assertThat(this.client.awaitEnd(id, Duration.ofSeconds(30)))
					.as("stream %d completes (closed %s, resets %s)", id, this.client.closed(), this.client.resets())
					.isTrue();
			}
		}

		@Override
		public void close() throws IOException {
			this.stop.set(true);
			this.client.close();
		}

	}

	/**
	 * Announces at most {@code maxConcurrent} streams (none = no limit), grants the
	 * connection and every stream all the window there is, reads until {@code heads}
	 * request heads arrived, and then nothing until the gate opens; from then on reads
	 * everything and answers every ended request.
	 */
	private static void serveGated(Socket socket, CountDownLatch gate, int heads, int maxConcurrent) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			byte[] settings = new byte[0];
			if (maxConcurrent > 0) {
				settings = new byte[6];
				settings[1] = TestH2.SETTINGS_MAX_CONCURRENT_STREAMS;
				System.arraycopy(TestH2.int32(maxConcurrent), 0, settings, 2, 4);
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, settings);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			int seen = 0;
			TestH2.@Nullable Frame frame;
			while (gate.getCount() > 0 && seen < heads && (frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS) {
					seen++;
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(),
							TestH2.int32(Integer.MAX_VALUE - 65535));
				}
			}
			gate.await();
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
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

	/**
	 * Answers every request with a head and {@code length} bytes, within the windows; the
	 * upstream connection goes to {@code accepted}.
	 */
	private static void serveResponses(Socket socket, int length,
			AtomicReference<@Nullable WindowedH2Upstream> accepted) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			accepted.set(upstream);
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					int streamId = frame.streamId();
					Thread.ofVirtual().name("e2e-share-respond").start(() -> {
						try {
							upstream.headers(streamId, false);
							upstream.sendData(streamId, new byte[length], true);
						}
						catch (IOException | InterruptedException e) {
							// the relay went away
						}
					});
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** Holds every request body: a stream window of 0, read and never granted. */
	private static void holdRequests(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			upstream.settings(0);
			while (upstream.next() != null) {
				// read and dropped
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** Waits until the value has not grown for a second; returns its maximum. */
	private static long awaitStable(LongSupplier value) throws InterruptedException {
		long max = 0;
		long grownAt = System.nanoTime();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (System.nanoTime() - grownAt < TimeUnit.SECONDS.toNanos(1)) {
			assertThat(System.nanoTime()).as("the queue settles").isLessThan(deadline);
			long current = value.getAsLong();
			if (current > max) {
				max = current;
				grownAt = System.nanoTime();
			}
			Thread.sleep(10);
		}
		return max;
	}

	/** Waits until the route queues nothing and the budget is back to empty. */
	private void awaitReleased(String route) {
		Awaitility.await()
			.atMost(TIMEOUT)
			.untilAsserted(() -> assertThat(queuedBytes(route)).as("queued for the route").isZero());
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> assertThat(budgetUsed()).as("budget used").isZero());
		assertThat(Objects.requireNonNull(this.limits).h2Budget().waiting()).as("no waiter left").isZero();
	}

	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find(H2DemuxMeters.QUEUED_GAUGE_NAME)
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.sum());
	}

	/** The part of the route's queue charged to the budget: its relay's share. */
	private long budgetShare(String route) {
		return Math.max(0, queuedBytes(route) - NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE);
	}

	private long budgetGauge() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.bytes").gauge().value());
	}

	private long budgetUsed() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private double withheld(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.window.withheld")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private double refused(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.refused")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private double resets(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private long trackedStreams(String route) {
		return Objects.requireNonNull(this.dataProxyServer).h2DemuxMeters().trackedStreams(route);
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-share").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-share-accept").start(() -> {
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
