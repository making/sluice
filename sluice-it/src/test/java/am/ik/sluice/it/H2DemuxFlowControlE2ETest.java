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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The h2 demux owns the stream windows towards the client: a client honoring every window
 * and the advertised stream limit is never torn down, however many streams it fills, and
 * what the relay queues for it stops at the connection's share of the queue budget. The
 * connection window is released for every DATA byte the relay consumes, padding and
 * refused streams included.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxFlowControlE2ETest {

	/** The stream limit the relay advertises. */
	private static final int MAX_CONCURRENT_STREAMS = 100;

	/**
	 * The stream window the relay announces on both of its faces, and the connection
	 * window it grants on both: {@code NettyHttp2DemuxRelay.STREAM_WINDOW} and
	 * {@code CONNECTION_WINDOW}.
	 */
	private static final int NETTY_STREAM_WINDOW = 1024 * 1024;

	/** The connection window the relay grants on both faces. */
	private static final int NETTY_CONNECTION_WINDOW = 1024 * 1024;

	/** What the relay grants its client's connection window behind its preface. */
	private static final int CONNECTION_WINDOW_RAISE = NETTY_CONNECTION_WINDOW - H2TestClient.INITIAL_WINDOW;

	/** The backlog rounding of a stream's buffer, less than one granule. */
	private static final int GRANULE = 1024;

	/** The most a hop's writer queue holds. */
	private static final long NETTY_HOP_QUEUE_BYTES = 256 * 1024 + 64 * 1024;

	/**
	 * What a connection holds before the relay withholds its windows while the budget has
	 * room: its allowance and its share of the budget,
	 * {@code NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + CONNECTION_SHARE}.
	 */
	private static final long NETTY_SHARE_BYTES = 128 * 1024 + 2L * NETTY_CONNECTION_WINDOW;

	/**
	 * What a client still sends once its connection window is withheld: the read under
	 * way, the connection window, a granule of rounding per stream;
	 * {@code NettyHttp2DemuxRelay.WITHHELD_ARRIVALS}.
	 */
	private static final long NETTY_WITHHELD_ARRIVALS = 16 * 1024 + NETTY_CONNECTION_WINDOW
			+ MAX_CONCURRENT_STREAMS * (long) GRANULE;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<Socket> held = new ArrayList<>();

	private final List<AutoCloseable> closeables = new ArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

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
		synchronized (this.held) {
			this.held.forEach(H2DemuxFlowControlE2ETest::close);
		}
		this.closeables.forEach(H2DemuxFlowControlE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * As many streams as the relay admits, each granted 1MiB by an upstream that then
	 * stops reading, each filled to whatever window the client holds, the connection
	 * window honored: their stream windows would let the relay hold 100MiB, yet the queue
	 * stops at the connection's share of the budget -- the test JVM's budget, far larger,
	 * having room -- plus what the client still sent once its window was withheld, and
	 * the connection stays up.
	 */
	@Test
	void compliantClientWithManyStreamsIsNotTornDown() throws Exception {
		String route = "fc.many.test";
		startClient(route, stub(socket -> grantHeadsThenStall(socket, MAX_CONCURRENT_STREAMS)));
		AtomicLong maxQueued = new AtomicLong();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			for (int i = 0; i < MAX_CONCURRENT_STREAMS; i++) {
				client.headers(1 + 2 * i, false,
						TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/up" + i));
				if (i == 0) {
					// the relay answers once the first head routed the connection
					assertThat(client.awaitSettings(Duration.ofSeconds(5))).isTrue();
				}
			}
			Long advertised = client.setting(TestH2.SETTINGS_MAX_CONCURRENT_STREAMS);
			// the upstream's grants precede the data: every stream sees its 1MiB
			Thread.sleep(500);
			// three streams in a row sending nothing for want of connection window: it is
			// withheld, and the streams left could not send either
			int starved = 0;
			for (int i = 0; i < MAX_CONCURRENT_STREAMS && !client.closed() && starved < 3; i++) {
				long sent = client.fill(1 + 2 * i, Duration.ofMillis(100));
				maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
				starved = sent == 0 && client.connectionWindow() <= 0 ? starved + 1 : 0;
			}
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			while (System.nanoTime() < deadline) {
				maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
				Thread.sleep(10);
			}
			assertThat(client.closed()).as("a compliant client is not torn down").isFalse();
			assertThat(advertised).as("the advertised stream limit").isEqualTo(MAX_CONCURRENT_STREAMS);
			assertThat(client.maxOvershoot()).as("the client stayed within every stream window").isLessThanOrEqualTo(0);
			assertThat(budgetUsed()).as("the budget has room").isLessThan(budget());
			assertThat(maxQueued.get()).as("queued high-water mark: the share, plus what the client still sent")
				.isGreaterThanOrEqualTo(NETTY_SHARE_BYTES)
				.isLessThanOrEqualTo(NETTY_SHARE_BYTES + NETTY_WITHHELD_ARRIVALS);
		}
	}

	/**
	 * A stream opened beyond the advertised limit is refused; once a stream closes, a new
	 * one is served again.
	 */
	@Test
	void streamBeyondTheConcurrencyLimitIsRefused() throws Exception {
		String route = "fc.limit.test";
		startClient(route, stub(H2DemuxFlowControlE2ETest::answerEndedRequests));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			for (int i = 0; i < MAX_CONCURRENT_STREAMS; i++) {
				client.headers(1 + 2 * i, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			}
			int beyond = 1 + 2 * MAX_CONCURRENT_STREAMS;
			client.headers(beyond, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitTrue(() -> client.reset(beyond) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(beyond)).isEqualTo(TestH2.REFUSED_STREAM);
			assertThat(client.reset(1)).as("the streams within the limit are not reset").isNull();
			// ending stream 1 lets the upstream answer it, which closes it
			client.sendData(1, new byte[0], TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			int next = beyond + 2;
			client.headers(next, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(next, Duration.ofSeconds(5))).as("a stream within the limit is served").isTrue();
			assertThat(client.reset(next)).isNull();
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * A bulk upload the upstream granted a huge window, on a leg whose upstream reads
	 * slower than the client sends, does not tear the connection down, and another stream
	 * of the same leg completes: the relay holds at most a stream window for the bulk
	 * stream, plus what the leg's writer queue holds.
	 */
	@Test
	void bulkUploadOnASlowLegLetsAnotherStreamOfTheLegComplete() throws Exception {
		String route = "fc.slow.test";
		AtomicLong upstreamReceived = new AtomicLong();
		startClient(route, stub(socket -> serveSlowlyGrantingStreamOne(socket, upstreamReceived)));
		AtomicBoolean stop = new AtomicBoolean();
		AtomicLong maxQueued = new AtomicLong();
		AtomicLong maxLag = new AtomicLong();
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/bulk"));
			// a dedicated fast sampler: the relay's queue swings between empty and full
			// as
			// the slow upstream drains it, so a sample taken at a poll point may read 0
			Thread sampler = Thread.ofPlatform().name("e2e-fc-sampler").daemon().start(() -> {
				while (!stop.get()) {
					maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
					maxLag.accumulateAndGet(client.streamSent(1) - upstreamReceived.get(), Math::max);
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
				}
			});
			Thread pusher = Thread.ofVirtual().name("e2e-fc-bulk").start(() -> {
				try {
					while (!stop.get() && client.data(1, 1024 * 1024, false, Duration.ofSeconds(30))) {
						// keep the leg's queue engaged
					}
				}
				catch (IOException | InterruptedException e) {
					// torn down
				}
			});
			// past the tunnel connection's credit and both hops' windows: the slow
			// upstream's reads pace the stream
			long paced = 2L * VirtualConnection.DEFAULT_WINDOW + 2L * NETTY_STREAM_WINDOW;
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
				return client.streamSent(1) > paced || client.closed();
			});
			client.headers(3, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/fast"));
			boolean answered = client.awaitTrue(() -> client.ended(3) || client.closed(), Duration.ofSeconds(20));
			stop.set(true);
			sampler.join(TimeUnit.SECONDS.toMillis(5));
			assertThat(client.closed()).as("the connection stays up (sent %d)", client.streamSent(1)).isFalse();
			assertThat(answered && client.ended(3)).as("the other stream of the leg completes").isTrue();
			assertThat(client.maxOvershoot()).isLessThanOrEqualTo(0);
			assertThat(maxLag.get()).as("the slow upstream held the bulk stream: sent ahead of what it read")
				.isGreaterThan(NETTY_STREAM_WINDOW);
			// the bulk stream's window, a granule of backlog rounding, the leg's writer
			// queue, and under 1KiB for the other stream's head
			assertThat(maxQueued.get()).as("queued for the bulk stream, plus the other stream's head")
				.isGreaterThan(0)
				.isLessThanOrEqualTo(NETTY_STREAM_WINDOW + GRANULE + NETTY_HOP_QUEUE_BYTES + 1024);
			client.close();
			pusher.join(TimeUnit.SECONDS.toMillis(5));
		}
	}

	/**
	 * DATA of a stream the relay refused (no route) still consumed the client's
	 * connection window, which the relay must release, or the connection starves: netty's
	 * auto refill grants it back once half of the window is spent.
	 */
	@Test
	void dataOnARefusedStreamReleasesTheConnectionWindow() throws Exception {
		String route = "fc.refused.test";
		startClient(route, stub(H2DemuxFlowControlE2ETest::answerEndedRequests));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.headers(3, false, TestH2.headerBlock(":authority=nowhere.test", ":method=POST", ":path=/"));
			assertThat(client.data(3, 40_000, true, Duration.ofSeconds(5))).isTrue();
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= 32_768, Duration.ofSeconds(5)))
				.as("connection window granted for the refused stream's DATA: %d", client.connectionGranted())
				.isTrue();
			assertThat(client.connectionWindow()).as("the window back above half")
				.isGreaterThan(H2TestClient.INITIAL_WINDOW / 2);
		}
	}

	/**
	 * Padding counts against both windows (RFC 9113 6.9.1): the relay releases the whole
	 * frames at connection level, gives the padding back at stream level since it never
	 * queues it, and relays the payload unpadded. Just past half of the window, so
	 * netty's auto refill grants the connection window once, exactly what was spent, and
	 * the stream window once half of it was written or padding.
	 */
	@Test
	void paddedDataIsReleasedInFullAndRelayedUnpadded() throws Exception {
		String route = "fc.padded.test";
		List<TestH2.Frame> upstreamData = new java.util.concurrent.CopyOnWriteArrayList<>();
		startClient(route, stub(socket -> recordData(socket, upstreamData)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			int data = 1000;
			int padding = 200;
			byte[] payload = new byte[1 + data + padding];
			// the last frame takes the spent bytes past half of the window
			int frames = NETTY_CONNECTION_WINDOW / 2 / payload.length + 1;
			// the relay's stream window applies once it read the ack of its SETTINGS,
			// which goes before the PING
			assertThat(client.awaitSettings(Duration.ofSeconds(5))).isTrue();
			client.frame(TestH2.PING, 0, 0, new byte[8]);
			assertThat(client.awaitTrue(() -> !client.pingAcks().isEmpty(), Duration.ofSeconds(5))).isTrue();
			payload[0] = (byte) padding;
			java.util.Arrays.fill(payload, 1, 1 + data, (byte) 'x');
			for (int i = 0; i < frames; i++) {
				client.sendData(1, payload, TestH2.FLAG_PADDED);
			}
			long spent = (long) frames * payload.length;
			assertThat(spent).as("past half of the window").isGreaterThan(NETTY_CONNECTION_WINDOW / 2);
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= CONNECTION_WINDOW_RAISE + spent,
					Duration.ofSeconds(5)))
				.as("connection grant %d", client.connectionGranted())
				.isTrue();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(upstreamData.stream().mapToInt(frame -> frame.payload().length).sum())
					.as("relayed payload")
					.isEqualTo(frames * data));
			assertThat(upstreamData).as("relayed unpadded")
				.allSatisfy(frame -> assertThat(frame.flags() & TestH2.FLAG_PADDED).isZero());
			assertThat(client.awaitTrue(() -> client.streamGranted(1) > NETTY_STREAM_WINDOW / 2, Duration.ofSeconds(5)))
				.as("stream grant %d", client.streamGranted(1))
				.isTrue();
			Thread.sleep(200); // a surplus grant would follow the exact one
			assertThat(client.connectionGranted()).isEqualTo(CONNECTION_WINDOW_RAISE + spent);
			assertThat(client.streamGranted(1)).isLessThanOrEqualTo(spent);
		}
	}

	/**
	 * The upstream's connection window is released for the DATA payload relayed to the
	 * client, not for frame headers or other frames: an over-grant accumulates until the
	 * upstream's window passes 2^31-1, a connection error. Past half of the window, so
	 * netty's auto refill grants: once, what was written to the client when half of the
	 * window was spent, never more than the DATA.
	 */
	@Test
	void upstreamConnectionWindowIsReleasedForDataOnly() throws Exception {
		String route = "fc.grant.test";
		AtomicLong granted = new AtomicLong();
		// frames of odd sizes, just past half of the window
		int[] pattern = { 10000, 16000, 14000, 7 };
		int[] chunks = new int[14 * pattern.length];
		for (int i = 0; i < chunks.length; i++) {
			chunks[i] = pattern[i % pattern.length];
		}
		long data = java.util.Arrays.stream(chunks).asLongStream().sum();
		long half = (NETTY_CONNECTION_WINDOW + 1) / 2;
		assertThat(data).as("past half of the window").isGreaterThanOrEqualTo(half);
		startClient(route, stub(socket -> respondCountingGrants(socket, chunks, granted)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.frame(TestH2.SETTINGS, 0, 0, settingInitialWindow(NETTY_STREAM_WINDOW));
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			// the raise behind the preface, then the auto refill's grant
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> granted.get() >= CONNECTION_WINDOW_RAISE + half);
			Thread.sleep(300); // a surplus grant would follow the exact one
			assertThat(granted.get() - CONNECTION_WINDOW_RAISE).isBetween(half, data);
		}
	}

	/**
	 * Grants the connection and each of the first {@code heads} streams a large window as
	 * their HEADERS arrive, then stops reading, holding the socket.
	 */
	private static void grantHeadsThenStall(Socket socket, int heads) {
		try {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			int seen = 0;
			TestH2.@Nullable Frame frame;
			while (seen < heads && (frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS) {
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, frame.streamId(), TestH2.int32(1024 * 1024));
					seen++;
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** An h2 stub answering every request once it ended (END_STREAM). */
	private static void answerEndedRequests(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					synchronized (out) {
						out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
						out.flush();
					}
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/**
	 * Grants stream 1 a huge window on its HEADERS, answers ended requests, and reads at
	 * about 4MiB/s.
	 */
	private static void serveSlowlyGrantingStreamOne(Socket socket, AtomicLong received) {
		try (socket) {
			InputStream in = new ThrottledInputStream(socket.getInputStream(), received);
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(Integer.MAX_VALUE - 65535));
				}
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "fast"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/**
	 * Records the DATA frames received; grants a window for them at once, answers
	 * nothing.
	 */
	private static void recordData(Socket socket, List<TestH2.Frame> data) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, settingInitialWindow(Integer.MAX_VALUE));
			TestH2.writeFrame(out, TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(Integer.MAX_VALUE - 65535));
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.DATA) {
					data.add(frame);
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/**
	 * Answers the request with HEADERS and DATA frames of the given sizes, then sums the
	 * connection-level WINDOW_UPDATE increments it receives.
	 */
	private static void respondCountingGrants(Socket socket, int[] chunks, AtomicLong granted) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
					// the relay's SETTINGS came before the head: its stream window
					// applies once acknowledged
					TestH2.writeFrame(out, TestH2.SETTINGS, TestH2.FLAG_ACK, 0, new byte[0]);
					TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, new byte[] { (byte) 0x88 });
					for (int i = 0; i < chunks.length; i++) {
						int flags = i == chunks.length - 1 ? TestH2.FLAG_END_STREAM : 0;
						TestH2.writeFrame(out, TestH2.DATA, flags, 1, new byte[chunks[i]]);
					}
				}
				if (frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() == 0) {
					granted.addAndGet(TestH2.increment(frame));
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	private static byte[] settingInitialWindow(int window) {
		return new byte[] { 0, 0x4, (byte) (window >>> 24), (byte) (window >>> 16), (byte) (window >>> 8),
				(byte) window };
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

	private long budget() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.bytes").gauge().value());
	}

	private long budgetUsed() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-fc").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/** A raw TCP stub serving every accepted socket with the handler. */
	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			// small, so the upstream's socket absorbs little of what the relay holds back
			server.setReceiveBufferSize(16 * 1024);
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-fc-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						synchronized (this.held) {
							this.held.add(socket);
						}
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

	/** Reads at most 64KiB per 16ms: about 4MiB/s. */
	private static final class ThrottledInputStream extends InputStream {

		private static final int SLICE = 64 * 1024;

		private final InputStream in;

		private final AtomicLong received;

		private int budget = SLICE;

		ThrottledInputStream(InputStream in, AtomicLong received) {
			this.in = in;
			this.received = received;
		}

		@Override
		public int read() throws IOException {
			byte[] one = new byte[1];
			int n = read(one, 0, 1);
			return n == 1 ? one[0] & 0xff : -1;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (len == 0) {
				return 0;
			}
			if (this.budget == 0) {
				try {
					Thread.sleep(16);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException(e);
				}
				this.budget = SLICE;
			}
			int n = this.in.read(b, off, Math.min(len, this.budget));
			if (n > 0) {
				this.budget -= n;
				this.received.addAndGet(n);
			}
			return n;
		}

	}

}
