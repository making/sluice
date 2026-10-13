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
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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
 * The h2 demux owns the stream windows towards the client: a client honoring every window
 * and the advertised stream limit is never torn down, however many streams it fills, and
 * what the relay queues for it stays below the bound those limits derive. The connection
 * window is released for every DATA byte the relay consumes, padding and refused streams
 * included.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
@LegacyOnly("flow-control isolation and bounds: step 5 (102)")
class H2DemuxFlowControlE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	/** The stream limit the relay advertises. */
	private static final int MAX_CONCURRENT_STREAMS = 100;

	/** The client window plus the queue allowance, per stream. */
	private static final long STREAM_BUDGET = 65535 + 64 * 1024;

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
	 * window honored: the queue grows past the old 4MiB leg cap, yet the connection stays
	 * up and the queue stays below the bound the stream limit and budget derive.
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
			for (int i = 0; i < MAX_CONCURRENT_STREAMS && !client.closed(); i++) {
				client.fill(1 + 2 * i, Duration.ofMillis(100));
				maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
			}
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			while (System.nanoTime() < deadline) {
				maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
				Thread.sleep(10);
			}
			assertThat(client.closed()).as("a compliant client is not torn down").isFalse();
			assertThat(advertised).as("the advertised stream limit").isEqualTo(MAX_CONCURRENT_STREAMS);
			assertThat(client.maxOvershoot()).as("the client stayed within every stream window").isLessThanOrEqualTo(0);
			assertThat(maxQueued.get()).as("queued high-water mark: past the old leg cap, below the derived bound")
				.isGreaterThan(4L * 1024 * 1024)
				.isLessThanOrEqualTo(MAX_CONCURRENT_STREAMS * (STREAM_BUDGET + 1024));
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
	 * of the same leg completes: the relay queues at most the stream's budget for the
	 * bulk stream.
	 */
	@Test
	void bulkUploadOnASlowLegLetsAnotherStreamOfTheLegComplete() throws Exception {
		String route = "fc.slow.test";
		startClient(route, stub(H2DemuxFlowControlE2ETest::serveSlowlyGrantingStreamOne));
		AtomicBoolean stop = new AtomicBoolean();
		AtomicLong maxQueued = new AtomicLong();
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/bulk"));
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
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.until(() -> maxQueued.accumulateAndGet(queuedBytes(route), Math::max) > 0 || client.closed());
			client.headers(3, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/fast"));
			boolean answered = client.awaitTrue(() -> {
				maxQueued.accumulateAndGet(queuedBytes(route), Math::max);
				return client.ended(3) || client.closed();
			}, Duration.ofSeconds(20));
			stop.set(true);
			assertThat(client.closed()).as("the connection stays up (sent %d)", client.streamSent(1)).isFalse();
			assertThat(answered && client.ended(3)).as("the other stream of the leg completes").isTrue();
			assertThat(client.maxOvershoot()).isLessThanOrEqualTo(0);
			// past the initial window, the relay's budget and the tunnel connection's
			// credit
			// window: the slow upstream's reads pace the stream
			assertThat(client.streamSent(1)).as("the bulk stream got past the initial window")
				.isGreaterThan(STREAM_BUDGET + 2L * VirtualConnection.DEFAULT_WINDOW);
			// the bulk stream's payload within its budget, accounted as the buffers that
			// hold it -- the pending buffer rounds up to a 1KiB granule, and the buffer a
			// batch in flight was taken from stays accounted until written, less than
			// another granule -- plus its data entry's frame header, and under 1KiB for
			// the other stream's head
			assertThat(maxQueued.get()).as("queued for the bulk stream, plus the other stream's head")
				.isGreaterThan(0)
				.isLessThanOrEqualTo(STREAM_BUDGET + 2 * 1024 + 9 + 1024);
			client.close();
			pusher.join(TimeUnit.SECONDS.toMillis(5));
		}
	}

	/**
	 * DATA of a stream the relay refused (no route) still consumed the client's
	 * connection window, which the relay must release, or the connection starves.
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
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= 40_000, Duration.ofSeconds(5)))
				.as("connection window granted for the refused stream's DATA: %d", client.connectionGranted())
				.isTrue();
		}
	}

	/**
	 * Padding counts against both windows (RFC 9113 6.9.1): the relay releases the whole
	 * frame at connection level, gives the padding back at stream level since it never
	 * queues it, and relays the payload unpadded.
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
			payload[0] = (byte) padding;
			java.util.Arrays.fill(payload, 1, 1 + data, (byte) 'x');
			client.sendData(1, payload, TestH2.FLAG_PADDED);
			assertThat(client.awaitTrue(() -> client.connectionGranted() >= payload.length, Duration.ofSeconds(5)))
				.as("connection grant %d", client.connectionGranted())
				.isTrue();
			// once the payload is written nothing stays queued: the stream's window is
			// back at the budget, the padding included
			long streamGrant = STREAM_BUDGET - H2TestClient.INITIAL_WINDOW + payload.length;
			assertThat(client.awaitTrue(() -> client.streamGranted(1) >= streamGrant, Duration.ofSeconds(5)))
				.as("stream grant %d", client.streamGranted(1))
				.isTrue();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !upstreamData.isEmpty());
			TestH2.Frame relayed = upstreamData.get(0);
			assertThat(relayed.flags() & TestH2.FLAG_PADDED).as("relayed unpadded").isZero();
			assertThat(relayed.payload()).hasSize(data);
			Thread.sleep(200); // a surplus grant would follow the exact one
			assertThat(client.connectionGranted()).isEqualTo(payload.length);
			assertThat(client.streamGranted(1)).isEqualTo(streamGrant);
		}
	}

	/**
	 * The upstream's connection window is released for the DATA payload relayed to the
	 * client, not for frame headers or other frames: an over-grant accumulates until the
	 * upstream's window passes 2^31-1, a connection error.
	 */
	@Test
	void upstreamConnectionWindowIsReleasedForDataOnly() throws Exception {
		String route = "fc.grant.test";
		AtomicLong granted = new AtomicLong();
		int[] chunks = { 1000, 3000, 7 };
		startClient(route, stub(socket -> respondCountingGrants(socket, chunks, granted)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			long expected = 1000 + 3000 + 7;
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> granted.get() >= expected);
			Thread.sleep(300); // a surplus grant would follow the exact one
			assertThat(granted.get()).isEqualTo(expected);
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
	private static void serveSlowlyGrantingStreamOne(Socket socket) {
		try (socket) {
			InputStream in = new ThrottledInputStream(socket.getInputStream());
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

	/** Records the DATA frames received; grants nothing, answers nothing. */
	private static void recordData(Socket socket, List<TestH2.Frame> data) {
		try (socket) {
			InputStream in = socket.getInputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
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

		private int budget = SLICE;

		ThrottledInputStream(InputStream in) {
			this.in = in;
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
			}
			return n;
		}

	}

}
