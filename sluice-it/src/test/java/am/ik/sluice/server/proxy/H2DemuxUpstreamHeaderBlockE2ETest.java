package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.route.Router;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The header blocks an upstream sends -- interim (1xx) heads, final heads, trailers --
 * are bounded like the client's. Each is admitted before the relay writes it on the
 * client hop: past the hop's read pause and the connection's allowance with the budget
 * exhausted it resets its stream ({@code ENHANCE_YOUR_CALM} to the client, {@code CANCEL}
 * to the upstream), so a client that does not read holds the relay to its bounds however
 * much the upstream sends, and however little: a block referring to the upstream's
 * dynamic table costs a few bytes on the tunnel and 60KiB on the client hop. The interim
 * heads of one response are capped at the header list limit: past it the upstream's
 * stream is reset ({@code ENHANCE_YOUR_CALM}) and the client gets the bad-gateway page.
 * The client's table-referencing request heads, read at once, are admitted only as far as
 * they fit.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxUpstreamHeaderBlockE2ETest {

	private static final int KIB = 1024;

	private static final long BUDGET = 256 * KIB;

	private static final int CANCEL = 0x8;

	private static final int ENHANCE_YOUR_CALM = 0xb;

	/** The dynamic-table entry every amplified block refers to: name and value. */
	private static final String BIG_NAME = "x-big";

	/** {@code ~} has a 13-bit Huffman code: the relay writes it raw, 4000 bytes. */
	private static final String BIG_VALUE = "~".repeat(4000);

	/** The references to the entry per amplified block: 15 x 4037 = 60555 HPACK bytes. */
	private static final int REFERENCES = 15;

	/** The size of the references of an amplified block as HPACK counts it (RFC 7541). */
	private static final int AMPLIFIED_BYTES = REFERENCES * (BIG_NAME.length() + BIG_VALUE.length() + 32);

	/** The {@code link} value of an interim head: the head is 1024 HPACK bytes. */
	private static final int LINK_LENGTH = 946;

	/** The 1KiB interim heads one response takes: 64 x 1024 = the header list limit. */
	private static final int INTERIMS_FIT = 64;

	/** What the relay may charge beyond the budget here: the resets it writes. */
	private static final int RESET_SLACK = 4 * KIB;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final ListAppender<ILoggingEvent> RELAY_EVENTS = new ListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void attachRelayLog() {
		Logger logger = (Logger) LoggerFactory.getLogger(NettyHttp2DemuxRelay.class);
		RELAY_EVENTS.start();
		logger.addAppender(RELAY_EVENTS);
	}

	@BeforeEach
	void awaitQuiet() {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		Awaitility.await().atMost(TIMEOUT).until(() -> server().activeConnections() == 0 && budgetUsed() == 0);
	}

	@AfterEach
	void relayEndedSilently() {
		Awaitility.await("every relay ended")
			.atMost(TIMEOUT)
			.until(() -> server().activeConnections() == 0 && budgetUsed() == 0);
		assertThat(RELAY_EVENTS.list).filteredOn(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
			.extracting(ILoggingEvent::getFormattedMessage)
			.as("the relay's warnings")
			.isEmpty();
	}

	@AfterAll
	void tearDown() {
		((Logger) LoggerFactory.getLogger(NettyHttp2DemuxRelay.class)).detachAppender(RELAY_EVENTS);
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxUpstreamHeaderBlockE2ETest::close);
	}

	/**
	 * A client opening 48 streams and then reading nothing, an upstream answering each
	 * with a header block of the kind referring to its dynamic table: about 20 bytes on
	 * the tunnel, 60KiB as the relay writes it -- 2.9MiB in all, without the bound held
	 * (and charged) in the client hop's writer queue but for what the sockets buffer. The
	 * relay admits blocks while they fit the client hop's read pause, then its allowance
	 * and the budget, and resets the rest's streams, so what it charges stays within the
	 * budget (nothing else is held here). Once the client reads, each stream has its
	 * block, intact, or the reset, and the connection goes on.
	 */
	@ParameterizedTest
	@EnumSource(BlockKind.class)
	void blocksAtAClientNotReadingStayWithinTheBudget(BlockKind kind) throws Exception {
		String route = route("silent-" + kind.name().toLowerCase());
		int streams = 48;
		AtomicInteger answered = new AtomicInteger();
		List<Integer> cancelled = new CopyOnWriteArrayList<>();
		startClient(route, upstream(upstream -> {
			upstream.settings(null);
			AmplifyingEncoder encoder = new AmplifyingEncoder();
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					answer(upstream, encoder, kind, frame.streamId());
					answered.incrementAndGet();
				}
				else if (frame.type() == TestH2.RST_STREAM && TestH2.errorCode(frame) == CANCEL) {
					cancelled.add(frame.streamId());
				}
			}
		}));
		double resetsBefore = resets("budget");
		try (Socket socket = new Socket()) {
			socket.setReceiveBufferSize(16 * KIB);
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort));
			ByteArrayOutputStream burst = new ByteArrayOutputStream();
			burst.write(TestH2.PREFACE);
			TestH2.writeFrame(burst, TestH2.SETTINGS, 0, 0, new byte[0]);
			for (int i = 0; i < streams; i++) {
				TestH2.writeFrame(burst, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 1 + 2 * i,
						get(route));
			}
			socket.getOutputStream().write(burst.toByteArray());
			socket.getOutputStream().flush();

			Awaitility.await().atMost(TIMEOUT).until(() -> answered.get() == streams);
			long charged = settled(this::budgetUsed, () -> (long) resets("budget"));
			assertThat(charged).as("charged: within the budget but for the resets written")
				.isLessThanOrEqualTo(BUDGET + RESET_SLACK);
			double refused = resets("budget") - resetsBefore;
			assertThat(refused).as("streams reset for want of budget").isPositive();
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(cancelled).as("the upstream was told").hasSize((int) refused));

			Map<Integer, Outcome> outcomes = readOutcomes(socket.getInputStream(), streams, kind);
			assertThat(outcomes.values().stream().filter(Outcome::reset).count()).as("streams reset")
				.isEqualTo((long) refused);
			for (Map.Entry<Integer, Outcome> outcome : outcomes.entrySet()) {
				if (outcome.getValue().reset()) {
					assertThat(outcome.getValue().resetCode()).as("stream %d's reset", outcome.getKey())
						.isEqualTo(ENHANCE_YOUR_CALM);
					assertThat(cancelled).contains(outcome.getKey());
				}
				else {
					Http2Headers block = Objects.requireNonNull(outcome.getValue().block());
					assertThat(block.getAll(BIG_NAME)).as("stream %d's block", outcome.getKey())
						.hasSize(REFERENCES)
						.allSatisfy(value -> assertThat(value.toString()).isEqualTo(BIG_VALUE));
				}
			}
			assertThat(pingAcked(socket)).as("the connection goes on").isTrue();
		}
	}

	/**
	 * An upstream sending interim heads of 1KiB each (as HPACK counts them) on a stream,
	 * past the 64KiB the relay takes of one response's interim heads: the client gets the
	 * 64 that fit, then the bad-gateway page instead of the final head, the upstream's
	 * stream is reset with {@code ENHANCE_YOUR_CALM}, and another stream of the leg
	 * completes.
	 */
	@Test
	void interimHeadsPastTheHeaderListLimitLoseTheirStream() throws Exception {
		String route = route("interim-flood");
		List<List<Integer>> resetsSeen = new CopyOnWriteArrayList<>();
		startClient(route, upstream(upstream -> serveInterims(upstream, 200, resetsSeen)));
		double resetsBefore = resets("interim");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("stream 1 answered").isTrue();
			List<String> statuses = statuses(client.frames(1));
			assertThat(statuses).as("the interim heads that fit, then the 502")
				.hasSize(INTERIMS_FIT + 1)
				.endsWith("502");
			assertThat(statuses.subList(0, INTERIMS_FIT)).containsOnly("103");
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(resetsSeen).as("the upstream was told")
					.contains(List.of(1, ENHANCE_YOUR_CALM)));
			assertThat(resets("interim") - resetsBefore).isEqualTo(1);

			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, TIMEOUT)).as("the leg's other stream completes").isTrue();
			assertThat(statuses(client.frames(3))).containsExactly("200");
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Interim heads summing exactly to the header list limit (64 of 1KiB) all reach the
	 * client, then the final head; nothing is reset.
	 */
	@Test
	void interimHeadsWithinTheHeaderListLimitPass() throws Exception {
		String route = route("interim-fit");
		List<List<Integer>> resetsSeen = new CopyOnWriteArrayList<>();
		startClient(route, upstream(upstream -> serveInterims(upstream, INTERIMS_FIT, resetsSeen)));
		assertThat(INTERIMS_FIT * interimBytes()).as("the interim heads sum to the limit exactly")
			.isEqualTo(NettyHttp2DemuxRelay.MAX_INTERIM_BYTES);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, TIMEOUT)).isTrue();
			List<String> statuses = statuses(client.frames(1));
			assertThat(statuses).hasSize(INTERIMS_FIT + 1).endsWith("200");
			assertThat(statuses.subList(0, INTERIMS_FIT)).containsOnly("103");
			assertThat(client.resets()).isEmpty();
			assertThat(resetsSeen).isEmpty();
		}
	}

	/**
	 * 98 request heads read at once, each referring to the client's dynamic table: a few
	 * bytes each, 60KiB as the relay writes them to the leg -- 5.8MiB in all. The relay
	 * admits those its allowance and the budget hold and refuses the rest
	 * ({@code REFUSED_STREAM}); the upstream gets just the admitted ones.
	 */
	@Test
	void tableReferencingRequestHeadsReadAtOnceAreAdmittedAsFarAsTheyFit() throws Exception {
		String route = route("request-amplified");
		AtomicInteger heads = new AtomicInteger();
		startClient(route, upstream(upstream -> {
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					heads.incrementAndGet();
				}
			}
		}));
		double refusedBefore = refused("budget");
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, get(route));
			Awaitility.await().atMost(TIMEOUT).until(() -> heads.get() == 1);
			int streams = 98;
			AmplifyingEncoder encoder = new AmplifyingEncoder();
			ByteArrayOutputStream burst = new ByteArrayOutputStream();
			for (int i = 0; i < streams; i++) {
				byte[] block = encoder.amplified(get(route));
				TestH2.writeFrame(burst, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3 + 2 * i,
						block);
			}
			assertThat(burst.size()).as("one read takes the burst").isLessThan(Hop.MAX_READ_BYTES);
			client.raw(burst.toByteArray());

			Awaitility.await()
				.atMost(TIMEOUT)
				.until(() -> refused("budget") - refusedBefore + heads.get() - 1 == streams);
			long admitted = heads.get() - 1;
			long fit = (NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE + BUDGET) / AMPLIFIED_BYTES;
			assertThat(admitted).as("admitted: what the allowance and the budget hold")
				.isPositive()
				.isLessThanOrEqualTo(fit);
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(
						client.resets().values().stream().filter(code -> code == TestH2.REFUSED_STREAM).count())
					.isEqualTo(streams - admitted));
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Answers stream 1 with {@code interims} interim heads then the final head, every
	 * other stream with the final head; records the relay's resets.
	 */
	private static void serveInterims(WindowedH2Upstream upstream, int interims, List<List<Integer>> resets)
			throws IOException {
		upstream.settings(null);
		TestH2.@Nullable Frame frame;
		while ((frame = upstream.next()) != null) {
			if (frame.type() == TestH2.HEADERS) {
				int streamId = frame.streamId();
				if (streamId == 1) {
					Thread.ofVirtual().start(() -> {
						try {
							for (int i = 0; i < interims; i++) {
								upstream.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, interimBlock());
							}
							upstream.headers(1, true);
						}
						catch (IOException e) {
							// the relay went away
						}
					});
				}
				else {
					upstream.headers(streamId, true);
				}
			}
			else if (frame.type() == TestH2.RST_STREAM) {
				resets.add(List.of(frame.streamId(), TestH2.errorCode(frame)));
			}
		}
	}

	/**
	 * {@code :status 103} and a {@code link}, literals never indexed: 1KiB as HPACK
	 * counts.
	 */
	private static byte[] interimBlock() {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		Hpack.literalIndexedName(block, 8, "103");
		Hpack.literal(block, 0x00, 4, "link", "x".repeat(LINK_LENGTH));
		return block.toByteArray();
	}

	/** The HPACK size of {@link #interimBlock()}. */
	private static int interimBytes() {
		return ":status".length() + 3 + 32 + "link".length() + LINK_LENGTH + 32;
	}

	/** Answers the stream with the kind's block, referring to the dynamic table. */
	private static void answer(WindowedH2Upstream upstream, AmplifyingEncoder encoder, BlockKind kind, int streamId)
			throws IOException {
		int headers = TestH2.FLAG_END_HEADERS;
		switch (kind) {
			case INTERIM -> {
				ByteArrayOutputStream status = new ByteArrayOutputStream();
				Hpack.literalIndexedName(status, 8, "103");
				upstream.write(TestH2.HEADERS, headers, streamId, encoder.amplified(status.toByteArray()));
			}
			case FINAL ->
				upstream.write(TestH2.HEADERS, headers, streamId, encoder.amplified(new byte[] { (byte) 0x88 }));
			case TRAILERS -> {
				upstream.headers(streamId, false);
				upstream.write(TestH2.HEADERS, headers | TestH2.FLAG_END_STREAM, streamId,
						encoder.amplified(new byte[0]));
			}
		}
	}

	/**
	 * Reads the relay's frames until every stream has its outcome: the kind's amplified
	 * block, decoded, or a reset.
	 */
	private static Map<Integer, Outcome> readOutcomes(InputStream in, int streams, BlockKind kind) throws IOException {
		Map<Integer, Outcome> outcomes = new HashMap<>();
		Map<Integer, Integer> blocks = new HashMap<>();
		ByteArrayOutputStream fragments = new ByteArrayOutputStream();
		int fragmentStream = 0;
		while (outcomes.size() < streams) {
			TestH2.Frame frame = Objects.requireNonNull(TestH2.readFrame(in), "the relay closed");
			int streamId = frame.streamId();
			if (frame.type() == TestH2.RST_STREAM) {
				outcomes.put(streamId, new Outcome(TestH2.errorCode(frame), null));
				continue;
			}
			if (frame.type() == TestH2.HEADERS) {
				fragments.reset();
				fragmentStream = streamId;
			}
			if (frame.type() != TestH2.HEADERS && frame.type() != TestH2.CONTINUATION) {
				continue;
			}
			assertThat(streamId).as("no frame inside a header block").isEqualTo(fragmentStream);
			fragments.writeBytes(frame.payload());
			if ((frame.flags() & TestH2.FLAG_END_HEADERS) == 0) {
				continue;
			}
			int seen = blocks.merge(streamId, 1, Integer::sum);
			if (seen == kind.blocks()) {
				outcomes.put(streamId, new Outcome(-1, decode(streamId, fragments.toByteArray())));
			}
		}
		return outcomes;
	}

	private static Http2Headers decode(int streamId, byte[] block) {
		try {
			// a fresh decoder: the relay's blocks are self-contained
			return new DefaultHttp2HeadersDecoder(true, 128 * KIB).decodeHeaders(streamId,
					Unpooled.wrappedBuffer(block));
		}
		catch (Http2Exception e) {
			throw new AssertionError("undecodable header block", e);
		}
	}

	/** The {@code :status} of each header block of the frames, in order. */
	private static List<String> statuses(List<TestH2.Frame> frames) {
		List<String> statuses = new ArrayList<>();
		for (TestH2.Frame frame : frames) {
			if (frame.type() == TestH2.HEADERS) {
				assertThat(frame.flags() & TestH2.FLAG_END_HEADERS).as("one frame per block").isNotZero();
				statuses.add(String.valueOf(decode(frame.streamId(), frame.payload()).status()));
			}
		}
		return statuses;
	}

	/** Sends a PING and reads until its ACK; {@code false} if the relay closed first. */
	private static boolean pingAcked(Socket socket) throws IOException {
		byte[] payload = "headers!".getBytes(StandardCharsets.US_ASCII);
		TestH2.writeFrame(socket.getOutputStream(), TestH2.PING, 0, 0, payload);
		socket.getOutputStream().flush();
		TestH2.@Nullable Frame frame;
		while ((frame = TestH2.readFrame(socket.getInputStream())) != null) {
			if (frame.type() == TestH2.PING && (frame.flags() & TestH2.FLAG_ACK) != 0) {
				return java.util.Arrays.equals(frame.payload(), payload);
			}
		}
		return false;
	}

	/**
	 * Polls until the values stopped changing for a second (20s at most), and returns the
	 * first.
	 */
	private static long settled(LongSupplier... values) throws InterruptedException {
		long deadline = System.nanoTime() + 2 * TIMEOUT.toNanos();
		long[] last = sample(values);
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			Thread.sleep(50);
			long[] now = sample(values);
			if (!java.util.Arrays.equals(now, last)) {
				last = now;
				since = System.nanoTime();
			}
			else if (System.nanoTime() - since >= TimeUnit.SECONDS.toNanos(1)) {
				return now[0];
			}
		}
		throw new AssertionError("still changing after 20s: " + java.util.Arrays.toString(last));
	}

	private static long[] sample(LongSupplier... values) {
		long[] sample = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			sample[i] = values[i].getAsLong();
		}
		return sample;
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/", ":scheme=http");
	}

	private static String route(String name) {
		return name + ".header-blocks.test";
	}

	private long budgetUsed() {
		return Math.round(meters().get("sluice.h2demux.budget.used.bytes").gauge().value());
	}

	private long budgetGauge() {
		return Math.round(meters().get("sluice.h2demux.budget.bytes").gauge().value());
	}

	private double resets(String reason) {
		return counter("sluice.h2demux.streams.reset", reason);
	}

	private double refused(String reason) {
		return counter("sluice.h2demux.streams.refused", reason);
	}

	private double counter(String name, String reason) {
		@Nullable Counter counter = meters().find(name).tag("reason", reason).counter();
		return counter == null ? 0 : counter.count();
	}

	private MeterRegistry meters() {
		return Objects.requireNonNull(this.meterRegistry);
	}

	private DataProxyServer server() {
		return Objects.requireNonNull(this.dataProxyServer);
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-hb").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/** A test upstream serving every accepted connection with the handler. */
	private ServerSocket upstream(Handler handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-hb-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						Thread.ofVirtual().name("e2e-hb-upstream").start(() -> serve(socket, handler));
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

	private static void serve(Socket socket, Handler handler) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream != null) {
				handler.serve(upstream);
			}
		}
		catch (Exception e) {
			// connection ended
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

	/** The upstream header blocks the bound is shown for. */
	enum BlockKind {

		/** A 103 head. */
		INTERIM,

		/** A 200 head. */
		FINAL,

		/** Trailers after a small 200 head: the second block of the stream. */
		TRAILERS;

		/** The blocks a stream gets when its amplified block arrives. */
		int blocks() {
			return this == TRAILERS ? 2 : 1;
		}

	}

	/** A stream's outcome at the client: a reset code, or the amplified block. */
	private record Outcome(int resetCode, @Nullable Http2Headers block) {

		boolean reset() {
			return this.block == null;
		}

	}

	/**
	 * Appends {@value #REFERENCES} references to one dynamic-table entry to a block; the
	 * first block of a connection adds the entry (literal with incremental indexing) and
	 * refers to it once less. Blocks must go out in the order they were made.
	 */
	private static final class AmplifyingEncoder {

		private boolean defined;

		byte[] amplified(byte[] prefix) {
			ByteArrayOutputStream block = new ByteArrayOutputStream();
			block.writeBytes(prefix);
			int references = REFERENCES;
			if (!this.defined) {
				this.defined = true;
				Hpack.literal(block, 0x40, 6, BIG_NAME, BIG_VALUE);
				references--;
			}
			for (int i = 0; i < references; i++) {
				// the newest dynamic entry: index 62
				block.write(0x80 | 62);
			}
			return block.toByteArray();
		}

	}

	/** The few HPACK representations the tests write (RFC 7541 5.1, 6.1, 6.2). */
	private static final class Hpack {

		private Hpack() {
		}

		/**
		 * A literal with a new name: {@code 0x40} adds it to the table, {@code 0x00} not.
		 */
		static void literal(ByteArrayOutputStream out, int pattern, int prefixBits, String name, String value) {
			integer(out, pattern, prefixBits, 0);
			string(out, name);
			string(out, value);
		}

		/** A literal without indexing, its name the static entry. */
		static void literalIndexedName(ByteArrayOutputStream out, int nameIndex, String value) {
			integer(out, 0x00, 4, nameIndex);
			string(out, value);
		}

		private static void string(ByteArrayOutputStream out, String value) {
			byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
			integer(out, 0x00, 7, bytes.length);
			out.writeBytes(bytes);
		}

		private static void integer(ByteArrayOutputStream out, int pattern, int prefixBits, int value) {
			int max = (1 << prefixBits) - 1;
			if (value < max) {
				out.write(pattern | value);
				return;
			}
			out.write(pattern | max);
			int rest = value - max;
			while (rest >= 128) {
				out.write((rest & 0x7f) | 0x80);
				rest >>>= 7;
			}
			out.write(rest);
		}

	}

	/** A test upstream's connection handler. */
	@FunctionalInterface
	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws Exception;

	}

}
