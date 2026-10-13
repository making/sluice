package am.ik.sluice.server.proxy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

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
import am.ik.sluice.it.ScriptedH2Upstream;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
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
 * An upstream's error ends no more than it covers: a stream error of an upstream (RFC
 * 9113 5.4.2) resets that stream alone, and an upstream's connection error (5.4.1), its
 * connection failing or its GOAWAY end its leg alone -- never the client's connection nor
 * the streams of other legs. A stream lost before a response frame was relayed on it is
 * answered with a {@code 502} by the relay -- bodiless on the legacy engine, the
 * bad-gateway page on the netty engine --, one lost after with RST_STREAM
 * {@code INTERNAL_ERROR}, and a stream the upstream never saw is refused
 * ({@code REFUSED_STREAM}). In the relay's package for the meters and the write seam.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
class H2DemuxUpstreamErrorE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(3);

	private static final Duration WITHIN = Duration.ofSeconds(5);

	private static final int NO_ERROR = 0x0;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int INTERNAL_ERROR = 0x2;

	private static final int FLOW_CONTROL_ERROR = 0x3;

	private static final int FRAME_SIZE_ERROR = 0x6;

	private static final int REFUSED_STREAM = 0x7;

	/** SETTINGS_MAX_CONCURRENT_STREAMS. */
	private static final int MAX_CONCURRENT_STREAMS = 0x3;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

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

	@Nullable
	@Autowired
	private ErrorResponse errorResponse;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.idle-timeout", () -> IDLE_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void release() {
		dataProxyServer().h2WriteSeam(Http2DemuxRelay.WriteSeam.NONE);
		this.held.forEach(HeldWrite::proceed);
		this.held.clear();
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxUpstreamErrorE2ETest::close);
	}

	/**
	 * An upstream stream WINDOW_UPDATE with an increment of 0 (RFC 9113 6.9) on a stream
	 * whose response head was relayed: the upstream gets RST_STREAM
	 * {@code PROTOCOL_ERROR} on it, the client RST_STREAM {@code INTERNAL_ERROR}; another
	 * stream of the same leg and one of another route complete, over the same upstream
	 * connection, and nobody gets a GOAWAY.
	 */
	@Test
	void upstreamStreamWindowUpdateOfZeroResetsOnlyItsStream() throws Exception {
		String route = route("ue.zero-stream");
		String otherRoute = route("ue.zero-stream-other");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
				connection.head(1, false);
				connection.write(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(0));
			}
			else if (frame.type() == TestH2.HEADERS) {
				connection.answer(frame.streamId(), "same-leg");
			}
		});
		startClient(route, upstream);
		startClient(otherRoute, upstream(new byte[0], ScriptedH2Upstream.answering("other")));
		double resets = counter("sluice.h2demux.streams.reset", "upstream");
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, WITHIN)).as("stream 1 reset").isTrue();
			assertThat(client.reset(1)).isEqualTo(INTERNAL_ERROR);
			assertThat(types(client.frames(1))).as("the response head, then the reset")
				.containsExactly(TestH2.HEADERS, TestH2.RST_STREAM);
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			TestH2.Frame reset = connection.await(frame -> frame.type() == TestH2.RST_STREAM, WITHIN);
			assertThat(reset).as("the upstream got a reset").isNotNull();
			assertThat(reset.streamId()).isEqualTo(1);
			assertThat(TestH2.errorCode(reset)).isEqualTo(PROTOCOL_ERROR);
			client.headers(3, true, get(route));
			client.headers(5, true, get(otherRoute));
			assertThat(client.awaitTrue(() -> client.ended(3) && client.ended(5), WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("same-leg");
			assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("other");
			assertThat(upstream.connections()).as("the leg went on").hasSize(1);
			assertThat(connection.frames()).noneMatch(frame -> frame.type() == TestH2.GOAWAY);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
			if (this.engine == H2DemuxEngine.LEGACY) {
				// the netty engine's reset meter comes with the metrics step
				assertThat(counter("sluice.h2demux.streams.reset", "upstream") - resets).isEqualTo(1);
			}
		}
	}

	/**
	 * An upstream stream WINDOW_UPDATE taking the stream's window past 2^31-1 (RFC 9113
	 * 6.9.1) before any response frame: the upstream gets RST_STREAM
	 * {@code FLOW_CONTROL_ERROR}, the client a {@code 502} the relay answers, then
	 * RST_STREAM {@code NO_ERROR} as its request was not finished (8.1); a stream of
	 * another route completes.
	 */
	@Test
	void upstreamStreamWindowPastTheMaximumAnswersTheStreamWith502() throws Exception {
		String route = route("ue.overflow-stream");
		String otherRoute = route("ue.overflow-stream-other");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
				connection.write(TestH2.WINDOW_UPDATE, 0, 1, TestH2.int32(Integer.MAX_VALUE));
			}
		});
		startClient(route, upstream);
		startClient(otherRoute, upstream(new byte[0], ScriptedH2Upstream.answering("other")));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, false, post(route));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, WITHIN)).as("stream 1 reset").isTrue();
			assertThat(types(client.frames(1))).as("the 502, then the reset").containsExactlyElementsOf(badGateway());
			assertBadGateway(client.frames(1), route, "POST");
			assertThat(client.ended(1)).as("the 502 ends the stream").isTrue();
			assertThat(client.reset(1)).isEqualTo(NO_ERROR);
			TestH2.Frame reset = upstream.awaitConnection(0, WITHIN)
				.await(frame -> frame.type() == TestH2.RST_STREAM, WITHIN);
			assertThat(reset).as("the upstream got a reset").isNotNull();
			assertThat(TestH2.errorCode(reset)).isEqualTo(FLOW_CONTROL_ERROR);
			client.headers(3, true, get(otherRoute));
			assertThat(client.awaitEnd(3, WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(3))).isEqualTo("other");
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream connection error (a connection WINDOW_UPDATE of 0, RFC 9113 6.9) while
	 * its leg carries a stream whose response head was relayed and one without: the
	 * upstream gets a GOAWAY {@code PROTOCOL_ERROR} naming no stream on the legacy engine
	 * (the relay processed none of its own), 2^31-1 on the netty engine, then the relay's
	 * EOF; the client gets RST_STREAM {@code INTERNAL_ERROR} on the first and a
	 * {@code 502} on the second, and no GOAWAY. A stream of another route opened before
	 * the error completes after it, and the route's next stream opens a new leg and
	 * completes.
	 */
	@Test
	void upstreamConnectionErrorEndsOnlyItsLeg() throws Exception {
		String route = route("ue.connection");
		String otherRoute = route("ue.connection-other");
		AtomicReference<@Nullable ScriptedH2Upstream> self = new AtomicReference<>();
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() != TestH2.HEADERS) {
				return;
			}
			if (Objects.requireNonNull(self.get()).connections().indexOf(connection) > 0) {
				connection.answer(frame.streamId(), "new-leg");
			}
			else if (frame.streamId() == 1) {
				connection.head(1, false);
			}
		});
		self.set(upstream);
		startClient(route, upstream);
		ScriptedH2Upstream other = upstream(new byte[0], (connection, frame) -> {
			// answered by the test
		});
		startClient(otherRoute, other);
		double failed = counter("sluice.h2demux.legs.failed", null);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			client.headers(5, true, get(otherRoute));
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertThat(connection.await(frame -> frame.type() == TestH2.HEADERS && frame.streamId() == 3, WITHIN))
				.as("both streams reached the upstream")
				.isNotNull();
			assertThat(client.awaitTrue(() -> client.frames(1).size() == 1, WITHIN)).as("stream 1's response head")
				.isTrue();
			ScriptedH2Upstream.Connection otherConnection = other.awaitConnection(0, WITHIN);
			assertThat(otherConnection.await(frame -> frame.type() == TestH2.HEADERS, WITHIN)).isNotNull();
			connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			TestH2.Frame goAway = connection.await(frame -> frame.type() == TestH2.GOAWAY, WITHIN);
			assertThat(goAway).as("the upstream got a GOAWAY").isNotNull();
			assertThat(TestH2.errorCode(goAway)).isEqualTo(PROTOCOL_ERROR);
			// netty names 2^31-1 on an error of its frame reader, the stream ids it saw
			// unknown to it (netty issue 10670)
			assertThat(TestH2.increment(goAway)).as("its last stream id")
				.isEqualTo(this.engine == H2DemuxEngine.NETTY ? Integer.MAX_VALUE : 0);
			assertThat(connection.awaitEof(WITHIN)).as("the relay half-closed after the GOAWAY").isTrue();
			assertThat(client.awaitTrue(() -> client.reset(1) != null && client.ended(3), WITHIN)).isTrue();
			assertThat(client.reset(1)).isEqualTo(INTERNAL_ERROR);
			assertBadGateway(client.frames(3), route, "GET");
			assertThat(client.reset(3)).as("the request of stream 3 had ended").isNull();
			otherConnection.answer(5, "other");
			assertThat(client.awaitEnd(5, WITHIN)).as("the other route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("other");
			client.headers(7, true, get(route));
			assertThat(client.awaitEnd(7, WITHIN)).as("the route's next stream").isTrue();
			assertThat(TestH2.bodyOf(client.frames(7))).isEqualTo("new-leg");
			assertThat(upstream.connections()).as("over a new leg").hasSize(2);
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
			if (this.engine == H2DemuxEngine.LEGACY) {
				// the netty engine's legs-failed meter comes with the metrics step
				assertThat(counter("sluice.h2demux.legs.failed", null) - failed).isEqualTo(1);
			}
			Awaitility.await()
				.atMost(WITHIN)
				.until(() -> dataProxyServer().h2DemuxMeters().trackedStreams(route) == 0 && queuedBytes(route) == 0);
		}
	}

	/**
	 * An upstream connection error while a response it completed waits for the client's
	 * connection window: the response is delivered whole once the client grants, while
	 * the stream it had not answered got a {@code 502} at once.
	 */
	@Test
	@LegacyOnly("end-to-end windows: the upstream sends within the client's window; the netty engine's per-hop "
			+ "counterpart is NettyH2DemuxRelayE2ETest#aResponseTheUpstreamCompletedOutlivesItsConnectionError")
	void aConnectionErrorStillDeliversTheResponsesTheUpstreamCompleted() throws Exception {
		String route = route("ue.completed");
		int length = 100 * 1024;
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS && frame.streamId() == 3) {
				connection.head(1, false);
				for (int sent = 0; sent < length; sent += 16 * 1024) {
					int chunk = Math.min(16 * 1024, length - sent);
					connection.write(TestH2.DATA, sent + chunk == length ? TestH2.FLAG_END_STREAM : 0, 1,
							new byte[chunk]);
				}
				connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			}
		});
		startClient(route, upstream);
		double failed = counter("sluice.h2demux.legs.failed", null);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.withholdConnectionWindow();
			// the stream window covers the response: only the connection window holds it
			client.frame(TestH2.SETTINGS, 0, 0, TestH2Settings.of(0x4, 1024 * 1024));
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			assertThat(client.awaitEnd(3, WITHIN)).isTrue();
			assertThat(TestH2.status(client.frames(3))).isEqualTo("502");
			assertThat(counter("sluice.h2demux.legs.failed", null) - failed).as("the leg ended").isEqualTo(1);
			assertThat(client.awaitTrue(() -> client.dataReceived(1) == H2TestClient.INITIAL_WINDOW, WITHIN))
				.as("the client's window spent")
				.isTrue();
			assertThat(client.ended(1)).as("the rest waits for the client's window").isFalse();
			client.grantConnectionWindowOnReceipt();
			assertThat(client.awaitEnd(1, WITHIN)).isTrue();
			assertThat(client.dataReceived(1)).isEqualTo(length);
			assertThat(client.reset(1)).isNull();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * A stream whose upstream sent only an interim response ({@code 100}) before its
	 * GOAWAY of fewer than 8 octets -- a connection error {@code FRAME_SIZE_ERROR} (RFC
	 * 9113 6.8, 4.2) -- is still answered with a {@code 502}: an interim head leaves the
	 * final response to come (RFC 9110 15.2).
	 */
	@Test
	void aStreamLostAfterAnInterimResponseIsAnswered502() throws Exception {
		String route = route("ue.interim");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				// :status 100, a literal with the indexed name of :status
				connection.write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS, frame.streamId(),
						new byte[] { 0x08, 0x03, '1', '0', '0' });
				connection.write(TestH2.GOAWAY, 0, 0, new byte[4]);
			}
		});
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, WITHIN)).isTrue();
			List<TestH2.Frame> heads = client.frames(1)
				.stream()
				.filter(frame -> frame.type() == TestH2.HEADERS)
				.toList();
			assertThat(heads).as("the interim head, then the 502").hasSize(2);
			assertThat(TestH2.status(heads.subList(0, 1))).isEqualTo("100");
			assertThat(TestH2.status(heads.subList(1, 2))).isEqualTo("502");
			if (this.engine == H2DemuxEngine.NETTY) {
				assertThat(TestH2.bodyOf(client.frames(1))).as("the page").isEqualTo(badGatewayPage(route, "GET"));
			}
			TestH2.Frame goAway = upstream.awaitConnection(0, WITHIN)
				.await(frame -> frame.type() == TestH2.GOAWAY, WITHIN);
			assertThat(goAway).as("the upstream got a GOAWAY").isNotNull();
			assertThat(TestH2.errorCode(goAway)).isEqualTo(FRAME_SIZE_ERROR);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream sending a frame on a stream the relay has not opened on it -- one
	 * waiting for its SETTINGS_MAX_CONCURRENT_STREAMS of 1, idle on the upstream -- makes
	 * a connection error {@code PROTOCOL_ERROR} (RFC 9113 5.1): the open stream is
	 * answered with a {@code 502}, the waiting one refused ({@code REFUSED_STREAM}, the
	 * upstream never saw it), and none of the waiting stream's frames reach the upstream.
	 */
	@Test
	@LegacyOnly("a stream waiting for the upstream's concurrency limit: a later migration step")
	void upstreamFrameOnAWaitingStreamEndsItsLegAndRefusesTheStream() throws Exception {
		String route = route("ue.idle-stream");
		ScriptedH2Upstream upstream = upstream(TestH2Settings.of(MAX_CONCURRENT_STREAMS, 1), (connection, frame) -> {
		});
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			awaitHandled(client, 3);
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertThat(connection.await(frame -> frame.type() == TestH2.HEADERS && frame.streamId() == 1, WITHIN))
				.isNotNull();
			connection.head(3, false);
			TestH2.Frame goAway = connection.await(frame -> frame.type() == TestH2.GOAWAY, WITHIN);
			assertThat(goAway).as("the upstream got a GOAWAY").isNotNull();
			assertThat(TestH2.errorCode(goAway)).isEqualTo(PROTOCOL_ERROR);
			assertThat(client.awaitTrue(() -> client.ended(1) && client.reset(3) != null, WITHIN)).isTrue();
			assertBadGateway(client.frames(1), route, "GET");
			assertThat(client.reset(3)).isEqualTo(REFUSED_STREAM);
			assertThat(types(client.frames(3))).as("nothing but the refusal").containsExactly(TestH2.RST_STREAM);
			assertThat(connection.awaitEof(WITHIN)).isTrue();
			assertThat(connection.frames()).as("nothing of stream 3 reached the upstream")
				.noneMatch(frame -> frame.streamId() == 3);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream closing its connection (FIN) before answering: its open stream is
	 * answered with a {@code 502}, the stream waiting for its concurrency limit refused;
	 * the client's connection goes on, and the route's next stream opens a new leg.
	 */
	@Test
	void upstreamClosingBeforeTheResponseHeadAnswers502() throws Exception {
		String route = route("ue.close-before-head");
		AtomicReference<@Nullable ScriptedH2Upstream> self = new AtomicReference<>();
		ScriptedH2Upstream upstream = upstream(TestH2Settings.of(MAX_CONCURRENT_STREAMS, 1), (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS
					&& Objects.requireNonNull(self.get()).connections().indexOf(connection) > 0) {
				connection.answer(frame.streamId(), "new-leg");
			}
		});
		self.set(upstream);
		startClient(route, upstream);
		// the netty engine does not hold a stream for the upstream's concurrency limit
		// yet
		// (a later migration step): its stream 3 would race the upstream's SETTINGS
		boolean waits = this.engine == H2DemuxEngine.LEGACY;
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5)) {
			client.headers(1, true, get(route));
			if (waits) {
				client.headers(3, true, get(route));
				awaitHandled(client, 3);
			}
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertThat(connection.await(frame -> frame.type() == TestH2.HEADERS && frame.streamId() == 1, WITHIN))
				.isNotNull();
			connection.close();
			assertThat(client.awaitTrue(() -> client.ended(1) && (!waits || client.reset(3) != null), WITHIN)).isTrue();
			assertBadGateway(client.frames(1), route, "GET");
			if (waits) {
				assertThat(client.reset(3)).isEqualTo(REFUSED_STREAM);
			}
			client.headers(5, true, get(route));
			assertThat(client.awaitEnd(5, WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(5))).isEqualTo("new-leg");
			assertThat(client.goAwayCode()).isEqualTo(-1);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * An upstream resetting its TCP connection mid-body: the client got the response head
	 * and the DATA relayed so far, then RST_STREAM {@code INTERNAL_ERROR}; the client's
	 * connection goes on.
	 */
	@Test
	void upstreamResetMidBodyResetsTheStreamWithInternalError() throws Exception {
		String route = route("ue.reset-mid-body");
		String otherRoute = route("ue.reset-mid-body-other");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.head(frame.streamId(), false);
				connection.write(TestH2.DATA, 0, frame.streamId(), "partial".getBytes(StandardCharsets.US_ASCII));
				// the relay read the frames before the reset
				sleep(200);
				connection.abort();
			}
		});
		startClient(route, upstream);
		startClient(otherRoute, upstream(new byte[0], ScriptedH2Upstream.answering("other")));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitTrue(() -> client.reset(1) != null, WITHIN)).isTrue();
			assertThat(types(client.frames(1))).containsExactly(TestH2.HEADERS, TestH2.DATA, TestH2.RST_STREAM);
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("partial");
			assertThat(client.reset(1)).isEqualTo(INTERNAL_ERROR);
			client.headers(3, true, get(otherRoute));
			assertThat(client.awaitEnd(3, WITHIN)).isTrue();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream's GOAWAY naming stream 1 as the last it processed, while streams 1 and
	 * 3 are open on it and stream 5 waits for its concurrency limit of 2: streams 3 and 5
	 * are refused ({@code REFUSED_STREAM}, RFC 9113 6.8, 8.7), stream 1 completes, the
	 * leg half-closes once it did, and the route's next stream opens a new leg. Nothing
	 * of stream 5 nor of the next stream reaches the upstream that went away.
	 */
	@Test
	void upstreamGoAwayRefusesTheStreamsItDidNotProcess() throws Exception {
		String route = route("ue.goaway");
		AtomicReference<@Nullable ScriptedH2Upstream> self = new AtomicReference<>();
		ScriptedH2Upstream upstream = upstream(TestH2Settings.of(MAX_CONCURRENT_STREAMS, 2), (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS
					&& Objects.requireNonNull(self.get()).connections().indexOf(connection) > 0) {
				connection.answer(frame.streamId(), "new-leg");
			}
		});
		self.set(upstream);
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			client.headers(5, true, get(route));
			awaitHandled(client, 5);
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertThat(connection.await(frame -> frame.type() == TestH2.HEADERS && frame.streamId() == 3, WITHIN))
				.isNotNull();
			byte[] goAway = new byte[8];
			System.arraycopy(TestH2.int32(1), 0, goAway, 0, 4);
			connection.write(TestH2.GOAWAY, 0, 0, goAway);
			assertThat(client.awaitTrue(() -> client.reset(3) != null && client.reset(5) != null, WITHIN)).isTrue();
			assertThat(client.reset(3)).isEqualTo(REFUSED_STREAM);
			assertThat(client.reset(5)).isEqualTo(REFUSED_STREAM);
			assertThat(connection.awaitEof(Duration.ofMillis(300))).as("stream 1 keeps the leg").isFalse();
			connection.answer(1, "processed");
			assertThat(client.awaitEnd(1, WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("processed");
			assertThat(connection.awaitEof(WITHIN)).as("the leg half-closed").isTrue();
			if (this.engine == H2DemuxEngine.NETTY) {
				// netty's graceful close: a GOAWAY of the relay's own came first
				assertThat(connection.frames()).filteredOn(frame -> frame.type() == TestH2.GOAWAY)
					.extracting(TestH2::errorCode)
					.containsExactly(NO_ERROR);
			}
			client.headers(7, true, get(route));
			assertThat(client.awaitEnd(7, WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(7))).isEqualTo("new-leg");
			assertThat(upstream.connections()).hasSize(2);
			if (this.engine == H2DemuxEngine.LEGACY) {
				// the netty engine does not hold stream 5 for the upstream's concurrency
				// limit yet (a later migration step): it may reach the upstream before
				// the upstream's SETTINGS do
				assertThat(connection.frames()).noneMatch(frame -> frame.streamId() == 5);
			}
			assertThat(connection.frames()).noneMatch(frame -> frame.streamId() == 7);
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * An upstream that keeps its connection open past the GOAWAY and the half-close the
	 * relay sent it on its connection error is closed by the relay once
	 * {@link Http2DemuxRelay#UPSTREAM_CLOSE_GRACE_NANOS} passed, not before, and long
	 * before the idle timeout ends the client's connection.
	 */
	@Test
	@LegacyOnly("the legacy relay's close grace; the netty engine's counterpart is "
			+ "NettyH2DemuxRelayE2ETest#anUpstreamIgnoringTheErrorGoAwayIsClosedAfterTheHopsCloseGrace")
	void upstreamIgnoringTheGoAwayIsClosedAfterTheGrace() throws Exception {
		String route = route("ue.grace");
		ScriptedH2Upstream upstream = ScriptedH2Upstream.holdingOnEof(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			}
		});
		this.closeables.add(upstream);
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(route));
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			assertThat(connection.awaitEof(WITHIN)).as("the relay half-closed").isTrue();
			long eof = System.nanoTime();
			assertThat(connection.frames()).anyMatch(frame -> frame.type() == TestH2.GOAWAY);
			assertThat(connection.awaitClosed(WITHIN)).as("the relay closed the connection").isTrue();
			long elapsed = System.nanoTime() - eof;
			// the grace, not the idle timeout ending the relay
			assertThat(elapsed).as("closed after the grace")
				.isGreaterThan(Http2DemuxRelay.UPSTREAM_CLOSE_GRACE_NANOS / 2)
				.isLessThan(Http2DemuxRelay.UPSTREAM_CLOSE_GRACE_NANOS + TimeUnit.SECONDS.toNanos(1));
			assertThat(TestH2.status(client.frames(1))).isEqualTo("502");
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * An upstream connection error while its stream is the only one the client has open
	 * still ends only the leg: the client gets the {@code 502} and no GOAWAY, and its
	 * connection idles until the idle timeout closes it with a GOAWAY {@code NO_ERROR}
	 * naming stream 1.
	 */
	@Test
	@LegacyOnly("the idle timeout: a later migration step")
	void aConnectionErrorWithNothingElseOpenLeavesTheConnectionToTheIdleTimeout() throws Exception {
		String route = route("ue.idle");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			}
		});
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, get(route));
			assertThat(client.awaitEnd(1, WITHIN)).isTrue();
			long answered = System.nanoTime();
			assertThat(TestH2.status(client.frames(1))).isEqualTo("502");
			assertThat(client.awaitTrue(() -> client.goAwayCode() != -1, IDLE_TIMEOUT.plusSeconds(3)))
				.as("the idle timeout's GOAWAY")
				.isTrue();
			long idled = System.nanoTime() - answered;
			assertThat(client.goAwayCode()).isEqualTo(NO_ERROR);
			assertThat(client.goAwayLastStreamId()).isEqualTo(1);
			assertThat(idled).as("the connection lived on until the idle timeout")
				.isGreaterThan(IDLE_TIMEOUT.toNanos() - TimeUnit.MILLISECONDS.toNanos(500));
		}
	}

	/**
	 * The client going away while a failing leg writes what it lost: the write fails, the
	 * relay ends, and nothing stays held -- no connection, no tracked stream, no queued
	 * byte, no slot of the connection limit.
	 */
	@Test
	@LegacyOnly("the legacy relay's write seam; the netty engine's counterpart is "
			+ "NettyH2DemuxRelayE2ETest#theClientGoingAwayWhileItsLegFailsEndsTheRelay")
	void clientGoneWhileTheLegFailsReleasesEverything() throws Exception {
		String route = route("ue.client-gone");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS) {
				connection.write(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32(0));
			}
		});
		startClient(route, upstream);
		DataProxyServer proxy = dataProxyServer();
		Awaitility.await().atMost(WITHIN).until(() -> proxy.activeConnections() == 0);
		double admitted = gauge("sluice.data.connections.admitted");
		HeldWrite held = HeldWrite.install(proxy,
				new Http2DemuxRelay.WriteSeam.Decision(Http2DemuxRelay.WriteSeam.Site.RESET, 1));
		this.held.add(held);
		H2TestClient client = H2TestClient.connect(dataPort, 1);
		client.headers(1, true, get(route));
		held.awaitDecided();
		assertThat(client.frames(1)).as("decided before the 502 was written").isEmpty();
		assertThat(proxy.activeConnections()).as("the relay is live").isEqualTo(1);
		client.abort();
		Thread.sleep(200); // the client's reset reached the relay's socket
		held.proceed();
		Awaitility.await()
			.atMost(WITHIN)
			.until(() -> proxy.activeConnections() == 0 && proxy.h2DemuxMeters().trackedStreams(route) == 0
					&& queuedBytes(route) == 0 && gauge("sluice.data.connections.admitted") == admitted);
		assertThat(upstream.awaitConnection(0, WITHIN).awaitClosed(WITHIN)).as("the upstream connection was closed")
			.isTrue();
	}

	/**
	 * An upstream's GOAWAY naming stream 1 as the last it processed after it relayed a
	 * response head on stream 3: stream 3 is lost (RST_STREAM {@code INTERNAL_ERROR}, its
	 * response began), not refused; stream 1 completes.
	 */
	@Test
	void upstreamGoAwayResetsAStreamItAnsweredWithoutProcessingIt() throws Exception {
		String route = route("ue.goaway-answered");
		ScriptedH2Upstream upstream = upstream(new byte[0], (connection, frame) -> {
			if (frame.type() == TestH2.HEADERS && frame.streamId() == 3) {
				connection.head(3, false);
			}
		});
		startClient(route, upstream);
		try (H2TestClient client = H2TestClient.connect(dataPort, 1, 3)) {
			client.headers(1, true, get(route));
			client.headers(3, true, get(route));
			assertThat(client.awaitTrue(() -> !client.frames(3).isEmpty(), WITHIN)).as("stream 3's response head")
				.isTrue();
			ScriptedH2Upstream.Connection connection = upstream.awaitConnection(0, WITHIN);
			byte[] goAway = new byte[8];
			System.arraycopy(TestH2.int32(1), 0, goAway, 0, 4);
			connection.write(TestH2.GOAWAY, 0, 0, goAway);
			assertThat(client.awaitTrue(() -> client.reset(3) != null, WITHIN)).isTrue();
			assertThat(client.reset(3)).isEqualTo(INTERNAL_ERROR);
			connection.answer(1, "processed");
			assertThat(client.awaitEnd(1, WITHIN)).isTrue();
			assertThat(TestH2.bodyOf(client.frames(1))).isEqualTo("processed");
			assertThat(client.reset(1)).isNull();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * The route {@code name} of this invocation: the runs on both engines share the
	 * router, and a route registered by the other run's client would take the stream.
	 */
	private String route(String name) {
		return name + "." + String.valueOf(this.engine).toLowerCase(Locale.ROOT) + ".test";
	}

	/** The frame types of a {@code 502} followed by a reset, as the engine answers it. */
	private List<Integer> badGateway() {
		return this.engine == H2DemuxEngine.NETTY ? List.of(TestH2.HEADERS, TestH2.DATA, TestH2.RST_STREAM)
				: List.of(TestH2.HEADERS, TestH2.RST_STREAM);
	}

	/**
	 * A {@code 502} the relay answered on the stream: bodiless on the legacy engine, the
	 * bad-gateway page of the route and request on the netty engine.
	 */
	private void assertBadGateway(List<TestH2.Frame> frames, String route, String method) {
		assertThat(TestH2.status(frames)).isEqualTo("502");
		String body = this.engine == H2DemuxEngine.NETTY ? badGatewayPage(route, method) : "";
		assertThat(TestH2.bodyOf(frames)).as("the 502's body").isEqualTo(body);
	}

	private String badGatewayPage(String route, String method) {
		return new String(Objects.requireNonNull(this.errorResponse)
			.badGatewayPage(route, new ConnectionHeadParser.Head.Request(method, "/", "2"))
			.body(), StandardCharsets.UTF_8);
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** The types of the frames, the relay's own stream window grants left out. */
	private static List<Integer> types(List<TestH2.Frame> frames) {
		return frames.stream().map(TestH2.Frame::type).filter(type -> type != TestH2.WINDOW_UPDATE).toList();
	}

	private static byte[] get(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/");
	}

	private static byte[] post(String route) {
		return TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/");
	}

	private double counter(String name, @Nullable String reason) {
		MeterRegistry registry = Objects.requireNonNull(this.meterRegistry);
		return reason == null ? registry.get(name).counter().count()
				: registry.get(name).tag("reason", reason).counter().count();
	}

	private double gauge(String name) {
		return Objects.requireNonNull(this.meterRegistry).get(name).gauge().value();
	}

	/** The route's leg queue gauge; zero before it exists. */
	private long queuedBytes(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find(H2DemuxMeters.QUEUED_GAUGE_NAME)
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.max()
			.orElse(0));
	}

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private ScriptedH2Upstream upstream(byte[] settings,
			BiConsumer<ScriptedH2Upstream.Connection, TestH2.Frame> handler) throws Exception {
		ScriptedH2Upstream upstream = ScriptedH2Upstream.start(settings, handler);
		this.closeables.add(upstream);
		return upstream;
	}

	private void startClient(String host, ScriptedH2Upstream upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.port()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-ue").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/** SETTINGS payloads. */
	private static final class TestH2Settings {

		private TestH2Settings() {
		}

		static byte[] of(int id, long value) {
			return new byte[] { (byte) (id >>> 8), (byte) id, (byte) (value >>> 24), (byte) (value >>> 16),
					(byte) (value >>> 8), (byte) value };
		}

	}

	/**
	 * Waits until the relay handled every frame the client sent so far, {@code stream}'s
	 * HEADERS last: the relay handles a client's frames in order and acks its PING when
	 * it gets to it. A stream beyond the upstream's concurrency limit waits from then on.
	 */
	private static void awaitHandled(H2TestClient client, int stream) throws Exception {
		byte[] ping = Arrays.copyOf(TestH2.int32(stream), 8);
		client.frame(TestH2.PING, 0, 0, ping);
		assertThat(client.awaitTrue(() -> client.pingAcks().stream().anyMatch(ack -> Arrays.equals(ack, ping)), WITHIN))
			.as("the relay acked the PING after stream %d", stream)
			.isTrue();
	}

}
