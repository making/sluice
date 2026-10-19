package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.ResourceLeakDetector;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.auth.PasswordEncoders;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * h2 streams whose credential check is slow ({@code bcrypt}): the check runs off the
 * relay's loop, the stream parked meanwhile, so the connection's other streams -- of
 * other routes and of the same route -- flow while it runs, and the parked stream is
 * relayed, with what the client sent while it was parked, once the check passed, or
 * answered once it failed. A parked stream the client resets, or whose connection goes
 * away, is dropped: the check's end finds nothing to resume. A check that throws resets
 * its stream; one that does not end by the wait deadline is refused; parked streams past
 * the connection's allowance are refused while the budget is exhausted. A stream parked
 * while its route's client withdraws keeps that client's leg open, is relayed there, and
 * then the leg closes, ending the client's drain. The checks are held by the server's
 * password encoder, replaced here by one holding a credential's check until the test lets
 * it go. The relay's exception sink stays silent but for the check that throws, and with
 * netty's paranoid leak detection on no buffer leaks.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Import(H2DemuxCredentialCheckE2ETest.HeldChecks.class)
class H2DemuxCredentialCheckE2ETest {

	/**
	 * A route whose bearer token is checked against bcrypt, its API token against sha256.
	 */
	private static final String GATED = "gated.check.test";

	private static final String OPEN = "open.check.test";

	/** A route requiring credentials, served by a client that withdraws. */
	private static final String WITHDRAWING = "withdrawing.check.test";

	private static final String FAST_TOKEN = "fast-token";

	private static final int INTERNAL_ERROR = 0x2;

	private static final int CANCEL = 0x8;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	/** {@code sluice.limits.h2-stream-wait-timeout} of this class. */
	private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

	/** Room for some of what the parked streams of the budget test hold, not for all. */
	private static final long BUDGET = 256 * 1024;

	/** What each parked POST of the budget test sends. */
	private static final int PARKED_BODY = H2TestClient.INITIAL_WINDOW;

	/** How long the client sends on a parked stream after its last grant. */
	private static final Duration QUIET = Duration.ofMillis(300);

	/** The checks held, by the raw credential, until the test lets them go. */
	private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

	/** The checks entered, by the raw credential. */
	private static final Map<String, AtomicInteger> ENTERED = new ConcurrentHashMap<>();

	/** The checks ended, by the raw credential. */
	private static final Map<String, AtomicInteger> ENDED = new ConcurrentHashMap<>();

	private static final ListAppender<ILoggingEvent> RELAY_EVENTS = new ListAppender<>();

	private static final ListAppender<ILoggingEvent> LEAK_EVENTS = new ListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	/** The upstream's connections, in accept order. */
	private final List<RecordingUpstream> upstreams = new CopyOnWriteArrayList<>();

	private ResourceLeakDetector.@Nullable Level leakLevel;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private SessionRegistry sessions;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.limits.h2-stream-wait-timeout", () -> WAIT_TIMEOUT.toMillis() + "ms");
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("sluice.route.auth.[" + GATED + "].bearer[0]",
				() -> "{bcrypt}" + new BCryptPasswordEncoder(4).encode("unused"));
		registry.add("sluice.route.auth.[" + GATED + "].api-token[0]", () -> "{sha256}" + sha256Hex(FAST_TOKEN));
		registry.add("sluice.route.auth.[" + WITHDRAWING + "].bearer[0]",
				() -> "{bcrypt}" + new BCryptPasswordEncoder(4).encode("unused"));
		registry.add("sluice.route.auth.[" + WITHDRAWING + "].api-token[0]", () -> "{sha256}" + sha256Hex(FAST_TOKEN));
	}

	/**
	 * Replaces the server's password encoder: a raw credential {@code gated-*} checked
	 * against a bcrypt secret is held until the test lets it go, then matches -- unless
	 * it ends with {@code -wrong}, or with {@code -failing}, which throws.
	 */
	@TestConfiguration
	static class HeldChecks {

		@Bean
		@Primary
		PasswordEncoder heldPasswordEncoder() {
			PasswordEncoder delegate = PasswordEncoders.delegating();
			return new PasswordEncoder() {

				@Override
				public @Nullable String encode(@Nullable CharSequence rawPassword) {
					return delegate.encode(rawPassword);
				}

				@Override
				public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
					String raw = String.valueOf(rawPassword);
					if (encodedPassword == null || !encodedPassword.startsWith("{bcrypt}")
							|| !raw.startsWith("gated-")) {
						return delegate.matches(rawPassword, encodedPassword);
					}
					ENTERED.computeIfAbsent(raw, key -> new AtomicInteger()).incrementAndGet();
					try {
						gate(raw).await();
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					finally {
						ENDED.computeIfAbsent(raw, key -> new AtomicInteger()).incrementAndGet();
					}
					if (raw.endsWith("-failing")) {
						throw new AssertionError("the check failed");
					}
					return !raw.endsWith("-wrong");
				}

			};
		}

	}

	@BeforeAll
	void start() throws IOException {
		this.leakLevel = ResourceLeakDetector.getLevel();
		ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
		attach(RELAY_EVENTS, NettyHttp2DemuxRelay.class.getName());
		attach(LEAK_EVENTS, "io.netty.util.ResourceLeakDetector");
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		this.closeables.add(server);
		Thread.ofVirtual().name("e2e-check-accept").start(() -> accept(server));
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.token("it-token")
				.upstream(Upstream.builder().host(GATED).target("tcp://127.0.0.1:" + server.getLocalPort()).build())
				.upstream(Upstream.builder().host(OPEN).target("tcp://127.0.0.1:" + server.getLocalPort()).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-check-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> router().lookup(GATED).isPresent() && router().lookup(OPEN).isPresent());
	}

	@BeforeEach
	void awaitQuiet() {
		Awaitility.await().atMost(TIMEOUT).until(() -> admitted() == 0 && server().activeConnections() == 0);
		this.upstreams.clear();
		RELAY_EVENTS.list.clear();
	}

	@AfterEach
	void relayEnded() {
		Awaitility.await("every relay ended")
			.atMost(TIMEOUT)
			.until(() -> admitted() == 0 && server().activeConnections() == 0 && hopThreads().isEmpty());
		assertThat(held()).as("nothing held by any relay").isZero();
	}

	@AfterAll
	void tearDown() throws Exception {
		try {
			GATES.values().forEach(CountDownLatch::countDown);
			// a leaked buffer is reported once collected and a later allocation polls
			for (int i = 0; i < 3; i++) {
				System.gc();
				try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
					h2.headers(1, true, get(OPEN, "/gc"));
					h2.awaitEnd(1, TIMEOUT);
				}
				TimeUnit.MILLISECONDS.sleep(100);
			}
			assertThat(LEAK_EVENTS.list).as("netty's leak reports").isEmpty();
		}
		finally {
			ResourceLeakDetector.setLevel(Objects.requireNonNull(this.leakLevel));
			this.clients.forEach(TunnelClient::stop);
			this.closeables.forEach(H2DemuxCredentialCheckE2ETest::close);
			((Logger) LoggerFactory.getLogger(NettyHttp2DemuxRelay.class)).detachAppender(RELAY_EVENTS);
			((Logger) LoggerFactory.getLogger("io.netty.util.ResourceLeakDetector")).detachAppender(LEAK_EVENTS);
		}
	}

	/**
	 * Stream 1's check is held, and stream 3's, of a wrong credential: stream 5 of
	 * another route and stream 7 of the same route (an API token, checked at once)
	 * complete meanwhile, nothing of streams 1 and 3 reaching the upstream or the client;
	 * once let go, stream 1 is relayed with its body and without its credential -- as the
	 * upstream's stream 9, stream 7 having gone first --, and stream 3 is answered 401.
	 */
	@Test
	void aParkedStreamHoldsNoOtherStreamAndIsRelayedOnceItsCheckPasses() throws Exception {
		String token = "gated-pass";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3, 5, 7)) {
			h2.headers(1, false, post(GATED, "/parked", "authorization=Bearer " + token));
			h2.sendData(1, bytes("parked-body"), TestH2.FLAG_END_STREAM);
			h2.headers(3, false, post(GATED, "/wrong", "authorization=Bearer " + token + "-wrong"));
			h2.sendData(3, bytes("refused-body"), 0);
			awaitEntered(token);
			awaitEntered(token + "-wrong");

			h2.headers(5, true, get(OPEN, "/open"));
			assertThat(h2.awaitEnd(5, TIMEOUT)).as("another route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(5))).isEqualTo("ok5 0");
			h2.headers(7, true, get(GATED, "/fast", "x-api-token=" + FAST_TOKEN));
			assertThat(h2.awaitEnd(7, TIMEOUT)).as("the same route's stream completes").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(7))).isEqualTo("ok7 0");
			assertThat(h2.frames(1)).as("nothing of the parked streams yet").isEmpty();
			assertThat(h2.frames(3)).isEmpty();
			assertThat(paths()).as("the parked streams reached no upstream").doesNotContain("/parked", "/wrong");

			open(token);
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("the parked stream completes").isTrue();
			assertThat(TestH2.status(h2.frames(1))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(1))).as("relayed with its body, as the leg's next stream")
				.isEqualTo("ok9 " + "parked-body".length());
			Http2Headers relayed = head("/parked");
			assertThat(relayed.get("authorization")).as("the credential removed").isNull();

			open(token + "-wrong");
			assertThat(h2.awaitEnd(3, TIMEOUT)).isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("401");
			assertThat(paths()).doesNotContain("/wrong");
		}
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * A stream parked on a route whose client withdraws meanwhile: the client's leg on
	 * the connection, idle, stays open for it -- the client's session keeps the leg's
	 * connection --, the stream is relayed there once its check passes, and then the leg
	 * closes, ending the client's drain well before its grace.
	 */
	@Test
	void aStreamParkedWhileItsClientWithdrawsIsRelayedOnTheClientsLeg() throws Exception {
		String clientId = "check-withdrawing";
		ServerSocket server = (ServerSocket) this.closeables.get(0);
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.clientId(clientId)
				.token("it-token")
				.drainGrace(Duration.ofMinutes(1))
				.upstream(
						Upstream.builder().host(WITHDRAWING).target("tcp://127.0.0.1:" + server.getLocalPort()).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-check-withdrawing").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> router().lookup(WITHDRAWING).isPresent());
		TunnelSession session = sessions().find(clientId).orElseThrow();
		String token = "gated-withdrawing";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			// the leg opens for a stream checked at once
			h2.headers(1, true, get(WITHDRAWING, "/first", "x-api-token=" + FAST_TOKEN));
			assertThat(h2.awaitEnd(1, TIMEOUT)).isTrue();
			h2.headers(3, true, get(WITHDRAWING, "/parked", "authorization=Bearer " + token));
			awaitEntered(token);
			CountDownLatch stopped = new CountDownLatch(1);
			Thread.ofVirtual().start(() -> client.stop(stopped::countDown));
			Awaitility.await().atMost(TIMEOUT).until(session::withdrawn);
			Awaitility.await("the leg is kept for the parked stream")
				.during(Duration.ofMillis(500))
				.atMost(TIMEOUT)
				.until(() -> session.connectionCount() == 1);

			open(token);
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the parked stream completes").isTrue();
			assertThat(TestH2.status(h2.frames(3))).isEqualTo("200");
			assertThat(TestH2.bodyOf(h2.frames(3))).as("on the leg of stream 1").isEqualTo("ok3 0");
			assertThat(stopped.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).as("the client's drain ended").isTrue();
		}
		assertThat(paths()).contains("/first", "/parked");
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * The client resets a parked stream: it is dropped -- once its check passes nothing
	 * of it reaches the upstream or the client -- and the connection goes on.
	 */
	@Test
	void aParkedStreamTheClientResetsIsDropped() throws Exception {
		String token = "gated-reset";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, false, post(GATED, "/reset", "authorization=Bearer " + token));
			h2.sendData(1, bytes("dropped"), 0);
			awaitEntered(token);
			Awaitility.await("the parked stream accounted").atMost(TIMEOUT).until(() -> held() > 0);
			h2.frame(TestH2.RST_STREAM, 0, 1, TestH2.int32(CANCEL));
			Awaitility.await("the parked stream let go").atMost(TIMEOUT).until(() -> held() == 0);

			open(token);
			awaitEnded(token);
			h2.headers(3, true, get(OPEN, "/after"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
			assertThat(h2.frames(1)).as("nothing of the dropped stream").isEmpty();
			assertThat(paths()).doesNotContain("/reset");
		}
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * The client goes away (a reset) while a stream is parked: the relay ends -- its
	 * connection count, admission slot, hop threads -- and the check's end later finds
	 * nothing to resume.
	 */
	@Test
	void theClientGoingAwayWhileAStreamIsParkedEndsTheRelay() throws Exception {
		String token = "gated-gone";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, get(GATED, "/gone", "authorization=Bearer " + token));
			awaitEntered(token);
			h2.abort();
		}
		Awaitility.await("the relay ended")
			.atMost(TIMEOUT)
			.until(() -> admitted() == 0 && server().activeConnections() == 0 && hopThreads().isEmpty());
		open(token);
		awaitEnded(token);
		assertThat(paths()).doesNotContain("/gone");
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * A client going away gracefully -- a GOAWAY, then the half-close of its connection
	 * --: with a parked stream's request ended the relay drains -- the stream is relayed
	 * once its check passes, then the relay ends --; with one unfinished the relay ends
	 * at once.
	 */
	@Test
	void theClientGoingAwayGracefullyWhileAStreamIsParked() throws Exception {
		String ended = "gated-half-closed";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, true, get(GATED, "/drained", "authorization=Bearer " + ended));
			awaitEntered(ended);
			h2.frame(TestH2.GOAWAY, 0, 0, new byte[8]);
			h2.halfClose();
			open(ended);
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("the parked stream completes").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo("ok1 0");
			assertThat(h2.awaitTrue(h2::closed, TIMEOUT)).as("the relay ends after it").isTrue();
		}
		String unfinished = "gated-half-closed-unfinished";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1)) {
			h2.headers(1, false, post(GATED, "/unfinished", "authorization=Bearer " + unfinished));
			awaitEntered(unfinished);
			h2.halfClose();
			assertThat(h2.awaitTrue(h2::closed, TIMEOUT)).as("the relay ends at once").isTrue();
		}
		open(unfinished);
		awaitEnded(unfinished);
		assertThat(paths()).containsExactly("/drained");
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * A check that throws resets its stream ({@code INTERNAL_ERROR}) with a warning; the
	 * connection goes on.
	 */
	@Test
	void aCheckThatThrowsResetsItsStream() throws Exception {
		String token = "gated-failing";
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			h2.headers(1, false, post(GATED, "/failing", "authorization=Bearer " + token));
			h2.sendData(1, bytes("unchecked"), 0);
			awaitEntered(token);
			open(token);
			assertThat(h2.awaitTrue(() -> h2.reset(1) != null, TIMEOUT)).as("stream 1 reset").isTrue();
			assertThat(h2.reset(1)).isEqualTo(INTERNAL_ERROR);
			h2.headers(3, true, get(OPEN, "/after"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
		}
		assertThat(paths()).doesNotContain("/failing");
		assertThat(warnings()).singleElement()
			.asString()
			.startsWith("h2 demux: the credential check of a stream of route " + GATED + " failed")
			.contains("the check failed");
	}

	/**
	 * A check that does not end by the wait deadline: its stream is refused
	 * ({@code REFUSED_STREAM}) at the deadline, not before, and counted; the check's end
	 * later finds nothing to resume, and the connection goes on.
	 */
	@Test
	void aCheckNotEndingByTheWaitDeadlineIsRefused() throws Exception {
		String token = "gated-deadline";
		double refusedBefore = refused("wait-timeout");
		try (H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			long sent = System.nanoTime();
			h2.headers(1, true, get(GATED, "/deadline", "authorization=Bearer " + token));
			awaitEntered(token);
			assertThat(h2.awaitTrue(() -> h2.reset(1) != null, WAIT_TIMEOUT.plus(TIMEOUT))).as("stream 1 reset")
				.isTrue();
			long elapsed = System.nanoTime() - sent;
			assertThat(h2.reset(1)).isEqualTo(TestH2.REFUSED_STREAM);
			assertThat(elapsed).as("refused at the deadline").isGreaterThanOrEqualTo(WAIT_TIMEOUT.toNanos());
			assertThat(refused("wait-timeout")).isEqualTo(refusedBefore + 1);
			open(token);
			awaitEnded(token);
			h2.headers(3, true, get(OPEN, "/after"));
			assertThat(h2.awaitEnd(3, TIMEOUT)).as("the connection goes on").isTrue();
			assertThat(h2.frames(1)).extracting(TestH2.Frame::type).containsExactly(TestH2.RST_STREAM);
		}
		assertThat(paths()).doesNotContain("/deadline");
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	/**
	 * Parked POSTs sending 64KiB each, all with the one credential under check, exhaust
	 * the budget: the window is withheld and the parked streams past the connection's
	 * allowance are refused ({@code REFUSED_STREAM}), counted, while stream 1 stays; once
	 * the check -- one for them all -- passes, the streams kept are relayed, the refused
	 * ones are not.
	 */
	@Test
	void parkedStreamsExhaustingTheBudgetAreRefusedPastTheAllowance() throws Exception {
		assertThat(budgetGauge()).as("the configured budget took effect").isEqualTo(BUDGET);
		String token = "gated-budget";
		double withheldBefore = withheld();
		double refusedBefore = refused("waiting");
		List<Integer> streams = List.of(1, 3, 5, 7, 9, 11, 13, 15);
		try (H2TestClient h2 = H2TestClient.connect(dataPort, streams.toArray(Integer[]::new))) {
			for (int streamId : streams) {
				h2.headers(streamId, false, post(GATED, "/budget" + streamId, "authorization=Bearer " + token));
				h2.fill(streamId, PARKED_BODY, QUIET);
				// the relay reads and accounts it, and withholds past the budget
				Thread.sleep(QUIET.toMillis());
			}
			assertThat(withheld()).as("the window was withheld").isGreaterThan(withheldBefore);
			Map<Integer, Integer> refused = h2.resets();
			assertThat(refused).as("the parked streams past the allowance were refused")
				.isNotEmpty()
				.doesNotContainKey(1)
				.allSatisfy((streamId, code) -> assertThat(code).isEqualTo(TestH2.REFUSED_STREAM));
			assertThat(refused("waiting") - refusedBefore).isEqualTo(refused.size());

			open(token);
			List<Integer> kept = streams.stream().filter(streamId -> !refused.containsKey(streamId)).toList();
			for (int streamId : kept) {
				h2.sendData(streamId, new byte[0], TestH2.FLAG_END_STREAM);
			}
			assertThat(h2.awaitTrue(() -> kept.stream().allMatch(h2::ended), TIMEOUT)).as("the streams kept complete")
				.isTrue();
			for (int streamId : kept) {
				assertThat(TestH2.bodyOf(h2.frames(streamId))).endsWith(" " + h2.streamSent(streamId));
			}
			assertThat(h2.resets()).as("nothing refused after the check").isEqualTo(refused);
			assertThat(paths()).as("the refused streams reached no upstream")
				.containsExactlyInAnyOrderElementsOf(kept.stream().map(streamId -> "/budget" + streamId).toList());
		}
		assertThat(ENTERED.get(token)).as("one check for every stream").hasValue(1);
		assertThat(warnings()).as("the relay's exception sink").isEmpty();
	}

	private static CountDownLatch gate(String raw) {
		return GATES.computeIfAbsent(raw, key -> new CountDownLatch(1));
	}

	private static void open(String raw) {
		gate(raw).countDown();
	}

	private static void awaitEntered(String raw) {
		Awaitility.await("the check of " + raw + " in flight")
			.atMost(TIMEOUT)
			.until(() -> ENTERED.containsKey(raw) && ENTERED.get(raw).get() > 0);
	}

	private static void awaitEnded(String raw) {
		Awaitility.await("the check of " + raw + " ended")
			.atMost(TIMEOUT)
			.until(() -> ENDED.containsKey(raw) && ENDED.get(raw).get() > 0);
	}

	/** The paths of every head the upstream got, in arrival order per connection. */
	private List<String> paths() {
		List<String> paths = new ArrayList<>();
		for (RecordingUpstream upstream : this.upstreams) {
			upstream.heads().values().forEach(head -> paths.add(String.valueOf(head.path())));
		}
		return paths;
	}

	/** The head of the path the upstream got. */
	private Http2Headers head(String path) {
		for (RecordingUpstream upstream : this.upstreams) {
			for (Http2Headers head : upstream.heads().values()) {
				if (path.contentEquals(String.valueOf(head.path()))) {
					return head;
				}
			}
		}
		throw new AssertionError("no head of " + path);
	}

	private void accept(ServerSocket server) {
		while (!server.isClosed()) {
			try {
				Socket socket = server.accept();
				this.closeables.add(socket);
				Thread.ofVirtual().name("e2e-check-upstream").start(() -> {
					try (socket) {
						WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
						if (upstream != null) {
							RecordingUpstream recording = new RecordingUpstream(upstream);
							this.upstreams.add(recording);
							recording.serve();
						}
					}
					catch (IOException | Http2Exception e) {
						// connection ended
					}
				});
			}
			catch (IOException e) {
				return;
			}
		}
	}

	private static byte[] get(String authority, String path, String... headers) {
		return head("GET", authority, path, headers);
	}

	private static byte[] post(String authority, String path, String... headers) {
		return head("POST", authority, path, headers);
	}

	private static byte[] head(String method, String authority, String path, String... headers) {
		List<String> fields = new ArrayList<>(
				List.of(":authority=" + authority, ":method=" + method, ":scheme=http", ":path=" + path));
		fields.addAll(List.of(headers));
		return TestH2.headerBlock(fields.toArray(String[]::new));
	}

	private static byte[] bytes(String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}

	private static String sha256Hex(String value) {
		try {
			return HexFormat.of()
				.formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private long held() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get(H2DemuxMeters.HELD_GAUGE_NAME).gauge().value());
	}

	private long budgetGauge() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.budget.bytes").gauge().value());
	}

	private double withheld() {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.window.withheld")
			.tag("reason", "budget")
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

	private static List<String> warnings() {
		return List.copyOf(RELAY_EVENTS.list)
			.stream()
			.filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
			.map(ILoggingEvent::getFormattedMessage)
			.toList();
	}

	/**
	 * The live threads of the netty relays' hops and loops, virtual ones included (which
	 * {@link Thread#getAllStackTraces()} leaves out).
	 */
	private static List<String> hopThreads() throws IOException {
		Path dump = Files.createTempDirectory("sluice-thread-dump").resolve("threads.txt");
		try {
			// dumpThreads refuses an existing path, hence the fresh directory per call
			ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
				.dumpThreads(dump.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
			return Files.readAllLines(dump).stream().filter(line -> line.contains("\"sluice-h2n")).toList();
		}
		finally {
			Files.deleteIfExists(dump);
			Files.deleteIfExists(dump.getParent());
		}
	}

	private static void attach(ListAppender<ILoggingEvent> appender, String logger) {
		appender.start();
		((Logger) LoggerFactory.getLogger(logger)).addAppender(appender);
	}

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.admitted").gauge().value();
	}

	private DataProxyServer server() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
	}

	private SessionRegistry sessions() {
		return Objects.requireNonNull(this.sessions, "sessions");
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
	 * One upstream connection: announces its SETTINGS, grants back every DATA frame as it
	 * reads it, records each stream's head, and answers a stream once its request ended
	 * ({@code :status 200}, body {@code ok<stream id> <request body bytes>}).
	 */
	private static final class RecordingUpstream {

		private final WindowedH2Upstream upstream;

		private final Map<Integer, Http2Headers> heads = new LinkedHashMap<>();

		RecordingUpstream(WindowedH2Upstream upstream) {
			this.upstream = upstream;
		}

		void serve() throws IOException, Http2Exception {
			this.upstream.write(TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = this.upstream.next()) != null) {
				int streamId = frame.streamId();
				if (frame.type() == TestH2.HEADERS) {
					Http2Headers head = new DefaultHttp2HeadersDecoder(true).decodeHeaders(streamId,
							Unpooled.wrappedBuffer(frame.payload()));
					synchronized (this) {
						this.heads.putIfAbsent(streamId, head);
					}
				}
				if (frame.type() == TestH2.DATA && frame.payload().length > 0) {
					this.upstream.grant(streamId, frame.payload().length);
					this.upstream.grant(0, frame.payload().length);
				}
				boolean ended = (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					this.upstream.headers(streamId, false);
					this.upstream.write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId,
							("ok" + streamId + " " + this.upstream.received(streamId))
								.getBytes(StandardCharsets.US_ASCII));
				}
			}
		}

		synchronized Map<Integer, Http2Headers> heads() {
			return new LinkedHashMap<>(this.heads);
		}

	}

}
