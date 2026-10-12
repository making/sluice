package am.ik.sluice.spike.h2demux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static am.ik.sluice.spike.h2demux.Fixture.settled;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The riskiest point of moving the h2 demux onto netty's HTTP/2 codec: one client
 * connection demuxed to two legs over tunnel {@code VirtualConnection}s, with netty doing
 * the framing, the stream states and the flow control of every hop, while our own virtual
 * threads do blocking I/O. Each flow-control test has a negative control: the same
 * scenario with the mechanism under test switched off must break the bound.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NettyDemuxSpikeTest {

	/**
	 * ByteBufs garbage-collected unreleased, as netty's paranoid leak detector reports
	 * them.
	 */
	private static final AtomicInteger LEAKS = new AtomicInteger();

	static {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		// before any ByteBuf class initializes its detector
		ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(new ResourceLeakDetectorFactory() {

			@Override
			@SuppressWarnings("deprecation")
			public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource, int samplingInterval,
					long maxActive) {
				ResourceLeakDetector<T> detector = new ResourceLeakDetector<>(resource, samplingInterval);
				detector.setLeakListener((type, records) -> LEAKS.incrementAndGet());
				return detector;
			}

		});
	}

	@AfterAll
	static void noByteBufLeaked() throws InterruptedException {
		assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.PARANOID);
		for (int i = 0; i < 10; i++) {
			System.gc();
			Thread.sleep(100);
			// the detector reports what it collected as it tracks new buffers
			for (int j = 0; j < 100; j++) {
				ByteBufAllocator.DEFAULT.buffer(1).release();
			}
		}
		assertThat(LEAKS).hasValue(0);
	}

	private static final int WINDOW = Http2CodecUtil.DEFAULT_WINDOW_SIZE;

	private static final int MIB = 1024 * 1024;

	private static final TestClient.Request A_ECHO = new TestClient.Request("a.test", "/echo");

	private static final TestClient.Request B_ECHO = new TestClient.Request("b.test", "/echo");

	private static final byte[] NO_BODY = new byte[0];

	private static Fixture.Builder twoRoutes() {
		return Fixture.builder().route("a.test", TestUpstream.named("a")).route("b.test", TestUpstream.named("b"));
	}

	@Test
	void streamsOfOneConnectionReachTheUpstreamOfTheirAuthorityWithTheirIdsKept() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			List<TestClient.Sent> sent = List.of(fixture.client.send(A_ECHO, NO_BODY),
					fixture.client.send(B_ECHO, NO_BODY), fixture.client.send(A_ECHO, NO_BODY),
					fixture.client.send(B_ECHO, NO_BODY));

			List<String> bodies = sent.stream().map(s -> s.response().join().text()).toList();
			assertThat(bodies).containsExactly("upstream=a authority=a.test stream=1 bytes=0 crc=0",
					"upstream=b authority=b.test stream=3 bytes=0 crc=0",
					"upstream=a authority=a.test stream=5 bytes=0 crc=0",
					"upstream=b authority=b.test stream=7 bytes=0 crc=0");
			assertThat(fixture.relay.dialed()).containsExactly("a.test", "b.test");
			assertThat(fixture.upstream("a.test").connections()).isEqualTo(1);
			assertThat(fixture.upstream("b.test").connections()).isEqualTo(1);
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void aRequestBodyIsHeldToTheUpstreamWindowAcrossTheBridge() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			TestUpstream b = fixture.upstream("b.test");
			b.holdRequests();
			byte[] body = TestUpstream.pattern(MIB);

			TestClient.Sent upload = fixture.client.send(B_ECHO, body);

			long relayed = settled(() -> fixture.relay.fromClient(upload.streamId()),
					() -> b.received(upload.streamId()));
			// the upstream took its stream window and holds it; the relay holds at most
			// the window it announced to the client on top of it
			assertThat(b.received(upload.streamId())).isEqualTo(WINDOW);
			assertThat(relayed).isGreaterThan(WINDOW).isLessThanOrEqualTo(2L * WINDOW);
			// another stream of the same client connection, on the other leg, flows
			TestClient.Sent other = fixture.client.send(A_ECHO, TestUpstream.pattern(200 * 1024));
			assertThat(other.response().get(5, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=a authority=a.test stream=3 bytes=204800 crc="
						+ TestUpstream.crc(TestUpstream.pattern(200 * 1024)));
			assertThat(upload.response().isDone()).isFalse();

			b.release();
			assertThat(upload.response().get(10, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=b authority=b.test stream=1 bytes=1048576 crc=" + TestUpstream.crc(body));
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void consumingOnArrivalBuffersTheWholeRequestBodyInTheRelay() throws Exception {
		try (Fixture fixture = twoRoutes().relay(spec -> spec.consumeOnWrite(false)).start()) {
			TestUpstream b = fixture.upstream("b.test");
			b.holdRequests();

			TestClient.Sent upload = fixture.client.send(B_ECHO, TestUpstream.pattern(MIB));

			long relayed = settled(() -> fixture.relay.fromClient(upload.streamId()),
					() -> b.received(upload.streamId()));
			assertThat(b.received(upload.streamId())).isEqualTo(WINDOW);
			assertThat(relayed).isEqualTo(MIB);
			b.release();
			upload.response().get(10, TimeUnit.SECONDS);
		}
	}

	@Test
	void withoutAutoRefillAHeldStreamStarvesTheOtherStreamsOfTheConnection() throws Exception {
		try (Fixture fixture = twoRoutes().relay(spec -> spec.autoRefillConnectionWindow(false)).start()) {
			TestUpstream b = fixture.upstream("b.test");
			b.holdRequests();
			TestClient.Sent upload = fixture.client.send(B_ECHO, TestUpstream.pattern(MIB));
			settled(() -> fixture.relay.fromClient(upload.streamId()), () -> b.received(upload.streamId()));

			TestClient.Sent other = fixture.client.send(A_ECHO, TestUpstream.pattern(200 * 1024));

			Thread.sleep(2000);
			assertThat(other.response().isDone()).isFalse();
			assertThat(fixture.relay.fromClient(other.streamId())).isZero();
			b.release();
			other.response().get(10, TimeUnit.SECONDS);
			upload.response().get(10, TimeUnit.SECONDS);
		}
	}

	@Test
	void aResponseBodyIsHeldToTheClientWindowAcrossTheBridge() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			fixture.client.holdResponses();

			TestClient.Sent download = fixture.client.send(new TestClient.Request("a.test", "/download/" + MIB),
					NO_BODY);

			long relayed = settled(() -> fixture.relay.fromUpstream(download.streamId()),
					() -> fixture.client.received(download.streamId()));
			assertThat(fixture.client.received(download.streamId())).isEqualTo(WINDOW);
			assertThat(relayed).isGreaterThan(WINDOW).isLessThanOrEqualTo(2L * WINDOW);

			fixture.client.releaseResponses();
			TestClient.Response response = download.response().get(10, TimeUnit.SECONDS);
			assertThat(response.status()).isEqualTo("200");
			assertThat(TestUpstream.crc(response.body())).isEqualTo(TestUpstream.crc(TestUpstream.pattern(MIB)));
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void consumingOnArrivalBuffersTheWholeResponseBodyInTheRelay() throws Exception {
		try (Fixture fixture = twoRoutes().relay(spec -> spec.consumeOnWrite(false)).start()) {
			fixture.client.holdResponses();

			TestClient.Sent download = fixture.client.send(new TestClient.Request("a.test", "/download/" + MIB),
					NO_BODY);

			long relayed = settled(() -> fixture.relay.fromUpstream(download.streamId()),
					() -> fixture.client.received(download.streamId()));
			assertThat(fixture.client.received(download.streamId())).isEqualTo(WINDOW);
			assertThat(relayed).isEqualTo(MIB);
			fixture.client.releaseResponses();
			download.response().get(10, TimeUnit.SECONDS);
		}
	}

	@Test
	void aClientNotReadingItsSocketStopsTheUpstreamThroughWritability() throws Exception {
		int length = 16 * MIB;
		try (Fixture fixture = twoRoutes().clientSettings(new Http2Settings().initialWindowSize(64 * MIB)).start()) {
			fixture.client.growConnectionWindow(64 * MIB);
			awaitTrue(() -> fixture.relay.clientConnectionWindow() > 64 * MIB);
			fixture.client.pauseReading();

			TestClient.Sent download = fixture.client.send(new TestClient.Request("a.test", "/download/" + length),
					NO_BODY);

			long relayed = settled(() -> fixture.relay.fromUpstream(download.streamId()),
					() -> fixture.client.received(download.streamId()));
			// socket buffers + the writer queue's high water + the leg window, far below
			// the 64MiB the client's windows allow
			assertThat(relayed).isLessThan(2L * MIB);
			assertThat(fixture.relay.clientEverUnwritable()).isTrue();

			fixture.client.resumeReading();
			TestClient.Response response = download.response().get(20, TimeUnit.SECONDS);
			assertThat(response.body()).hasSize(length);
			assertThat(TestUpstream.crc(response.body())).isEqualTo(TestUpstream.crc(TestUpstream.pattern(length)));
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void withoutWritabilityFeedbackTheRelayQueuesWhatTheClientWindowAllows() throws Exception {
		int length = 16 * MIB;
		try (Fixture fixture = twoRoutes().clientSettings(new Http2Settings().initialWindowSize(64 * MIB))
			.relay(spec -> spec.writabilityFeedback(false))
			.start()) {
			fixture.client.growConnectionWindow(64 * MIB);
			awaitTrue(() -> fixture.relay.clientConnectionWindow() > 64 * MIB);
			fixture.client.pauseReading();

			TestClient.Sent download = fixture.client.send(new TestClient.Request("a.test", "/download/" + length),
					NO_BODY);

			long relayed = settled(() -> fixture.relay.fromUpstream(download.streamId()),
					() -> fixture.client.received(download.streamId()));
			assertThat(relayed).isEqualTo(length);
			assertThat(fixture.relay.clientEverUnwritable()).isFalse();
			fixture.client.resumeReading();
			download.response().get(20, TimeUnit.SECONDS);
		}
	}

	@Test
	void aClientResetReachesTheUpstreamAndFreesTheStreamOnBothHops() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			TestUpstream b = fixture.upstream("b.test");
			TestClient.Sent hanging = fixture.client.sendOpen(new TestClient.Request("b.test", "/hang"),
					TestUpstream.pattern(32 * 1024));
			awaitTrue(() -> b.received(hanging.streamId()) == 32 * 1024);

			fixture.client.reset(hanging.streamId(), Http2Error.CANCEL.code());

			awaitTrue(() -> b.resets().equals(Map.of(1, Http2Error.CANCEL.code())), () -> "resets=" + b.resets());
			awaitTrue(
					() -> fixture.relay.clientActiveStreams() == 0 && fixture.relay.legActiveStreams("b.test") == 0
							&& fixture.relay.exchanges() == 0,
					() -> "client=%d leg=%d exchanges=%d".formatted(fixture.relay.clientActiveStreams(),
							fixture.relay.legActiveStreams("b.test"), fixture.relay.exchanges()));
			// the leg and both hops' windows are intact: 300KiB pass the same leg
			byte[] body = TestUpstream.pattern(300 * 1024);
			TestClient.Sent next = fixture.client.send(B_ECHO, body);
			assertThat(next.response().get(10, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=b authority=b.test stream=3 bytes=307200 crc=" + TestUpstream.crc(body));
			assertThat(fixture.relay.dialed()).containsExactly("b.test");
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void anUpstreamResetReachesTheClientAndTheOtherLegGoesOn() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			TestClient.Sent reset = fixture.client.send(new TestClient.Request("a.test", "/reset"), NO_BODY);
			TestClient.Sent other = fixture.client.send(B_ECHO, NO_BODY);

			assertThat(reset.response().get(10, TimeUnit.SECONDS).resetCode())
				.isEqualTo(Http2Error.ENHANCE_YOUR_CALM.code());
			assertThat(other.response().get(10, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=b authority=b.test stream=3 bytes=0 crc=0");
			awaitTrue(() -> fixture.relay.clientActiveStreams() == 0 && fixture.relay.exchanges() == 0);
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void streamsBeyondTheUpstreamConcurrencyLimitWaitInTheRelay() throws Exception {
		TestUpstream limited = new TestUpstream("c", new Http2Settings().maxConcurrentStreams(1));
		try (Fixture fixture = Fixture.builder().route("c.test", limited).start()) {
			TestClient.Request slow = new TestClient.Request("c.test", "/slow");
			List<TestClient.Sent> sent = List.of(fixture.client.send(slow, NO_BODY), fixture.client.send(slow, NO_BODY),
					fixture.client.send(slow, NO_BODY));

			for (TestClient.Sent s : sent) {
				assertThat(s.response().get(10, TimeUnit.SECONDS).status()).isEqualTo("200");
			}
			assertThat(limited.maxActive()).isEqualTo(1);
			assertThat(limited.streamIds()).containsExactly(1, 3, 5);
			assertThat(limited.resets()).isEmpty();
			assertThat(fixture.relay.failures()).isEmpty();
		}
	}

	@Test
	void anUpstreamProtocolErrorEndsOnlyItsLeg() throws Exception {
		try (Fixture fixture = twoRoutes().start()) {
			TestClient.Sent malformed = fixture.client.send(new TestClient.Request("a.test", "/malformed"), NO_BODY);
			TestClient.Sent other = fixture.client.send(new TestClient.Request("b.test", "/slow"), NO_BODY);

			// netty's frame reader rejects the SETTINGS on a stream; the leg sends the
			// upstream a GOAWAY PROTOCOL_ERROR and closes, and its stream is answered 502
			assertThat(malformed.response().get(10, TimeUnit.SECONDS).status()).isEqualTo("502");
			awaitTrue(() -> fixture.upstream("a.test").goAwayCode() == Http2Error.PROTOCOL_ERROR.code());
			assertThat(other.response().get(10, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=b authority=b.test stream=3 bytes=0 crc=0");
			// the client's connection survives: the route's next stream opens a new leg
			TestClient.Sent next = fixture.client.send(A_ECHO, NO_BODY);
			assertThat(next.response().get(10, TimeUnit.SECONDS).text())
				.isEqualTo("upstream=a authority=a.test stream=5 bytes=0 crc=0");
			assertThat(fixture.relay.dialed()).containsExactly("a.test", "b.test", "a.test");
			assertThat(fixture.client.goAwayCode()).isEqualTo(-1);
			assertThat(fixture.client.open()).isTrue();
		}
	}

	private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("condition not met within 10s");
			}
			Thread.sleep(20);
		}
	}

	private static void awaitTrue(BooleanSupplier condition, Supplier<String> state) throws InterruptedException {
		try {
			awaitTrue(condition);
		}
		catch (AssertionError e) {
			throw new AssertionError(e.getMessage() + ": " + state.get(), e);
		}
	}

}
