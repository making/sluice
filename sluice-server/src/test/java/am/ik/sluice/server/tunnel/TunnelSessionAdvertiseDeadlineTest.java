package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.KeepAlive;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A tunnel stream becomes its client id's session only with its first {@code Advertise},
 * and is failed when that does not arrive within the advertise timeout. Until then it
 * serves no connection and leaves the client's live session -- and its routes -- in
 * place.
 */
@Timeout(30)
class TunnelSessionAdvertiseDeadlineTest {

	private static final String CLIENT_ID = "client-a";

	private static final int LISTEN_PORT = 19124;

	private static final Duration TIMEOUT = Duration.ofMillis(300);

	private final SessionRegistry registry = new SessionRegistry(SluiceServerProperties.builder().build());

	private final Router router = new Router();

	private final List<Set<Integer>> reconciles = new CopyOnWriteArrayList<>();

	private final AtomicInteger timedOut = new AtomicInteger();

	private final List<TunnelSession> sessions = new CopyOnWriteArrayList<>();

	@AfterEach
	void closeSessions() {
		this.sessions.forEach(TunnelSession::close);
	}

	private TunnelSession session(String clientId, RecordingStream stream, Duration advertiseTimeout) {
		TunnelSession session = TunnelSession.builder()
			.clientId(clientId)
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(this.registry)
			.tcpRoutes((id, listenPorts) -> {
				this.reconciles.add(Set.copyOf(listenPorts));
				return Set.of();
			})
			.advertiseTimeout(advertiseTimeout)
			.advertiseTimedOut(this.timedOut::incrementAndGet)
			.build();
		this.sessions.add(session);
		session.start();
		return session;
	}

	private TunnelSession session(RecordingStream stream, Duration advertiseTimeout) {
		return session(CLIENT_ID, stream, advertiseTimeout);
	}

	/** A session that advertised, with its ack received. */
	private TunnelSession live(RecordingStream stream) throws InterruptedException {
		TunnelSession live = session(stream, Duration.ofSeconds(10));
		live.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		assertThat(this.registry.find(CLIENT_ID)).as("precondition: the live session is registered").contains(live);
		assertThat(this.router.lookupByPort(LISTEN_PORT)).as("precondition: its route is registered").isPresent();
		return live;
	}

	private static Frame advertise(int connectionWindow) {
		return Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder()
				.addUpstreams(Upstream.newBuilder()
					.setHost("tcp.local")
					.setTargetUrl("tcp://127.0.0.1:1")
					.setListenPort(LISTEN_PORT))
				.setConnectionWindow(connectionWindow))
			.build();
	}

	private static void assertTimedOut(Throwable failure, Duration timeout) {
		Status status = Status.fromThrowable(failure);
		assertThat(status.getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
		assertThat(status.getDescription()).isEqualTo(TunnelSession.advertiseTimeoutDescription(timeout));
	}

	private void awaitNoPending() throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (!this.registry.pending().isEmpty() && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(this.registry.pending()).as("no stream left waiting").isEmpty();
	}

	@Test
	void aStreamThatNeverAdvertisesIsFailedAtTheBound() throws Exception {
		RecordingStream stream = new RecordingStream();
		long start = System.nanoTime();
		TunnelSession session = session(stream, TIMEOUT);
		assertThat(session.advertiseTimeout()).as("precondition: the timeout in effect").isEqualTo(TIMEOUT);
		assertThat(this.registry.pending()).as("tracked while it waits").containsExactly(session);
		assertThat(this.registry.find(CLIENT_ID)).as("not the client's session yet").isEmpty();
		// frames other than an advertise do not count
		session.handle(Frame.newBuilder().setKeepAlive(KeepAlive.getDefaultInstance()).build());

		Throwable failure = stream.awaitFailure();
		Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

		assertThat(elapsed).as("not before the bound").isGreaterThanOrEqualTo(TIMEOUT);
		// failed by the advertise deadline of the configured bound, no other path
		assertTimedOut(failure, TIMEOUT);
		assertThat(this.timedOut).as("counted").hasValue(1);
		awaitNoPending();
		assertThat(this.registry.find(CLIENT_ID)).isEmpty();
		assertThat(stream.frames).as("nothing sent before the advertise").isEmpty();
		// an advertise arriving late registers nothing
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		TimeUnit.MILLISECONDS.sleep(100);
		assertThat(this.registry.find(CLIENT_ID)).isEmpty();
		assertThat(this.router.clientCount()).isZero();
		assertThat(this.reconciles).as("only the removal of close()").containsExactly(Set.of());
		assertThat(stream.failures).as("failed once").hasSize(1);
		assertThat(stream.completions).as("failed, not completed").hasValue(0);
	}

	@Test
	void aClientAdvertisingInTimeIsUnaffected() throws Exception {
		RecordingStream stream = new RecordingStream();
		long start = System.nanoTime();
		TunnelSession session = session(stream, TIMEOUT);
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() / 3);
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		assertThat(Duration.ofNanos(System.nanoTime() - start)).as("precondition: acknowledged within the bound")
			.isLessThan(TIMEOUT);
		TimeUnit.NANOSECONDS.sleep(start + TIMEOUT.multipliedBy(2).toNanos() - System.nanoTime());

		assertThat(stream.failures).isEmpty();
		assertThat(stream.completions).hasValue(0);
		assertThat(this.timedOut).hasValue(0);
		assertThat(this.registry.find(CLIENT_ID)).contains(session);
		assertThat(this.registry.pending()).isEmpty();
		VirtualConnection connection = session.open("127.0.0.1:1");
		connection.sink().write(new byte[3]);
		assertThat(stream.await(Frame.BodyCase.DATA, 1)).as("the connection got the client's window at open")
			.singleElement()
			.satisfies(frame -> assertThat(frame.getData().getPayload().size()).isEqualTo(3));
	}

	@Test
	void openBeforeTheAdvertiseIsRefused() {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = session(stream, TIMEOUT);
		assertThatThrownBy(() -> session.open("127.0.0.1:1")).isInstanceOf(IllegalStateException.class);
		assertThat(stream.frames).as("no CONNECT sent").isEmpty();
		assertThat(session.connectionCount()).isZero();
	}

	@Test
	void aPendingStreamLeavesTheLiveSessionOfItsClientInPlace() throws Exception {
		RecordingStream liveStream = new RecordingStream();
		TunnelSession live = live(liveStream);
		RecordingStream pendingStream = new RecordingStream();
		TunnelSession pending = session(pendingStream, TIMEOUT);

		assertThat(this.registry.find(CLIENT_ID)).as("the live session still serves the client").contains(live);
		assertThat(this.registry.pending()).containsExactly(pending);
		assertTimedOut(pendingStream.awaitFailure(), TIMEOUT);
		awaitNoPending();
		TimeUnit.MILLISECONDS.sleep(100);
		assertThat(this.registry.find(CLIENT_ID)).contains(live);
		assertThat(liveStream.failures).isEmpty();
		assertThat(liveStream.completions).hasValue(0);
		assertThat(this.router.lookupByPort(LISTEN_PORT)).as("its route stays").isPresent();
		assertThat(this.reconciles).as("nothing released").containsExactly(Set.of(LISTEN_PORT));
		assertThat(pendingStream.matching(Frame.BodyCase.CONNECT)).isEmpty();
	}

	@Test
	void aNewStreamReplacesTheLiveSessionOnceItAdvertised() throws Exception {
		RecordingStream liveStream = new RecordingStream();
		live(liveStream);
		RecordingStream nextStream = new RecordingStream();
		TunnelSession next = session(nextStream, TIMEOUT);
		next.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		nextStream.await(Frame.BodyCase.ADVERTISE_ACK, 1);

		assertThat(this.registry.find(CLIENT_ID)).contains(next);
		assertThat(liveStream.completions).as("the replaced session's stream ended").hasValue(1);
		assertThat(this.router.lookupByPort(LISTEN_PORT)).as("the route moved over, not released").isPresent();
		assertThat(this.reconciles).as("no removal").containsExactly(Set.of(LISTEN_PORT), Set.of(LISTEN_PORT));
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() * 2);
		assertThat(nextStream.failures).as("no deadline after the advertise").isEmpty();
	}

	@Test
	void aWindowlessStreamLeavesTheLiveSessionInPlace() throws Exception {
		RecordingStream liveStream = new RecordingStream();
		TunnelSession live = live(liveStream);
		RecordingStream windowless = new RecordingStream();
		TunnelSession rejected = session(windowless, TIMEOUT);
		rejected.handle(advertise(0));

		assertThat(Status.fromThrowable(windowless.awaitFailure()).getCode())
			.isEqualTo(Status.Code.FAILED_PRECONDITION);
		awaitNoPending();
		assertThat(this.registry.find(CLIENT_ID)).contains(live);
		assertThat(liveStream.completions).hasValue(0);
		assertThat(this.router.lookupByPort(LISTEN_PORT)).isPresent();
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() * 2);
		assertThat(windowless.failures).as("rejected once, no deadline after").hasSize(1);
		assertThat(this.timedOut).hasValue(0);
	}

	/**
	 * A second stream of the same client, opened while the first has not advertised:
	 * whichever advertises becomes the session; the other still fails at its own bound,
	 * without touching the routes.
	 */
	@Test
	void aSecondStreamAdvertisingWhileTheFirstWaitsBecomesTheSession() throws Exception {
		RecordingStream firstStream = new RecordingStream();
		long start = System.nanoTime();
		session(firstStream, TIMEOUT);
		RecordingStream secondStream = new RecordingStream();
		TunnelSession second = session(secondStream, TIMEOUT);
		assertThat(this.registry.pending()).as("precondition: both wait").hasSize(2);
		second.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		secondStream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		assertThat(firstStream.failures).as("precondition: the first still waits").isEmpty();

		assertTimedOut(firstStream.awaitFailure(), TIMEOUT);
		assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(TIMEOUT);
		awaitNoPending();
		TimeUnit.MILLISECONDS.sleep(100);
		assertThat(this.registry.find(CLIENT_ID)).contains(second);
		assertThat(secondStream.failures).isEmpty();
		assertThat(this.router.lookupByPort(LISTEN_PORT)).isPresent();
		assertThat(this.reconciles).as("no removal").containsExactly(Set.of(LISTEN_PORT));
	}

	/** The client disconnecting before it advertised: nothing fires at the bound. */
	@Test
	void aStreamEndingBeforeItsAdvertiseLeavesNothingBehind() throws Exception {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = session(stream, TIMEOUT);
		session.close();

		assertThat(this.registry.pending()).isEmpty();
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() * 2);
		assertThat(stream.failures).as("no deadline after the end").isEmpty();
		assertThat(this.timedOut).hasValue(0);
		assertThat(this.registry.find(CLIENT_ID)).isEmpty();
		assertThat(this.reconciles).as("only the removal of close()").containsExactly(Set.of());
	}

	/**
	 * An advertise handled right after the deadline decided -- from the deadline's own
	 * metrics hook, before the stream is failed -- is ignored: no registration, no ack.
	 */
	@Test
	void anAdvertiseRightAfterTheDeadlineDecidedIsIgnored() throws Exception {
		RecordingStream stream = new RecordingStream();
		AtomicReference<@Nullable TunnelSession> holder = new AtomicReference<>();
		AtomicInteger registeredSeen = new AtomicInteger();
		TunnelSession session = TunnelSession.builder()
			.clientId(CLIENT_ID)
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(this.registry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.advertiseTimeout(TIMEOUT)
			.advertiseTimedOut(() -> {
				Objects.requireNonNull(holder.get()).handle(advertise(VirtualConnection.DEFAULT_WINDOW));
				// time for an advertise that was not ignored to register and be acked
				// before the stream fails
				sleepQuietly(200);
				this.registry.find(CLIENT_ID).ifPresent(found -> registeredSeen.incrementAndGet());
			})
			.build();
		this.sessions.add(session);
		holder.set(session);
		session.start();

		assertTimedOut(stream.awaitFailure(), TIMEOUT);
		awaitNoPending();
		assertThat(registeredSeen).as("never registered").hasValue(0);
		assertThat(stream.frames).as("no ack").isEmpty();
		assertThat(this.router.clientCount()).isZero();
	}

	private static void sleepQuietly(long millis) {
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * An advertise arriving at the bound is decided once: either the session is
	 * registered and acknowledged and its stream lives, or the stream fails and nothing
	 * is registered -- never both, never neither.
	 */
	@Test
	void anAdvertiseRacingTheBoundIsDecidedOnce() throws Exception {
		Duration timeout = Duration.ofMillis(5);
		int inTime = 0;
		int late = 0;
		for (int i = 0; i < 60; i++) {
			String clientId = CLIENT_ID + "-" + i;
			RecordingStream stream = new RecordingStream();
			TunnelSession session = session(clientId, stream, timeout);
			TimeUnit.MILLISECONDS.sleep(i % 12);
			session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
			long deadline = System.currentTimeMillis() + 5000;
			while (stream.failures.isEmpty() && stream.matching(Frame.BodyCase.ADVERTISE_ACK).isEmpty()
					&& System.currentTimeMillis() < deadline) {
				TimeUnit.MILLISECONDS.sleep(1);
			}
			TimeUnit.MILLISECONDS.sleep(20);
			boolean failed = !stream.failures.isEmpty();
			boolean acked = !stream.matching(Frame.BodyCase.ADVERTISE_ACK).isEmpty();
			assertThat(failed ^ acked).as("round %d decided once: failed=%s acked=%s", i, failed, acked).isTrue();
			if (failed) {
				late++;
				assertTimedOut(stream.failures.getFirst(), timeout);
				assertThat(this.registry.find(clientId)).isEmpty();
			}
			else {
				inTime++;
				assertThat(this.registry.find(clientId)).contains(session);
			}
			session.close();
		}
		assertThat(inTime).as("precondition: some advertises were in time").isPositive();
		assertThat(late).as("precondition: some were late").isPositive();
		assertThat(this.timedOut).hasValue(late);
	}

}
