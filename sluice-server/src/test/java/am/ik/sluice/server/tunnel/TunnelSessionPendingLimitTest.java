package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounds on the tunnel streams waiting for their client's first {@code Advertise}:
 * past the global or the per-address bound a stream is refused at once and starts
 * nothing; a pending stream frees its place when it advertises, when its deadline fails
 * it, or when it ends.
 */
@Timeout(30)
class TunnelSessionPendingLimitTest {

	private static final Duration TIMEOUT = Duration.ofMillis(300);

	private final Router router = new Router();

	private final AtomicInteger timedOut = new AtomicInteger();

	private final List<TunnelSession> sessions = new CopyOnWriteArrayList<>();

	@AfterEach
	void closeSessions() {
		this.sessions.forEach(TunnelSession::close);
	}

	private static SessionRegistry registry(int max, int perAddress) {
		return new SessionRegistry(SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder()
				.maxPendingTunnelStreams(max)
				.maxPendingTunnelStreamsPerAddress(perAddress)
				.build())
			.build());
	}

	private TunnelSession session(SessionRegistry registry, String clientId, String source, RecordingStream stream) {
		TunnelSession session = TunnelSession.builder()
			.clientId(clientId)
			.sourceAddress(source)
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(registry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.advertiseTimeout(TIMEOUT)
			.advertiseTimedOut(this.timedOut::incrementAndGet)
			.build();
		this.sessions.add(session);
		return session;
	}

	private TunnelSession started(SessionRegistry registry, String clientId, String source, RecordingStream stream) {
		TunnelSession session = session(registry, clientId, source, stream);
		assertThat(session.start()).as("precondition: %s admitted", clientId).isEmpty();
		return session;
	}

	private TunnelSession started(SessionRegistry registry, String clientId) {
		return started(registry, clientId, "10.0.0.1", new RecordingStream());
	}

	private static Frame advertise() {
		return Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder().setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
			.build();
	}

	private static void awaitPending(SessionRegistry registry, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (registry.pending().size() != count && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(5);
		}
		assertThat(registry.pending()).hasSize(count);
	}

	@Test
	void pastTheCapAStreamIsRefusedAndStartsNothing() throws Exception {
		SessionRegistry registry = registry(2, 0);
		started(registry, "a");
		started(registry, "b");
		RecordingStream refusedStream = new RecordingStream();
		TunnelSession refused = session(registry, "c", "10.0.0.2", refusedStream);

		assertThat(refused.start()).contains(SessionRegistry.Refusal.LIMIT);

		assertThat(registry.pending()).as("not tracked").hasSize(2).doesNotContain(refused);
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() * 2);
		assertThat(refusedStream.failures).as("no advertise deadline runs for it").isEmpty();
		assertThat(refusedStream.completions).as("no sender runs for it").hasValue(0);
		assertThat(this.timedOut).as("only the two admitted streams timed out").hasValue(2);
	}

	@Test
	void anAdvertiseFreesThePlace() throws Exception {
		SessionRegistry registry = registry(1, 0);
		RecordingStream stream = new RecordingStream();
		TunnelSession first = started(registry, "a", "10.0.0.1", stream);
		assertThat(session(registry, "b", "10.0.0.1", new RecordingStream()).start())
			.as("precondition: the cap is reached")
			.contains(SessionRegistry.Refusal.LIMIT);

		first.handle(advertise());
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);

		assertThat(registry.find("a")).contains(first);
		assertThat(registry.pending()).isEmpty();
		assertThat(registry.trackedAddresses()).isZero();
		assertThat(session(registry, "c", "10.0.0.1", new RecordingStream()).start()).isEmpty();
	}

	@Test
	void theDeadlineFreesThePlace() throws Exception {
		SessionRegistry registry = registry(1, 0);
		RecordingStream stream = new RecordingStream();
		started(registry, "a", "10.0.0.1", stream);
		assertThat(session(registry, "b", "10.0.0.1", new RecordingStream()).start())
			.as("precondition: the cap is reached")
			.contains(SessionRegistry.Refusal.LIMIT);

		stream.awaitFailure();
		awaitPending(registry, 0);

		assertThat(this.timedOut).hasValue(1);
		assertThat(registry.trackedAddresses()).isZero();
		assertThat(session(registry, "c", "10.0.0.1", new RecordingStream()).start()).isEmpty();
	}

	@Test
	void theEndOfTheStreamFreesThePlace() {
		SessionRegistry registry = registry(1, 1);
		TunnelSession first = started(registry, "a");
		assertThat(session(registry, "b", "10.0.0.9", new RecordingStream()).start())
			.as("precondition: the cap is reached")
			.contains(SessionRegistry.Refusal.LIMIT);

		first.close();

		assertThat(registry.pending()).isEmpty();
		assertThat(registry.pending("10.0.0.1")).isZero();
		assertThat(registry.trackedAddresses()).isZero();
		assertThat(session(registry, "c", "10.0.0.1", new RecordingStream()).start()).isEmpty();
	}

	@Test
	void theBoundPerAddressRefusesOnlyThatAddress() throws Exception {
		SessionRegistry registry = registry(10, 2);
		started(registry, "a1", "10.0.0.1", new RecordingStream());
		TunnelSession second = started(registry, "a2", "10.0.0.1", new RecordingStream());
		RecordingStream refusedStream = new RecordingStream();

		assertThat(session(registry, "a3", "10.0.0.1", refusedStream).start())
			.contains(SessionRegistry.Refusal.ADDRESS_LIMIT);
		assertThat(registry.pending("10.0.0.1")).isEqualTo(2);
		assertThat(session(registry, "b1", "10.0.0.2", new RecordingStream()).start()).as("another address is admitted")
			.isEmpty();
		assertThat(session(registry, "u1", "", new RecordingStream()).start())
			.as("an unknown address is bounded by the global cap only")
			.isEmpty();
		assertThat(session(registry, "u2", "", new RecordingStream()).start()).isEmpty();
		assertThat(session(registry, "u3", "", new RecordingStream()).start()).isEmpty();
		assertThat(registry.pending()).hasSize(6);

		second.close();
		assertThat(session(registry, "a4", "10.0.0.1", new RecordingStream()).start()).as("the place is freed")
			.isEmpty();
		TimeUnit.MILLISECONDS.sleep(TIMEOUT.toMillis() * 2);
		assertThat(refusedStream.failures).as("nothing runs for the refused stream").isEmpty();
	}

	/**
	 * Admissions racing each other never pass the cap, global or per address: rounds of
	 * platform threads released at once, each admitting a few streams.
	 */
	@Test
	void concurrentAdmissionsNeverPassTheCap() throws Exception {
		int max = 5;
		int perAddress = 2;
		int threads = 8;
		int perThread = 4;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		try {
			for (int round = 0; round < 200; round++) {
				SessionRegistry registry = registry(max, perAddress);
				CyclicBarrier go = new CyclicBarrier(threads);
				List<Future<Integer>> admitted = new ArrayList<>();
				for (int t = 0; t < threads; t++) {
					List<TunnelSession> candidates = new ArrayList<>();
					for (int i = 0; i < perThread; i++) {
						candidates.add(untracked(registry, "10.0.0." + (i % 3)));
					}
					admitted.add(pool.submit(() -> {
						go.await();
						int count = 0;
						for (TunnelSession candidate : candidates) {
							count += registry.admit(candidate).isEmpty() ? 1 : 0;
						}
						return count;
					}));
				}
				int total = 0;
				for (Future<Integer> future : admitted) {
					total += future.get(5, TimeUnit.SECONDS);
				}
				assertThat(total).as("round %d: admitted up to the cap", round).isEqualTo(max);
				assertThat(registry.pending()).hasSize(max);
				for (int i = 0; i < 3; i++) {
					assertThat(registry.pending("10.0.0." + i)).as("round %d", round).isLessThanOrEqualTo(perAddress);
				}
			}
		}
		finally {
			pool.shutdownNow();
		}
	}

	/** A session that is never started nor closed: only admitted. */
	private TunnelSession untracked(SessionRegistry registry, String source) {
		return TunnelSession.builder()
			.clientId("racer")
			.sourceAddress(source)
			.router(this.router)
			.sender(new SessionSender(new RecordingStream()))
			.registry(registry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.build();
	}

}
