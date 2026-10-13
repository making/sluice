package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import io.grpc.Status;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Once the drain began, the registry admits no session: every session admitted is among
 * the pending ones the drain took, which it closes, so none appears after it.
 */
@Timeout(30)
class SessionRegistryDrainTest {

	private final SessionRegistry registry = new SessionRegistry(SluiceServerProperties.builder().build());

	private final Router router = new Router();

	private TunnelSession session(SessionRegistry registry, String clientId, RecordingStream stream) {
		return TunnelSession.builder()
			.clientId(clientId)
			.sourceAddress("10.0.0.1")
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(registry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.build();
	}

	@Test
	void theDrainTakesThePendingSessionsAndRefusesLaterOnesAsDraining() throws Exception {
		TunnelSession waiting = session(this.registry, "waiting", new RecordingStream());
		assertThat(this.registry.admit(waiting)).as("precondition: admitted before the drain").isEmpty();

		assertThat(this.registry.beginDrain()).containsExactly(waiting);

		RecordingStream lateStream = new RecordingStream();
		TunnelSession late = session(this.registry, "late", lateStream);
		assertThat(late.start()).contains(SessionRegistry.Refusal.DRAINING);
		assertThat(SessionRegistry.Refusal.DRAINING.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
		assertThat(SessionRegistry.Refusal.DRAINING.status().getDescription()).isEqualTo("node is draining");
		assertThat(this.registry.pending()).containsExactly(waiting);
		TimeUnit.MILLISECONDS.sleep(100);
		assertThat(lateStream.frames).as("nothing started for the refused session").isEmpty();
		assertThat(lateStream.completions).hasValue(0);
		assertThat(this.registry.beginDrain()).as("a second call takes the same").containsExactly(waiting);
	}

	/**
	 * The drain closing a session right after its admission, before the session started
	 * its sender: the stream still ends, nothing is left pending, and no deadline fires.
	 */
	@Test
	void aSessionClosedByTheDrainRightAfterItsAdmissionEndsItsStream() throws Exception {
		SessionRegistry closingRegistry = new SessionRegistry(SluiceServerProperties.builder().build()) {

			@Override
			Optional<Refusal> admit(TunnelSession session) {
				Optional<Refusal> refusal = super.admit(session);
				beginDrain().forEach(TunnelSession::close);
				return refusal;
			}

		};
		RecordingStream stream = new RecordingStream();
		AtomicInteger timedOut = new AtomicInteger();
		TunnelSession session = TunnelSession.builder()
			.clientId("closed-early")
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(closingRegistry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.advertiseTimeout(Duration.ofMillis(200))
			.advertiseTimedOut(timedOut::incrementAndGet)
			.build();

		assertThat(session.start()).as("precondition: admitted").isEmpty();

		long deadline = System.currentTimeMillis() + 5000;
		while (stream.completions.get() == 0 && stream.failures.isEmpty() && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(5);
		}
		assertThat(stream.completions).as("the stream ended").hasValue(1);
		assertThat(closingRegistry.pending()).isEmpty();
		TimeUnit.MILLISECONDS.sleep(400);
		assertThat(stream.failures).as("no deadline after the close").isEmpty();
		assertThat(timedOut).hasValue(0);
	}

	/**
	 * Admissions racing the drain's start: each session is either refused or among those
	 * the drain took -- never admitted behind its back.
	 */
	@Test
	void noAdmissionRacingTheDrainEscapesIt() throws Exception {
		int threads = 4;
		int perThread = 8;
		ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
		try {
			for (int round = 0; round < 200; round++) {
				SessionRegistry registry = new SessionRegistry(SluiceServerProperties.builder().build());
				CyclicBarrier go = new CyclicBarrier(threads + 1);
				List<Future<List<TunnelSession>>> admissions = new ArrayList<>();
				for (int t = 0; t < threads; t++) {
					List<TunnelSession> candidates = new ArrayList<>();
					for (int i = 0; i < perThread; i++) {
						candidates.add(session(registry, "c" + t + "-" + i, new RecordingStream()));
					}
					admissions.add(pool.submit(() -> {
						go.await();
						List<TunnelSession> admitted = new ArrayList<>();
						for (TunnelSession candidate : candidates) {
							if (registry.admit(candidate).isEmpty()) {
								admitted.add(candidate);
							}
						}
						return admitted;
					}));
				}
				Future<List<TunnelSession>> drain = pool.submit(() -> {
					go.await();
					return registry.beginDrain();
				});
				Set<TunnelSession> taken = new HashSet<>(drain.get(5, TimeUnit.SECONDS));
				Set<TunnelSession> admitted = new HashSet<>();
				for (Future<List<TunnelSession>> future : admissions) {
					admitted.addAll(future.get(5, TimeUnit.SECONDS));
				}
				assertThat(taken).as("round %d: every admitted session is among those the drain took", round)
					.containsExactlyInAnyOrderElementsOf(admitted);
			}
		}
		finally {
			pool.shutdownNow();
		}
	}

}
