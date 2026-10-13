package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.Status;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounds on the registered tunnel sessions
 * ({@code sluice.limits.max-tunnel-sessions},
 * {@code sluice.limits.max-tunnel-sessions-per-address}): a stream that could not
 * register is refused at its admission when that is already known, and otherwise at its
 * advertise, where the registration decides; a newer stream of a registered client id
 * replaces that session and takes no further place; a session ending frees its place.
 */
@Timeout(30)
class TunnelSessionRegisteredLimitTest {

	private final Router router = new Router();

	private final List<TunnelSession> sessions = new CopyOnWriteArrayList<>();

	private final List<SessionRegistry.Refusal> refusals = new CopyOnWriteArrayList<>();

	private final List<TunnelSession> registrations = new CopyOnWriteArrayList<>();

	@AfterEach
	void closeSessions() {
		this.sessions.forEach(TunnelSession::close);
	}

	private static SessionRegistry registry(int max, int perAddress) {
		return new SessionRegistry(SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder()
				.maxTunnelSessions(max)
				.maxTunnelSessionsPerAddress(perAddress)
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
			.advertiseTimeout(Duration.ofSeconds(20))
			.refused(this.refusals::add)
			.registered(this.registrations::add)
			.build();
		this.sessions.add(session);
		return session;
	}

	/** A session admitted, not advertised yet. */
	private TunnelSession pending(SessionRegistry registry, String clientId, String source, RecordingStream stream) {
		TunnelSession session = session(registry, clientId, source, stream);
		assertThat(session.start()).as("precondition: %s admitted", clientId).isEmpty();
		return session;
	}

	/** A session admitted and registered by its advertise of one upstream. */
	private TunnelSession registered(SessionRegistry registry, String clientId, String source) throws Exception {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = pending(registry, clientId, source, stream);
		session.handle(advertise(clientId + ".example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		assertThat(registry.find(clientId)).as("precondition: %s registered", clientId).contains(session);
		return session;
	}

	private static Frame advertise(String host) {
		return Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder()
				.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
				.addUpstreams(Upstream.newBuilder().setHost(host).setTargetUrl("http://127.0.0.1:9")))
			.build();
	}

	private static void assertRefusedAtAdvertise(RecordingStream stream, SessionRegistry.Refusal refusal)
			throws InterruptedException {
		Status status = Status.fromThrowable(stream.awaitFailure());
		assertThat(status.getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(status.getDescription()).isEqualTo(refusal.status().getDescription());
		assertThat(stream.matching(Frame.BodyCase.ADVERTISE_ACK)).as("no ack for a refused advertise").isEmpty();
	}

	@Test
	void theRefusalsAreResourceExhausted() {
		assertThat(SessionRegistry.Refusal.SESSION_LIMIT.reason()).isEqualTo("session-limit");
		assertThat(SessionRegistry.Refusal.SESSION_LIMIT.status().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(SessionRegistry.Refusal.SESSION_LIMIT.status().getDescription())
			.isEqualTo("too many tunnel clients are connected to this node");
		assertThat(SessionRegistry.Refusal.SESSION_ADDRESS_LIMIT.reason()).isEqualTo("session-address-limit");
		assertThat(SessionRegistry.Refusal.SESSION_ADDRESS_LIMIT.status().getCode())
			.isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(SessionRegistry.Refusal.SESSION_ADDRESS_LIMIT.status().getDescription())
			.isEqualTo("too many tunnel clients from this address are connected to this node");
	}

	@Test
	void pastTheCapANewClientIdIsRefusedAtItsAdmissionWhileTheSessionsGoOn() throws Exception {
		SessionRegistry registry = registry(2, 0);
		TunnelSession a = registered(registry, "a", "10.0.0.1");
		TunnelSession b = registered(registry, "b", "10.0.0.2");
		RecordingStream refusedStream = new RecordingStream();

		assertThat(session(registry, "c", "10.0.0.3", refusedStream).start())
			.contains(SessionRegistry.Refusal.SESSION_LIMIT);

		assertThat(registry.pending()).as("not tracked").isEmpty();
		assertThat(registry.all()).containsExactlyInAnyOrder(a, b);
		assertThat(this.router.resolve("a.example.com")).as("the sessions go on serving").isPresent();
		assertThat(this.router.resolve("b.example.com")).isPresent();
		TimeUnit.MILLISECONDS.sleep(100);
		assertThat(refusedStream.frames).as("nothing started for the refused stream").isEmpty();
		assertThat(refusedStream.completions).hasValue(0);
	}

	@Test
	void aSessionEndingFreesItsPlace() throws Exception {
		SessionRegistry registry = registry(2, 0);
		TunnelSession a = registered(registry, "a", "10.0.0.1");
		registered(registry, "b", "10.0.0.1");
		assertThat(session(registry, "c", "10.0.0.1", new RecordingStream()).start())
			.as("precondition: the cap is reached")
			.contains(SessionRegistry.Refusal.SESSION_LIMIT);

		a.close();

		assertThat(registry.count()).isEqualTo(1);
		registered(registry, "c", "10.0.0.1");
		assertThat(registry.count()).isEqualTo(2);
	}

	@Test
	void aNewerStreamOfARegisteredClientIdReplacesItAtTheCap() throws Exception {
		SessionRegistry registry = registry(2, 0);
		TunnelSession old = registered(registry, "a", "10.0.0.1");
		registered(registry, "b", "10.0.0.2");

		TunnelSession newer = registered(registry, "a", "10.0.0.3");

		assertThat(registry.count()).isEqualTo(2);
		assertThat(registry.find("a")).contains(newer);
		assertThat(registry.all()).doesNotContain(old);
		assertThat(this.refusals).isEmpty();
	}

	/**
	 * Two streams of new client ids admitted while one place is left: the one advertising
	 * first registers, the other is refused at its advertise -- failed, no ack, no
	 * routes, no membership push -- and frees its pending place; an advertise it sends
	 * again before it sees the failure registers nothing.
	 */
	@Test
	void aPendingStreamThatCannotRegisterIsRefusedAtItsAdvertise() throws Exception {
		SessionRegistry registry = registry(2, 0);
		registered(registry, "a", "10.0.0.1");
		RecordingStream firstStream = new RecordingStream();
		TunnelSession first = pending(registry, "b", "10.0.0.1", firstStream);
		RecordingStream secondStream = new RecordingStream();
		TunnelSession second = pending(registry, "c", "10.0.0.1", secondStream);
		this.registrations.clear();

		first.handle(advertise("b.example.com"));
		firstStream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		second.handle(advertise("c.example.com"));

		assertRefusedAtAdvertise(secondStream, SessionRegistry.Refusal.SESSION_LIMIT);
		assertThat(this.refusals).containsExactly(SessionRegistry.Refusal.SESSION_LIMIT);
		assertThat(this.registrations).containsExactly(first);
		assertThat(registry.find("c")).isEmpty();
		assertThat(this.router.resolve("c.example.com")).isEmpty();
		assertThat(registry.pending()).isEmpty();
		assertThat(registry.count()).isEqualTo(2);

		registry.remove(first);
		second.handle(advertise("c.example.com"));
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(registry.find("c")).as("a refused session never registers").isEmpty();
		assertThat(this.refusals).hasSize(1);
	}

	/**
	 * A newer stream admitted as the replacement of its client id, whose old session ends
	 * before the newer advertises while another client takes the place: the newer is no
	 * replacement anymore and is refused at its advertise.
	 */
	@Test
	void aReplacementWhoseOldSessionEndedIsRefusedWhenThePlaceIsTaken() throws Exception {
		SessionRegistry registry = registry(1, 0);
		TunnelSession old = registered(registry, "a", "10.0.0.1");
		RecordingStream newerStream = new RecordingStream();
		TunnelSession newer = pending(registry, "a", "10.0.0.1", newerStream);

		old.close();
		registered(registry, "b", "10.0.0.2");
		newer.handle(advertise("a.example.com"));

		assertRefusedAtAdvertise(newerStream, SessionRegistry.Refusal.SESSION_LIMIT);
		assertThat(registry.find("a")).isEmpty();
		assertThat(registry.count()).isEqualTo(1);
	}

	@Test
	void theBoundPerAddressRefusesOnlyThatAddress() throws Exception {
		SessionRegistry registry = registry(10, 2);
		registered(registry, "a1", "10.0.0.1");
		TunnelSession a2 = registered(registry, "a2", "10.0.0.1");

		assertThat(session(registry, "a3", "10.0.0.1", new RecordingStream()).start())
			.contains(SessionRegistry.Refusal.SESSION_ADDRESS_LIMIT);
		registered(registry, "b1", "10.0.0.2");
		registered(registry, "u1", "");
		registered(registry, "u2", "");
		registered(registry, "u3", "");
		assertThat(registry.sessions("10.0.0.1")).isEqualTo(2);
		assertThat(registry.count()).as("an unknown address is bounded by the global cap only").isEqualTo(6);

		registered(registry, "a2", "10.0.0.1");
		assertThat(registry.sessions("10.0.0.1")).as("a replacement from the same address takes no place").isEqualTo(2);
		assertThat(registry.all()).doesNotContain(a2);

		registry.find("a1").orElseThrow().close();
		registered(registry, "a4", "10.0.0.1");
		assertThat(registry.sessions("10.0.0.1")).isEqualTo(2);
	}

	/**
	 * A client id moving to an address at its bound: the replacement would add a session
	 * there, so it is refused at its advertise and the old session, from its old address,
	 * goes on serving its routes.
	 */
	@Test
	void aReplacementFromAnotherAddressAtItsBoundIsRefusedAndTheOldSessionGoesOn() throws Exception {
		SessionRegistry registry = registry(10, 1);
		TunnelSession old = registered(registry, "a", "10.0.0.1");
		registered(registry, "b", "10.0.0.2");
		RecordingStream movedStream = new RecordingStream();
		TunnelSession moved = pending(registry, "a", "10.0.0.9", movedStream);
		registered(registry, "c", "10.0.0.9");

		moved.handle(advertise("a.example.com"));

		assertRefusedAtAdvertise(movedStream, SessionRegistry.Refusal.SESSION_ADDRESS_LIMIT);
		assertThat(registry.find("a")).contains(old);
		assertThat(this.router.resolve("a.example.com")).as("the old session's routes stay").isPresent();
		assertThat(registry.sessions("10.0.0.1")).isEqualTo(1);
		assertThat(registry.sessions("10.0.0.9")).isEqualTo(1);
	}

	@Test
	void aSessionClosedRightAfterItsRefusalLeavesNothingBehind() throws Exception {
		SessionRegistry registry = registry(1, 0);
		RecordingStream stream = new RecordingStream();
		TunnelSession refused = pending(registry, "b", "10.0.0.1", stream);
		registered(registry, "a", "10.0.0.1");

		refused.handle(advertise("b.example.com"));
		stream.awaitFailure();
		refused.close();

		assertThat(registry.pending()).isEmpty();
		assertThat(registry.trackedAddresses()).isZero();
		assertThat(registry.count()).isEqualTo(1);
		assertThat(registry.trackedSessionAddresses()).isEqualTo(1);
	}

	/**
	 * Advertises racing each other at the cap never pass it, global or per address:
	 * rounds of pending sessions of distinct client ids advertising at once from platform
	 * threads; the others are refused, and the per-address counts match the sessions.
	 */
	@Test
	void concurrentAdvertisesNeverPassTheCap() throws Exception {
		int max = 5;
		int perAddress = 2;
		int candidates = 12;
		ExecutorService pool = Executors.newFixedThreadPool(candidates);
		try {
			for (int round = 0; round < 50; round++) {
				SessionRegistry registry = registry(max, perAddress);
				List<RecordingStream> streams = new ArrayList<>();
				List<TunnelSession> racers = new ArrayList<>();
				for (int i = 0; i < candidates; i++) {
					RecordingStream stream = new RecordingStream();
					streams.add(stream);
					racers.add(pending(registry, "r" + round + "-" + i, "10.0.0." + (i % 4), stream));
				}
				CyclicBarrier go = new CyclicBarrier(candidates);
				List<Future<?>> advertises = new ArrayList<>();
				for (TunnelSession racer : racers) {
					advertises.add(pool.submit(() -> {
						go.await();
						racer.handle(advertise(racer.clientId() + ".example.com"));
						return null;
					}));
				}
				for (Future<?> advertise : advertises) {
					advertise.get(5, TimeUnit.SECONDS);
				}
				awaitDecided(streams);
				assertThat(registry.count()).as("round %d: registered up to the cap", round).isEqualTo(max);
				for (int i = 0; i < 4; i++) {
					String source = "10.0.0." + i;
					assertThat(registry.sessions(source)).as("round %d", round)
						.isLessThanOrEqualTo(perAddress)
						.isEqualTo((int) registry.all()
							.stream()
							.filter(session -> session.sourceAddress().equals(source))
							.count());
				}
				assertThat(registry.pending()).isEmpty();
				racers.forEach(TunnelSession::close);
				assertThat(registry.count()).isZero();
				assertThat(registry.trackedSessionAddresses()).as("round %d: no count outlives its sessions", round)
					.isZero();
			}
		}
		finally {
			pool.shutdownNow();
		}
	}

	/**
	 * Sessions ending while others register at the cap: a freed place is taken at most
	 * once, and the per-address counts never drift from the sessions.
	 */
	@Test
	void sessionsEndingWhileOthersRegisterKeepTheCountsExact() throws Exception {
		int max = 4;
		long registeredOverall = 0;
		long refusedOverall = 0;
		ExecutorService pool = Executors.newFixedThreadPool(8);
		try {
			for (int round = 0; round < 50; round++) {
				SessionRegistry registry = registry(max, 0);
				List<RecordingStream> streams = new ArrayList<>();
				List<TunnelSession> racers = new ArrayList<>();
				for (int i = 0; i < 4; i++) {
					RecordingStream stream = new RecordingStream();
					streams.add(stream);
					// admitted below the bound, advertising once it is reached
					racers.add(pending(registry, "new" + round + "-" + i, "10.0.0." + (i % 2), stream));
				}
				List<TunnelSession> live = new ArrayList<>();
				for (int i = 0; i < max; i++) {
					live.add(registered(registry, "live" + round + "-" + i, "10.0.0." + (i % 2)));
				}
				CountDownLatch go = new CountDownLatch(1);
				List<Future<?>> tasks = new ArrayList<>();
				for (int i = 0; i < 2; i++) {
					TunnelSession ending = live.get(i);
					tasks.add(pool.submit(() -> {
						go.await();
						ending.close();
						return null;
					}));
				}
				for (TunnelSession racer : racers) {
					tasks.add(pool.submit(() -> {
						go.await();
						racer.handle(advertise(racer.clientId() + ".example.com"));
						return null;
					}));
				}
				go.countDown();
				for (Future<?> task : tasks) {
					task.get(5, TimeUnit.SECONDS);
				}
				awaitDecided(streams);
				long racersRegistered = racers.stream()
					.filter(racer -> registry.find(racer.clientId()).isPresent())
					.count();
				assertThat(registry.count()).as("round %d", round).isLessThanOrEqualTo(max);
				assertThat(racersRegistered).as("round %d: at most the two freed places", round).isLessThanOrEqualTo(2);
				registeredOverall += racersRegistered;
				refusedOverall += streams.stream().filter(stream -> !stream.failures.isEmpty()).count();
				for (int i = 0; i < 2; i++) {
					String source = "10.0.0." + i;
					assertThat(registry.sessions(source)).as("round %d", round)
						.isEqualTo((int) registry.all()
							.stream()
							.filter(session -> session.sourceAddress().equals(source))
							.count());
				}
				live.forEach(TunnelSession::close);
				racers.forEach(TunnelSession::close);
				assertThat(registry.trackedSessionAddresses()).as("round %d", round).isZero();
			}
		}
		finally {
			pool.shutdownNow();
		}
		assertThat(registeredOverall).as("precondition: freed places were taken").isPositive();
		assertThat(refusedOverall).as("precondition: advertises were refused at the bound").isPositive();
	}

	/** Waits until every stream is acknowledged or failed. */
	private static void awaitDecided(List<RecordingStream> streams) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (System.currentTimeMillis() < deadline && streams.stream()
			.anyMatch(stream -> stream.failures.isEmpty() && stream.matching(Frame.BodyCase.ADVERTISE_ACK).isEmpty())) {
			TimeUnit.MILLISECONDS.sleep(5);
		}
		assertThat(streams).allSatisfy(stream -> assertThat(
				!stream.failures.isEmpty() || !stream.matching(Frame.BodyCase.ADVERTISE_ACK).isEmpty())
			.as("decided")
			.isTrue());
	}

}
