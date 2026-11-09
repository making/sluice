package am.ik.sluice.server.route;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import am.ik.sluice.v1.proto.Upstream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class RouterTest {

	private static Upstream upstream(String host, String targetUrl) {
		return Upstream.newBuilder().setHost(host).setTargetUrl(targetUrl).setRewriteHost(false).build();
	}

	@Test
	void exactHostMatch() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		Router.Route route = router.lookup("demo.local").orElseThrow();
		assertThat(route.clientId()).isEqualTo("c1");
		assertThat(route.address()).isEqualTo("127.0.0.1:3000");
		assertThat(route.rewriteHost()).isFalse();
	}

	@Test
	void rewriteHostFlagPropagatesToRoute() {
		Router router = new Router();
		Upstream upstream = Upstream.newBuilder()
			.setHost("demo.local")
			.setTargetUrl("http://127.0.0.1:3000")
			.setRewriteHost(true)
			.build();
		router.register("c1", List.of(upstream));
		assertThat(router.lookup("demo.local").orElseThrow().rewriteHost()).isTrue();
	}

	@Test
	void forceHttp1FlagPropagatesToRoute() {
		Router router = new Router();
		Upstream flagged = Upstream.newBuilder()
			.setHost("legacy.local")
			.setTargetUrl("http://127.0.0.1:3000")
			.setRewriteHost(false)
			.setForceHttp1(true)
			.build();
		Upstream plain = Upstream.newBuilder()
			.setHost("demo.local")
			.setTargetUrl("http://127.0.0.1:3001")
			.setRewriteHost(false)
			.build();
		router.register("c1", List.of(flagged, plain));
		assertThat(router.lookup("legacy.local").orElseThrow().forceHttp1()).isTrue();
		assertThat(router.lookup("demo.local").orElseThrow().forceHttp1()).isFalse();
	}

	@Test
	void verbatimIdleTimeoutPropagatesToRouteZeroWhenUnset() {
		Router router = new Router();
		Upstream timed = Upstream.newBuilder()
			.setHost("redis.local")
			.setTargetUrl("tcp://127.0.0.1:6379")
			.setVerbatimIdleTimeoutMillis(90_000)
			.build();
		Upstream tcp = Upstream.newBuilder()
			.setTargetUrl("tcp://127.0.0.1:5432")
			.setListenPort(15432)
			.setVerbatimIdleTimeoutMillis(1500)
			.build();
		// past the signed range of the unsigned field: the longest timeout, not none
		Upstream huge = Upstream.newBuilder()
			.setHost("huge.local")
			.setTargetUrl("tcp://127.0.0.1:1")
			.setVerbatimIdleTimeoutMillis(-1)
			.build();
		router.register("c1", List.of(timed, tcp, huge, upstream("demo.local", "http://127.0.0.1:3001")));
		assertThat(router.lookup("redis.local").orElseThrow().verbatimIdleTimeout()).isEqualTo(Duration.ofSeconds(90));
		assertThat(router.lookupByPort(15432).orElseThrow().verbatimIdleTimeout()).isEqualTo(Duration.ofMillis(1500));
		assertThat(router.lookup("huge.local").orElseThrow().verbatimIdleTimeout())
			.isEqualTo(Duration.ofMillis(Long.MAX_VALUE));
		assertThat(router.lookup("demo.local").orElseThrow().verbatimIdleTimeout()).isZero();
	}

	@Test
	void hostWithPortMatchesBareHostRoute() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		assertThat(router.lookup("demo.local:8000")).isPresent();
	}

	@Test
	void catchAllFallback() {
		Router router = new Router();
		router.register("c1", List.of(upstream("", "http://127.0.0.1:3000")));
		Router.Route route = router.lookup("anything.example").orElseThrow();
		assertThat(route.clientId()).isEqualTo("c1");
	}

	@Test
	void unknownHostIsEmpty() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		assertThat(router.lookup("other.local")).isEmpty();
	}

	@Test
	void shutGateStopsTheRouteResolvingAndReopeningRestoresIt() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("other.local", "http://127.0.0.1:3001")));
		router.gate().set("demo.local", GateState.SHUT);
		assertThat(router.lookup("demo.local")).isEmpty();
		// the other route is unaffected
		assertThat(router.lookup("other.local")).isPresent();
		router.gate().set("demo.local", GateState.OPEN);
		assertThat(router.lookup("demo.local")).isPresent();
	}

	@Test
	void shutExactRouteFallsThroughToTheCatchAll() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("", "http://127.0.0.1:3001")));
		router.gate().set("demo.local", GateState.SHUT);
		Router.Route route = router.lookup("demo.local").orElseThrow();
		assertThat(route.clientId()).isEqualTo("c2");
	}

	@Test
	void shutCatchAllStopsTheFallback() {
		Router router = new Router();
		router.register("c1", List.of(upstream("", "http://127.0.0.1:3000")));
		router.gate().set("*", GateState.SHUT);
		assertThat(router.lookup("anything.example")).isEmpty();
	}

	@Test
	void shutGateStopsThePortResolving() {
		Router router = new Router();
		router.register("c1", List
			.of(Upstream.newBuilder().setHost("").setTargetUrl("http://127.0.0.1:3000").setListenPort(9000).build()));
		router.gate().set("tcp:9000", GateState.SHUT);
		assertThat(router.lookupByPort(9000)).isEmpty();
		router.gate().set("tcp:9000", GateState.OPEN);
		assertThat(router.lookupByPort(9000)).isPresent();
	}

	@Test
	void gateDefaultsDecideTheInitialState() {
		RouteGate gate = new RouteGate(Map.of("demo.local", GateState.SHUT, "*", GateState.SHUT),
				new InMemoryRouteGateStore());
		Router router = new Router(LoadBalance.SMALLEST_CLIENT_ID, LoadBalance.SMALLEST_CLIENT_ID, gate);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("", "http://127.0.0.1:3001")));
		assertThat(router.lookup("demo.local")).isEmpty();
		assertThat(router.lookup("anything.example")).isEmpty();
		assertThat(router.gate().stateOf("demo.local")).isEqualTo(GateState.SHUT);
	}

	@Test
	void gateOverrideBeatsTheDefaultAndAnEmptyKeyMeansTheCatchAll() {
		RouteGate gate = new RouteGate(Map.of("*", GateState.SHUT), new InMemoryRouteGateStore());
		assertThat(gate.stateOf("")).isEqualTo(GateState.SHUT);
		gate.set("", GateState.OPEN);
		assertThat(gate.stateOf("*")).isEqualTo(GateState.OPEN);
		assertThat(gate.stateOf("demo.local")).isEqualTo(GateState.OPEN); // no default =
																			// open
		assertThat(gate.defaultOf("demo.local")).isEqualTo(GateState.OPEN);
	}

	@Test
	void firstRegisteredTargetWins() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
	}

	@Test
	void smallestClientIdWinsTheDomainRegardlessOfRegistrationOrder() {
		Router router = new Router();
		router.register("c5", List.of(upstream("demo.local", "http://127.0.0.5:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		router.register("c9", List.of(upstream("demo.local", "http://127.0.0.9:3000")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
	}

	@Test
	void listenPortTieBreakAlsoPicksTheSmallestClientIdentifier() {
		Router router = new Router();
		router.register("c5", List.of(portUpstream(16379, "tcp://127.0.0.1:7005")));
		router.register("c2", List.of(portUpstream(16379, "tcp://127.0.0.1:7002")));
		assertThat(router.lookupByPort(16379).orElseThrow().clientId()).isEqualTo("c2");
	}

	@Test
	void reRegistrationReplacesClientRoutes() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.9:3000")));
		assertThat(router.lookup("demo.local").orElseThrow().address()).isEqualTo("127.0.0.9:3000");
		assertThat(router.clientCount()).isEqualTo(1);
		// a client re-advertising no routes (a wicket reloaded to none) keeps none
		assertThat(router.register("c1", List.of())).isZero();
		assertThat(router.lookup("demo.local")).isEmpty();
		assertThat(router.clientCount()).isZero();
	}

	@Test
	void removeDropsClientRoutesButKeepsOthers() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		router.remove("c1");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
		assertThat(router.clientCount()).isEqualTo(1);
	}

	@Test
	void registrationWithoutTargetsIsIgnored() {
		Router router = new Router();
		assertThat(router.register("c1", List.of())).isZero();
		assertThat(router.register("", List.of(upstream("a", "http://x:1")))).isZero();
		assertThat(router.clientCount()).isZero();
	}

	@Test
	void unparseableTargetIsSkipped() {
		Router router = new Router();
		assertThat(router.register("c1", List.of(upstream("a", "::::")))).isZero();
	}

	@ParameterizedTest
	@ValueSource(
			strings = { "wasm:file:///opt/hello.wasm", "firecracker:/var/lib/vms/app.ext4?mode=per-connection&warm=4" })
	void wicketTargetIsPassedThroughVerbatim(String target) {
		Router router = new Router();
		assertThat(router.register("c1", List.of(upstream("demo.local", target)))).isOne();
		assertThat(router.lookup("demo.local").orElseThrow().address()).isEqualTo(target);
	}

	private static Upstream portUpstream(int listenPort, String targetUrl) {
		return Upstream.newBuilder()
			.setHost("ignored.local")
			.setTargetUrl(targetUrl)
			.setRewriteHost(false)
			.setListenPort(listenPort)
			.build();
	}

	@Test
	void listenPortRouteIsResolvedByPort() {
		Router router = new Router();
		router.register("c1", List.of(portUpstream(16379, "tcp://127.0.0.1:6379")));
		Router.Route route = router.lookupByPort(16379).orElseThrow();
		assertThat(route.clientId()).isEqualTo("c1");
		assertThat(route.address()).isEqualTo("127.0.0.1:6379");
		assertThat(router.lookupByPort(12345)).isEmpty();
	}

	@Test
	void reRegistrationReplacesListenPortRoutes() {
		Router router = new Router();
		router.register("c1", List.of(portUpstream(16379, "tcp://127.0.0.1:6379")));
		router.register("c1", List.of(portUpstream(16380, "tcp://127.0.0.1:6380")));
		assertThat(router.lookupByPort(16379)).isEmpty();
		assertThat(router.lookupByPort(16380).orElseThrow().address()).isEqualTo("127.0.0.1:6380");
	}

	@Test
	void removeDropsListenPortRoutes() {
		Router router = new Router();
		router.register("c1", List.of(portUpstream(16379, "tcp://127.0.0.1:6379")));
		router.remove("c1");
		assertThat(router.lookupByPort(16379)).isEmpty();
	}

	@Test
	void roundRobinRotatesThroughTheRegisteredTargets() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		router.register("c3", List.of(upstream("demo.local", "http://127.0.0.3:3000")));
		assertThat(picks(router, "demo.local", 4)).containsExactly("c1", "c2", "c3", "c1");
		// a lookup moves the rotation no further
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(picks(router, "demo.local", 1)).containsExactly("c2");
	}

	@Test
	void roundRobinCountersAreIndependentPerKey() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("a.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("a.local", "http://127.0.0.2:3000")));
		router.register("c3", List.of(upstream("b.local", "http://127.0.0.3:3001")));
		router.register("c4", List.of(upstream("b.local", "http://127.0.0.4:3001")));
		assertThat(picks(router, "a.local", 1)).containsExactly("c1");
		assertThat(picks(router, "b.local", 1)).containsExactly("c3");
		assertThat(picks(router, "a.local", 1)).containsExactly("c2");
		assertThat(picks(router, "b.local", 1)).containsExactly("c4");
	}

	@Test
	void roundRobinAlsoAppliesToListenPortRoutes() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(portUpstream(16379, "tcp://10.0.0.1:7001")));
		router.register("c2", List.of(portUpstream(16379, "tcp://10.0.0.2:7002")));
		assertThat(portPicks(router, 16379, 3)).containsExactly("c1", "c2", "c1");
	}

	@Test
	void randomPicksOneOfTheRegisteredTargets() {
		Router router = new Router(LoadBalance.RANDOM, LoadBalance.RANDOM);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		assertThat(picks(router, "demo.local", 10))
			.allMatch(clientId -> clientId.equals("c1") || clientId.equals("c2"));
	}

	@Test
	void httpAndTcpLoadBalancingAreConfiguredIndependently() {
		Router router = new Router(LoadBalance.SMALLEST_CLIENT_ID, LoadBalance.ROUND_ROBIN);
		router.register("c1",
				List.of(upstream("demo.local", "http://127.0.0.1:3000"), portUpstream(16379, "tcp://127.0.0.1:7001")));
		router.register("c2",
				List.of(upstream("demo.local", "http://127.0.0.2:3000"), portUpstream(16379, "tcp://127.0.0.2:7002")));
		assertThat(picks(router, "demo.local", 2)).containsExactly("c1", "c1");
		assertThat(portPicks(router, 16379, 2)).containsExactly("c1", "c2");
	}

	@Test
	void removalRebasesTheRoundRobinRotation() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		assertThat(picks(router, "demo.local", 1)).containsExactly("c1");
		router.remove("c1");
		assertThat(picks(router, "demo.local", 2)).containsExactly("c2", "c2");
		// a key left without a target starts its rotation afresh
		router.remove("c2");
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		assertThat(picks(router, "demo.local", 1)).containsExactly("c1");
	}

	private static Upstream weighted(Upstream upstream, int weight) {
		return upstream.toBuilder().setWeight(weight).build();
	}

	/** The clients of {@code times} picks for the host, each request ending at once. */
	private static List<String> picks(Router router, String host, int times) {
		return IntStream.range(0, times).mapToObj(i -> ended(router.pick(host).orElseThrow())).toList();
	}

	/**
	 * The clients of {@code times} picks for the port, each connection ending at once.
	 */
	private static List<String> portPicks(Router router, int port, int times) {
		return IntStream.range(0, times).mapToObj(i -> ended(router.pickByPort(port).orElseThrow())).toList();
	}

	private static String ended(Router.Pick pick) {
		pick.close();
		return pick.route().clientId();
	}

	/**
	 * Targets each carrying a weight share their key by it, whatever the strategy: each
	 * request ending before the next, smooth weighted round robin, each its weight in
	 * every round of their sum, interleaved, http and tcp routes alike. A weight changed
	 * by a re-registration goes on from where the rotation is; a key left without a
	 * target starts afresh.
	 */
	@ParameterizedTest
	@EnumSource(LoadBalance.class)
	void targetsCarryingWeightsShareTheirKeyByThemWhateverTheStrategy(LoadBalance loadBalance) {
		Router router = new Router(loadBalance, loadBalance);
		Upstream one = upstream("demo.local", "http://127.0.0.1:3001");
		Upstream two = upstream("demo.local", "http://127.0.0.2:3002");
		router.register("c1", List.of(weighted(one, 2), weighted(portUpstream(16379, "tcp://127.0.0.1:7001"), 2)));
		router.register("c2", List.of(weighted(two, 1), weighted(portUpstream(16379, "tcp://127.0.0.2:7002"), 1)));
		assertThat(picks(router, "demo.local", 6)).containsExactly("c1", "c2", "c1", "c1", "c2", "c1");
		assertThat(portPicks(router, 16379, 3)).containsExactly("c1", "c2", "c1");

		router.register("c2", List.of(weighted(two, 2)));
		assertThat(picks(router, "demo.local", 5)).containsExactly("c1", "c2", "c1", "c2", "c1");

		router.remove("c1");
		router.remove("c2");
		router.register("c1", List.of(weighted(one, 2)));
		router.register("c2", List.of(weighted(two, 1)));
		assertThat(picks(router, "demo.local", 3)).containsExactly("c1", "c2", "c1");
	}

	/**
	 * A request goes to the candidate with the fewest in flight for its weight: one whose
	 * request has not ended passes its turn while the other's end, and gets it back once
	 * it ended, the rotation going on from where it was. A pick counts once however often
	 * closed. A candidate keeps its count over a re-registration, a sole one's included,
	 * and over leaving and coming back; the route table tells the counts.
	 */
	@Test
	void aCandidateWithMoreInFlightForItsWeightPassesItsTurnUntilTheyEnd() {
		Router router = new Router();
		Upstream one = upstream("demo.local", "http://127.0.0.1:3001");
		Upstream two = upstream("demo.local", "http://127.0.0.2:3002");
		router.register("c1", List.of(weighted(one, 2)));
		router.register("c2", List.of(weighted(two, 1)));
		Router.Pick slow = router.pick("demo.local").orElseThrow();
		assertThat(slow.route().clientId()).isEqualTo("c1");
		// 1 in flight for 2 against none: c1's turns pass to c2, whose requests end
		assertThat(picks(router, "demo.local", 3)).containsExactly("c2", "c2", "c2");
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(1L, 0L);
		slow.close();
		slow.close();
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(0L, 0L);
		assertThat(picks(router, "demo.local", 3)).containsExactly("c2", "c1", "c1");

		Router.Pick held = router.pick("demo.local").orElseThrow();
		assertThat(held.route().clientId()).isEqualTo("c2");
		router.register("c2", List.of(weighted(two, 3)));
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(0L, 1L);
		router.remove("c2");
		assertThat(picks(router, "demo.local", 1)).containsExactly("c1");
		router.register("c2", List.of(weighted(two, 1)));
		assertThat(picks(router, "demo.local", 2)).containsExactly("c1", "c1");
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(0L, 1L);
		held.close();
		router.remove("c2");
		Router.Pick sole = router.pick("demo.local").orElseThrow();
		router.register("c1", List.of(weighted(one, 4)));
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(1L);
		sole.close();
		assertThat(router.resolve("demo.local").orElseThrow().inFlight()).containsExactly(0L);
	}

	/**
	 * A key not every target of which carries a weight goes by the strategy, the weights
	 * aside: a client predating them among wickets carrying theirs, its requests counted
	 * nowhere. The route table tells the weights, and no winner of a key shared by them;
	 * a weight is unsigned.
	 */
	@Test
	void aKeyWithATargetCarryingNoWeightGoesByTheStrategy() {
		Router router = new Router();
		router.register("c5", List.of(weighted(upstream("demo.local", "http://127.0.0.5:3005"), 3)));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3002")));
		Router.Pick open = router.pick("demo.local").orElseThrow();
		assertThat(picks(router, "demo.local", 3)).containsExactly("c2", "c2", "c2");
		Router.RouteGroup mixed = router.resolve("demo.local").orElseThrow();
		assertThat(mixed.weights()).containsExactly(3L, 0L);
		assertThat(mixed.inFlight()).containsExactly(0L, 0L);
		assertThat(mixed.weighted()).isFalse();
		assertThat(mixed.preferred()).extracting(Router.Route::clientId).isEqualTo("c2");
		open.close();

		router.register("c2", List.of(weighted(upstream("demo.local", "http://127.0.0.2:3002"), 1)));
		Router.RouteGroup shared = router.httpRoutes().get(0);
		assertThat(shared.weighted()).isTrue();
		assertThat(shared.preferred()).isNull();
		assertThat(shared.totalWeight()).isEqualTo(4);
		assertThat(picks(router, "demo.local", 4)).containsExactly("c5", "c5", "c2", "c5");

		router.register("c2", List.of(weighted(upstream("demo.local", "http://127.0.0.2:3002"), -1)));
		assertThat(router.resolve("demo.local").orElseThrow().weights()).containsExactly(3L, 0xFFFF_FFFFL);
	}

	@Test
	void httpRoutesListEveryDomainSortedWithTheDeterministicWinner() {
		Router router = new Router();
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.1:3002")));
		router.register("c1",
				List.of(upstream("demo.local", "http://127.0.0.1:3001"), upstream("", "http://127.0.0.1:3009")));
		List<Router.RouteGroup> routes = router.httpRoutes();
		assertThat(routes).extracting(Router.RouteGroup::key).containsExactly("", "demo.local");
		Router.RouteGroup demo = routes.get(1);
		assertThat(demo.candidates()).extracting(Router.Route::clientId).containsExactly("c2", "c1");
		assertThat(demo.preferred()).isNotNull();
		assertThat(demo.preferred().clientId()).isEqualTo("c1");
	}

	@Test
	void rotatingStrategyHasNoPreferredRouteAndResolveDoesNotAdvanceIt() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3001")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.1:3002")));
		String first = picks(router, "demo.local", 1).get(0);
		Router.RouteGroup resolved = router.resolve("demo.local").orElseThrow();
		assertThat(resolved.preferred()).isNull();
		assertThat(resolved.candidates()).hasSize(2);
		// resolve() leaves the rotation where pick() left it
		assertThat(picks(router, "demo.local", 1).get(0)).isNotEqualTo(first);
	}

	@Test
	void resolveReportsTheMatchedKey() {
		Router router = new Router();
		router.register("c1",
				List.of(upstream("demo.local", "http://127.0.0.1:3001"), upstream("", "http://127.0.0.1:3009")));
		assertThat(router.resolve("demo.local:8000").orElseThrow().key()).isEqualTo("demo.local");
		assertThat(router.resolve("other.local").orElseThrow().key()).isEmpty();
		assertThat(new Router().resolve("demo.local")).isEmpty();
	}

	@Test
	void tcpRoutesAreKeyedByListenPort() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHost("db.local")
					.setTargetUrl("tcp://127.0.0.1:5432")
					.setListenPort(15432)
					.build(), upstream("demo.local", "http://127.0.0.1:3001")));
		List<Router.RouteGroup> tcp = router.tcpRoutes();
		assertThat(tcp).extracting(Router.RouteGroup::key).containsExactly("15432");
		assertThat(tcp.get(0).candidates()).extracting(Router.Route::address).containsExactly("127.0.0.1:5432");
	}

	private static Upstream tcpUpstream(String host, String targetUrl, int listenPort) {
		return Upstream.newBuilder().setHost(host).setTargetUrl(targetUrl).setListenPort(listenPort).build();
	}

	@Test
	void tcpUpstreamIsNotAnHttpRouteForItsHost() {
		Router router = new Router();
		router.register("c1", List.of(tcpUpstream("db.local", "tcp://127.0.0.1:5432", 15432)));
		assertThat(router.lookup("db.local")).isEmpty();
		assertThat(router.lookupByPort(15432).orElseThrow().address()).isEqualTo("127.0.0.1:5432");
	}

	@Test
	void tcpUpstreamWithoutHostDoesNotBecomeTheCatchAll() {
		Router router = new Router();
		router.register("c1", List.of(tcpUpstream("", "tcp://127.0.0.1:7", 10007)));
		assertThat(router.lookup("anything.example")).isEmpty();
		assertThat(router.lookupByPort(10007)).isPresent();
	}

	@Test
	void httpLookupIgnoresATcpUpstreamDeclaredFirstOnTheSameHost() {
		Router router = new Router();
		router.register("c1", List.of(tcpUpstream("dual.local", "tcp://127.0.0.1:6000", 16000),
				upstream("dual.local", "http://127.0.0.1:3000")));
		assertThat(router.lookup("dual.local").orElseThrow().address()).isEqualTo("127.0.0.1:3000");
		assertThat(router.lookupByPort(16000).orElseThrow().address()).isEqualTo("127.0.0.1:6000");
	}

	@Test
	void allowedCidrsPropagateToTheRoute() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHost("demo.local")
					.setTargetUrl("http://127.0.0.1:3000")
					.addAllowedCidrs("10.0.0.0/8")
					.addAllowedCidrs("192.168.1.1")
					.build()));
		Router.Route route = router.lookup("demo.local").orElseThrow();
		assertThat(route.allowedCidrs()).containsExactly("10.0.0.0/8", "192.168.1.1");
	}

	@Test
	void allowedCidrsDefaultToEmpty() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		assertThat(router.lookup("demo.local").orElseThrow().allowedCidrs()).isEmpty();
	}

	@Test
	void allowedCidrsSurviveReRegistrationAndRemovalOfOtherClients() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHost("demo.local")
					.setTargetUrl("http://127.0.0.1:3000")
					.addAllowedCidrs("10.0.0.0/8")
					.build()));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		router.remove("c2");
		assertThat(router.lookup("demo.local").orElseThrow().allowedCidrs()).containsExactly("10.0.0.0/8");
	}

	private static Upstream patternUpstream(String hostPattern, String targetUrl) {
		return Upstream.newBuilder().setHostPattern(hostPattern).setTargetUrl(targetUrl).build();
	}

	@Test
	void hostPatternMatchesTheWholeHostNameWithoutItsPort() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.1:3000")));
		assertThat(router.lookup("svc-a.local").orElseThrow().address()).isEqualTo("127.0.0.1:3000");
		assertThat(router.lookup("svc-b.local:8000")).isPresent();
		// the dot is escaped: the sub domain is not a whole match
		assertThat(router.lookup("svc-a.local.sub")).isEmpty();
		assertThat(router.lookup("other.local")).isEmpty();
	}

	@Test
	void exactMatchBeatsThePatternAndThePatternBeatsTheCatchAll() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3001"),
				upstream("kept.local", "http://127.0.0.1:3004")));
		router.register("c2", List.of(patternUpstream(".*", "http://127.0.0.1:3002")));
		router.register("c3", List.of(upstream("", "http://127.0.0.1:3003")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookup("anything.example").orElseThrow().clientId()).isEqualTo("c2");
		// a host its client advertises no more (a wicket's route without an instance
		// ready) falls through as an unknown host: the pattern, the catch-all, else none
		router.register("c1", List.of(upstream("kept.local", "http://127.0.0.1:3004")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
		router.register("c2", List.of());
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c3");
		router.remove("c3");
		assertThat(router.lookup("demo.local")).isEmpty();
		assertThat(router.lookup("kept.local").orElseThrow().clientId()).isEqualTo("c1");
	}

	@Test
	void patternRoutesApplyTheirKeysInNaturalOrder() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("b.*", "http://127.0.0.1:3001")));
		router.register("c2", List.of(patternUpstream(".*", "http://127.0.0.1:3002")));
		// ".*" sorts before "b.*" and matches everything, so it wins for both hosts
		assertThat(router.lookup("berry.local").orElseThrow().clientId()).isEqualTo("c2");
		assertThat(router.lookup("apple.local").orElseThrow().clientId()).isEqualTo("c2");
	}

	@Test
	void everyClientOnOnePatternGoesThroughLoadBalancing() {
		Router router = new Router();
		router.register("c5", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.5:3005")));
		router.register("c2", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.2:3002")));
		assertThat(router.lookup("svc-a.local").orElseThrow().clientId()).isEqualTo("c2");
	}

	/**
	 * The literal host a client announces alongside its host pattern is carried
	 * separately, so the operator's policy for that host can still be found.
	 */
	@Test
	void theLiteralHostIsCarriedAlongsideThePatternKey() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHost("api.local")
					.setHostPattern("api\\.local")
					.setTargetUrl("http://127.0.0.1:3000")
					.build()));
		Router.Route route = router.lookup("api.local").orElseThrow();
		assertThat(route.routeKey()).isEqualTo("api\\.local");
		assertThat(route.literalHost()).isEqualTo("api.local");
	}

	/**
	 * A pattern with catastrophic backtracking is abandoned once its character budget is
	 * spent, so the lookup falls through instead of holding the event loop (the timeout
	 * is a hang guard, not a speed assertion).
	 */
	@Test
	@Timeout(10)
	void aBacktrackingHostPatternGivesUpInsteadOfBlockingTheLookup() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("(a+)+$", "http://127.0.0.1:3000")));
		assertThat(router.lookup("a".repeat(64) + "!")).isEmpty();
	}

	@Test
	void invalidHostPatternFallsBackToTheLiteralHost() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHost("demo.local")
					.setHostPattern("[")
					.setTargetUrl("http://127.0.0.1:3000")
					.build()));
		assertThat(router.lookup("demo.local").orElseThrow().address()).isEqualTo("127.0.0.1:3000");
		assertThat(router.lookup("other.local")).isEmpty();
	}

	@Test
	void patternRoutesShowUpInTheRouteTableAndResolve() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.1:3000")));
		assertThat(router.httpRoutes()).extracting(Router.RouteGroup::key).containsExactly("svc-.*\\.local");
		Router.RouteGroup resolved = router.resolve("svc-a.local").orElseThrow();
		assertThat(resolved.key()).isEqualTo("svc-.*\\.local");
		assertThat(resolved.candidates()).extracting(Router.Route::clientId).containsExactly("c1");
		assertThat(router.isPattern("svc-.*\\.local")).isTrue();
		assertThat(router.isPattern("svc-a.local")).isFalse();
	}

	@Test
	void reRegistrationReplacesPatternRoutes() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.1:3001")));
		router.register("c1", List.of(patternUpstream("app-.*\\.local", "http://127.0.0.1:3002")));
		assertThat(router.lookup("svc-a.local")).isEmpty();
		assertThat(router.lookup("app-a.local").orElseThrow().address()).isEqualTo("127.0.0.1:3002");
		assertThat(router.isPattern("svc-.*\\.local")).isFalse();
	}

	@Test
	void removeDropsPatternRoutes() {
		Router router = new Router();
		router.register("c1", List.of(patternUpstream("svc-.*\\.local", "http://127.0.0.1:3000")));
		router.remove("c1");
		assertThat(router.lookup("svc-a.local")).isEmpty();
		assertThat(router.httpRoutes()).isEmpty();
		assertThat(router.isPattern("svc-.*\\.local")).isFalse();
	}

	@Test
	void tcpUpstreamKeepsItsPortRouteWhenItDeclaresAPattern() {
		Router router = new Router();
		router.register("c1",
				List.of(Upstream.newBuilder()
					.setHostPattern("svc-.*\\.local")
					.setTargetUrl("tcp://127.0.0.1:5432")
					.setListenPort(15432)
					.build()));
		assertThat(router.lookup("svc-a.local")).isEmpty();
		assertThat(router.lookupByPort(15432).orElseThrow().address()).isEqualTo("127.0.0.1:5432");
	}

}
