package am.ik.sluice.server.route;

import java.util.List;

import am.ik.sluice.v1.proto.Upstream;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RouterTest {

	private static Upstream upstream(String host, String targetUrl) {
		return Upstream.newBuilder().setHost(host).setTargetUrl(targetUrl).setPreserveHost(true).build();
	}

	@Test
	void exactHostMatch() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		Router.Route route = router.lookup("demo.local").orElseThrow();
		assertThat(route.clientId()).isEqualTo("c1");
		assertThat(route.address()).isEqualTo("127.0.0.1:3000");
		assertThat(route.preserveHost()).isTrue();
	}

	@Test
	void preserveHostFlagPropagatesToRoute() {
		Router router = new Router();
		Upstream upstream = Upstream.newBuilder()
			.setHost("demo.local")
			.setTargetUrl("http://127.0.0.1:3000")
			.setPreserveHost(false)
			.build();
		router.register("c1", List.of(upstream));
		assertThat(router.lookup("demo.local").orElseThrow().preserveHost()).isFalse();
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

	@Test
	void wasmLocatorTargetIsPassedThroughVerbatim() {
		Router router = new Router();
		String locator = "wasm:file:///opt/hello.wasm";
		assertThat(router.register("c1", List.of(upstream("demo.local", locator)))).isOne();
		assertThat(router.lookup("demo.local").orElseThrow().address()).isEqualTo(locator);
	}

	private static Upstream portUpstream(int listenPort, String targetUrl) {
		return Upstream.newBuilder()
			.setHost("ignored.local")
			.setTargetUrl(targetUrl)
			.setPreserveHost(true)
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
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c3");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
	}

	@Test
	void roundRobinCountersAreIndependentPerKey() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("a.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("a.local", "http://127.0.0.2:3000")));
		router.register("c3", List.of(upstream("b.local", "http://127.0.0.3:3001")));
		router.register("c4", List.of(upstream("b.local", "http://127.0.0.4:3001")));
		assertThat(router.lookup("a.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookup("b.local").orElseThrow().clientId()).isEqualTo("c3");
		assertThat(router.lookup("a.local").orElseThrow().clientId()).isEqualTo("c2");
		assertThat(router.lookup("b.local").orElseThrow().clientId()).isEqualTo("c4");
	}

	@Test
	void roundRobinAlsoAppliesToListenPortRoutes() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(portUpstream(16379, "tcp://10.0.0.1:7001")));
		router.register("c2", List.of(portUpstream(16379, "tcp://10.0.0.2:7002")));
		assertThat(router.lookupByPort(16379).orElseThrow().address()).isEqualTo("10.0.0.1:7001");
		assertThat(router.lookupByPort(16379).orElseThrow().address()).isEqualTo("10.0.0.2:7002");
		assertThat(router.lookupByPort(16379).orElseThrow().address()).isEqualTo("10.0.0.1:7001");
	}

	@Test
	void randomPicksOneOfTheRegisteredTargets() {
		Router router = new Router(LoadBalance.RANDOM, LoadBalance.RANDOM);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		for (int i = 0; i < 10; i++) {
			assertThat(router.lookup("demo.local").orElseThrow().clientId()).isIn("c1", "c2");
		}
	}

	@Test
	void httpAndTcpLoadBalancingAreConfiguredIndependently() {
		Router router = new Router(LoadBalance.SMALLEST_CLIENT_ID, LoadBalance.ROUND_ROBIN);
		router.register("c1",
				List.of(upstream("demo.local", "http://127.0.0.1:3000"), portUpstream(16379, "tcp://127.0.0.1:7001")));
		router.register("c2",
				List.of(upstream("demo.local", "http://127.0.0.2:3000"), portUpstream(16379, "tcp://127.0.0.2:7002")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookupByPort(16379).orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookupByPort(16379).orElseThrow().clientId()).isEqualTo("c2");
	}

	@Test
	void removalRebasesTheRoundRobinRotation() {
		Router router = new Router(LoadBalance.ROUND_ROBIN, LoadBalance.ROUND_ROBIN);
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		router.register("c2", List.of(upstream("demo.local", "http://127.0.0.2:3000")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		router.remove("c1");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c2");
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
		String first = router.lookup("demo.local").orElseThrow().clientId();
		Router.RouteGroup resolved = router.resolve("demo.local").orElseThrow();
		assertThat(resolved.preferred()).isNull();
		assertThat(resolved.candidates()).hasSize(2);
		// resolve() leaves the rotation where lookup() left it
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isNotEqualTo(first);
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
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3001")));
		router.register("c2", List.of(patternUpstream(".*", "http://127.0.0.1:3002")));
		router.register("c3", List.of(upstream("", "http://127.0.0.1:3003")));
		assertThat(router.lookup("demo.local").orElseThrow().clientId()).isEqualTo("c1");
		assertThat(router.lookup("anything.example").orElseThrow().clientId()).isEqualTo("c2");
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
