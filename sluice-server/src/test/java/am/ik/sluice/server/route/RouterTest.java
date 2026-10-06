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

}
