package am.ik.sluice.server.route;

import java.util.List;

import am.ik.sluice.v1.proto.Upstream;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RouterTest {

	private static Upstream upstream(String host, String targetUrl) {
		return Upstream.newBuilder().setHost(host).setTargetUrl(targetUrl).build();
	}

	@Test
	void exactHostMatch() {
		Router router = new Router();
		router.register("c1", List.of(upstream("demo.local", "http://127.0.0.1:3000")));
		Router.Route route = router.lookup("demo.local").orElseThrow();
		assertThat(route.clientId()).isEqualTo("c1");
		assertThat(route.address()).isEqualTo("127.0.0.1:3000");
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

}
