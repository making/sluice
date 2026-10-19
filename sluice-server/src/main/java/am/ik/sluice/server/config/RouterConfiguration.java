package am.ik.sluice.server.config;

import am.ik.sluice.server.route.InMemoryRouteGateStore;
import am.ik.sluice.server.route.RouteGate;
import am.ik.sluice.server.route.RouteGateStore;
import am.ik.sluice.server.route.Router;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link Router} with the load balancing strategies configured through
 * {@code sluice.http-load-balance} / {@code sluice.tcp-load-balance}, and the route gates
 * ({@code sluice.route.gate.<route>} the console can toggle).
 */
@Configuration(proxyBeanMethods = false)
class RouterConfiguration {

	@Bean
	RouteGateStore routeGateStore() {
		// in memory: the gates come back to their configured defaults on restart
		return new InMemoryRouteGateStore();
	}

	@Bean
	RouteGate routeGate(SluiceServerProperties properties, RouteGateStore store) {
		return new RouteGate(properties.route().gate(), store);
	}

	@Bean
	Router router(SluiceServerProperties properties, RouteGate routeGate) {
		return new Router(properties.httpLoadBalance(), properties.tcpLoadBalance(), routeGate);
	}

}
