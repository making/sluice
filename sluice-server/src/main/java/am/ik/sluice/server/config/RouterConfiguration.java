package am.ik.sluice.server.config;

import am.ik.sluice.server.route.Router;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link Router} with the load balancing strategies configured through
 * {@code sluice.http-load-balance} / {@code sluice.tcp-load-balance}.
 */
@Configuration(proxyBeanMethods = false)
class RouterConfiguration {

	@Bean
	Router router(SluiceServerProperties properties) {
		return new Router(properties.httpLoadBalance(), properties.tcpLoadBalance());
	}

}
