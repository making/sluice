package am.ik.sluice.server.cluster;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * Tags every metric with the node id, so a scrapes aggregation over N nodes stays
 * attributable.
 */
@Configuration(proxyBeanMethods = false)
public class NodeMetricsCustomizer {

	@Bean
	MeterRegistryCustomizer<MeterRegistry> nodeCommonTag(SluiceServerProperties properties) {
		return registry -> registry.config().commonTags("node", properties.node().id());
	}

}
