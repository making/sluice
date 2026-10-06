package am.ik.sluice.server.config;

import java.util.Map;

import am.ik.sluice.server.route.LoadBalance;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

class SluiceServerPropertiesTest {

	private static SluiceServerProperties bind(Map<String, String> properties) {
		return new Binder(new MapConfigurationPropertySource(properties))
			.bind("sluice", Bindable.of(SluiceServerProperties.class))
			.get();
	}

	@Test
	void loadBalanceDefaultsToSmallestClientId() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.httpLoadBalance()).isEqualTo(LoadBalance.SMALLEST_CLIENT_ID);
		assertThat(properties.tcpLoadBalance()).isEqualTo(LoadBalance.SMALLEST_CLIENT_ID);
	}

	@Test
	void loadBalanceStrategiesAreBoundIndependentlyInKebabCase() {
		SluiceServerProperties properties = bind(
				Map.of("sluice.http-load-balance", "round-robin", "sluice.tcp-load-balance", "random"));
		assertThat(properties.httpLoadBalance()).isEqualTo(LoadBalance.ROUND_ROBIN);
		assertThat(properties.tcpLoadBalance()).isEqualTo(LoadBalance.RANDOM);
	}

}
