package am.ik.sluice.server.config;

import java.time.Duration;
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

	@Test
	void accessControlDefaultsToEmptyLists() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.accessControl().allowCidrs()).isEmpty();
		assertThat(properties.accessControl().denyCidrs()).isEmpty();
	}

	@Test
	void accessControlListsAreBoundInKebabCase() {
		SluiceServerProperties properties = bind(Map.of("sluice.access-control.allow-cidrs", "10.0.0.0/8,127.0.0.1",
				"sluice.access-control.deny-cidrs", "203.0.113.0/24"));
		assertThat(properties.accessControl().allowCidrs()).containsExactly("10.0.0.0/8", "127.0.0.1");
		assertThat(properties.accessControl().denyCidrs()).containsExactly("203.0.113.0/24");
	}

	@Test
	void accessControlTrustedProxiesDefaultToEmpty() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.accessControl().trustedProxyCidrs()).isEmpty();
	}

	@Test
	void accessControlTrustedProxiesAreBoundInKebabCase() {
		SluiceServerProperties properties = bind(
				Map.of("sluice.access-control.trusted-proxy-cidrs", "10.0.0.0/8,127.0.0.1"));
		assertThat(properties.accessControl().trustedProxyCidrs()).containsExactly("10.0.0.0/8", "127.0.0.1");
	}

	@Test
	void proxyProtocolDefaultsToDisabled() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.proxyProtocol()).isFalse();
	}

	@Test
	void proxyProtocolIsBoundInKebabCase() {
		SluiceServerProperties properties = bind(Map.of("sluice.proxy-protocol", "true"));
		assertThat(properties.proxyProtocol()).isTrue();
	}

	@Test
	void limitsDefaultToDerivedConnectionLimitsATenSecondHeadDeadlineAndAMinuteOfIdle() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.limits().maxConnections()).isZero();
		assertThat(properties.limits().maxConnectionsPerAddress()).isZero();
		assertThat(properties.limits().headTimeout()).isEqualTo(Duration.ofSeconds(10));
		assertThat(properties.limits().idleTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().h2QueueBudget().toBytes()).isZero();
		assertThat(properties.limits().halfCloseProbeInterval()).isEqualTo(Duration.ofSeconds(1));
	}

	@Test
	void aZeroHalfCloseProbeIntervalTurnsTheProbeOffAndANegativeOneFallsBackToTheDefault() {
		assertThat(bind(Map.of("sluice.limits.half-close-probe-interval", "0")).limits().halfCloseProbeInterval())
			.isZero();
		assertThat(bind(Map.of("sluice.limits.half-close-probe-interval", "-1s")).limits().halfCloseProbeInterval())
			.isEqualTo(Duration.ofSeconds(1));
	}

	@Test
	void aNonPositiveIdleTimeoutFallsBackToTheDefault() {
		SluiceServerProperties properties = bind(Map.of("sluice.limits.idle-timeout", "0s"));
		assertThat(properties.limits().idleTimeout()).isEqualTo(Duration.ofSeconds(60));
	}

	@Test
	void limitsAreBoundInKebabCase() {
		SluiceServerProperties properties = bind(
				Map.of("sluice.limits.max-connections", "100", "sluice.limits.max-connections-per-address", "10",
						"sluice.limits.head-timeout", "2s", "sluice.limits.idle-timeout", "3s",
						"sluice.limits.h2-queue-budget", "512KB", "sluice.limits.half-close-probe-interval", "250ms"));
		assertThat(properties.limits().maxConnections()).isEqualTo(100);
		assertThat(properties.limits().maxConnectionsPerAddress()).isEqualTo(10);
		assertThat(properties.limits().headTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(properties.limits().idleTimeout()).isEqualTo(Duration.ofSeconds(3));
		assertThat(properties.limits().h2QueueBudget().toBytes()).isEqualTo(512 * 1024);
		assertThat(properties.limits().halfCloseProbeInterval()).isEqualTo(Duration.ofMillis(250));
	}

}
