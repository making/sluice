package am.ik.sluice.server.config;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import am.ik.sluice.server.config.SluiceServerProperties.AccessLog.Type;
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
	void accessLogTypesBindWithConnectionShortForAcceptAndCloseAndRoutesInheritWhatTheyLeaveOut() {
		SluiceServerProperties defaults = bind(Map.of("sluice.data-port", "8000"));
		assertThat(defaults.accessLog().types()).containsExactly(Type.ACCEPT, Type.CLOSE, Type.REJECT, Type.REQUEST);
		assertThat(defaults.route().accessLog()).isEmpty();
		SluiceServerProperties bound = bind(Map.of("sluice.access-log.types", "connection",
				"sluice.route.access-log.[a.local].types", "request,close", "sluice.route.access-log.[tcp:9000].types",
				"connection", "sluice.route.access-log.[b.local].enabled", "false"));
		assertThat(bound.accessLog().types()).containsExactly(Type.ACCEPT, Type.CLOSE, Type.REJECT);
		assertThat(bound.route().accessLog()).containsOnlyKeys("a.local", "tcp:9000", "b.local");
		assertThat(bound.route().accessLog().get("a.local"))
			.isEqualTo(new SluiceServerProperties.Route.AccessLog(null, Set.of(Type.CLOSE, Type.REQUEST)));
		assertThat(bound.route().accessLog().get("tcp:9000"))
			.isEqualTo(new SluiceServerProperties.Route.AccessLog(null, Set.of(Type.ACCEPT, Type.CLOSE, Type.REJECT)));
		assertThat(bound.route().accessLog().get("b.local"))
			.isEqualTo(new SluiceServerProperties.Route.AccessLog(false, null));
	}

	@Test
	void proxyProtocolDefaultsToDisabled() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.dataProxyProtocol()).isFalse();
		assertThat(properties.controlProxyProtocol()).isFalse();
	}

	@Test
	void proxyProtocolIsBoundInKebabCase() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-proxy-protocol", "true"));
		assertThat(properties.dataProxyProtocol()).isTrue();
		assertThat(properties.controlProxyProtocol()).isFalse();
		properties = bind(Map.of("sluice.control-proxy-protocol", "true"));
		assertThat(properties.dataProxyProtocol()).isFalse();
		assertThat(properties.controlProxyProtocol()).isTrue();
	}

	@Test
	void limitsDefaultToDerivedConnectionLimitsATenSecondHeadDeadlineAndAMinuteOfIdleAndOfStall() {
		SluiceServerProperties properties = bind(Map.of("sluice.data-port", "8000"));
		assertThat(properties.limits().maxConnections()).isZero();
		assertThat(properties.limits().maxConnectionsPerAddress()).isZero();
		assertThat(properties.limits().headTimeout()).isEqualTo(Duration.ofSeconds(10));
		assertThat(properties.limits().idleTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().h2QueueBudget().toBytes()).isZero();
		assertThat(properties.limits().h2StreamWaitTimeout()).isEqualTo(Duration.ofSeconds(30));
		assertThat(properties.limits().halfCloseProbeInterval()).isEqualTo(Duration.ofSeconds(1));
		assertThat(properties.limits().stallTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().stallMinBytes().toBytes()).isEqualTo(16 * 1024);
		assertThat(properties.limits().tunnelStallTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().verbatimIdleTimeout()).isZero();
		assertThat(properties.limits().tcpKeepAlive()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().advertiseTimeout()).isEqualTo(Duration.ofSeconds(10));
		assertThat(properties.limits().maxPendingTunnelStreams()).isEqualTo(256);
		assertThat(properties.limits().maxPendingTunnelStreamsPerAddress()).isZero();
		assertThat(properties.limits().maxTunnelSessions()).isEqualTo(1024);
		assertThat(properties.limits().maxTunnelSessionsPerAddress()).isZero();
	}

	@Test
	void tunnelSessionLimitsBindAndANonPositiveGlobalOneFallsBackToTheDefault() {
		SluiceServerProperties bound = bind(
				Map.of("sluice.limits.max-tunnel-sessions", "8", "sluice.limits.max-tunnel-sessions-per-address", "2"));
		assertThat(bound.limits().maxTunnelSessions()).isEqualTo(8);
		assertThat(bound.limits().maxTunnelSessionsPerAddress()).isEqualTo(2);
		assertThat(bind(Map.of("sluice.limits.max-tunnel-sessions", "0")).limits().maxTunnelSessions()).isEqualTo(1024);
		assertThat(bind(Map.of("sluice.limits.max-tunnel-sessions-per-address", "-1")).limits()
			.maxTunnelSessionsPerAddress()).isZero();
		assertThat(SluiceServerProperties.Limits.builder().build().maxTunnelSessions()).as("the builder's default")
			.isEqualTo(1024);
	}

	@Test
	void pendingTunnelStreamLimitsBindAndANonPositiveGlobalOneFallsBackToTheDefault() {
		SluiceServerProperties bound = bind(Map.of("sluice.limits.max-pending-tunnel-streams", "8",
				"sluice.limits.max-pending-tunnel-streams-per-address", "2"));
		assertThat(bound.limits().maxPendingTunnelStreams()).isEqualTo(8);
		assertThat(bound.limits().maxPendingTunnelStreamsPerAddress()).isEqualTo(2);
		assertThat(bind(Map.of("sluice.limits.max-pending-tunnel-streams", "0")).limits().maxPendingTunnelStreams())
			.isEqualTo(256);
		assertThat(bind(Map.of("sluice.limits.max-pending-tunnel-streams-per-address", "-1")).limits()
			.maxPendingTunnelStreamsPerAddress()).isZero();
		assertThat(SluiceServerProperties.Limits.builder().build().maxPendingTunnelStreams())
			.as("the builder's default")
			.isEqualTo(256);
	}

	@Test
	void anAdvertiseTimeoutBindsAndANonPositiveOneFallsBackToTheDefault() {
		assertThat(bind(Map.of("sluice.limits.advertise-timeout", "3s")).limits().advertiseTimeout())
			.isEqualTo(Duration.ofSeconds(3));
		assertThat(bind(Map.of("sluice.limits.advertise-timeout", "0")).limits().advertiseTimeout())
			.isEqualTo(Duration.ofSeconds(10));
		assertThat(bind(Map.of("sluice.limits.advertise-timeout", "-1s")).limits().advertiseTimeout())
			.isEqualTo(Duration.ofSeconds(10));
	}

	@Test
	void aVerbatimIdleTimeoutBindsAndANegativeOneIsOff() {
		assertThat(bind(Map.of("sluice.limits.verbatim-idle-timeout", "10m")).limits().verbatimIdleTimeout())
			.isEqualTo(Duration.ofMinutes(10));
		assertThat(bind(Map.of("sluice.limits.verbatim-idle-timeout", "-1s")).limits().verbatimIdleTimeout()).isZero();
	}

	@Test
	void aZeroTunnelStallTimeoutTurnsTheWatchdogOffAndANegativeOneFallsBackToTheDefault() {
		assertThat(bind(Map.of("sluice.limits.tunnel-stall-timeout", "0")).limits().tunnelStallTimeout()).isZero();
		assertThat(bind(Map.of("sluice.limits.tunnel-stall-timeout", "-1s")).limits().tunnelStallTimeout())
			.isEqualTo(Duration.ofSeconds(60));
		assertThat(bind(Map.of("sluice.limits.tunnel-stall-timeout", "90s")).limits().tunnelStallTimeout())
			.isEqualTo(Duration.ofSeconds(90));
	}

	@Test
	void aZeroH2StreamWaitTimeoutTurnsTheDeadlineOffAndANegativeOneFallsBackToTheDefault() {
		assertThat(bind(Map.of("sluice.limits.h2-stream-wait-timeout", "0")).limits().h2StreamWaitTimeout()).isZero();
		assertThat(bind(Map.of("sluice.limits.h2-stream-wait-timeout", "-1s")).limits().h2StreamWaitTimeout())
			.isEqualTo(Duration.ofSeconds(30));
		assertThat(bind(Map.of("sluice.limits.h2-stream-wait-timeout", "5s")).limits().h2StreamWaitTimeout())
			.isEqualTo(Duration.ofSeconds(5));
	}

	@Test
	void aNonPositiveStallTimeoutOrMinimumFallsBackToTheDefault() {
		SluiceServerProperties properties = bind(
				Map.of("sluice.limits.stall-timeout", "0s", "sluice.limits.stall-min-bytes", "0B"));
		assertThat(properties.limits().stallTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(properties.limits().stallMinBytes().toBytes()).isEqualTo(16 * 1024);
	}

	@Test
	void aZeroHalfCloseProbeIntervalOrTcpKeepAliveTurnsItOffAndANegativeOneFallsBackToTheDefault() {
		assertThat(bind(Map.of("sluice.limits.half-close-probe-interval", "0")).limits().halfCloseProbeInterval())
			.isZero();
		assertThat(bind(Map.of("sluice.limits.half-close-probe-interval", "-1s")).limits().halfCloseProbeInterval())
			.isEqualTo(Duration.ofSeconds(1));
		assertThat(bind(Map.of("sluice.limits.tcp-keep-alive", "0")).limits().tcpKeepAlive()).isZero();
		assertThat(bind(Map.of("sluice.limits.tcp-keep-alive", "-1s")).limits().tcpKeepAlive())
			.isEqualTo(Duration.ofSeconds(60));
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
						"sluice.limits.h2-queue-budget", "512KB", "sluice.limits.half-close-probe-interval", "250ms",
						"sluice.limits.stall-timeout", "5s", "sluice.limits.stall-min-bytes", "1B"));
		assertThat(properties.limits().maxConnections()).isEqualTo(100);
		assertThat(properties.limits().maxConnectionsPerAddress()).isEqualTo(10);
		assertThat(properties.limits().headTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(properties.limits().idleTimeout()).isEqualTo(Duration.ofSeconds(3));
		assertThat(properties.limits().h2QueueBudget().toBytes()).isEqualTo(512 * 1024);
		assertThat(properties.limits().halfCloseProbeInterval()).isEqualTo(Duration.ofMillis(250));
		assertThat(properties.limits().stallTimeout()).isEqualTo(Duration.ofSeconds(5));
		assertThat(properties.limits().stallMinBytes().toBytes()).isEqualTo(1);
	}

}
