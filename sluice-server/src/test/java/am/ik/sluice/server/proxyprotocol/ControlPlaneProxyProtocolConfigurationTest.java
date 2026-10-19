package am.ik.sluice.server.proxyprotocol;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.proxyprotocol.ControlPlaneProxyProtocolConfiguration.ProxiedControlPlane;

import static org.assertj.core.api.Assertions.assertThat;

class ControlPlaneProxyProtocolConfigurationTest {

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withUserConfiguration(ControlPlaneProxyProtocolConfiguration.class);

	/**
	 * The switch is read when the bean is created, not by a condition (evaluated at build
	 * time in the native image): the bean definition is the same either way.
	 */
	@Test
	void theControlPlaneKeepsGrpcChannelsUnlessEnabled() {
		this.runner.withBean(SluiceServerProperties.class, () -> SluiceServerProperties.builder().build())
			.run(context -> {
				assertThat(context).hasBean("controlPlaneProxyProtocol");
				assertThat(context).doesNotHaveBean(ProxiedControlPlane.class);
			});
	}

	@Test
	void theEventLoopsOfTheProxiedChannelsShutDownWithTheContext() {
		AtomicReference<ProxiedControlPlane> controlPlane = new AtomicReference<>();
		this.runner
			.withBean(SluiceServerProperties.class,
					() -> SluiceServerProperties.builder().controlProxyProtocol(true).build())
			.run(context -> {
				controlPlane.set(context.getBean(ProxiedControlPlane.class));
				assertThat(controlPlane.get().boss().isShuttingDown()).isFalse();
				assertThat(controlPlane.get().worker().isShuttingDown()).isFalse();
			});
		assertThat(controlPlane.get().boss().isShuttingDown()).isTrue();
		assertThat(controlPlane.get().worker().isShuttingDown()).isTrue();
	}

}
