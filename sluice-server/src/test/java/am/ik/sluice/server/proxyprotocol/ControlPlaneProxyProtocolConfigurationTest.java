package am.ik.sluice.server.proxyprotocol;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.proxyprotocol.ControlPlaneProxyProtocolConfiguration.ProxiedControlPlane;

import static org.assertj.core.api.Assertions.assertThat;

class ControlPlaneProxyProtocolConfigurationTest {

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withUserConfiguration(ControlPlaneProxyProtocolConfiguration.class)
		.withBean(SluiceServerProperties.class, () -> SluiceServerProperties.builder().build());

	@Test
	void theControlPlaneKeepsGrpcChannelsUnlessEnabled() {
		this.runner.run(context -> assertThat(context).doesNotHaveBean(ProxiedControlPlane.class));
		this.runner.withPropertyValues("sluice.control-proxy-protocol=false")
			.run(context -> assertThat(context).doesNotHaveBean(ProxiedControlPlane.class));
	}

	@Test
	void theEventLoopsOfTheProxiedChannelsShutDownWithTheContext() {
		AtomicReference<ProxiedControlPlane> controlPlane = new AtomicReference<>();
		this.runner.withPropertyValues("sluice.control-proxy-protocol=true").run(context -> {
			controlPlane.set(context.getBean(ProxiedControlPlane.class));
			assertThat(controlPlane.get().boss().isShuttingDown()).isFalse();
			assertThat(controlPlane.get().worker().isShuttingDown()).isFalse();
		});
		assertThat(controlPlane.get().boss().isShuttingDown()).isTrue();
		assertThat(controlPlane.get().worker().isShuttingDown()).isTrue();
	}

}
