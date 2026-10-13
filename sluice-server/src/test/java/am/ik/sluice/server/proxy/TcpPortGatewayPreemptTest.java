package am.ik.sluice.server.proxy;

import java.util.Set;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.AdvertisedSessions;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.TaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tcp listen port follows the deterministic owner: the smallest client id. A larger
 * id is rejected while a live smaller-id owner holds the port; a gone owner always loses
 * the port.
 */
class TcpPortGatewayPreemptTest {

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private final TaskExecutor directExecutor = Runnable::run;

	private final SessionRegistry sessions = new SessionRegistry();

	private final Router router = new Router();

	private final TcpPortGateway gateway = new TcpPortGateway(this.router, this.sessions,
			SluiceServerProperties.builder().dataPort(0).tcpPortRange("19000").build(),
			new AccessControl(SluiceServerProperties.builder().build()), this.directExecutor, this.meters,
			new AccessLogger(SluiceServerProperties.builder().build()),
			new ConnectionLimits(SluiceServerProperties.builder().build(), this.meters));

	private TunnelSession session(String clientId) {
		return AdvertisedSessions.advertised(TunnelSession.builder()
			.clientId(clientId)
			.router(this.router)
			.registry(this.sessions)
			.tcpRoutes(this.gateway));
	}

	@AfterEach
	void cleanup() {
		this.sessions.all().forEach(TunnelSession::close);
		this.gateway.close();
	}

	@Test
	void laterClientWithSmallerIdentifierPreemptsThePort() {
		TunnelSession large = session("client-b");
		assertThat(this.gateway.reconcile("client-b", Set.of(19000))).isEmpty();
		assertThat(this.gateway.isBound(19000)).isTrue();
		TunnelSession small = session("client-a");
		assertThat(this.gateway.reconcile("client-a", Set.of(19000))).isEmpty();
		// the smaller id took the port over and keeps it after the larger id leaves
		large.close();
		assertThat(this.gateway.isBound(19000)).isTrue();
		small.close();
		assertThat(this.gateway.isBound(19000)).isFalse();
	}

	@Test
	void laterClientWithLargerIdentifierIsRejected() {
		TunnelSession small = session("client-a");
		assertThat(this.gateway.reconcile("client-a", Set.of(19000))).isEmpty();
		TunnelSession large = session("client-b");
		assertThat(this.gateway.reconcile("client-b", Set.of(19000))).containsExactly(19000);
		// the owner keeps the port
		assertThat(this.gateway.isBound(19000)).isTrue();
		large.close();
		small.close();
	}

	@Test
	void goneOwnerPortIsTakenOver() {
		TunnelSession owner = session("client-a");
		assertThat(this.gateway.reconcile("client-a", Set.of(19000))).isEmpty();
		// drop the owner session without its unbind having run
		this.sessions.remove(owner);
		TunnelSession newcomer = session("client-c");
		assertThat(this.gateway.reconcile("client-c", Set.of(19000))).isEmpty();
		assertThat(this.gateway.isBound(19000)).isTrue();
		newcomer.close();
		owner.close();
	}

}
