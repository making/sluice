package am.ik.sluice.server.health;

import am.ik.sluice.server.tunnel.ClusterLifecycle;
import am.ik.sluice.server.tunnel.SessionRegistry;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Liveness of the server process: always UP while the context lives. The connected client
 * count and the draining flag are details -- a node with zero clients must still enter
 * the load balancer rotation.
 */
@Component
public class TunnelHealthIndicator implements HealthIndicator {

	private final SessionRegistry sessions;

	private final ClusterLifecycle lifecycle;

	public TunnelHealthIndicator(SessionRegistry sessions, ClusterLifecycle lifecycle) {
		this.sessions = sessions;
		this.lifecycle = lifecycle;
	}

	@Override
	public Health health() {
		return Health.up()
			.withDetail("clients", this.sessions.count())
			.withDetail("draining", this.lifecycle.isDraining())
			.build();
	}

}
