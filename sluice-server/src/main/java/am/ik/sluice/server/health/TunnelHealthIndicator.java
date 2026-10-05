package am.ik.sluice.server.health;

import am.ik.sluice.server.tunnel.SessionRegistry;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * UP while at least one tunnel client is connected.
 */
@Component
public class TunnelHealthIndicator implements HealthIndicator {

	private final SessionRegistry sessions;

	public TunnelHealthIndicator(SessionRegistry sessions) {
		this.sessions = sessions;
	}

	@Override
	public Health health() {
		int count = this.sessions.count();
		if (count == 0) {
			return Health.down().withDetail("clients", 0).build();
		}
		return Health.up().withDetail("clients", count).build();
	}

}
