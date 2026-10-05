package am.ik.sluice.client.health;

import am.ik.sluice.client.tunnel.TunnelClient;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * UP while the tunnel stream towards the server is established.
 */
@Component
public class TunnelHealthIndicator implements HealthIndicator {

	private final TunnelClient client;

	public TunnelHealthIndicator(TunnelClient client) {
		this.client = client;
	}

	@Override
	public Health health() {
		return this.client.isConnected() ? Health.up().build()
				: Health.down().withDetail("tunnel", "not connected").build();
	}

}
