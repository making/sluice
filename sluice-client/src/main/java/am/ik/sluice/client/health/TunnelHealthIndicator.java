package am.ik.sluice.client.health;

import java.util.StringJoiner;

import am.ik.sluice.client.tunnel.TunnelClient;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * UP while at least one per-node tunnel stream is established; the per-node states are
 * reported as details.
 */
@Component
public class TunnelHealthIndicator implements HealthIndicator {

	private final TunnelClient client;

	public TunnelHealthIndicator(TunnelClient client) {
		this.client = client;
	}

	@Override
	public Health health() {
		boolean connected = this.client.isConnected();
		Health.Builder builder = connected ? Health.up() : Health.down();
		StringJoiner nodes = new StringJoiner(",", "nodes=[", "]");
		this.client.nodeStates().forEach((node, up) -> nodes.add(node + ":" + (up ? "up" : "down")));
		return builder.withDetail("tunnel", connected ? "connected" : "not connected")
			.withDetail("nodes", nodes.toString())
			.build();
	}

}
