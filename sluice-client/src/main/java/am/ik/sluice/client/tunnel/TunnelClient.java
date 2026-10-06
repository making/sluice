package am.ik.sluice.client.tunnel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.v1.proto.ListNodesResponse;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Fan-out supervisor: keeps one {@link NodeConnection} tunnel stream per server node. The
 * {@code sluice.server-url} is the bootstrap; the node list learned via {@code ListNodes}
 * and membership updates opens one stream per node (keyed by node id) and closes streams
 * of nodes that left. All membership transitions run on a single-threaded executor.
 */
@Component
public class TunnelClient implements SmartLifecycle, NodeConnection.Listener {

	private static final Logger log = LoggerFactory.getLogger(TunnelClient.class);

	private final SluiceClientProperties properties;

	private final TaskExecutor taskExecutor;

	private final MeterRegistry meterRegistry;

	/** Process-stable client identity sent on every stream. */
	private final String clientId;

	private final ConcurrentHashMap<String, NodeConnection> connectionsByNode = new ConcurrentHashMap<>();

	private final ExecutorService supervisor = Executors
		.newSingleThreadExecutor(r -> Thread.ofVirtual().name("sluice-supervisor").unstarted(r));

	private final AtomicBoolean membershipLearned = new AtomicBoolean();

	private volatile boolean running;

	TunnelClient(SluiceClientProperties properties, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			MeterRegistry meterRegistry) {
		this.properties = properties;
		this.taskExecutor = taskExecutor;
		this.meterRegistry = meterRegistry;
		String configured = clientid(properties);
		this.clientId = configured == null || configured.isBlank() ? UUID.randomUUID().toString() : configured;
		meterRegistry.gauge("sluice.connections.active", this.connectionsByNode,
				map -> map.values().stream().mapToInt(NodeConnection::activeConnections).sum());
	}

	private static @Nullable String clientid(SluiceClientProperties properties) {
		SluiceClientProperties.@Nullable Client client = properties.client();
		return client == null ? null : client.id();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private @Nullable SluiceClientProperties properties;

		private @Nullable TaskExecutor taskExecutor;

		private @Nullable MeterRegistry meterRegistry;

		private Builder() {
		}

		public Builder properties(SluiceClientProperties properties) {
			this.properties = properties;
			return this;
		}

		public Builder taskExecutor(TaskExecutor taskExecutor) {
			this.taskExecutor = taskExecutor;
			return this;
		}

		public Builder meterRegistry(MeterRegistry meterRegistry) {
			this.meterRegistry = meterRegistry;
			return this;
		}

		public TunnelClient build() {
			return new TunnelClient(Objects.requireNonNull(this.properties, "properties is required"),
					Objects.requireNonNull(this.taskExecutor, "taskExecutor is required"),
					Objects.requireNonNull(this.meterRegistry, "meterRegistry is required"));
		}

	}

	public boolean isConnected() {
		for (NodeConnection connection : this.connectionsByNode.values()) {
			if (connection.isConnected()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Connection state per node id (or the bootstrap url before the node id is known).
	 */
	public Map<String, Boolean> nodeStates() {
		Map<String, Boolean> states = new LinkedHashMap<>();
		this.connectionsByNode.forEach((key, connection) -> states.put(key, connection.isConnected()));
		return states;
	}

	@Override
	public void start() {
		if (this.properties.serverUrl() == null || this.properties.serverUrl().isBlank()) {
			throw new IllegalStateException("sluice.server-url is required");
		}
		if (this.properties.upstreamMap().isEmpty()) {
			throw new IllegalStateException("sluice.client.upstream is required");
		}
		this.running = true;
		this.startConnection("", this.properties.serverUrl());
	}

	private void startConnection(String nodeId, String url) {
		NodeConnection connection = NodeConnection.builder()
			.key(nodeId.isBlank() ? url : nodeId)
			.nodeId(nodeId)
			.url(url)
			.properties(this.properties)
			.clientId(this.clientId)
			.taskExecutor(this.taskExecutor)
			.listener(this)
			.meterRegistry(this.meterRegistry)
			.build();
		this.connectionsByNode.put(nodeId.isBlank() ? url : nodeId, connection);
		connection.start();
	}

	@Override
	public void onConnected(NodeConnection connection, String nodeId) {
		this.supervisor.execute(() -> {
			// rename the connection after the node id is known, so the bootstrap
			// connection becomes the stream of that node instead of a duplicate
			if (!nodeId.isBlank()) {
				this.rekey(connection, nodeId);
			}
			// self-healing: re-fetch the node list on every established stream
			this.reconcile(this.fetchNodes(connection));
		});
	}

	private void rekey(NodeConnection connection, String nodeId) {
		NodeConnection existing = this.connectionsByNode.put(nodeId, connection);
		if (existing == connection) {
			return;
		}
		// remove the old key only when it still maps to this connection
		this.connectionsByNode.remove(connection.key(), connection);
		if (existing != null && existing != connection) {
			log.info("duplicate stream for node {}; closing the older one", nodeId);
			existing.close();
			this.connectionsByNode.remove(nodeId, existing);
		}
	}

	private List<am.ik.sluice.v1.proto.Node> fetchNodes(NodeConnection connection) {
		try {
			ListNodesResponse response = connection.listNodes();
			this.membershipLearned.set(true);
			return response.getNodesList();
		}
		catch (Exception e) {
			log.debug("ListNodes via {} failed: {}", connection.key(), e.toString());
			return List.of();
		}
	}

	@Override
	public void onMembership(List<am.ik.sluice.v1.proto.Node> nodes, long membershipVersion) {
		this.supervisor.execute(() -> this.reconcile(nodes));
	}

	private @org.jspecify.annotations.Nullable NodeConnection byEndpoint(String url) {
		for (NodeConnection connection : this.connectionsByNode.values()) {
			if (connection.url().equals(url)) {
				return connection;
			}
		}
		return null;
	}

	@Override
	public void onDrain(NodeConnection connection, String reason) {
		// the stream is closing; the per-node loop retries with backoff until the node
		// re-appears in the membership
	}

	/**
	 * Adopts the given membership: opens streams to new nodes, closes streams of nodes
	 * that are no longer listed. The bootstrap connection doubles as the stream of a
	 * listed node without a public url.
	 */
	private void reconcile(List<am.ik.sluice.v1.proto.Node> nodes) {
		if (!this.running || nodes.isEmpty()) {
			return;
		}
		for (am.ik.sluice.v1.proto.Node node : nodes) {
			String nodeId = node.getNodeId();
			if (nodeId.isBlank() || this.connectionsByNode.containsKey(nodeId)) {
				continue;
			}
			if (node.getPublicUrl().isBlank()) {
				continue; // no reachable address known (the bootstrap stream covers it
							// once acked)
			}
			// reuse the connection that already points at this endpoint (the bootstrap
			// stream): a second stream to the same node would make the server drop the
			// first session on register and churn the routes
			NodeConnection sameEndpoint = this.byEndpoint(node.getPublicUrl());
			if (sameEndpoint != null) {
				log.info("node {} is the endpoint of the {} stream; adopting it", nodeId, sameEndpoint.key());
				this.rekey(sameEndpoint, nodeId);
				continue;
			}
			log.info("opening tunnel stream to node {} at {}", nodeId, node.getPublicUrl());
			this.startConnection(nodeId, node.getPublicUrl());
		}
		// close streams of nodes that left the membership
		java.util.List<String> nodeIds = nodes.stream().map(am.ik.sluice.v1.proto.Node::getNodeId).toList();
		this.connectionsByNode.keySet().retainAll(nodeIds);
	}

	@Override
	public void stop() {
		this.running = false;
		this.supervisor.shutdownNow();
		for (NodeConnection connection : this.connectionsByNode.values()) {
			connection.close();
		}
		this.connectionsByNode.clear();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

}
