package am.ik.sluice.client.tunnel;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.tunnel.ConnectionLimit;
import am.ik.sluice.tunnel.TunnelBudget;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.v1.proto.ListNodesResponse;
import am.ik.sluice.v1.proto.PeerBuild;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Fan-out supervisor: keeps one {@link NodeConnection} tunnel stream per server node. The
 * {@code sluice.server-url} is the bootstrap; the node list learned via {@code ListNodes}
 * and membership updates opens one stream per node (keyed by node id) and closes streams
 * of nodes that left. All membership transitions run on a single-threaded executor.
 * <p>
 * Stopped by the application context ({@link #stop(Runnable)}), it drains first
 * ({@link #drain()}): the routes are withdrawn from every node and the connections open
 * keep relaying until they end or {@code sluice.client.drain-grace} runs out.
 * {@link #stop()} closes at once.
 */
@Component
public class TunnelClient implements SmartLifecycle, NodeConnection.Listener {

	private static final Logger log = LoggerFactory.getLogger(TunnelClient.class);

	private final SluiceClientProperties properties;

	private final TaskExecutor taskExecutor;

	private final MeterRegistry meterRegistry;

	private final @Nullable SslBundles sslBundles;

	/** Process-stable client identity sent on every stream. */
	private final String clientId;

	private final ConcurrentHashMap<String, NodeConnection> connectionsByNode = new ConcurrentHashMap<>();

	private final ExecutorService supervisor = Executors
		.newSingleThreadExecutor(r -> Thread.ofVirtual().name("sluice-supervisor").unstarted(r));

	private final AtomicBoolean membershipLearned = new AtomicBoolean();

	/** Upstream connections open at once, shared by every node stream. */
	private final ConnectionLimit connectionLimit;

	/** What the connections of every node stream buffer beyond their allowance. */
	private final TunnelBudget budget;

	/** This client's build, announced on every stream. */
	private final PeerBuild clientBuild;

	private final Supplier<RandomGenerator> randomFactory;

	private volatile boolean running;

	/** Set once the drain began: no stream is opened from then on. */
	private volatile boolean draining;

	/** Completed to end a drain at once ({@link #abortDrain()}). */
	private final CompletableFuture<Void> abort = new CompletableFuture<>();

	@Autowired
	TunnelClient(SluiceClientProperties properties, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			MeterRegistry meterRegistry, @Nullable SslBundles sslBundles, PeerBuild clientBuild) {
		this(properties, taskExecutor, meterRegistry, sslBundles, clientBuild, SplittableRandom::new);
	}

	private TunnelClient(SluiceClientProperties properties, TaskExecutor taskExecutor, MeterRegistry meterRegistry,
			@Nullable SslBundles sslBundles, PeerBuild clientBuild, Supplier<RandomGenerator> randomFactory) {
		this.properties = properties;
		this.taskExecutor = taskExecutor;
		this.meterRegistry = meterRegistry;
		this.sslBundles = sslBundles;
		this.clientBuild = clientBuild;
		this.randomFactory = randomFactory;
		log.info("client build: {}", PeerBuilds.describe(clientBuild));
		String configured = clientid(properties);
		this.clientId = configured == null || configured.isBlank() ? UUID.randomUUID().toString() : configured;
		meterRegistry.gauge("sluice.connections.active", this.connectionsByNode,
				map -> map.values().stream().mapToInt(NodeConnection::activeConnections).sum());
		Gauge
			.builder("sluice.tunnel.sessions", this.connectionsByNode,
					map -> map.values().stream().filter(NodeConnection::isConnected).count())
			.description("Tunnel streams established, one per server node")
			.register(meterRegistry);
		long maxHeap = Runtime.getRuntime().maxMemory();
		this.budget = new TunnelBudget(TunnelBudget.resolve(properties.tunnelBudget().toBytes(), maxHeap));
		Gauge.builder("sluice.tunnel.budget.bytes", this.budget, TunnelBudget::max)
			.baseUnit("bytes")
			.description("Bytes the tunnel connections may buffer beyond each connection's allowance, shared")
			.register(meterRegistry);
		Gauge.builder("sluice.tunnel.budget.used.bytes", this.budget, TunnelBudget::used)
			.baseUnit("bytes")
			.description("Bytes the tunnel connections buffer, or granted the server to send, beyond each "
					+ "connection's allowance")
			.register(meterRegistry);
		Gauge.builder("sluice.tunnel.buffered.bytes", this.budget, TunnelBudget::buffered)
			.baseUnit("bytes")
			.description("Payload the server sent that the upstream connections have not taken yet")
			.register(meterRegistry);
		this.connectionLimit = new ConnectionLimit(
				ConnectionLimit.resolve(properties.maxConnections(), this.budget.max()));
		Gauge.builder("sluice.connections.limit", this.connectionLimit, ConnectionLimit::max)
			.description("Upstream connections allowed open at once")
			.register(meterRegistry);
		Gauge.builder("sluice.connections.admitted", this.connectionLimit, ConnectionLimit::open)
			.description("Upstream connections holding a slot of the connection limit")
			.register(meterRegistry);
		log.info(
				"upstream connection limit {} ({}), tunnel budget {}KiB ({}; a connection's window grows from {}KiB "
						+ "to {}KiB against it), gRPC flow control window {}KiB",
				this.connectionLimit.max(),
				properties.maxConnections() > 0 ? "configured"
						: "derived: (half of max heap %dMiB - tunnel budget) / %dKiB per connection"
							.formatted(maxHeap >> 20, ConnectionLimit.CONNECTION_BYTES >> 10),
				this.budget.max() >> 10,
				properties.tunnelBudget().toBytes() > 0 ? "configured" : "derived: an eighth of half of max heap",
				VirtualConnection.INITIAL_WINDOW >> 10, VirtualConnection.DEFAULT_WINDOW >> 10,
				this.budget.transportWindow() >> 10);
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

		private @Nullable SslBundles sslBundles;

		private Supplier<RandomGenerator> randomFactory = SplittableRandom::new;

		private PeerBuild clientBuild = PeerBuild.newBuilder()
			.setImplementation(ClientBuildConfiguration.IMPLEMENTATION)
			.build();

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

		public Builder sslBundles(@Nullable SslBundles sslBundles) {
			this.sslBundles = sslBundles;
			return this;
		}

		/**
		 * The build announced to the servers; defaults to the implementation name alone.
		 */
		public Builder clientBuild(PeerBuild clientBuild) {
			this.clientBuild = clientBuild;
			return this;
		}

		/** Supplies the randomness of the reconnect jitter; tests seed it. */
		public Builder randomFactory(Supplier<RandomGenerator> randomFactory) {
			this.randomFactory = randomFactory;
			return this;
		}

		public TunnelClient build() {
			return new TunnelClient(Objects.requireNonNull(this.properties, "properties is required"),
					Objects.requireNonNull(this.taskExecutor, "taskExecutor is required"),
					Objects.requireNonNull(this.meterRegistry, "meterRegistry is required"), this.sslBundles,
					this.clientBuild, this.randomFactory);
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
		if (this.properties.upstreamTargets().isEmpty()) {
			throw new IllegalStateException("sluice.client.upstream is required");
		}
		this.running = true;
		this.startConnection("", this.properties.serverUrl());
	}

	private void startConnection(String nodeId, String url) {
		String bundleName = this.properties.tlsBundle();
		SslBundle sslBundle = bundleName == null || bundleName.isBlank() || this.sslBundles == null ? null
				: this.sslBundles.getBundle(bundleName);
		NodeConnection connection = NodeConnection.builder()
			.key(nodeId.isBlank() ? url : nodeId)
			.nodeId(nodeId)
			.url(url)
			.properties(this.properties)
			.sslBundle(sslBundle)
			.clientId(this.clientId)
			.taskExecutor(this.taskExecutor)
			.listener(this)
			.meterRegistry(this.meterRegistry)
			.connectionLimit(this.connectionLimit)
			.budget(this.budget)
			.clientBuild(this.clientBuild)
			.randomFactory(this.randomFactory)
			.build();
		this.connectionsByNode.put(nodeId.isBlank() ? url : nodeId, connection);
		connection.start();
	}

	@Override
	public void onConnected(NodeConnection connection, String nodeId) {
		this.supervise(() -> {
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
		this.supervise(() -> this.reconcile(nodes));
	}

	/**
	 * Runs a membership transition on the supervisor; dropped once stopping or draining,
	 * when the supervisor is shut down.
	 */
	private void supervise(Runnable transition) {
		try {
			this.supervisor.execute(transition);
		}
		catch (RejectedExecutionException e) {
			// stopping: no stream is opened any more
		}
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
		if (!this.running || this.draining || nodes.isEmpty()) {
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

	/**
	 * Drains, then closes: the application context's stop, which waits for it.
	 */
	@Override
	public void stop(Runnable callback) {
		try {
			drain();
		}
		finally {
			stop();
			callback.run();
		}
	}

	/**
	 * Withdraws the routes from every node and waits until each node acknowledged the
	 * withdrawal and the connections open on it ended, at most
	 * {@code sluice.client.drain-grace} or until {@link #abortDrain()}; no stream is
	 * opened or reconnected meanwhile. Returns at once with a zero grace or when not
	 * running. The connections still open afterwards are left to {@link #stop()}.
	 */
	public void drain() {
		Duration grace = this.properties.drainGrace();
		if (!this.running || this.draining || grace.isZero()) {
			return;
		}
		this.draining = true;
		// no membership change opens a stream behind the snapshot below
		this.supervisor.shutdownNow();
		try {
			this.supervisor.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
		List<NodeConnection> connections = List.copyOf(this.connectionsByNode.values());
		log.info("draining: withdrawing the routes from {} server node(s); waiting up to {} for {} open connection(s)",
				connections.size(), grace, openConnections());
		CompletableFuture<?>[] drains = connections.stream()
			.map(NodeConnection::beginDrain)
			.toArray(CompletableFuture<?>[]::new);
		try {
			CompletableFuture.anyOf(CompletableFuture.allOf(drains), this.abort)
				.get(grace.toNanos(), TimeUnit.NANOSECONDS);
			if (this.abort.isDone()) {
				log.warn("drain aborted; closing {} open connection(s)", openConnections());
			}
			else {
				log.info("drain ended with {} open connection(s)", openConnections());
			}
		}
		catch (TimeoutException e) {
			log.warn("drain grace {} expired; closing {} open connection(s)", grace, openConnections());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (ExecutionException e) {
			// the futures complete normally only
			log.warn("drain failed; closing {} open connection(s)", openConnections(), e);
		}
	}

	/** Ends a drain in progress at once, and any drain begun later. */
	public void abortDrain() {
		this.abort.complete(null);
	}

	private int openConnections() {
		return this.connectionsByNode.values().stream().mapToInt(NodeConnection::activeConnections).sum();
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
