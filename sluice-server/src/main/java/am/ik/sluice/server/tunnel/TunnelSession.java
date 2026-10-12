package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Node;
import am.ik.sluice.v1.proto.PeerBuild;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.Status;

/**
 * State of one connected tunnel client: the multiplexed virtual connections and the
 * router registration.
 * <p>
 * The receive path ({@link #handle(Frame)}) never waits on anything slow: ADVERTISE is
 * applied on the session's own serial executor (route registration and port binding can
 * take a while), and windowed connections accept DATA without blocking. A connection is
 * windowed when the client announced a receive window ({@code Advertise}).
 */
public final class TunnelSession implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(TunnelSession.class);

	private final String clientId;

	private final String nodeId;

	private final String remoteAddress;

	private final Instant connectedAt = Instant.now();

	private final AtomicLong bytesInbound = new AtomicLong();

	private final AtomicLong bytesOutbound = new AtomicLong();

	private volatile List<Upstream> upstreams = List.of();

	private volatile Set<Integer> rejectedPorts = Set.of();

	private final Router router;

	private final SessionSender sender;

	private final TcpRouteListener tcpRoutes;

	private final SessionRegistry registry;

	private final AtomicLong sequence = new AtomicLong(1);

	private final ConcurrentMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	private static Set<Integer> listenPorts(List<Upstream> upstreams) {
		return upstreams.stream()
			.map(Upstream::getListenPort)
			.filter(port -> port > 0)
			.collect(Collectors.toUnmodifiableSet());
	}

	private final AtomicBoolean closed = new AtomicBoolean();

	private volatile boolean drained;

	/**
	 * The client's per-connection receive window from its latest {@code Advertise}; 0 =
	 * the client has no per-connection flow control.
	 */
	private volatile long clientWindow;

	/**
	 * The build the client announced in its latest {@code Advertise}, sanitized; null
	 * when it announced none (it predates the field) or has not advertised yet.
	 */
	private volatile @Nullable PeerBuild clientBuild;

	/** Whether an {@code Advertise} arrived; written after the fields it carries. */
	private volatile boolean advertised;

	/** This server's build, sent in every {@code AdvertiseAck}. */
	private final PeerBuild serverBuild;

	/**
	 * Applies ADVERTISE off the receive path, in order; the route removal of
	 * {@link #close()} runs here too, after the queued advertises.
	 */
	private final ExecutorService advertiser;

	private final Duration closeTimeout;

	private final Runnable windowUpdateReceived;

	private TunnelSession(Builder builder) {
		this.clientId = Objects.requireNonNull(builder.clientId, "clientId is required");
		this.nodeId = builder.nodeId;
		this.remoteAddress = builder.remoteAddress;
		this.router = Objects.requireNonNull(builder.router, "router is required");
		this.sender = Objects.requireNonNull(builder.sender, "sender is required");
		this.tcpRoutes = Objects.requireNonNull(builder.tcpRoutes, "tcpRoutes is required");
		this.registry = Objects.requireNonNull(builder.registry, "registry is required");
		this.closeTimeout = builder.closeTimeout;
		this.windowUpdateReceived = builder.windowUpdateReceived;
		this.serverBuild = builder.serverBuild;
		this.advertiser = Executors.newSingleThreadExecutor(
				task -> Thread.ofVirtual().name("sluice-advertise-" + this.clientId).unstarted(task));
		this.registry.register(this);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private @Nullable String clientId;

		private String nodeId = "";

		private String remoteAddress = "";

		private @Nullable Router router;

		private @Nullable SessionSender sender;

		private @Nullable SessionRegistry registry;

		private @Nullable TcpRouteListener tcpRoutes;

		private Duration closeTimeout = Duration.ofSeconds(5);

		private Runnable windowUpdateReceived = () -> {
		};

		private PeerBuild serverBuild = PeerBuild.newBuilder()
			.setImplementation(ServerBuildConfiguration.IMPLEMENTATION)
			.build();

		private Builder() {
		}

		public Builder clientId(String clientId) {
			this.clientId = clientId;
			return this;
		}

		public Builder nodeId(String nodeId) {
			this.nodeId = nodeId;
			return this;
		}

		public Builder remoteAddress(String remoteAddress) {
			this.remoteAddress = remoteAddress;
			return this;
		}

		public Builder router(Router router) {
			this.router = router;
			return this;
		}

		public Builder sender(SessionSender sender) {
			this.sender = sender;
			return this;
		}

		public Builder registry(SessionRegistry registry) {
			this.registry = registry;
			return this;
		}

		public Builder tcpRoutes(TcpRouteListener tcpRoutes) {
			this.tcpRoutes = tcpRoutes;
			return this;
		}

		/** This server's build, sent in every {@code AdvertiseAck}. */
		public Builder serverBuild(PeerBuild serverBuild) {
			this.serverBuild = serverBuild;
			return this;
		}

		/** Runs for every WINDOW_UPDATE received; a metrics hook. */
		public Builder windowUpdateReceived(Runnable windowUpdateReceived) {
			this.windowUpdateReceived = windowUpdateReceived;
			return this;
		}

		/**
		 * How long {@link TunnelSession#close()} waits for a pending advertise and the
		 * route removal queued behind it.
		 */
		Builder closeTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
			return this;
		}

		public TunnelSession build() {
			return new TunnelSession(this);
		}

	}

	public String clientId() {
		return this.clientId;
	}

	public FrameWriter sender() {
		return this.sender;
	}

	/**
	 * Peer address ({@code host:port}) of the client's tunnel stream; empty when unknown.
	 */
	public String remoteAddress() {
		return this.remoteAddress;
	}

	public Instant connectedAt() {
		return this.connectedAt;
	}

	/**
	 * Upstreams announced by the latest {@code Advertise}; empty until the first one.
	 */
	public List<Upstream> upstreams() {
		return this.upstreams;
	}

	/**
	 * Listen ports of the latest {@code Advertise} that were not bound.
	 */
	public Set<Integer> rejectedPorts() {
		return this.rejectedPorts;
	}

	/**
	 * Whether the client sent its {@code Advertise}; until then its build and window are
	 * not known.
	 */
	public boolean advertised() {
		return this.advertised;
	}

	/**
	 * The build the client announced in its latest {@code Advertise}, with its strings
	 * bounded ({@link PeerBuilds#sanitize}); empty when the client announced none (it
	 * predates the field) or has not advertised yet ({@link #advertised()}).
	 */
	public Optional<PeerBuild> clientBuild() {
		return Optional.ofNullable(this.clientBuild);
	}

	/**
	 * Whether the client advertised without a per-connection receive window: every
	 * connection of this session then runs without per-connection flow control.
	 */
	public boolean legacy() {
		return this.advertised && this.clientWindow == 0;
	}

	void start() {
		this.sender.start();
	}

	/**
	 * Handles one frame received from the client; never blocks on a slow consumer, except
	 * for DATA of a legacy connection.
	 */
	void handle(Frame frame) {
		switch (frame.getBodyCase()) {
			case ADVERTISE -> {
				Advertise advertise = frame.getAdvertise();
				// before the routes exist: every CONNECT they lead to sees the window
				this.clientWindow = Integer.toUnsignedLong(advertise.getConnectionWindow());
				PeerBuild build = advertise.hasBuild() ? PeerBuilds.sanitize(advertise.getBuild()) : null;
				this.clientBuild = build;
				boolean first = !this.advertised;
				this.advertised = true;
				if (first) {
					log.info("client {} runs {}{}", this.clientId,
							build == null ? PeerBuilds.UNKNOWN : PeerBuilds.describe(build),
							this.clientWindow == 0 ? "; it announced no connection window (legacy flow control)" : "");
				}
				try {
					this.advertiser.execute(() -> advertise(advertise.getUpstreamsList()));
				}
				catch (RejectedExecutionException e) {
					// closed: the routes of a closed session are not registered again
				}
			}
			case DATA -> {
				am.ik.sluice.v1.proto.Data data = frame.getData();
				VirtualConnection connection = this.connections.get(data.getConnId());
				if (connection != null) {
					connection.acceptData(data.getPayload().toByteArray());
				}
			}
			case WINDOW_UPDATE -> {
				this.windowUpdateReceived.run();
				VirtualConnection connection = this.connections.get(frame.getWindowUpdate().getConnId());
				if (connection != null) {
					connection.grant(frame.getWindowUpdate().getIncrement());
				}
			}
			case CLOSE -> {
				VirtualConnection connection = this.connections.get(frame.getClose().getConnId());
				if (connection != null) {
					connection.remoteClosed();
				}
			}
			case ERROR -> {
				am.ik.sluice.v1.proto.Error error = frame.getError();
				VirtualConnection connection = this.connections.get(error.getConnId());
				if (connection != null) {
					connection.remoteFailed(error.getMessage());
				}
			}
			case CONNECT -> log.warn("unexpected CONNECT from client {}", this.clientId);
			case KEEP_ALIVE -> {
				// liveness no-op
			}
			default -> {
				// empty or unknown future body: ignore
			}
		}
	}

	/**
	 * Registers the advertised routes and binds their ports, then acknowledges; runs on
	 * {@link #advertiser}. Skipped once the session is closed: the route removal of
	 * {@link #close()} runs after every advertise started before it.
	 */
	private void advertise(List<Upstream> advertised) {
		if (this.closed.get()) {
			return;
		}
		int registered = this.router.register(this.clientId, advertised);
		Set<Integer> rejected = this.tcpRoutes.reconcile(this.clientId, listenPorts(advertised));
		this.upstreams = List.copyOf(advertised);
		this.rejectedPorts = Set.copyOf(rejected);
		this.sender.sendAdvertiseAck(AdvertiseAck.newBuilder()
			.addAllRejectedPorts(rejected)
			.setNodeId(this.nodeId)
			.setBuild(this.serverBuild)
			.build());
		log.info("client {} advertised {} upstream(s), {} listen port(s) rejected", this.clientId, registered,
				rejected.size());
		for (Upstream upstream : advertised) {
			log.info("client {} upstream: host=[{}] target={} rewrite-host={} tls-passthrough={} listen-port={}",
					this.clientId, upstream.getHost(), upstream.getTargetUrl(), upstream.getRewriteHost(),
					upstream.getTlsPassthrough(), upstream.getListenPort());
		}
	}

	/**
	 * Opens a virtual connection to the given dial address and sends the CONNECT request
	 * to the client. Windowed when the client announced a receive window.
	 */
	public VirtualConnection open(String address) {
		long connectionId = this.sequence.getAndIncrement();
		long clientWindow = this.clientWindow;
		VirtualConnection connection = clientWindow > 0 ? VirtualConnection.builder(connectionId, this.sender)
			.window(VirtualConnection.DEFAULT_WINDOW, clientWindow)
			.build() : new VirtualConnection(connectionId, this.sender);
		this.connections.put(connectionId, connection);
		this.sender.sendConnect(Connect.newBuilder()
			.setConnId(connectionId)
			.setAddress(address)
			.setConnectionWindow(connection.windowed() ? VirtualConnection.DEFAULT_WINDOW : 0)
			.build());
		return connection;
	}

	public void remove(long connectionId) {
		this.connections.remove(connectionId);
	}

	/**
	 * Number of in-flight virtual connections relayed by this session.
	 */
	public int connectionCount() {
		return this.connections.size();
	}

	/**
	 * Number of virtual connections opened over this session since it connected.
	 */
	public long connectionsOpened() {
		return this.sequence.get() - 1;
	}

	/**
	 * Accounts relayed bytes: {@link StreamRelay.Direction#TO_REMOTE} flows from the
	 * public peer into the tunnel (inbound), {@link StreamRelay.Direction#TO_LOCAL} back
	 * to the peer (outbound).
	 */
	public void recordRelayed(long count, StreamRelay.Direction direction) {
		switch (direction) {
			case TO_REMOTE -> this.bytesInbound.addAndGet(count);
			case TO_LOCAL -> this.bytesOutbound.addAndGet(count);
		}
	}

	public long bytesInbound() {
		return this.bytesInbound.get();
	}

	public long bytesOutbound() {
		return this.bytesOutbound.get();
	}

	/**
	 * Marks the session as drained and notifies the client that this node is going away.
	 */
	public void drain(String reason) {
		this.drained = true;
		try {
			this.sender.sendDrain(reason);
		}
		catch (RuntimeException e) {
			// the stream may already be dead
		}
	}

	public boolean drained() {
		return this.drained;
	}

	/**
	 * Pushes the current cluster membership to the client.
	 */
	public void pushMembership(List<Node> nodes, long membershipVersion) {
		this.sender.sendMembership(nodes, membershipVersion);
	}

	/**
	 * Tears the session down: unregisters it, removes its routes, stops the sender and
	 * fails its connections. The route removal runs on {@link #advertiser} after the
	 * advertises queued before it, so none of them registers the routes again; close
	 * waits for it at most {@code closeTimeout}, and an advertise stuck past that leaves
	 * the removal to run once it finishes. That advertise still registers its routes when
	 * it ends, after a newer session of the client may have registered its own.
	 */
	@Override
	public void close() {
		if (!this.closed.compareAndSet(false, true)) {
			return;
		}
		this.registry.remove(this);
		log.info("closing session for client {}", this.clientId);
		this.advertiser.execute(this::removeRoutes);
		this.advertiser.shutdown();
		try {
			if (!this.advertiser.awaitTermination(this.closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				log.warn("client {}: advertise still running after {}; its routes are removed once it ends",
						this.clientId, this.closeTimeout);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		// the sender first: a dead or stalled stream must not leave the connection
		// teardown below waiting on a full send queue (the worker stops and releases
		// every producer); from here on the frames of the closing connections are
		// dropped, the peer is gone anyway
		this.sender.close();
		for (VirtualConnection connection : List.copyOf(this.connections.values())) {
			connection.remoteFailed("tunnel session closed");
			connection.close();
		}
		this.connections.clear();
	}

	/**
	 * Releases the client's routes and ports, unless a newer session of the same client
	 * id is live: the routes are keyed by client id, and that session owns them now (its
	 * advertise replaces them, its close removes them).
	 */
	private void removeRoutes() {
		if (this.registry.find(this.clientId).isPresent()) {
			return;
		}
		this.tcpRoutes.reconcile(this.clientId, Set.of());
		this.router.remove(this.clientId);
	}

}
