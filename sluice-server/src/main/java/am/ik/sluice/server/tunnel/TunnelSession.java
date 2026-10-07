package am.ik.sluice.server.tunnel;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Node;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.Status;

/**
 * State of one connected tunnel client: the multiplexed virtual connections and the
 * router registration.
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

	private volatile boolean closed;

	private volatile boolean drained;

	TunnelSession(String clientId, String nodeId, String remoteAddress, Router router, SessionSender sender,
			SessionRegistry registry, TcpRouteListener tcpRoutes) {
		this.clientId = clientId;
		this.nodeId = nodeId;
		this.remoteAddress = remoteAddress;
		this.router = router;
		this.sender = sender;
		this.tcpRoutes = tcpRoutes;
		this.registry = registry;
		registry.register(this);
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

		public TunnelSession build() {
			return new TunnelSession(Objects.requireNonNull(this.clientId, "clientId is required"), this.nodeId,
					this.remoteAddress, Objects.requireNonNull(this.router, "router is required"),
					Objects.requireNonNull(this.sender, "sender is required"),
					Objects.requireNonNull(this.registry, "registry is required"),
					Objects.requireNonNull(this.tcpRoutes, "tcpRoutes is required"));
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

	void start() {
		this.sender.start();
	}

	/**
	 * Handles one frame received from the client.
	 */
	void handle(Frame frame) {
		switch (frame.getBodyCase()) {
			case ADVERTISE -> {
				List<Upstream> advertised = frame.getAdvertise().getUpstreamsList();
				int registered = this.router.register(this.clientId, advertised);
				Set<Integer> rejected = this.tcpRoutes.reconcile(this.clientId, listenPorts(advertised));
				this.upstreams = List.copyOf(advertised);
				this.rejectedPorts = Set.copyOf(rejected);
				this.sender.sendAdvertiseAck(List.copyOf(rejected), this.nodeId);
				log.info("client {} advertised {} upstream(s), {} listen port(s) rejected", this.clientId, registered,
						rejected.size());
				for (Upstream upstream : advertised) {
					log.info(
							"client {} upstream: host=[{}] target={} preserve-host={} tls-passthrough={} listen-port={}",
							this.clientId, upstream.getHost(), upstream.getTargetUrl(), upstream.getPreserveHost(),
							upstream.getTlsPassthrough(), upstream.getListenPort());
				}
			}
			case DATA -> {
				am.ik.sluice.v1.proto.Data data = frame.getData();
				VirtualConnection connection = this.connections.get(data.getConnId());
				if (connection != null) {
					connection.acceptData(data.getPayload().toByteArray());
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
	 * Opens a virtual connection to the given dial address and sends the CONNECT request
	 * to the client.
	 */
	public VirtualConnection open(String address) {
		long connectionId = this.sequence.getAndIncrement();
		VirtualConnection connection = new VirtualConnection(connectionId, this.sender);
		this.connections.put(connectionId, connection);
		this.sender.sendConnect(connectionId, address);
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

	@Override
	public void close() {
		if (this.closed) {
			return;
		}
		this.closed = true;
		this.registry.remove(this);
		log.info("closing session for client {}", this.clientId);
		this.tcpRoutes.reconcile(this.clientId, Set.of());
		this.router.remove(this.clientId);
		for (VirtualConnection connection : List.copyOf(this.connections.values())) {
			connection.remoteFailed("tunnel session closed");
			connection.close();
		}
		this.connections.clear();
		this.sender.close();
	}

}
