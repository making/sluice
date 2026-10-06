package am.ik.sluice.client.tunnel;

import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.SocketRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.ListNodesRequest;
import am.ik.sluice.v1.proto.ListNodesResponse;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.core.task.TaskExecutor;

/**
 * One tunnel stream toward one server node: connects, advertises the upstream map, dials
 * local upstreams on CONNECT, and reconnects with exponential backoff (1s..30s) when the
 * stream drops. The lifecycle of the per-node virtual connections lives here; the fan-out
 * across nodes is the supervisor's ({@link TunnelClient}) concern.
 */
public final class NodeConnection implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(NodeConnection.class);

	private static final long BACKOFF_INITIAL_SECONDS = 1;

	private static final long BACKOFF_MAX_SECONDS = 30;

	private static final Metadata.Key<String> TOKEN_HEADER = Metadata.Key.of("authorization",
			Metadata.ASCII_STRING_MARSHALLER);

	private static final Metadata.Key<String> CLIENT_ID_HEADER = Metadata.Key.of("x-sluice-id",
			Metadata.ASCII_STRING_MARSHALLER);

	/** Supervisor callbacks; invoked from the receive path, keep them fast. */
	public interface Listener {

		/** The stream is up and the advertise was acknowledged. */
		void onConnected(NodeConnection connection, String nodeId);

		/** The server pushed a membership update. */
		void onMembership(List<am.ik.sluice.v1.proto.Node> nodes, long membershipVersion);

		/** The server is draining; the stream is going away. */
		void onDrain(NodeConnection connection, String reason);

	}

	private final String key;

	private final String url;

	private final SluiceClientProperties properties;

	private final String clientId;

	private final TaskExecutor taskExecutor;

	private final Listener listener;

	private final LocalConnector connector;

	private final List<am.ik.sluice.v1.proto.Upstream> advertised;

	private final ConcurrentHashMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	private final Counter reconnects;

	private final Counter rejectedAdvertises;

	private final AtomicReference<SessionSender> senderRef = new AtomicReference<>();

	private volatile boolean running;

	private volatile boolean connected;

	private volatile @Nullable ManagedChannel channel;

	private volatile String nodeId;

	private NodeConnection(Builder builder) {
		this.key = Objects.requireNonNull(builder.key, "key is required");
		this.nodeId = builder.nodeId == null ? "" : builder.nodeId;
		this.url = Objects.requireNonNull(builder.url, "url is required");
		this.properties = Objects.requireNonNull(builder.properties, "properties is required");
		this.clientId = Objects.requireNonNull(builder.clientId, "clientId is required");
		this.taskExecutor = Objects.requireNonNull(builder.taskExecutor, "taskExecutor is required");
		this.listener = Objects.requireNonNull(builder.listener, "listener is required");
		this.connector = LocalConnector.builder()
			.upstreams(builder.properties.upstreamMap())
			.strict(builder.properties.strictForwarding())
			.insecure(builder.properties.insecure())
			.build();
		this.advertised = builder.properties.toProtoUpstreams();
		String metricsTag = this.nodeId.isBlank() ? this.url : this.nodeId;
		MeterRegistry meterRegistry = Objects.requireNonNull(builder.meterRegistry, "meterRegistry is required");
		this.reconnects = Counter.builder("sluice.reconnect.total")
			.tag("node", metricsTag)
			.description("Tunnel stream re-establishments after a drop")
			.register(meterRegistry);
		this.rejectedAdvertises = Counter.builder("sluice.advertise.rejected")
			.tag("node", metricsTag)
			.description("Advertised listen ports rejected by the server")
			.register(meterRegistry);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private @Nullable String key;

		private @Nullable String nodeId;

		private @Nullable String url;

		private @Nullable SluiceClientProperties properties;

		private @Nullable String clientId;

		private @Nullable TaskExecutor taskExecutor;

		private @Nullable Listener listener;

		private @Nullable MeterRegistry meterRegistry;

		private Builder() {
		}

		/**
		 * Registry key of this connection; the node id once known, else stable
		 * placeholder.
		 */
		public Builder key(String key) {
			this.key = key;
			return this;
		}

		public Builder nodeId(String nodeId) {
			this.nodeId = nodeId;
			return this;
		}

		public Builder url(String url) {
			this.url = url;
			return this;
		}

		public Builder properties(SluiceClientProperties properties) {
			this.properties = properties;
			return this;
		}

		public Builder clientId(String clientId) {
			this.clientId = clientId;
			return this;
		}

		public Builder taskExecutor(TaskExecutor taskExecutor) {
			this.taskExecutor = taskExecutor;
			return this;
		}

		public Builder listener(Listener listener) {
			this.listener = listener;
			return this;
		}

		public Builder meterRegistry(MeterRegistry meterRegistry) {
			this.meterRegistry = meterRegistry;
			return this;
		}

		public NodeConnection build() {
			return new NodeConnection(this);
		}

	}

	/** Starts the connect/reconnect loop on a dedicated thread. */
	public void start() {
		this.running = true;
		Thread.ofVirtual().name("sluice-node-" + this.key).start(this::runLoop);
	}

	public boolean isConnected() {
		return this.connected;
	}

	public String nodeId() {
		return this.nodeId;
	}

	public String key() {
		return this.key;
	}

	public String url() {
		return this.url;
	}

	private void runLoop() {
		long backoff = BACKOFF_INITIAL_SECONDS;
		while (this.running) {
			try {
				this.channel = buildChannel();
				runSession(this.channel);
				if (this.connected) {
					backoff = BACKOFF_INITIAL_SECONDS;
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			catch (Exception e) {
				if (this.running) {
					log.warn("[{}] tunnel stream ended: {}", this.key, e.toString());
				}
			}
			finally {
				shutdownChannel();
				this.connected = false;
			}
			if (!this.running) {
				return;
			}
			// the next loop iteration is the reconnection attempt
			this.reconnects.increment();
			try {
				TimeUnit.SECONDS.sleep(backoff);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			backoff = Math.min(BACKOFF_MAX_SECONDS, backoff * 2);
		}
	}

	private ManagedChannel buildChannel() {
		String url = this.url;
		boolean secure = url.startsWith("grpcs://");
		String authority = url.replaceFirst("^grpcs?://", "");
		int portSeparator = authority.lastIndexOf(':');
		String host = portSeparator > 0 ? authority.substring(0, portSeparator) : authority;
		int port = portSeparator > 0 ? Integer.parseInt(authority.substring(portSeparator + 1)) : 443;
		ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port)
			.enableRetry()
			.keepAliveTime(this.properties.keepAliveTime().toSeconds(), TimeUnit.SECONDS)
			.keepAliveTimeout(this.properties.keepAliveTimeout().toSeconds(), TimeUnit.SECONDS);
		if (!secure) {
			builder.usePlaintext();
		}
		else if (this.properties.insecure()) {
			try {
				((NettyChannelBuilder) builder)
					.sslContext(GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build());
			}
			catch (Exception e) {
				throw new IllegalStateException("failed to build insecure SSL context", e);
			}
		}
		return builder.build();
	}

	private void runSession(ManagedChannel channel) throws InterruptedException {
		CountDownLatch closed = new CountDownLatch(1);
		ClientResponseObserver<Frame, Frame> responseObserver = new ClientResponseObserver<>() {

			@Override
			public void beforeStart(ClientCallStreamObserver<Frame> call) {
				// the onReady handler may only be installed during beforeStart
				SessionSender sender = new SessionSender(call);
				sender.start();
				senderRef.set(sender);
			}

			@Override
			public void onNext(Frame frame) {
				handle(frame, closed);
			}

			@Override
			public void onError(Throwable t) {
				log.info("[{}] tunnel stream error: {}", NodeConnection.this.key, t.toString());
				closed.countDown();
			}

			@Override
			public void onCompleted() {
				log.info("[{}] tunnel stream closed by server", NodeConnection.this.key);
				closed.countDown();
			}

		};
		Metadata metadata = new Metadata();
		String token = this.properties.tokenValue();
		if (!token.isBlank()) {
			metadata.put(TOKEN_HEADER, "Bearer " + token);
		}
		metadata.put(CLIENT_ID_HEADER, this.clientId);
		log.info("[{}] connecting to {} ...", this.key, this.url);
		TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(responseObserver);
		SessionSender sender = this.senderRef.get();
		if (sender == null) {
			return; // stream failed before starting
		}
		sender.sendAdvertise(this.advertised);
		closed.await();
		sender.close();
	}

	/**
	 * Performs a unary {@code ListNodes} on this connection's channel.
	 */
	public ListNodesResponse listNodes() {
		ManagedChannel channel = this.channel;
		if (channel == null || channel.isShutdown()) {
			throw new IllegalStateException("channel is not connected");
		}
		Metadata metadata = new Metadata();
		String token = this.properties.tokenValue();
		if (!token.isBlank()) {
			metadata.put(TOKEN_HEADER, "Bearer " + token);
		}
		metadata.put(CLIENT_ID_HEADER, this.clientId);
		return TunnelGrpc.newBlockingStub(channel)
			.withDeadlineAfter(10, TimeUnit.SECONDS)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.listNodes(ListNodesRequest.getDefaultInstance());
	}

	private void handle(Frame frame, CountDownLatch closed) {
		switch (frame.getBodyCase()) {
			case CONNECT -> {
				// register the virtual connection synchronously so DATA frames that
				// follow the CONNECT are not dropped before the dial task runs
				SessionSender sender = this.senderRef.get();
				if (sender == null) {
					return;
				}
				am.ik.sluice.v1.proto.Connect request = frame.getConnect();
				VirtualConnection connection = new VirtualConnection(request.getConnId(), sender);
				this.connections.put(request.getConnId(), connection);
				this.taskExecutor.execute(() -> dial(request, connection));
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
			case ADVERTISE_ACK -> {
				List<Integer> rejected = frame.getAdvertiseAck().getRejectedPortsList();
				String ackNodeId = frame.getAdvertiseAck().getNodeId();
				if (!ackNodeId.isBlank()) {
					this.nodeId = ackNodeId;
				}
				if (rejected.isEmpty()) {
					// the server acknowledged the advertise: the tunnel is up for real
					this.connected = true;
					log.info("[{}] tunnel established; advertising {} upstream(s)", this.key,
							this.properties.upstreamMap().size());
					this.listener.onConnected(this, this.nodeId);
					return;
				}
				log.warn("server rejected listen ports {} on advertise; closing the stream to re-advertise", rejected);
				this.rejectedAdvertises.increment();
				closed.countDown();
			}
			case MEMBERSHIP_UPDATE -> this.listener.onMembership(frame.getMembershipUpdate().getNodesList(),
					frame.getMembershipUpdate().getMembershipVersion());
			case DRAIN -> {
				// keep the stream: the server drains in-flight connections before it
				// closes the stream itself, killing it here would cut them off
				log.info("[{}] server draining: {}", this.key, frame.getDrain().getReason());
				this.listener.onDrain(this, frame.getDrain().getReason());
			}
			case ADVERTISE -> log.warn("unexpected ADVERTISE from server");
			case KEEP_ALIVE -> {
				// liveness no-op
			}
			default -> {
				// empty or unknown future body: ignore
			}
		}
	}

	private void dial(am.ik.sluice.v1.proto.Connect request, VirtualConnection connection) {
		long connectionId = request.getConnId();
		String address = request.getAddress();
		try {
			if (!this.connector.permits(address)) {
				throw new IllegalArgumentException("upstream not permitted: " + address);
			}
			Socket socket = this.connector.dial(address);
			SessionSender sender = this.senderRef.get();
			if (sender == null) {
				socket.close();
				return;
			}
			SocketRelay relay = SocketRelay.builder(socket, connection, sender)
				.onComplete(() -> this.connections.remove(connectionId))
				.build();
			relay.start();
		}
		catch (Exception e) {
			log.debug("dial {} failed: {}", address, e.toString());
			SessionSender sender = this.senderRef.get();
			if (sender != null) {
				sender.sendError(connectionId, e.toString());
			}
			this.connections.remove(connectionId);
			connection.close();
		}
	}

	int activeConnections() {
		return this.connections.size();
	}

	private void shutdownChannel() {
		ManagedChannel channel = this.channel;
		if (channel != null && !channel.isShutdown()) {
			channel.shutdownNow();
		}
	}

	/**
	 * Stops the reconnect loop and closes the channel; idempotent.
	 */
	@Override
	public void close() {
		this.running = false;
		this.connected = false;
		shutdownChannel();
	}

	Map<Long, VirtualConnection> connectionsForTest() {
		return Map.copyOf(this.connections);
	}

}
