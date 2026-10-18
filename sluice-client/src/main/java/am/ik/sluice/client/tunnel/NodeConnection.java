package am.ik.sluice.client.tunnel;

import java.net.Socket;
import java.time.Duration;
import javax.net.ssl.TrustManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.tunnel.ConnectionLimit;
import am.ik.sluice.tunnel.TunnelBudget;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.SocketRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.ListNodesRequest;
import am.ik.sluice.v1.proto.ListNodesResponse;
import am.ik.sluice.v1.proto.PeerBuild;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.core.task.TaskExecutor;

/**
 * One tunnel stream toward one server node: connects, advertises the upstream map, dials
 * local upstreams on CONNECT, and reconnects with jittered exponential backoff when the
 * stream drops: the ceiling doubles from 1s to 30s and each wait is drawn from its upper
 * half ({@link ReconnectBackoff}). Listen ports the server rejects (taken, or outside its
 * range) are re-advertised on the same stream with the same backoff, so the other routes
 * stay up. The lifecycle of the per-node virtual connections lives here; the fan-out
 * across nodes is the supervisor's ({@link TunnelClient}) concern.
 */
public final class NodeConnection implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(NodeConnection.class);

	/**
	 * The ERROR message answering a CONNECT that announces no per-connection receive
	 * window; names the first server build that announces one.
	 */
	public static final String SERVER_WINDOW_REQUIRED = "the server announced no per-connection receive window "
			+ "(Connect.connection_window); this client requires sluice-server 046cd21 (2026-10-12) or later";

	private static final Metadata.Key<String> TOKEN_HEADER = Metadata.Key.of("authorization",
			Metadata.ASCII_STRING_MARSHALLER);

	private static final Metadata.Key<String> CLIENT_ID_HEADER = Metadata.Key.of("x-sluice-id",
			Metadata.ASCII_STRING_MARSHALLER);

	/** Supervisor callbacks; invoked from the receive path, keep them fast. */
	public interface Listener {

		/**
		 * The stream is up and the first advertise was acknowledged, possibly with
		 * rejected listen ports.
		 */
		void onConnected(NodeConnection connection, String nodeId);

		/** The server pushed a membership update. */
		void onMembership(List<am.ik.sluice.v1.proto.Node> nodes, long membershipVersion);

		/** The server is draining; the stream is going away. */
		void onDrain(NodeConnection connection, String reason);

	}

	private final String key;

	private final String url;

	private final SluiceClientProperties properties;

	private final @Nullable SslBundle sslBundle;

	private final String clientId;

	private final TaskExecutor taskExecutor;

	private final Listener listener;

	private final LocalConnector connector;

	/** The routes, the receive window and the build, sent on every stream. */
	private final Advertise advertise;

	private final ConcurrentHashMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	/**
	 * The slot each registered connection holds; released once it is forgotten and the
	 * frames it queued left the sender.
	 */
	private final ConcurrentHashMap<Long, ConnectionLimit.Permit> permits = new ConcurrentHashMap<>();

	private final ConnectionLimit connectionLimit;

	/** What the connections' windows and send queues past their allowance charge. */
	private final TunnelBudget budget;

	private final Counter creditWaits;

	private final Counter rejectedConnections;

	private final Counter reconnects;

	private final Counter rejectedAdvertises;

	private final Counter windowUpdatesSent;

	private final Counter windowUpdatesReceived;

	private final Counter stallFailures;

	private final AtomicReference<SessionSender> senderRef = new AtomicReference<>();

	private volatile boolean running;

	private volatile boolean connected;

	private final ReconnectBackoff readvertiseBackoff;

	private final ReconnectBackoff reconnectBackoff;

	private volatile @Nullable ManagedChannel channel;

	private volatile String nodeId;

	private NodeConnection(Builder builder) {
		this.key = Objects.requireNonNull(builder.key, "key is required");
		this.readvertiseBackoff = new ReconnectBackoff(builder.randomFactory.get());
		this.reconnectBackoff = new ReconnectBackoff(builder.randomFactory.get());
		this.nodeId = builder.nodeId == null ? "" : builder.nodeId;
		this.url = Objects.requireNonNull(builder.url, "url is required");
		this.properties = Objects.requireNonNull(builder.properties, "properties is required");
		this.sslBundle = builder.sslBundle;
		this.clientId = Objects.requireNonNull(builder.clientId, "clientId is required");
		this.taskExecutor = Objects.requireNonNull(builder.taskExecutor, "taskExecutor is required");
		this.listener = Objects.requireNonNull(builder.listener, "listener is required");
		this.connector = LocalConnector.builder()
			.upstreams(builder.properties.upstreamTargets())
			.strict(builder.properties.strictForwarding())
			.insecure(builder.properties.insecure())
			.tcpKeepAlive(builder.properties.tcpKeepAlive())
			.connectTimeout(builder.properties.connectTimeout())
			.build();
		this.advertise = Advertise.newBuilder()
			.addAllUpstreams(builder.properties.toProtoUpstreams())
			.setConnectionWindow(VirtualConnection.INITIAL_WINDOW)
			.setBuild(builder.clientBuild)
			.build();
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
		this.connectionLimit = Objects.requireNonNull(builder.connectionLimit, "connectionLimit is required");
		this.budget = builder.budget == null ? TunnelBudget.unlimited() : builder.budget;
		this.creditWaits = Counter.builder("sluice.tunnel.credit.waits")
			.tag("node", metricsTag)
			.description("Times an upstream connection had payload for the server and waited for its credit")
			.register(meterRegistry);
		this.rejectedConnections = Counter.builder("sluice.connections.rejected")
			.tag("reason", "limit")
			.description("CONNECTs answered with an ERROR because the connection limit was reached")
			.register(meterRegistry);
		this.windowUpdatesSent = windowUpdates(meterRegistry, metricsTag, "sent");
		this.windowUpdatesReceived = windowUpdates(meterRegistry, metricsTag, "received");
		this.stallFailures = Counter.builder("sluice.tunnel.stall.failures")
			.tag("node", metricsTag)
			.description("Tunnel streams cancelled because they held frames queued for the server, "
					+ "not ready, with none taken for the tunnel stall timeout")
			.register(meterRegistry);
		Gauge.builder("sluice.tunnel.stall", this.senderRef, senderRef -> {
			SessionSender sender = senderRef.get();
			return sender == null ? 0 : sender.stalled().toNanos() / (double) TimeUnit.SECONDS.toNanos(1);
		})
			.tag("node", metricsTag)
			.baseUnit("seconds")
			.description("How long the tunnel stream has held frames queued for the server, not ready, "
					+ "with none taken; 0 when it is not stalled")
			.register(meterRegistry);
	}

	private static Counter windowUpdates(MeterRegistry meterRegistry, String node, String direction) {
		return Counter.builder("sluice.tunnel.window.updates")
			.tag("node", node)
			.tag("direction", direction)
			.description("Per-connection flow control credit grants (WINDOW_UPDATE frames) sent / received")
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

		private @Nullable SslBundle sslBundle;

		private @Nullable String clientId;

		private @Nullable TaskExecutor taskExecutor;

		private @Nullable Listener listener;

		private @Nullable MeterRegistry meterRegistry;

		private @Nullable ConnectionLimit connectionLimit;

		private @Nullable TunnelBudget budget;

		private Supplier<RandomGenerator> randomFactory = SplittableRandom::new;

		private PeerBuild clientBuild = PeerBuild.newBuilder()
			.setImplementation(ClientBuildConfiguration.IMPLEMENTATION)
			.build();

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

		/**
		 * SSL bundle supplying the client certificate (mTLS) and the truststore; takes
		 * precedence over {@code insecure}.
		 */
		public Builder sslBundle(@Nullable SslBundle sslBundle) {
			this.sslBundle = sslBundle;
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

		/** The limit shared by the connections of every node stream of the process. */
		public Builder connectionLimit(ConnectionLimit connectionLimit) {
			this.connectionLimit = connectionLimit;
			return this;
		}

		/**
		 * What the connections of every node stream of the process charge past their
		 * allowance; unlimited by default.
		 */
		public Builder budget(TunnelBudget budget) {
			this.budget = budget;
			return this;
		}

		/**
		 * The client's build, announced in every {@code Advertise}; defaults to the
		 * implementation name alone.
		 */
		public Builder clientBuild(PeerBuild clientBuild) {
			this.clientBuild = clientBuild;
			return this;
		}

		/**
		 * Supplies the randomness of the backoff jitter, one generator per backoff; tests
		 * seed it.
		 */
		public Builder randomFactory(Supplier<RandomGenerator> randomFactory) {
			this.randomFactory = randomFactory;
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
		while (this.running) {
			try {
				this.channel = buildChannel();
				runSession(this.channel);
				if (this.connected) {
					this.reconnectBackoff.reset();
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
			long backoff = this.reconnectBackoff.next().toMillis();
			log.info("[{}] reconnecting in {}ms", this.key, backoff);
			try {
				TimeUnit.MILLISECONDS.sleep(backoff);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	private ManagedChannel buildChannel() {
		String url = this.url;
		boolean secure = url.startsWith("grpcs://");
		String authority = url.replaceFirst("^grpcs?://", "");
		int portSeparator = authority.lastIndexOf(':');
		String host = portSeparator > 0 ? authority.substring(0, portSeparator) : authority;
		int port = portSeparator > 0 ? Integer.parseInt(authority.substring(portSeparator + 1)) : 443;
		// a fixed window, gRPC's own tuning off (see TunnelBudget#transportWindow)
		NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port)
			.enableRetry()
			.keepAliveTime(this.properties.keepAliveTime().toSeconds(), TimeUnit.SECONDS)
			.keepAliveTimeout(this.properties.keepAliveTimeout().toSeconds(), TimeUnit.SECONDS)
			.flowControlWindow(this.budget.transportWindow());
		if (!secure) {
			builder.usePlaintext();
		}
		else if (this.sslBundle != null) {
			try {
				SslContextBuilder ssl = GrpcSslContexts.forClient()
					.keyManager(this.sslBundle.getManagers().getKeyManagerFactory());
				TrustManagerFactory trustManagerFactory = this.sslBundle.getManagers().getTrustManagerFactory();
				if (trustManagerFactory != null) {
					ssl.trustManager(trustManagerFactory);
				}
				builder.sslContext(ssl.build());
			}
			catch (Exception e) {
				throw new IllegalStateException("failed to build SSL context from bundle " + this.sslBundle, e);
			}
		}
		else if (this.properties.insecure()) {
			try {
				builder
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
				SessionSender sender = SessionSender.builder(call)
					.windowUpdateSent(windowUpdatesSent::increment)
					.stallTimeout(properties.tunnelStallTimeout())
					.budget(budget)
					.stallAction(stalled -> failStalled(call, stalled))
					.build();
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
				markCallClosed();
				closed.countDown();
			}

			@Override
			public void onCompleted() {
				log.info("[{}] tunnel stream closed by server", NodeConnection.this.key);
				markCallClosed();
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
		this.readvertiseBackoff.reset();
		sender.sendAdvertise(this.advertise);
		closed.await();
		// the sender first, as in TunnelSession#close: a dead or stalled stream must not
		// leave the connection teardown below waiting on a full send queue (closing the
		// sender releases every producer); from here on the frames of the closing
		// connections are dropped, the peer is gone anyway
		sender.close();
		this.closeConnections();
	}

	/**
	 * The sender's watchdog fired: cancels the call, which resets the stream at once (a
	 * half-close would wait behind the frames the server does not take); its end tears
	 * the session down and the loop reconnects.
	 */
	private void failStalled(ClientCallStreamObserver<Frame> call, Duration stalled) {
		this.stallFailures.increment();
		log.warn("[{}] tunnel stream held frames queued, not ready, with none taken for {}ms: cancelling it", this.key,
				stalled.toMillis());
		call.cancel("tunnel stream stalled", null);
	}

	/**
	 * The receive side of the stream ended: producers and the worker parked on the sender
	 * give up without waiting for the session teardown.
	 */
	private void markCallClosed() {
		SessionSender sender = this.senderRef.get();
		if (sender != null) {
			sender.markCallClosed();
		}
	}

	/**
	 * Fails and closes every virtual connection of the ended stream, mirroring
	 * {@code TunnelSession#close}: relays waiting on the connections end and the local
	 * sockets are released. Idempotent; safe to run while relays are still registering.
	 */
	private void closeConnections() {
		for (Long connectionId : this.connections.keySet()) {
			VirtualConnection connection = this.forget(connectionId);
			if (connection != null) {
				connection.remoteFailed("tunnel stream closed");
				connection.close();
			}
		}
	}

	/**
	 * Unregisters a connection and frees its slot of the connection limit once the frames
	 * it queued left the sender ({@link VirtualConnection#afterQueued(Runnable)}); the
	 * first call for an id does both, later ones nothing. Call it once the connection
	 * queued its last frame: its relay ended, or it was aborted.
	 */
	private @Nullable VirtualConnection forget(long connectionId) {
		VirtualConnection connection = this.connections.remove(connectionId);
		ConnectionLimit.Permit permit = this.permits.remove(connectionId);
		if (permit != null) {
			if (connection != null) {
				connection.afterQueued(permit::release);
			}
			else {
				permit.release();
			}
		}
		return connection;
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
				if (request.getConnectionWindow() == 0) {
					// a server predating per-connection flow control: neither dialed nor
					// registered, like a CONNECT beyond the limit
					log.warn("[{}] {}; rejecting conn {}", this.key, SERVER_WINDOW_REQUIRED, request.getConnId());
					sender.sendError(request.getConnId(), SERVER_WINDOW_REQUIRED);
					return;
				}
				ConnectionLimit.Permit permit = this.connectionLimit.tryAcquire();
				if (permit == null) {
					// neither dialed nor registered: the server fails its side on the
					// ERROR, and DATA that follows the CONNECT finds no connection
					this.rejectedConnections.increment();
					log.debug("[{}] {}; rejecting conn {}", this.key, ConnectionLimit.LIMIT_REACHED,
							request.getConnId());
					sender.sendError(request.getConnId(), ConnectionLimit.LIMIT_REACHED);
					return;
				}
				VirtualConnection connection = VirtualConnection.builder(request.getConnId(), sender)
					.window(VirtualConnection.INITIAL_WINDOW, Integer.toUnsignedLong(request.getConnectionWindow()))
					.growWindow(VirtualConnection.DEFAULT_WINDOW, this.budget)
					.creditWaited(this.creditWaits::increment)
					.build();
				this.permits.put(request.getConnId(), permit);
				this.connections.put(request.getConnId(), connection);
				try {
					this.taskExecutor.execute(() -> dial(request, connection));
				}
				catch (RuntimeException e) {
					connection.abort(e.toString());
					this.forget(request.getConnId());
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
				this.windowUpdatesReceived.increment();
				VirtualConnection connection = this.connections.get(frame.getWindowUpdate().getConnId());
				if (connection != null) {
					connection.windowUpdate(frame.getWindowUpdate());
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
				AdvertiseAck ack = frame.getAdvertiseAck();
				List<Integer> rejected = ack.getRejectedPortsList();
				String ackNodeId = ack.getNodeId();
				if (!ackNodeId.isBlank()) {
					this.nodeId = ackNodeId;
				}
				if (rejected.isEmpty()) {
					this.readvertiseBackoff.reset();
				}
				else {
					// the other routes are up; only the rejected ports are retried
					Duration delay = this.readvertiseBackoff.next();
					log.warn("[{}] server rejected listen ports {}; re-advertising in {}ms", this.key, rejected,
							delay.toMillis());
					this.rejectedAdvertises.increment();
					readvertiseAfter(delay, closed);
				}
				if (!this.connected) {
					// the first advertise is acknowledged: the tunnel is up for real; the
					// lines are logged before isConnected() turns true
					log.info("[{}] tunnel established; advertising {} upstream(s)", this.key,
							this.properties.upstreamTargets().size());
					log.info("[{}] server build: {}", this.key, ack.hasBuild()
							? PeerBuilds.describe(PeerBuilds.sanitize(ack.getBuild())) : PeerBuilds.UNKNOWN);
					this.connected = true;
					this.listener.onConnected(this, this.nodeId);
				}
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

	/** Re-sends the advertise on this stream after the delay unless it closes first. */
	private void readvertiseAfter(Duration delay, CountDownLatch closed) {
		Thread.ofVirtual().name("sluice-readvertise-" + this.key).start(() -> {
			try {
				if (closed.await(delay.toMillis(), TimeUnit.MILLISECONDS)) {
					return;
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			SessionSender sender = this.senderRef.get();
			if (sender != null) {
				sender.sendAdvertise(this.advertise);
			}
		});
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
				this.forget(connectionId);
				return;
			}
			SocketRelay relay = SocketRelay.builder(socket, connection, sender)
				.onComplete(() -> this.forget(connectionId))
				.build();
			relay.start();
		}
		catch (Exception e) {
			log.debug("dial {} failed: {}", address, e.toString());
			// the ERROR first: the slot is held until it left
			connection.abort(e.toString());
			this.forget(connectionId);
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

	/** The live queues of the stream's sender; 0 without a stream. For tests. */
	int queuedConnectionsForTest() {
		SessionSender sender = this.senderRef.get();
		return sender == null ? 0 : sender.connectionQueues();
	}

	/** The stall timeout of the stream's sender; zero without a stream. For tests. */
	Duration stallTimeoutForTest() {
		SessionSender sender = this.senderRef.get();
		return sender == null ? Duration.ZERO : sender.stallTimeout();
	}

	Map<Long, VirtualConnection> connectionsForTest() {
		return Map.copyOf(this.connections);
	}

}
