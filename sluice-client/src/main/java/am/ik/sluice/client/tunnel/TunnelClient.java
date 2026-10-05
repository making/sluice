package am.ik.sluice.client.tunnel;

import org.jspecify.annotations.Nullable;

import java.net.Socket;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.SocketRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Maintains the tunnel stream towards the server: connects, advertises the upstream map,
 * dials local upstreams on CONNECT, and reconnects with exponential backoff (1s..30s)
 * when the stream drops.
 */
@Component
public class TunnelClient implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(TunnelClient.class);

	private static final long BACKOFF_INITIAL_SECONDS = 1;

	private static final long BACKOFF_MAX_SECONDS = 30;

	private static final Metadata.Key<String> TOKEN_HEADER = Metadata.Key.of("authorization",
			Metadata.ASCII_STRING_MARSHALLER);

	private static final Metadata.Key<String> CLIENT_ID_HEADER = Metadata.Key.of("x-sluice-id",
			Metadata.ASCII_STRING_MARSHALLER);

	private final SluiceClientProperties properties;

	private final TaskExecutor taskExecutor;

	private final MeterRegistry meterRegistry;

	private final Counter reconnects;

	private final Counter rejectedAdvertises;

	private final ConcurrentHashMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	private volatile boolean running;

	private volatile boolean connected;

	private volatile @Nullable ManagedChannel channel;

	private volatile @Nullable CountDownLatch shutdown;

	private volatile @Nullable SessionSender currentSender;

	TunnelClient(SluiceClientProperties properties, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			MeterRegistry meterRegistry) {
		this.properties = properties;
		this.taskExecutor = taskExecutor;
		this.meterRegistry = meterRegistry;
		this.reconnects = Counter.builder("sluice.reconnect.total")
			.description("Tunnel stream re-establishments after a drop")
			.register(meterRegistry);
		this.rejectedAdvertises = Counter.builder("sluice.advertise.rejected")
			.description("Advertised listen ports rejected by the server")
			.register(meterRegistry);
		meterRegistry.gauge("sluice.connections.active", this.connections, Map::size);
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
		return this.connected;
	}

	@Override
	public void start() {
		if (this.properties.serverUrl() == null || this.properties.serverUrl().isBlank()) {
			throw new IllegalStateException("sluice.server-url is required");
		}
		Map<String, String> upstreams = this.properties.upstreamMap();
		if (upstreams.isEmpty()) {
			throw new IllegalStateException("sluice.client.upstream is required");
		}
		List<am.ik.sluice.v1.proto.Upstream> advertised = this.properties.toProtoUpstreams();
		this.running = true;
		this.shutdown = new CountDownLatch(1);
		this.taskExecutor.execute(() -> runLoop(upstreams, advertised));
	}

	private void runLoop(Map<String, String> upstreams, List<am.ik.sluice.v1.proto.Upstream> advertised) {
		LocalConnector connector = LocalConnector.builder()
			.upstreams(upstreams)
			.strict(this.properties.strictForwarding())
			.insecure(this.properties.insecure())
			.build();
		long backoff = BACKOFF_INITIAL_SECONDS;
		while (this.running) {
			try {
				this.channel = buildChannel();
				runSession(this.channel, connector, advertised);
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
					log.warn("tunnel stream ended: {}", e.toString());
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
		String url = this.properties.serverUrl();
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

	private void runSession(ManagedChannel channel, LocalConnector connector,
			List<am.ik.sluice.v1.proto.Upstream> upstreams) throws InterruptedException {
		CountDownLatch closed = new CountDownLatch(1);
		AtomicReference<SessionSender> senderRef = new AtomicReference<>();
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
				handle(frame, connector, closed::countDown);
			}

			@Override
			public void onError(Throwable t) {
				log.info("tunnel stream error: {}", t.toString());
				closed.countDown();
			}

			@Override
			public void onCompleted() {
				log.info("tunnel stream closed by server");
				closed.countDown();
			}

		};
		Metadata metadata = new Metadata();
		String token = this.properties.tokenValue();
		if (!token.isBlank()) {
			metadata.put(TOKEN_HEADER, "Bearer " + token);
		}
		metadata.put(CLIENT_ID_HEADER, UUID.randomUUID().toString());
		log.info("connecting to {} ...", this.properties.serverUrl());
		TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(responseObserver);
		SessionSender sender = senderRef.get();
		if (sender == null) {
			return; // stream failed before starting
		}
		this.currentSender = sender;
		sender.sendAdvertise(upstreams);
		closed.await();
		sender.close();
	}

	private void handle(Frame frame, LocalConnector connector, Runnable terminateStream) {
		switch (frame.getType()) {
			case CONNECT -> {
				// register the virtual connection synchronously so DATA frames that
				// follow the CONNECT are not dropped before the dial task runs
				SessionSender sender0 = this.currentSender;
				if (sender0 == null) {
					return;
				}
				VirtualConnection connection = new VirtualConnection(frame.getConnId(), sender0);
				this.connections.put(frame.getConnId(), connection);
				this.taskExecutor.execute(() -> dial(frame, connector, connection));
			}
			case DATA -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.acceptData(frame.getPayload().toByteArray());
				}
			}
			case CLOSE -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.remoteClosed();
				}
			}
			case ERROR -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.remoteFailed(frame.getMessage());
				}
			}
			case ADVERTISED -> {
				List<Integer> rejected = frame.getRejectedPortsList();
				if (rejected.isEmpty()) {
					// the server acknowledged the advertise: the tunnel is up for real
					this.connected = true;
					log.info("tunnel established; advertising {} upstream(s)", this.properties.upstreamMap().size());
					return;
				}
				log.warn("server rejected listen ports {} on advertise; closing the stream to re-advertise", rejected);
				this.rejectedAdvertises.increment();
				terminateStream.run();
			}
			case ADVERTISE -> log.warn("unexpected ADVERTISE from server");
			case KEEPALIVE -> {
				// liveness no-op
			}
			default -> {
				// unknown future type: ignore
			}
		}
	}

	private void dial(Frame frame, LocalConnector connector, VirtualConnection connection) {
		long connectionId = frame.getConnId();
		String address = frame.getAddress();
		try {
			if (!connector.permits(address)) {
				throw new IllegalArgumentException("upstream not permitted: " + address);
			}
			Socket socket = connector.dial(address);
			SessionSender sender = this.currentSender;
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
			SessionSender sender = this.currentSender;
			if (sender != null) {
				sender.sendError(connectionId, e.toString());
			}
			this.connections.remove(connectionId);
			connection.close();
		}
	}

	private void shutdownChannel() {
		ManagedChannel channel = this.channel;
		if (channel != null && !channel.isShutdown()) {
			channel.shutdownNow();
		}
	}

	@Override
	public void stop() {
		this.running = false;
		this.connected = false;
		CountDownLatch shutdown = this.shutdown;
		if (shutdown != null) {
			shutdown.countDown();
		}
		shutdownChannel();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

}
