package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import am.ik.sluice.server.auth.TokenValidator;
import am.ik.sluice.server.cluster.NodeDirectory;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.PeerBuild;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.grpc.server.service.GrpcService;

/**
 * gRPC control plane: a single bidirectional stream per client carrying the multiplexed
 * tunnel (the /tunnel WebSocket endpoint of the original).
 */
@GrpcService
public class TunnelService extends TunnelGrpc.TunnelImplBase {

	private static final Logger log = LoggerFactory.getLogger(TunnelService.class);

	/** The period of {@link #refusalLog}. */
	static final Duration REFUSAL_LOG_PERIOD = Duration.ofSeconds(10);

	private final Router router;

	private final TokenValidator tokenValidator;

	private final SessionRegistry sessions;

	private final TcpRouteListener tcpRoutes;

	private final NodeDirectory nodeDirectory;

	private final MembershipBroadcaster membershipBroadcaster;

	private final SluiceServerProperties properties;

	private final Counter windowUpdatesSent;

	private final Counter windowUpdatesReceived;

	private final PeerBuild serverBuild;

	private final Counter stallFailures;

	private final Counter advertiseTimeouts;

	private final Map<SessionRegistry.Refusal, Counter> refusals = new EnumMap<>(SessionRegistry.Refusal.class);

	/** At most one refusal line per period, so refused streams cannot flood the log. */
	private final LogThrottle refusalLog = new LogThrottle(REFUSAL_LOG_PERIOD, System::nanoTime);

	public TunnelService(Router router, TokenValidator tokenValidator, SessionRegistry sessions,
			TcpRouteListener tcpRoutes, NodeDirectory nodeDirectory, MembershipBroadcaster membershipBroadcaster,
			SluiceServerProperties properties, MeterRegistry meterRegistry, PeerBuild serverBuild) {
		this.router = router;
		this.tokenValidator = tokenValidator;
		this.sessions = sessions;
		this.tcpRoutes = tcpRoutes;
		this.nodeDirectory = nodeDirectory;
		this.membershipBroadcaster = membershipBroadcaster;
		this.properties = properties;
		this.windowUpdatesSent = windowUpdates(meterRegistry, "sent");
		this.windowUpdatesReceived = windowUpdates(meterRegistry, "received");
		this.serverBuild = serverBuild;
		this.stallFailures = Counter.builder("sluice.tunnel.stall.failures")
			.description("Tunnel sessions failed because their stream held frames queued for the client, "
					+ "not ready, with none taken for the tunnel stall timeout")
			.register(meterRegistry);
		Gauge.builder("sluice.tunnel.stall", sessions,
				registry -> registry.all().stream().mapToLong(session -> session.stalled().toNanos()).max().orElse(0)
						/ (double) TimeUnit.SECONDS.toNanos(1))
			.baseUnit("seconds")
			.description("How long the most stalled tunnel stream has held frames queued for its client, "
					+ "not ready, with none taken; 0 when none is stalled")
			.register(meterRegistry);
		this.advertiseTimeouts = Counter.builder("sluice.tunnel.advertise.timeouts")
			.description("Tunnel streams failed because the client sent no Advertise within the advertise timeout")
			.register(meterRegistry);
		for (SessionRegistry.Refusal refusal : SessionRegistry.Refusal.values()) {
			this.refusals.put(refusal,
					Counter.builder("sluice.tunnel.streams.refused")
						.tag("reason", refusal.reason())
						.description("Tunnel streams refused at once: past a bound on the streams waiting for "
								+ "their client's first Advertise, or while the node drains")
						.register(meterRegistry));
		}
		log.info("server build: {}", PeerBuilds.describe(serverBuild));
		Gauge
			.builder("sluice.tunnel.sessions.build.unknown", sessions,
					registry -> registry.all().stream().filter(session -> session.clientBuild().isEmpty()).count())
			.description("Connected clients that advertised no build (they predate build reporting)")
			.register(meterRegistry);
	}

	private static Counter windowUpdates(MeterRegistry meterRegistry, String direction) {
		return Counter.builder("sluice.tunnel.window.updates")
			.tag("direction", direction)
			.description("Per-connection flow control credit grants (WINDOW_UPDATE frames) sent / received")
			.register(meterRegistry);
	}

	@Override
	public void listNodes(am.ik.sluice.v1.proto.ListNodesRequest request,
			StreamObserver<am.ik.sluice.v1.proto.ListNodesResponse> responseObserver) {
		responseObserver.onNext(am.ik.sluice.v1.proto.ListNodesResponse.newBuilder()
			.addAllNodes(this.nodeDirectory.nodes()
				.stream()
				.map(member -> am.ik.sluice.v1.proto.Node.newBuilder()
					.setNodeId(member.nodeId())
					.setPublicUrl(member.publicUrl())
					.build())
				.toList())
			.setMembershipVersion(this.nodeDirectory.version())
			.build());
		responseObserver.onCompleted();
	}

	@Override
	public StreamObserver<Frame> connect(StreamObserver<Frame> responseObserver) {
		String clientId = TunnelAuthInterceptor.CLIENT_ID.get();
		String finalClientId = clientId == null ? "" : clientId;
		@Nullable Runnable reset = TunnelStreamReset.current();
		// set before the sender starts, so before the watchdog can fire
		AtomicReference<@Nullable TunnelSession> stalledSession = new AtomicReference<>();
		SessionSender sender = SessionSender.builder(responseObserver)
			.windowUpdateSent(this.windowUpdatesSent::increment)
			.stallTimeout(this.properties.limits().tunnelStallTimeout())
			.stallAction(stalled -> {
				this.stallFailures.increment();
				log.warn("tunnel stream of client {} held frames queued, not ready, with none taken for {}ms: "
						+ "failing the session", finalClientId, stalled.toMillis());
				reset(reset, responseObserver);
				TunnelSession failed = stalledSession.get();
				if (failed != null) {
					failed.close();
				}
			})
			.build();
		TunnelSession session = TunnelSession.builder()
			.clientId(clientId == null ? "" : clientId)
			.nodeId(this.properties.node().id())
			.remoteAddress(Objects.requireNonNullElse(TunnelAuthInterceptor.REMOTE_ADDRESS.get(), ""))
			.sourceAddress(Objects.requireNonNullElse(TunnelAuthInterceptor.SOURCE_ADDRESS.get(), ""))
			.router(this.router)
			.sender(sender)
			.registry(this.sessions)
			.tcpRoutes(this.tcpRoutes)
			.windowUpdateReceived(this.windowUpdatesReceived::increment)
			.serverBuild(this.serverBuild)
			.advertiseTimeout(this.properties.limits().advertiseTimeout())
			.advertiseTimedOut(this.advertiseTimeouts::increment)
			.registered(this.membershipBroadcaster::onSessionRegistered)
			.build();
		stalledSession.set(session);
		Optional<SessionRegistry.Refusal> refused = session.start();
		if (refused.isPresent()) {
			refuse(refused.get(), session);
			responseObserver.onError(refused.get().status().asRuntimeException());
			return noopObserver();
		}
		// logged once admitted: a refused stream is logged by the throttled refusal line
		log.info("tunnel stream established for client {}", finalClientId);
		return new StreamObserver<>() {

			@Override
			public void onNext(Frame frame) {
				session.handle(frame);
			}

			@Override
			public void onError(Throwable t) {
				log.info("tunnel stream failed for client {}: {}", finalClientId, t.toString());
				session.close();
			}

			@Override
			public void onCompleted() {
				log.info("tunnel stream closed by client {}", finalClientId);
				session.close();
			}

		};
	}

	/**
	 * Counts a refused stream and logs it, at most once per {@link #REFUSAL_LOG_PERIOD}.
	 */
	private void refuse(SessionRegistry.Refusal refusal, TunnelSession session) {
		Objects.requireNonNull(this.refusals.get(refusal), "refusal counter").increment();
		long suppressed = this.refusalLog.tryAcquire();
		if (suppressed < 0) {
			return;
		}
		// a drain refusing the clients reconnecting to it is expected
		log.atLevel(refusal == SessionRegistry.Refusal.DRAINING ? Level.INFO : Level.WARN)
			.log("refusing tunnel stream of client {} ({}): {}{}", session.clientId(), session.remoteAddress(),
					refusal.status().getDescription(),
					suppressed == 0 ? "" : " (%d more refused since the last report)".formatted(suppressed));
	}

	/**
	 * Ends the tunnel stream of a stalled session: resets it ({@link TunnelStreamReset}),
	 * or without a reset closes the call with a status, which reaches the client only
	 * behind the frames already buffered for it.
	 */
	private static void reset(@Nullable Runnable reset, StreamObserver<Frame> responseObserver) {
		if (reset != null) {
			reset.run();
			return;
		}
		try {
			responseObserver.onError(Status.UNAVAILABLE.withDescription("tunnel stream stalled").asRuntimeException());
		}
		catch (RuntimeException e) {
			// the call is already closed
		}
	}

	private static StreamObserver<Frame> noopObserver() {
		return new StreamObserver<>() {

			@Override
			public void onNext(Frame value) {
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onCompleted() {
			}

		};
	}

}
