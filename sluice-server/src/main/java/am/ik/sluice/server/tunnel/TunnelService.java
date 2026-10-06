package am.ik.sluice.server.tunnel;

import java.util.Objects;

import am.ik.sluice.server.auth.TokenValidator;
import am.ik.sluice.server.cluster.NodeDirectory;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

/**
 * gRPC control plane: a single bidirectional stream per client carrying the multiplexed
 * tunnel (the /tunnel WebSocket endpoint of the original).
 */
@GrpcService
public class TunnelService extends TunnelGrpc.TunnelImplBase {

	private static final Logger log = LoggerFactory.getLogger(TunnelService.class);

	private final Router router;

	private final TokenValidator tokenValidator;

	private final SessionRegistry sessions;

	private final TcpRouteListener tcpRoutes;

	private final NodeDirectory nodeDirectory;

	private final MembershipBroadcaster membershipBroadcaster;

	private final ClusterLifecycle clusterLifecycle;

	private final SluiceServerProperties properties;

	public TunnelService(Router router, TokenValidator tokenValidator, SessionRegistry sessions,
			TcpRouteListener tcpRoutes, NodeDirectory nodeDirectory, MembershipBroadcaster membershipBroadcaster,
			ClusterLifecycle clusterLifecycle, SluiceServerProperties properties) {
		this.router = router;
		this.tokenValidator = tokenValidator;
		this.sessions = sessions;
		this.tcpRoutes = tcpRoutes;
		this.nodeDirectory = nodeDirectory;
		this.membershipBroadcaster = membershipBroadcaster;
		this.clusterLifecycle = clusterLifecycle;
		this.properties = properties;
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
		if (this.clusterLifecycle.isDraining()) {
			// the node is going away; a stream accepted here would stall the graceful
			// shutdown until the client gives up
			log.info("rejecting tunnel stream: node is draining");
			responseObserver.onError(Status.UNAVAILABLE.withDescription("node is draining").asException());
			return noopObserver();
		}
		String clientId = TunnelAuthInterceptor.CLIENT_ID.get();
		String finalClientId = clientId == null ? "" : clientId;
		log.info("tunnel stream established for client {}", finalClientId);
		TunnelSession session = TunnelSession.builder()
			.clientId(clientId == null ? "" : clientId)
			.nodeId(this.properties.node().id())
			.remoteAddress(Objects.requireNonNullElse(TunnelAuthInterceptor.REMOTE_ADDRESS.get(), ""))
			.router(this.router)
			.sender(new SessionSender(responseObserver))
			.registry(this.sessions)
			.tcpRoutes(this.tcpRoutes)
			.build();
		session.start();
		this.membershipBroadcaster.onSessionCreated(session);
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
