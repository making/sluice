package am.ik.sluice.server.tunnel;

import am.ik.sluice.server.auth.TokenValidator;
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

	public TunnelService(Router router, TokenValidator tokenValidator, SessionRegistry sessions) {
		this.router = router;
		this.tokenValidator = tokenValidator;
		this.sessions = sessions;
	}

	@Override
	public StreamObserver<Frame> connect(StreamObserver<Frame> responseObserver) {
		String clientId = TunnelAuthInterceptor.CLIENT_ID.get();
		String finalClientId = clientId == null ? "" : clientId;
		log.info("tunnel stream established for client {}", finalClientId);
		TunnelSession session = new TunnelSession(clientId, this.router, new SessionSender(responseObserver),
				this.sessions);
		session.start();
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

}
