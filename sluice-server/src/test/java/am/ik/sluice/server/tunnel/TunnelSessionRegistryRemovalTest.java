package am.ik.sluice.server.tunnel;

import java.util.Set;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.v1.proto.Frame;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A closed session leaves the registry; a newer session of the same client id survives
 * the close of its predecessor.
 */
class TunnelSessionRegistryRemovalTest {

	private final SessionRegistry registry = new SessionRegistry();

	private final Router router = new Router();

	private TunnelSession session(String clientId) {
		return TunnelSession.builder()
			.clientId(clientId)
			.router(this.router)
			.sender(new SessionSender(new NoopStream()))
			.registry(this.registry)
			.tcpRoutes(new NoopTcpRouteListener())
			.build();
	}

	@Test
	void closedSessionIsRemovedFromTheRegistry() {
		TunnelSession session = session("client-a");
		assertThat(this.registry.count()).isEqualTo(1);
		session.close();
		assertThat(this.registry.count()).isZero();
	}

	@Test
	void closingAPredecessorKeepsTheNewerSessionOfTheSameId() {
		TunnelSession first = session("client-a");
		TunnelSession second = session("client-a");
		assertThat(this.registry.count()).isEqualTo(1);
		first.close();
		assertThat(this.registry.find("client-a")).contains(second);
	}

	private static final class NoopTcpRouteListener implements TcpRouteListener {

		@Override
		public Set<Integer> reconcile(String clientId, Set<Integer> listenPorts) {
			return Set.of();
		}

	}

	private static final class NoopStream implements io.grpc.stub.StreamObserver<Frame> {

		@Override
		public void onNext(Frame value) {
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}

	}

}
