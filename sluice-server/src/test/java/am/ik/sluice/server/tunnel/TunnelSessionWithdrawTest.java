package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Upstream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * A withdrawing advertise removes the client's routes and releases its ports at once,
 * keeps the session opening connections, and is acknowledged only once no connection is
 * left; from that ack on the session opens none.
 */
class TunnelSessionWithdrawTest {

	private static final String HOST = "withdraw.test";

	private final Router router = new Router();

	private final List<Set<Integer>> reconciled = new CopyOnWriteArrayList<>();

	@Test
	void withdrawalIsAcknowledgedOnceTheLastConnectionEndsAndSealsTheSession() throws Exception {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = TunnelSession.builder()
			.clientId("client-a")
			.router(this.router)
			.registry(new SessionRegistry(SluiceServerProperties.builder().build()))
			.tcpRoutes((clientId, ports) -> {
				this.reconciled.add(ports);
				return Set.of();
			})
			.sender(new SessionSender(stream))
			.build();
		session.start();
		Upstream upstream = Upstream.newBuilder().setHost(HOST).setTargetUrl("http://127.0.0.1:8080").build();
		session.handle(advertise(upstream, false));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		assertThat(this.router.lookup(HOST)).isPresent();
		VirtualConnection inFlight = session.open("127.0.0.1:8080");

		session.handle(advertise(upstream, true));
		Awaitility.await().until(() -> session.upstreams().isEmpty());
		assertThat(session.withdrawn()).isTrue();
		assertThat(this.router.lookup(HOST)).isEmpty();
		assertThat(this.reconciled).last().isEqualTo(Set.of());
		// a relay that resolved the route before the withdrawal still gets its connection
		VirtualConnection late = session.open("127.0.0.1:8080");
		session.remove(inFlight.connectionId());
		assertThat(stream.matching(Frame.BodyCase.ADVERTISE_ACK)).hasSize(1);

		session.remove(late.connectionId());
		AdvertiseAck withdrawn = stream.await(Frame.BodyCase.ADVERTISE_ACK, 2).get(1).getAdvertiseAck();
		assertThat(withdrawn.getWithdrawn()).isTrue();
		assertThatIllegalStateException().isThrownBy(() -> session.open("127.0.0.1:8080"))
			.withMessageContaining("withdrew");
		// the CONNECTs of both connections precede the ack on the stream
		List<Frame.BodyCase> order = stream.frames.stream()
			.map(Frame::getBodyCase)
			.filter(body -> body == Frame.BodyCase.CONNECT || body == Frame.BodyCase.ADVERTISE_ACK)
			.toList();
		assertThat(order).containsExactly(Frame.BodyCase.ADVERTISE_ACK, Frame.BodyCase.CONNECT, Frame.BodyCase.CONNECT,
				Frame.BodyCase.ADVERTISE_ACK);

		// an advertise after the withdrawal registers nothing and is answered as
		// withdrawn
		session.handle(advertise(upstream, false));
		assertThat(stream.await(Frame.BodyCase.ADVERTISE_ACK, 3).get(2).getAdvertiseAck().getWithdrawn()).isTrue();
		assertThat(this.router.lookup(HOST)).isEmpty();
		session.close();
	}

	private static Frame advertise(Upstream upstream, boolean withdraw) {
		return Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder()
				.addUpstreams(upstream)
				.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
				.setWithdraw(withdraw))
			.build();
	}

}
