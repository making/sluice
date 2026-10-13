package am.ik.sluice.server.tunnel;

import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;

/** Sessions whose client advertised: registered, serving connections. */
public final class AdvertisedSessions {

	private AdvertisedSessions() {
	}

	/**
	 * Builds and starts a session over an always-ready stream, hands it its client's
	 * first advertise -- a window, no upstream -- and returns once that is acknowledged,
	 * so the advertise has registered the session and run its route reconcile.
	 */
	public static TunnelSession advertised(TunnelSession.Builder builder) {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = builder.sender(new SessionSender(stream)).build();
		session.start();
		session.handle(Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder().setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
			.build());
		try {
			stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
		return session;
	}

}
