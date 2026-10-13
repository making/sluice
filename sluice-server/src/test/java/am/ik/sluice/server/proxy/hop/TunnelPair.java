package am.ik.sluice.server.proxy.hop;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Connect;

/**
 * Two windowed {@link VirtualConnection}s wired back to back in place of the gRPC tunnel:
 * what one side sends -- DATA, CLOSE, ERROR, its window grants -- is delivered to the
 * other on the sending thread. Delivery never blocks, so a writer waiting for credit is
 * the only backpressure, as over the real tunnel.
 */
final class TunnelPair {

	static final int WINDOW = 64 * 1024;

	private static final AtomicLong IDS = new AtomicLong();

	private final VirtualConnection local;

	private final VirtualConnection remote;

	private TunnelPair() {
		long id = IDS.incrementAndGet();
		AtomicReference<VirtualConnection> localRef = new AtomicReference<>();
		AtomicReference<VirtualConnection> remoteRef = new AtomicReference<>();
		this.local = VirtualConnection.builder(id, new Wire(remoteRef)).window(WINDOW, WINDOW).build();
		this.remote = VirtualConnection.builder(id, new Wire(localRef)).window(WINDOW, WINDOW).build();
		localRef.set(this.local);
		remoteRef.set(this.remote);
	}

	static TunnelPair open() {
		return new TunnelPair();
	}

	/** The relay's end: a leg. */
	VirtualConnection local() {
		return this.local;
	}

	/** The upstream's end. */
	VirtualConnection remote() {
		return this.remote;
	}

	private record Wire(AtomicReference<VirtualConnection> peer) implements FrameWriter {

		@Override
		public void sendConnect(Connect connect) {
			// connected from the start
		}

		@Override
		public boolean sendData(long connectionId, byte[] payload) {
			this.peer.get().acceptData(payload.clone());
			return true;
		}

		@Override
		public void sendClose(long connectionId) {
			this.peer.get().remoteClosed();
		}

		@Override
		public void sendError(long connectionId, String message) {
			this.peer.get().remoteFailed(message);
		}

		@Override
		public void sendWindowUpdate(long connectionId, int increment) {
			this.peer.get().grant(increment);
		}

	}

}
