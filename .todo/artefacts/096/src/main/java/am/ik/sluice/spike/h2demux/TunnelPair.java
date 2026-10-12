package am.ik.sluice.spike.h2demux;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Connect;

/**
 * Two windowed {@link VirtualConnection}s wired back to back in place of the gRPC tunnel:
 * what one side sends -- DATA, CLOSE, ERROR, its window grants -- is delivered to the
 * other on the sending thread. Delivery never blocks ({@code acceptData} and
 * {@code grant} of a windowed connection do not), so a writer waiting for credit is the
 * only backpressure, as over the real tunnel.
 */
public final class TunnelPair {

	private static final AtomicLong IDS = new AtomicLong();

	private final VirtualConnection local;

	private final VirtualConnection remote;

	private TunnelPair(int window) {
		long id = IDS.incrementAndGet();
		AtomicReference<VirtualConnection> localRef = new AtomicReference<>();
		AtomicReference<VirtualConnection> remoteRef = new AtomicReference<>();
		this.local = VirtualConnection.builder(id, new Wire(remoteRef)).window(window, window).build();
		this.remote = VirtualConnection.builder(id, new Wire(localRef)).window(window, window).build();
		localRef.set(this.local);
		remoteRef.set(this.remote);
	}

	/** A pair whose two directions each have the given credit window. */
	public static TunnelPair open(int window) {
		return new TunnelPair(window);
	}

	/** The relay's end: a leg. */
	public VirtualConnection local() {
		return this.local;
	}

	/** The upstream's end, where the tunnel client would dial the upstream. */
	public VirtualConnection remote() {
		return this.remote;
	}

	private record Wire(AtomicReference<VirtualConnection> peer) implements FrameWriter {

		@Override
		public void sendConnect(Connect connect) {
			// the pair is connected from the start
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
