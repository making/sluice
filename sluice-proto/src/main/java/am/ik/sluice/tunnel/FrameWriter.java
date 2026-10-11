package am.ik.sluice.tunnel;

import am.ik.sluice.v1.proto.Connect;

/**
 * Abstraction over the outbound direction of the tunnel stream. Implemented by
 * {@link SessionSender} on both the server and the client side.
 */
public interface FrameWriter {

	/**
	 * Sends a CONNECT frame, requesting the client to dial the address.
	 */
	void sendConnect(Connect connect);

	/**
	 * Sends a DATA frame. Payloads larger than 64KiB are split into multiple frames; the
	 * array is copied before queueing. Returns false when the frame could not be fully
	 * queued (the tunnel stream is gone); callers are expected to end their side.
	 */
	boolean sendData(long connectionId, byte[] payload);

	/**
	 * Half-closes the local -> remote direction of the given connection.
	 */
	void sendClose(long connectionId);

	/**
	 * Abnormally terminates the given connection with a cause.
	 */
	void sendError(long connectionId, String message);

	/**
	 * Grants the remote {@code increment} more payload bytes of the given windowed
	 * connection. Never blocks: called by the local consumer of the connection.
	 */
	void sendWindowUpdate(long connectionId, int increment);

	/**
	 * Runs {@code action} once none of the frames queued for the connection so far waits
	 * any more: at once when none is queued, else once the last one was handed to the
	 * stream, or when the writer stops and drops them. Runs it exactly once, never on the
	 * caller's thread while holding a lock of the writer.
	 */
	default void afterQueued(long connectionId, Runnable action) {
		action.run();
	}

	/**
	 * False once no frame can reach the remote any more (the stream ended); a producer
	 * waiting for credit gives up instead of waiting for a grant that cannot arrive.
	 */
	default boolean isOpen() {
		return true;
	}

}
