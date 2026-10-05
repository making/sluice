package am.ik.sluice.tunnel;

/**
 * Abstraction over the outbound direction of the tunnel stream. Implemented by
 * {@link SessionSender} on both the server and the client side.
 */
public interface FrameWriter {

	/**
	 * Sends a CONNECT frame, requesting the client to dial the address.
	 */
	void sendConnect(long connectionId, String address);

	/**
	 * Sends a DATA frame. Payloads larger than 64KiB are split into multiple frames; the
	 * array is copied before queueing.
	 */
	void sendData(long connectionId, byte[] payload);

	/**
	 * Half-closes the local -> remote direction of the given connection.
	 */
	void sendClose(long connectionId);

	/**
	 * Abnormally terminates the given connection with a cause.
	 */
	void sendError(long connectionId, String message);

}
