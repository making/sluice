package am.ik.sluice.server.proxy.hop;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** HTTP/2 frames as bytes, for a raw socket peer. */
final class RawFrames {

	private RawFrames() {
	}

	/** The client connection preface and an empty SETTINGS frame. */
	static byte[] clientPreface() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.writeBytes("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
		out.writeBytes(frame(0x4, 0, 0, new byte[0]));
		return out.toByteArray();
	}

	/** SETTINGS with SETTINGS_INITIAL_WINDOW_SIZE. */
	static byte[] initialWindowSize(int size) {
		return frame(0x4, 0, 0, ByteBuffer.allocate(6).putShort((short) 0x4).putInt(size).array());
	}

	static byte[] settingsAck() {
		return frame(0x4, 0x1, 0, new byte[0]);
	}

	static byte[] windowUpdate(int streamId, int increment) {
		return frame(0x8, 0, streamId, ByteBuffer.allocate(4).putInt(increment).array());
	}

	static byte[] ping(long data) {
		return frame(0x6, 0, 0, ByteBuffer.allocate(8).putLong(data).array());
	}

	static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		ByteBuffer frame = ByteBuffer.allocate(9 + payload.length);
		frame.put((byte) (payload.length >>> 16)).put((byte) (payload.length >>> 8)).put((byte) payload.length);
		frame.put((byte) type).put((byte) flags).putInt(streamId).put(payload);
		return frame.array();
	}

}
