package am.ik.sluice.server.proxy.hop;

import io.netty.handler.codec.http2.Http2CodecUtil;

/**
 * Splits what a hop's channel writes -- an HTTP/2 frame stream, behind the connection
 * preface on a client endpoint -- into DATA payload, header block fragments (the payload
 * of HEADERS and CONTINUATION frames) and the rest, the control bytes: frame headers, the
 * other frames, the preface. Fed the bytes in the order the channel wrote them; with the
 * loop's lock.
 */
final class FrameSplitter {

	private static final byte DATA = 0x0;

	private static final byte HEADERS = 0x1;

	private static final byte CONTINUATION = 0x9;

	private final byte[] header = new byte[Http2CodecUtil.FRAME_HEADER_LENGTH];

	private int headerFilled;

	private int prefaceLeft;

	private int payloadLeft;

	private byte payloadType;

	FrameSplitter(boolean client) {
		this.prefaceLeft = client ? Http2CodecUtil.connectionPrefaceBuf().readableBytes() : 0;
	}

	/** The control bytes and the header block bytes among the next bytes written. */
	Kinds split(byte[] bytes) {
		int control = 0;
		int block = 0;
		int i = 0;
		while (i < bytes.length) {
			int left = bytes.length - i;
			if (this.prefaceLeft > 0) {
				int n = Math.min(this.prefaceLeft, left);
				this.prefaceLeft -= n;
				control += n;
				i += n;
			}
			else if (this.payloadLeft > 0) {
				int n = Math.min(this.payloadLeft, left);
				this.payloadLeft -= n;
				switch (this.payloadType) {
					case DATA -> {
					}
					case HEADERS, CONTINUATION -> block += n;
					default -> control += n;
				}
				i += n;
			}
			else {
				this.header[this.headerFilled++] = bytes[i++];
				control++;
				if (this.headerFilled == this.header.length) {
					this.headerFilled = 0;
					this.payloadLeft = (this.header[0] & 0xff) << 16 | (this.header[1] & 0xff) << 8
							| (this.header[2] & 0xff);
					this.payloadType = this.header[3];
				}
			}
		}
		return new Kinds(control, block);
	}

	/**
	 * The kinds of some bytes written: {@code control} bytes and {@code block} bytes; the
	 * rest is DATA payload.
	 */
	record Kinds(int control, int block) {
	}

}
