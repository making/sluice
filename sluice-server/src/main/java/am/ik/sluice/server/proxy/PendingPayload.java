package am.ik.sluice.server.proxy;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;

/**
 * The DATA payload of one h2 stream waiting to be written on the other hop: appended at
 * the back, taken whole. Its array holds what is pending rounded up to {@link #GRANULE},
 * and is handed over or dropped once taken, so a drained stream keeps no buffer and the
 * capacity -- the bytes the relay accounts -- exceeds the payload by less than one
 * granule.
 */
final class PendingPayload {

	/** The step the array is sized in. */
	static final int GRANULE = 1024;

	private static final byte[] EMPTY = new byte[0];

	private byte[] bytes = EMPTY;

	private int size;

	/** The payload bytes pending. */
	int size() {
		return this.size;
	}

	/** The bytes the buffer holds. */
	int capacity() {
		return this.bytes.length;
	}

	/** Appends the readable bytes of {@code source}, leaving its indexes as they are. */
	void append(ByteBuf source) {
		int length = source.readableBytes();
		if (this.size + length > this.bytes.length) {
			this.bytes = Arrays.copyOf(this.bytes, roundUp(this.size + length));
		}
		source.getBytes(source.readerIndex(), this.bytes, this.size, length);
		this.size += length;
	}

	/**
	 * Takes the whole buffer, uncopied: the payload is the first {@link #size()} bytes of
	 * the array returned, read before the call; leaves this empty.
	 */
	byte[] takeAll() {
		byte[] taken = this.bytes;
		this.bytes = EMPTY;
		this.size = 0;
		return taken;
	}

	void clear() {
		this.bytes = EMPTY;
		this.size = 0;
	}

	private static int roundUp(int length) {
		return (length + GRANULE - 1) / GRANULE * GRANULE;
	}

}
