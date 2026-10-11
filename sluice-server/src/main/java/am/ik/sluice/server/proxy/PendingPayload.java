package am.ik.sluice.server.proxy;

import java.util.Arrays;

/**
 * The DATA payload of one h2 stream queued for its leg writer: appended at the back,
 * taken from the front. Its array holds what is pending rounded up to {@link #GRANULE},
 * and is dropped once empty, so a drained stream keeps no buffer and the capacity -- the
 * bytes the relay accounts -- exceeds the payload by less than one granule.
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

	void append(byte[] source, int offset, int length) {
		if (this.size + length > this.bytes.length) {
			this.bytes = Arrays.copyOf(this.bytes, roundUp(this.size + length));
		}
		System.arraycopy(source, offset, this.bytes, this.size, length);
		this.size += length;
	}

	/** Takes the first {@code length} bytes; the rest moves to an array sized to it. */
	byte[] take(int length) {
		if (length == 0) {
			return EMPTY;
		}
		byte[] taken = Arrays.copyOf(this.bytes, length);
		int rest = this.size - length;
		this.bytes = rest == 0 ? EMPTY : Arrays.copyOfRange(this.bytes, length, length + roundUp(rest));
		this.size = rest;
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
