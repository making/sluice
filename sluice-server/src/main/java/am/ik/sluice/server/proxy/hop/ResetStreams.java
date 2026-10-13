package am.ik.sluice.server.proxy.hop;

/**
 * The last streams a hop reset while its peer could still send on them, each with what
 * the peer may still send on it: the DATA its stream window held at the reset and one
 * header list of header blocks. A frame within that is one the peer had in flight; one
 * past it, or after a frame that ended the stream, is not. The oldest stream is forgotten
 * once the capacity is reached. Not thread-safe; a hop uses it with its loop's lock.
 */
final class ResetStreams {

	private static final int[] NONE = new int[0];

	/**
	 * The ints of one stream: its id (0 = a free slot), its DATA and header allowance.
	 */
	private static final int SLOT = 3;

	private final int capacity;

	/** The streams, allocated with the first one. */
	private int[] slots = NONE;

	/** The slot the next stream takes. */
	private int next;

	ResetStreams(int capacity) {
		if (capacity < 0) {
			throw new IllegalArgumentException("capacity must not be negative");
		}
		this.capacity = capacity;
	}

	/**
	 * Remembers a stream reset, the peer allowed {@code data} more flow-controlled bytes
	 * of DATA and {@code headers} bytes of header blocks (as HPACK counts them) on it.
	 */
	void remember(int streamId, int data, int headers) {
		if (this.capacity == 0) {
			return;
		}
		if (this.slots == NONE) {
			this.slots = new int[this.capacity * SLOT];
		}
		int slot = this.next * SLOT;
		this.slots[slot] = streamId;
		this.slots[slot + 1] = Math.max(data, 0);
		this.slots[slot + 2] = Math.max(headers, 0);
		this.next = (this.next + 1) % this.capacity;
	}

	/**
	 * Whether a DATA frame of {@code bytes} flow-controlled bytes on the stream is one
	 * the peer may have had in flight, charging it to the stream's allowance -- at least
	 * one byte unless it ends the stream. A frame that ends the stream, or does not fit,
	 * forgets it.
	 */
	boolean admitData(int streamId, int bytes, boolean endStream) {
		int slot = find(streamId);
		return slot >= 0 && charge(slot, slot + 1, endStream ? bytes : Math.max(bytes, 1), endStream);
	}

	/**
	 * Whether a header block of {@code bytes} (as HPACK counts them) on the stream is one
	 * the peer may have had in flight, charging it to the stream's allowance. A block
	 * that ends the stream, or does not fit, forgets it.
	 */
	boolean admitHeaders(int streamId, long bytes, boolean endStream) {
		int slot = find(streamId);
		return slot >= 0 && charge(slot, slot + 2, bytes, endStream);
	}

	private boolean charge(int slot, int allowance, long cost, boolean endStream) {
		if (cost > this.slots[allowance]) {
			this.slots[slot] = 0;
			return false;
		}
		this.slots[allowance] -= (int) cost;
		if (endStream) {
			this.slots[slot] = 0;
		}
		return true;
	}

	private int find(int streamId) {
		for (int slot = 0; slot < this.slots.length; slot += SLOT) {
			if (this.slots[slot] == streamId) {
				return slot;
			}
		}
		return -1;
	}

}
