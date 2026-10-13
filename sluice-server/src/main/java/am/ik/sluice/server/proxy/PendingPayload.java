package am.ik.sluice.server.proxy;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * The DATA payload of one h2 stream waiting to be written on the other hop: appended at
 * the back, taken whole. It is held in arrays filled in order, every one full but the
 * last: an append fills the last array's room, then adds one array of the rest rounded up
 * to {@link #GRANULE}. So each byte is copied once however many frames the backlog holds,
 * and the capacity -- the bytes of the arrays, which the relay accounts -- exceeds the
 * payload by less than one granule. Taking hands the arrays over uncopied and leaves none
 * behind, so a drained stream keeps no buffer.
 */
final class PendingPayload {

	/** The step the arrays are sized in. */
	static final int GRANULE = 1024;

	/** The arrays in order, every one full but the last; a new list once taken. */
	private List<byte[]> arrays = new ArrayList<>();

	/** The payload bytes in the last array. */
	private int lastUsed;

	private int size;

	private int capacity;

	/** The payload bytes pending. */
	int size() {
		return this.size;
	}

	/** The bytes the arrays hold. */
	int capacity() {
		return this.capacity;
	}

	/** Appends the readable bytes of {@code source}, leaving its indexes as they are. */
	void append(ByteBuf source) {
		int offset = source.readerIndex();
		int length = source.readableBytes();
		if (!this.arrays.isEmpty()) {
			byte[] last = this.arrays.getLast();
			int room = Math.min(length, last.length - this.lastUsed);
			source.getBytes(offset, last, this.lastUsed, room);
			this.lastUsed += room;
			offset += room;
			length -= room;
			this.size += room;
		}
		if (length > 0) {
			byte[] array = new byte[roundUp(length)];
			source.getBytes(offset, array, 0, length);
			this.arrays.add(array);
			this.lastUsed = length;
			this.size += length;
			this.capacity += array.length;
		}
	}

	/**
	 * Takes the whole payload, uncopied: the returned buffer's readable bytes, wrapping
	 * the arrays; leaves this empty.
	 */
	ByteBuf takeAll() {
		ByteBuf taken;
		int count = this.arrays.size();
		if (count == 0) {
			taken = Unpooled.EMPTY_BUFFER;
		}
		else if (count == 1) {
			taken = Unpooled.wrappedBuffer(this.arrays.getFirst(), 0, this.lastUsed);
		}
		else {
			ByteBuf[] parts = new ByteBuf[count];
			for (int i = 0; i < count - 1; i++) {
				parts[i] = Unpooled.wrappedBuffer(this.arrays.get(i));
			}
			parts[count - 1] = Unpooled.wrappedBuffer(this.arrays.getLast(), 0, this.lastUsed);
			taken = Unpooled.wrappedBuffer(count, parts);
		}
		clear();
		return taken;
	}

	void clear() {
		this.arrays = new ArrayList<>();
		this.lastUsed = 0;
		this.size = 0;
		this.capacity = 0;
	}

	private static int roundUp(int length) {
		return (length + GRANULE - 1) / GRANULE * GRANULE;
	}

}
