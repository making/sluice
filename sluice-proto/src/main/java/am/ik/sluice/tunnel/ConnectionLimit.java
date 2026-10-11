package am.ik.sluice.tunnel;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

/**
 * A cap on the connections open at once. Each admitted connection holds a {@link Permit}
 * that frees its slot when released; releasing is idempotent, so every exit path of a
 * connection may release without counting twice.
 * <p>
 * The buffering of one relayed virtual connection is bounded ({@link #CONNECTION_BYTES}),
 * so a cap on connections bounds the total. When no cap is configured, {@link #resolve}
 * derives one that keeps the worst case within half the maximum heap.
 */
public final class ConnectionLimit {

	/**
	 * The most one relayed virtual connection buffers on one side of the tunnel: its
	 * receive buffer ({@link VirtualConnection#MAX_RECEIVE_BUFFER}: the window plus the
	 * segments' slack and overhead), the cap of its send queue
	 * ({@link SessionSender#MAX_QUEUED_PAYLOAD}) and the buffers of the relay's two
	 * pumps.
	 */
	public static final long CONNECTION_BYTES = VirtualConnection.MAX_RECEIVE_BUFFER + SessionSender.MAX_QUEUED_PAYLOAD
			+ 2L * StreamRelay.BUFFER_SIZE;

	/** The ERROR message a CONNECT beyond the limit is answered with. */
	public static final String LIMIT_REACHED = "connection limit reached";

	private final int max;

	private final AtomicInteger open = new AtomicInteger();

	/**
	 * A limit of {@code max} connections open at once.
	 * @throws IllegalArgumentException when {@code max} is not positive
	 */
	public ConnectionLimit(int max) {
		if (max < 1) {
			throw new IllegalArgumentException("max must be positive: " + max);
		}
		this.max = max;
	}

	/**
	 * The configured limit when positive, else the one derived from this JVM's maximum
	 * heap ({@link #derive}).
	 */
	public static int resolve(int configured) {
		return configured > 0 ? configured : derive(Runtime.getRuntime().maxMemory());
	}

	/**
	 * The connections whose worst-case buffering ({@link #CONNECTION_BYTES} each) fits in
	 * half of {@code maxHeap}, the rest left to everything else; at least 1.
	 */
	public static int derive(long maxHeap) {
		return derive(maxHeap / 2, CONNECTION_BYTES);
	}

	/**
	 * The connections of {@code connectionBytes} worst-case buffering each that fit in
	 * {@code bytes}; at least 1.
	 */
	public static int derive(long bytes, long connectionBytes) {
		return (int) Math.clamp(Math.max(0, bytes) / connectionBytes, 1, Integer.MAX_VALUE);
	}

	/** A slot for one more connection, or null when {@link #max()} are open. */
	public @Nullable Permit tryAcquire() {
		while (true) {
			int current = this.open.get();
			if (current >= this.max) {
				return null;
			}
			if (this.open.compareAndSet(current, current + 1)) {
				return new Permit();
			}
		}
	}

	public int max() {
		return this.max;
	}

	/** The permits currently held. */
	public int open() {
		return this.open.get();
	}

	/** The slot of one admitted connection. */
	public final class Permit {

		private final AtomicBoolean released = new AtomicBoolean();

		private Permit() {
		}

		/** Frees the slot; only the first call counts. */
		public void release() {
			if (this.released.compareAndSet(false, true)) {
				ConnectionLimit.this.open.decrementAndGet();
			}
		}

	}

}
