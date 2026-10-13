package am.ik.sluice.server.proxy;

import org.jspecify.annotations.Nullable;

/**
 * The slots and the bytes the data plane's connections reserve at once: a reservation
 * takes one slot and the bytes of its worst case, and may grow by more bytes once the
 * connection turns out to need them ({@link Reservation#tryGrow}); a reservation that
 * does not fit is not taken. Releasing gives both back; only the first release counts.
 */
final class ConnectionReserve {

	/** No bound on the bytes. */
	static final long UNBOUNDED = Long.MAX_VALUE;

	private final int maxSlots;

	private final long maxBytes;

	/** Guarded by this. */
	private int slots;

	/** Guarded by this. */
	private long bytes;

	/**
	 * At most {@code maxSlots} reservations of at most {@code maxBytes} together
	 * ({@link #UNBOUNDED} = none).
	 */
	ConnectionReserve(int maxSlots, long maxBytes) {
		if (maxSlots < 1) {
			throw new IllegalArgumentException("maxSlots must be positive: " + maxSlots);
		}
		if (maxBytes < 0) {
			throw new IllegalArgumentException("maxBytes must not be negative: " + maxBytes);
		}
		this.maxSlots = maxSlots;
		this.maxBytes = maxBytes;
	}

	/** A slot and {@code bytes}, or null when either does not fit. */
	synchronized @Nullable Reservation tryReserve(long bytes) {
		if (this.slots >= this.maxSlots || !fitsLocked(bytes)) {
			return null;
		}
		this.slots++;
		this.bytes += bytes;
		return new Reservation(bytes);
	}

	private boolean fitsLocked(long more) {
		return more <= this.maxBytes - this.bytes;
	}

	int maxSlots() {
		return this.maxSlots;
	}

	/** The bytes the reservations may take together; {@link #UNBOUNDED} = no bound. */
	long maxBytes() {
		return this.maxBytes;
	}

	/** The slots taken. */
	synchronized int slots() {
		return this.slots;
	}

	/** The bytes taken. */
	synchronized long bytes() {
		return this.bytes;
	}

	/** One connection's slot and bytes. */
	final class Reservation {

		/** Guarded by the reserve. */
		private long held;

		/** Guarded by the reserve. */
		private boolean released;

		private Reservation(long held) {
			this.held = held;
		}

		/**
		 * Takes {@code more} bytes besides those held, if they fit; false leaves the
		 * reservation as it was, and so does one released already.
		 */
		boolean tryGrow(long more) {
			synchronized (ConnectionReserve.this) {
				if (this.released || !fitsLocked(more)) {
					return false;
				}
				this.held += more;
				ConnectionReserve.this.bytes += more;
				return true;
			}
		}

		/** Gives the slot and the bytes back; only the first call counts. */
		void release() {
			synchronized (ConnectionReserve.this) {
				if (this.released) {
					return;
				}
				this.released = true;
				ConnectionReserve.this.slots--;
				ConnectionReserve.this.bytes -= this.held;
			}
		}

	}

}
