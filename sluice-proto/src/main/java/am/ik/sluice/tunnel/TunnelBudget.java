package am.ik.sluice.tunnel;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The bytes the virtual connections of one process may buffer beyond each connection's
 * allowance, shared by all of them: the receive windows granted past the initial window
 * ({@link VirtualConnection}), and the DATA a connection's send queue holds past
 * {@link SessionSender#SEND_ALLOWANCE}. What a remote may still send is memory as much as
 * what it sent, so a window is charged from the grant that raises it until the bytes are
 * read or the remote gave the credit back, idle connections included.
 * <p>
 * Each connection and each send queue charges through an {@link Account}. An account
 * grows only within its share, the budget over the accounts holding a charge plus one --
 * the one an account newly growing would add -- so the accounts holding a charge leave
 * room for one more, and one connection cannot hold what the others share. An account
 * over its share is not cut: its owner shrinks it as the bytes drain
 * ({@link VirtualConnection} grants less back than it reads).
 */
public final class TunnelBudget {

	private final long max;

	private final AtomicLong used = new AtomicLong();

	/** The accounts holding a charge. */
	private final AtomicInteger holders = new AtomicInteger();

	/** The payload the connections received and not yet read; a gauge. */
	private final AtomicLong buffered = new AtomicLong();

	/**
	 * A budget of {@code max} bytes.
	 * @throws IllegalArgumentException when {@code max} is negative
	 */
	public TunnelBudget(long max) {
		if (max < 0) {
			throw new IllegalArgumentException("max must not be negative: " + max);
		}
		this.max = max;
	}

	/** A budget without a bound, for owners that bound the connections otherwise. */
	public static TunnelBudget unlimited() {
		return new TunnelBudget(Long.MAX_VALUE);
	}

	/** The budget an unset value derives: an eighth of half {@code maxHeap}. */
	public static long derive(long maxHeap) {
		return Math.max(0, maxHeap) / 2 / 8;
	}

	/** The configured budget when positive, else the one derived from {@code maxHeap}. */
	public static long resolve(long configured, long maxHeap) {
		return configured > 0 ? configured : derive(maxHeap);
	}

	/** The budget, in bytes. */
	public long max() {
		return this.max;
	}

	/** The bytes charged; never above {@link #max()}. */
	public long used() {
		return this.used.get();
	}

	/** The accounts holding a charge. */
	public int holders() {
		return this.holders.get();
	}

	/** The payload received over the tunnel and not yet read, every connection's. */
	public long buffered() {
		return this.buffered.get();
	}

	void addBuffered(long delta) {
		this.buffered.addAndGet(delta);
	}

	/** A new account, holding nothing. */
	public Account account() {
		return new Account();
	}

	/**
	 * Takes up to {@code bytes}, as much as is left; the bytes taken.
	 */
	private long take(long bytes) {
		while (true) {
			long current = this.used.get();
			long taken = Math.min(bytes, this.max - current);
			if (taken <= 0) {
				return 0;
			}
			if (this.used.compareAndSet(current, current + taken)) {
				return taken;
			}
		}
	}

	/** What one connection or send queue charges; thread-safe, closed once. */
	public final class Account {

		private long charged;

		private boolean closed;

		private Account() {
		}

		/** The bytes this account holds. */
		public synchronized long charged() {
			return this.charged;
		}

		/**
		 * The most this account may grow to: the budget over the accounts holding a
		 * charge but this one, plus two (this one and one more).
		 */
		public long share() {
			int others;
			synchronized (this) {
				others = TunnelBudget.this.holders.get() - (this.charged > 0 ? 1 : 0);
			}
			return TunnelBudget.this.max / (Math.max(0, others) + 2);
		}

		/**
		 * Moves the charge towards {@code wanted}: down to it at once, up by as much as
		 * the budget has left and the {@link #share()} allows, never past {@code wanted}.
		 * The charge held after; 0 once closed.
		 */
		public synchronized long resize(long wanted) {
			if (this.closed) {
				return 0;
			}
			long target = Math.max(0, wanted);
			if (target < this.charged) {
				set(target);
			}
			else if (target > this.charged) {
				long allowed = Math.min(target, Math.max(this.charged, share()));
				set(this.charged + take(allowed - this.charged));
			}
			return this.charged;
		}

		/**
		 * Grows the charge to {@code wanted} if all of it fits the budget and the
		 * {@link #share()}; true when the charge is at least {@code wanted} after, at
		 * once when it was already. False once closed.
		 */
		public synchronized boolean tryGrow(long wanted) {
			if (this.closed) {
				return false;
			}
			if (wanted <= this.charged) {
				return true;
			}
			if (wanted > share()) {
				return false;
			}
			long missing = wanted - this.charged;
			long taken = take(missing);
			if (taken < missing) {
				TunnelBudget.this.used.addAndGet(-taken);
				return false;
			}
			set(wanted);
			return true;
		}

		/** Releases the charge for good; later charges take nothing. Idempotent. */
		public synchronized void close() {
			if (this.closed) {
				return;
			}
			set(0);
			this.closed = true;
		}

		/**
		 * Records the new charge, releasing what it drops (the growth was taken already);
		 * counts the account as a holder while it holds a charge.
		 */
		private void set(long charge) {
			long previous = this.charged;
			if (charge < previous) {
				TunnelBudget.this.used.addAndGet(charge - previous);
			}
			this.charged = charge;
			if (previous == 0 && charge > 0) {
				TunnelBudget.this.holders.incrementAndGet();
			}
			else if (previous > 0 && charge == 0) {
				TunnelBudget.this.holders.decrementAndGet();
			}
		}

	}

}
