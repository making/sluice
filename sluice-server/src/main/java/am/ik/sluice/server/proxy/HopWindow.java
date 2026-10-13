package am.ik.sluice.server.proxy;

import java.util.function.LongPredicate;

import am.ik.sluice.server.proxy.hop.Hop;

/**
 * The connection window an h2 demux relay grants one hop's peer: the credit the peer
 * holds, what it may still send, starts at RFC 9113's initial window ({@link #FLOOR}) and
 * grows with what the peer sends per round trip, up to a maximum, its part past the floor
 * charged before the grant raising it leaves.
 * <p>
 * The window aims at a target, starting at the floor; once the peer spent its credit to
 * half the target, the credit is refilled to the target. What the peer sends per round
 * trip of its hop is measured as the gRPC transport does: a PING is sent with the DATA it
 * sends ({@link #wantsPing}) and the DATA read until its ack is the sample
 * ({@link #acked}). A sample of half the target or more is a peer the window may bound --
 * it sends at most the target per round trip --: the target doubles (to a power of two)
 * at the next refill, up to the maximum. A peer sending little per round trip -- a small
 * request body, a slow stream -- keeps the floor. The part of a credit past the floor is
 * charged ({@link #charged}) before the grant leaves: a refill the charge does not take
 * is tried at the current target, then at the floor, which costs nothing -- so a peer
 * whose larger window does not fit keeps sending within RFC 9113's window instead of
 * stopping. A credit granted cannot be taken back (HTTP/2 has no negative WINDOW_UPDATE):
 * it stays charged until the peer sent it or its hop closed. Not thread-safe: with the
 * relay's loop's lock.
 */
final class HopWindow {

	/**
	 * The credit charged to no budget: RFC 9113's initial connection window, which every
	 * peer holds before any grant, so what the connection reserves covers it.
	 */
	static final int FLOOR = Hop.DEFAULT_CONNECTION_WINDOW;

	private final int max;

	private int target = FLOOR;

	/** The DATA bytes read from the peer. */
	private long received;

	/** {@link #received} when the PING under way was sent; -1 = none under way. */
	private long receivedAtPing = -1;

	/** A sample asked for a larger target at the next refill. */
	private boolean growthDue;

	HopWindow(int max) {
		if (max < FLOOR) {
			throw new IllegalArgumentException("max must be at least " + FLOOR + ": " + max);
		}
		this.max = max;
	}

	/** The part of a peer's credit charged to the budget: what is past the floor. */
	static long charged(long credit) {
		return Math.max(0, credit - FLOOR);
	}

	/** The credit the window aims at. */
	int target() {
		return this.target;
	}

	/**
	 * Counts DATA read from the peer, padding included; whether a PING should go to the
	 * peer now to measure its round trip: none is under way and the target may still
	 * grow. The caller sends it and calls {@link #pinged}.
	 */
	boolean wantsPing(int bytes) {
		this.received += bytes;
		return this.receivedAtPing < 0 && this.target < this.max && !this.growthDue;
	}

	/** The PING measuring the peer's round trip went out. */
	void pinged() {
		this.receivedAtPing = this.received;
	}

	/**
	 * The peer acknowledged the PING: what it sent since is what it sends per round trip;
	 * half the target or more makes the target grow at the next refill.
	 */
	void acked() {
		if (this.receivedAtPing < 0) {
			return;
		}
		long sample = this.received - this.receivedAtPing;
		this.receivedAtPing = -1;
		if (sample >= this.target / 2 && this.target < this.max) {
			this.growthDue = true;
		}
	}

	/**
	 * What to grant a peer holding {@code credit}, with {@code owed} held back to grant
	 * from: once the peer spent its credit to half the target, a refill to the target --
	 * doubled first when a sample asked for it. A grant taking the credit's charged part
	 * up is made only if {@code charge} takes the increase; failing that the refill is
	 * tried at the current target, then at the floor, and the target falls to the floor.
	 */
	Grant grant(int credit, int owed, LongPredicate charge) {
		if (owed <= 0) {
			return Grant.NONE;
		}
		int current = this.target;
		int wanted = this.growthDue ? doubled(current) : current;
		int refill = refill(wanted, credit, owed);
		if (refill == 0) {
			return Grant.NONE;
		}
		this.growthDue = false;
		if (fits(credit, refill, charge)) {
			this.target = wanted;
			return new Grant(refill, false);
		}
		if (wanted != current) {
			refill = refill(current, credit, owed);
			if (refill > 0 && fits(credit, refill, charge)) {
				return new Grant(refill, true);
			}
		}
		this.target = FLOOR;
		return new Grant(refill(FLOOR, credit, owed), true);
	}

	/**
	 * The window doubled to a power of two -- the floor's 65535 to 128KiB --, at most the
	 * maximum.
	 */
	private int doubled(int window) {
		return (int) Math.min(2L * Integer.highestOneBit(window + 1), this.max);
	}

	/** The refill to {@code target}, once the credit fell to half of it; at most owed. */
	private static int refill(int target, int credit, int owed) {
		if (credit > target / 2) {
			return 0;
		}
		return Math.min(owed, target - credit);
	}

	private static boolean fits(int credit, int grant, LongPredicate charge) {
		long cost = charged((long) credit + grant) - charged(credit);
		return cost == 0 || charge.test(cost);
	}

	/**
	 * A grant: its bytes, and whether a larger window was declined for want of the
	 * charge.
	 */
	record Grant(int bytes, boolean declined) {

		static final Grant NONE = new Grant(0, false);

	}

}
