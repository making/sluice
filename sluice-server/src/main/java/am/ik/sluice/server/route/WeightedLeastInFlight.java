package am.ik.sluice.server.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

/**
 * Spreads a key over candidates that each carry a weight: a request goes to the candidate
 * with the fewest requests in flight for its weight, the candidates tied for it taking
 * turns by smooth weighted round robin -- nginx's {@code least_conn} with weights. A
 * candidate whose requests take longer, or wait in it for room, holds more of them and
 * passes its turn while another has room, until they end. With none in flight, as when
 * each request ends before the next, the rotation alone decides: each pick adds every
 * tied candidate's weight to its score and takes the highest score, the first of equals,
 * which then gives back the sum of their weights -- weights 2 and 1 pick the first, the
 * second, the first, and again.
 * <p>
 * A request counts from its pick until its {@link Router.Pick} is closed. The counts and
 * scores are per lookup key and per instance (per node). A change of the candidates keeps
 * the count and score of each one staying, told by its client and address, a new one
 * starting at none, the scores then centered on zero; one leaving with requests in flight
 * keeps their count until they end, for when it comes back.
 */
final class WeightedLeastInFlight {

	private final ConcurrentMap<String, Shares> shares = new ConcurrentHashMap<>();

	/**
	 * Picks one of the candidates and counts the request in flight to it.
	 * @param key lookup key, the counts and scores are kept per key
	 * @param candidates the key's targets, each carrying a weight; never empty
	 */
	Picked pick(String key, List<Router.Target> candidates) {
		Shares shares = this.shares.computeIfAbsent(key, k -> new Shares());
		synchronized (shares) {
			return shares.pick(candidates);
		}
	}

	/**
	 * The requests in flight to each candidate, in their order; none before the key's
	 * first pick.
	 */
	List<Long> inFlight(String key, List<Router.Target> candidates) {
		Shares shares = this.shares.get(key);
		if (shares == null) {
			return Collections.nCopies(candidates.size(), 0L);
		}
		synchronized (shares) {
			return shares.inFlight(candidates);
		}
	}

	/**
	 * The key has no candidate left: its counts and scores go. A pick still open counts
	 * down nothing anyone reads.
	 */
	void forget(String key) {
		this.shares.remove(key);
	}

	/** The candidate a pick chose, and the count of the requests in flight to it. */
	record Picked(Router.Target target, AtomicLong inFlight) {
	}

	/**
	 * One candidate's place: the requests in flight to it, and its score in the rotation.
	 */
	private static final class Slot {

		final String clientId;

		final String address;

		final AtomicLong inFlight = new AtomicLong();

		long score;

		Slot(Router.Target target) {
			this.clientId = target.clientId();
			this.address = target.address();
		}

		boolean holds(Router.Target target) {
			return this.clientId.equals(target.clientId()) && this.address.equals(target.address());
		}

	}

	/**
	 * The slots of one key's candidates, in their order. The candidates are the router's
	 * list, replaced on every change of them: a list it did not see yet is rebased.
	 */
	private static final class Shares {

		private List<Router.Target> candidates = List.of();

		private Slot[] slots = new Slot[0];

		/** The slots of candidates gone with requests still in flight. */
		private final List<Slot> departed = new ArrayList<>();

		Picked pick(List<Router.Target> candidates) {
			if (candidates != this.candidates) {
				rebase(candidates);
			}
			// one reading of the counts: requests ending meanwhile change no comparison
			long[] inFlight = new long[this.slots.length];
			int least = 0;
			for (int i = 0; i < inFlight.length; i++) {
				inFlight[i] = this.slots[i].inFlight.get();
				if (fewer(inFlight[i], weightOf(i), inFlight[least], weightOf(least))) {
					least = i;
				}
			}
			long total = 0;
			int best = -1;
			for (int i = least; i < inFlight.length; i++) {
				if (fewer(inFlight[least], weightOf(least), inFlight[i], weightOf(i))) {
					continue; // more in flight for its weight: not its turn
				}
				this.slots[i].score += weightOf(i);
				total += weightOf(i);
				if (best < 0 || this.slots[i].score > this.slots[best].score) {
					best = i;
				}
			}
			this.slots[best].score -= total;
			this.slots[best].inFlight.incrementAndGet();
			return new Picked(candidates.get(best), this.slots[best].inFlight);
		}

		List<Long> inFlight(List<Router.Target> candidates) {
			List<Long> counts = new ArrayList<>(candidates.size());
			List<Slot> unclaimed = new ArrayList<>(List.of(this.slots));
			unclaimed.addAll(this.departed);
			for (Router.Target candidate : candidates) {
				Slot slot = claim(unclaimed, candidate);
				counts.add(slot == null ? 0 : slot.inFlight.get());
			}
			return counts;
		}

		private long weightOf(int index) {
			return this.candidates.get(index).weight();
		}

		/**
		 * Whether {@code a} requests in flight for weight {@code wa} are fewer than
		 * {@code b} for {@code wb}: {@code a / wa < b / wb}, multiplied out.
		 */
		private static boolean fewer(long a, long wa, long b, long wb) {
			return a * wb < b * wa;
		}

		/**
		 * Carries the slot of each candidate staying, or coming back with requests in
		 * flight, over to the new list, a new one starting with none, then centers the
		 * scores on zero: their sum, which the picks keep as it is, would otherwise drift
		 * with every candidate leaving, one score running far ahead of the others and
		 * taking a long run of picks. A slot left behind with requests in flight is kept
		 * until they end, its score dropped.
		 */
		private void rebase(List<Router.Target> candidates) {
			Slot[] rebased = new Slot[candidates.size()];
			List<Slot> staying = new ArrayList<>(List.of(this.slots));
			long sum = 0;
			for (int i = 0; i < rebased.length; i++) {
				Router.Target candidate = candidates.get(i);
				Slot slot = claim(staying, candidate);
				if (slot == null) {
					slot = claim(this.departed, candidate);
				}
				rebased[i] = slot != null ? slot : new Slot(candidate);
				sum += rebased[i].score;
			}
			for (Slot left : staying) {
				left.score = 0;
				this.departed.add(left);
			}
			this.departed.removeIf(slot -> slot.inFlight.get() == 0);
			long mean = Math.floorDiv(sum, rebased.length);
			for (Slot slot : rebased) {
				slot.score -= mean;
			}
			this.candidates = candidates;
			this.slots = rebased;
		}

		/**
		 * Takes the first of the slots holding the candidate out of them; null if none.
		 */
		private static @Nullable Slot claim(List<Slot> slots, Router.Target candidate) {
			for (int i = 0; i < slots.size(); i++) {
				if (slots.get(i).holds(candidate)) {
					return slots.remove(i);
				}
			}
			return null;
		}

	}

}
