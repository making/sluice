package am.ik.sluice.server.route;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Spreads a key over candidates that each carry a weight, in proportion to it: smooth
 * weighted round robin, as nginx's. Each pick adds every candidate's weight to its score
 * and takes the highest score, the first of equals, which then gives back the sum of the
 * weights: weights 2 and 1 pick the first, the second, the first, and again, each
 * candidate its weight in every round of their sum, interleaved. The scores are per
 * lookup key and per instance (per node). A change of the candidates keeps the score of
 * each one staying, told by its client and address, a new one starting at none, all then
 * centered on zero.
 */
final class WeightedRoundRobinStrategy implements LoadBalanceStrategy {

	private final ConcurrentMap<String, Scores> scores = new ConcurrentHashMap<>();

	@Override
	public Router.Target pick(String key, List<Router.Target> candidates) {
		if (candidates.size() == 1) {
			return candidates.get(0);
		}
		Scores scores = this.scores.computeIfAbsent(key, k -> new Scores());
		synchronized (scores) {
			return scores.pick(candidates);
		}
	}

	@Override
	public void forget(String key) {
		this.scores.remove(key);
	}

	/**
	 * The scores of one key's candidates, in their order. The candidates are the router's
	 * list, replaced on every change of them: a list it did not score yet is rebased.
	 */
	private static final class Scores {

		private List<Router.Target> candidates = List.of();

		private long[] scores = new long[0];

		Router.Target pick(List<Router.Target> candidates) {
			if (candidates != this.candidates) {
				rebase(candidates);
			}
			long total = 0;
			int best = 0;
			for (int i = 0; i < this.scores.length; i++) {
				long weight = candidates.get(i).weight();
				this.scores[i] += weight;
				total += weight;
				if (this.scores[i] > this.scores[best]) {
					best = i;
				}
			}
			this.scores[best] -= total;
			return candidates.get(best);
		}

		/**
		 * Carries the score of each candidate staying over to the new list, a new one
		 * starting at none, then centers them on zero: their sum, which the picks keep as
		 * it is, would otherwise drift with every candidate leaving, one score running
		 * far ahead of the others and taking a long run of picks.
		 */
		private void rebase(List<Router.Target> candidates) {
			long[] rebased = new long[candidates.size()];
			boolean[] carried = new boolean[this.candidates.size()];
			long sum = 0;
			for (int i = 0; i < rebased.length; i++) {
				Router.Target candidate = candidates.get(i);
				for (int j = 0; j < carried.length; j++) {
					Router.Target before = this.candidates.get(j);
					if (!carried[j] && before.clientId().equals(candidate.clientId())
							&& before.address().equals(candidate.address())) {
						carried[j] = true;
						rebased[i] = this.scores[j];
						sum += rebased[i];
						break;
					}
				}
			}
			long mean = Math.floorDiv(sum, rebased.length);
			for (int i = 0; i < rebased.length; i++) {
				rebased[i] -= mean;
			}
			this.candidates = candidates;
			this.scores = rebased;
		}

	}

}
