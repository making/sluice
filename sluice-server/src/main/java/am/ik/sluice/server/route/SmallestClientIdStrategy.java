package am.ik.sluice.server.route;

import java.util.List;

/**
 * Deterministic winner: the smallest client id owns the route, regardless of registration
 * order -- every node of a cluster picks the same target.
 */
final class SmallestClientIdStrategy implements LoadBalanceStrategy {

	@Override
	public Router.Target pick(String key, List<Router.Target> candidates) {
		Router.Target best = candidates.get(0);
		for (Router.Target candidate : candidates) {
			if (candidate.clientId().compareTo(best.clientId()) < 0) {
				best = candidate;
			}
		}
		return best;
	}

}
