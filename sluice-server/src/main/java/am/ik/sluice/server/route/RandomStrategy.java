package am.ik.sluice.server.route;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks a uniformly random candidate per lookup.
 */
final class RandomStrategy implements LoadBalanceStrategy {

	@Override
	public Router.Target pick(String key, List<Router.Target> candidates) {
		return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
	}

}
