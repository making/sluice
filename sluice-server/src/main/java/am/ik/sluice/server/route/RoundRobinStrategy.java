package am.ik.sluice.server.route;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rotates through the candidates per lookup key. The counters are per instance (per
 * node); no state is shared cluster-wide.
 */
final class RoundRobinStrategy implements LoadBalanceStrategy {

	private final ConcurrentMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();

	@Override
	public Router.Target pick(String key, List<Router.Target> candidates) {
		AtomicInteger counter = this.counters.computeIfAbsent(key, k -> new AtomicInteger());
		int index = Math.floorMod(counter.getAndIncrement(), candidates.size());
		return candidates.get(index);
	}

}
