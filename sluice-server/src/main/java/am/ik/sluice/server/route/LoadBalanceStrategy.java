package am.ik.sluice.server.route;

import java.util.List;

/**
 * Strategy resolving which registered target serves a lookup when several clients
 * announce the same domain or listen port.
 */
public interface LoadBalanceStrategy {

	/**
	 * Picks one of the given targets.
	 * @param key lookup key (the matched domain, or the listen port for tcp routes);
	 * stateful implementations such as round-robin keep an independent counter per key
	 * @param candidates registered targets, never empty
	 * @return the selected target
	 */
	Router.Target pick(String key, List<Router.Target> candidates);

}
