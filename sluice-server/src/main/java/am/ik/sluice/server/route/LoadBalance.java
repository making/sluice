package am.ik.sluice.server.route;

/**
 * Load balancing strategy applied when several clients serve the same domain or listen
 * port.
 */
public enum LoadBalance {

	/** The client with the smallest client id wins; deterministic across nodes. */
	SMALLEST_CLIENT_ID {
		@Override
		public LoadBalanceStrategy instance() {
			return new SmallestClientIdStrategy();
		}
	},

	/** Targets rotate per lookup key (per node; counters are not shared cluster-wide). */
	ROUND_ROBIN {
		@Override
		public LoadBalanceStrategy instance() {
			return new RoundRobinStrategy();
		}
	},

	/** A uniformly random target is picked per lookup. */
	RANDOM {
		@Override
		public LoadBalanceStrategy instance() {
			return new RandomStrategy();
		}
	};

	/**
	 * Creates a new strategy instance. Round-robin instances hold mutable state and must
	 * not be shared between independent route spaces.
	 */
	public abstract LoadBalanceStrategy instance();

}
