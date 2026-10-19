package am.ik.sluice.server.route;

import org.jspecify.annotations.Nullable;

/**
 * Persistence of the route gate overrides, keyed by route tag. An abstraction so a later
 * store (Redis, ...) can keep the overrides across restarts; the default keeps them in
 * memory.
 */
public interface RouteGateStore {

	/**
	 * The override stored for the route tag; {@code null} when the route follows its
	 * default.
	 */
	@Nullable GateState get(String routeTag);

	/** Stores the override for the route tag, replacing any previous one. */
	void put(String routeTag, GateState state);

}
