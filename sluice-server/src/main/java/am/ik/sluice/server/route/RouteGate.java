package am.ik.sluice.server.route;

import java.util.Map;
import java.util.Objects;

/**
 * The gate state of the routes: the configured defaults, plus the overrides the console
 * set (in memory, until restart). Every route defaults to open.
 * <p>
 * The tags are the route keys as shown in the console: the domain, the host pattern,
 * {@code tcp:<port>} for tcp routes and {@code *} for the catch-all (an empty key means
 * the same).
 */
public class RouteGate {

	private final Map<String, GateState> defaults;

	private final RouteGateStore overrides;

	/**
	 * @param defaults gate state per route tag, from {@code sluice.route.gate.<tag>}
	 * @param overrides where the console's toggles are kept
	 */
	public RouteGate(Map<String, GateState> defaults, RouteGateStore overrides) {
		this.defaults = Map.copyOf(Objects.requireNonNull(defaults, "defaults is required"));
		this.overrides = Objects.requireNonNull(overrides, "overrides is required");
	}

	/** A gate that is open for every route. */
	public static RouteGate alwaysOpen() {
		return new RouteGate(Map.of(), new InMemoryRouteGateStore());
	}

	/**
	 * Whether the route serves traffic: its override when the console touched it, its
	 * configured default otherwise, open when neither.
	 */
	public boolean isOpen(String routeTag) {
		return this.stateOf(routeTag) == GateState.OPEN;
	}

	/** The effective gate state of the route (see {@link #isOpen}). */
	public GateState stateOf(String routeTag) {
		String key = normalize(routeTag);
		GateState override = this.overrides.get(key);
		if (override != null) {
			return override;
		}
		return this.defaults.getOrDefault(key, GateState.OPEN);
	}

	/** The state the route starts from, before any override. */
	public GateState defaultOf(String routeTag) {
		return this.defaults.getOrDefault(normalize(routeTag), GateState.OPEN);
	}

	/** Records the console's override for the route. */
	public void set(String routeTag, GateState state) {
		this.overrides.put(normalize(routeTag), Objects.requireNonNull(state, "state is required"));
	}

	private static String normalize(String routeTag) {
		if (routeTag == null || routeTag.isBlank()) {
			return "*";
		}
		return routeTag.strip();
	}

}
