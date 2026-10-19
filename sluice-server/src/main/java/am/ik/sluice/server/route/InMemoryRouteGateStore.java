package am.ik.sluice.server.route;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

/**
 * In-memory {@link RouteGateStore}: the overrides live as long as the process does.
 */
public class InMemoryRouteGateStore implements RouteGateStore {

	private final Map<String, GateState> overrides = new ConcurrentHashMap<>();

	@Override
	public @Nullable GateState get(String routeTag) {
		return this.overrides.get(routeTag);
	}

	@Override
	public void put(String routeTag, GateState state) {
		this.overrides.put(routeTag, state);
	}

}
