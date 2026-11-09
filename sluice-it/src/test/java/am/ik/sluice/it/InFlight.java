package am.ik.sluice.it;

import java.time.Duration;
import java.util.stream.Stream;

import org.awaitility.Awaitility;

import am.ik.sluice.server.route.Router;

/**
 * The requests a node's router counts in flight: the picks its relays have not closed, on
 * the routes shared by weights (a route carrying a weight alone is one).
 */
public final class InFlight {

	private InFlight() {
	}

	/** The requests in flight over every http and tcp route. */
	public static long of(Router router) {
		return Stream.concat(router.httpRoutes().stream(), router.tcpRoutes().stream())
			.flatMap(group -> group.inFlight().stream())
			.mapToLong(Long::longValue)
			.sum();
	}

	/** The requests in flight to the host's route. */
	public static long of(Router router, String host) {
		return router.resolve(host).map(group -> group.inFlight().stream().mapToLong(Long::longValue).sum()).orElse(0L);
	}

	/** Waits until the relays closed every pick. */
	public static void awaitNone(Router router) {
		Awaitility.await("every pick closed").atMost(Duration.ofSeconds(10)).until(() -> of(router) == 0);
	}

}
