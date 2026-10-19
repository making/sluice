package am.ik.sluice.server.auth;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * What {@link RouteAuthenticator} made of a request: it passes -- the headers to remove
 * before it is relayed with it --, it is unauthorized, or its credential could not be
 * checked for the verifications in flight.
 */
public sealed interface Verdict {

	/** A request of a route without credentials: passes, nothing removed. */
	Verdict OPEN = new Pass(Set.of());

	/**
	 * The request passes.
	 *
	 * @param strip the lowercase names of the headers removed before it is relayed
	 */
	record Pass(Set<String> strip) implements Verdict {

		public Pass {
			strip = Set.copyOf(strip);
		}

	}

	/**
	 * The request is refused {@code 401}.
	 *
	 * @param reason {@code missing} (no credential presented) or {@code invalid}
	 * @param challenges the {@code WWW-Authenticate} values the answer carries
	 */
	record Unauthorized(String reason, List<String> challenges) implements Verdict {

		public Unauthorized {
			challenges = List.copyOf(challenges);
		}

	}

	/**
	 * The request is refused {@code 503}: checking its credential would take a
	 * verification past the bound of those in flight.
	 *
	 * @param retryAfter the {@code Retry-After} of the answer
	 */
	record Busy(Duration retryAfter) implements Verdict {
	}

}
