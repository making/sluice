package am.ik.sluice.server.auth;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What {@link RouteAuthenticator} made of a request: it passes -- the headers to remove
 * before it is relayed with it, and those to add --, it is unauthorized, forbidden,
 * answered with a redirect of the browser sign-in, or its credential could not be
 * checked: for the verifications in flight, or for its provider not loaded.
 */
public sealed interface Verdict {

	/** A request of a route without credentials: passes, nothing removed. */
	Verdict OPEN = new Pass(Set.of());

	/**
	 * The request passes.
	 *
	 * @param strip the lowercase names of the headers removed before it is relayed
	 * @param add the fields added after the removal, lowercase names to their values, in
	 * order
	 */
	record Pass(Set<String> strip, Map<String, String> add) implements Verdict {

		public Pass {
			strip = Set.copyOf(strip);
			add = Collections.unmodifiableMap(new LinkedHashMap<>(add));
		}

		/** Passes, the headers removed and none added. */
		public Pass(Set<String> strip) {
			this(strip, Map.of());
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
	 * The request is refused {@code 403} by the browser sign-in.
	 *
	 * @param reason {@code denied} (a signed-in user the route does not admit),
	 * {@code insecure} (a sign-in over plaintext) or {@code failed} (a callback whose
	 * sign-in failed)
	 */
	record Forbidden(String reason) implements Verdict {

		/** A signed-in user the route's claims do not admit. */
		public static final String DENIED = "denied";

		/** A sign-in the client began over plaintext. */
		public static final String INSECURE = "insecure";

		/** A callback whose sign-in failed. */
		public static final String FAILED = "failed";

	}

	/**
	 * The request is answered {@code 302 Found} by the browser sign-in, never relayed.
	 *
	 * @param location the {@code Location}
	 * @param cookies the {@code Set-Cookie} values the answer carries
	 * @param reason {@code sign-in} (to the provider), {@code signed-in} (the callback,
	 * back to the page the sign-in began at) or {@code signed-out}
	 */
	record Redirect(String location, List<String> cookies, String reason) implements Verdict {

		/** To the provider's authorization endpoint. */
		public static final String SIGN_IN = "sign-in";

		/** From the callback back to the page the sign-in began at. */
		public static final String SIGNED_IN = "signed-in";

		/** From the sign-out path, the session cookie removed. */
		public static final String SIGNED_OUT = "signed-out";

		public Redirect {
			cookies = List.copyOf(cookies);
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

	/**
	 * The request is refused {@code 503}: its credential is a token of an OIDC provider,
	 * or it is a browser sign-in, whose keys or endpoints have not loaded.
	 *
	 * @param retryAfter the {@code Retry-After} of the answer
	 */
	record Unavailable(Duration retryAfter) implements Verdict {
	}

}
