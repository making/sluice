package am.ik.sluice.server.auth;

import java.time.Duration;
import java.util.List;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

/**
 * One way a route takes credentials ({@link RouteAuthenticator}): the header carrying
 * them, its challenge, and how a presented value is checked. Schemes sharing
 * {@code Authorization} are told apart by their {@link #authScheme()} token, and schemes
 * sharing that token by which credential each {@link #takes}.
 */
interface CredentialScheme {

	/** The scheme's name, as the console shows it. */
	String name();

	/** The lowercase name of the header carrying the credential. */
	String header();

	/**
	 * The auth-scheme token (lowercase) the credential follows in {@link #header()}, as
	 * {@code Authorization: Basic ...}; null when the header's whole value is the
	 * credential.
	 */
	@Nullable String authScheme();

	/**
	 * The {@code WWW-Authenticate} challenge of the scheme.
	 * @param realm the quoted realm parameter, {@code realm="..."}
	 */
	String challenge(String realm);

	/**
	 * The {@code WWW-Authenticate} challenge of the scheme answering a credential it took
	 * that did not match; {@link #challenge} unless the scheme names the error.
	 * @param realm the quoted realm parameter, {@code realm="..."}
	 */
	default String refusal(String realm) {
		return challenge(realm);
	}

	/**
	 * Whether the scheme takes the credential, as {@link #credential} would get it: a
	 * scheme sharing its header and auth-scheme token with another takes only its own
	 * kind.
	 */
	default boolean takes(String credential) {
		return true;
	}

	/**
	 * The credential of a presented value: the header's value past the auth-scheme token
	 * when the scheme has one, its whole value otherwise, stripped.
	 */
	Credential credential(String value);

	/** A credential a request presented, ready to be checked. */
	interface Credential {

		/** A credential that matches nothing: a malformed value. */
		Credential NONE = new Credential() {

			@Override
			public String id() {
				return "";
			}

			@Override
			public boolean bounded() {
				return false;
			}

			@Override
			public Check check() {
				return new Check.Mismatch("malformed");
			}

		};

		/**
		 * What identifies the credential among those that matched (its SHA-256 is
		 * remembered); empty for one that cannot match.
		 */
		String id();

		/**
		 * Whether checking it is deliberately slow and so counts against the bound of the
		 * checks in flight.
		 */
		boolean bounded();

		/**
		 * Checks the credential. An unbounded one is checked on the request's own thread
		 * (the h2 demux's loop among them): it stays cheap and does no I/O.
		 */
		Check check();

	}

	/** What checking a credential found. */
	sealed interface Check {

		/** A match the cache may remember as long as it likes. */
		Check MATCH = new Match(null);

		/**
		 * The credential matched.
		 *
		 * @param ttl how long the match may be remembered at most; null for no bound
		 * besides the cache's
		 */
		record Match(@Nullable Duration ttl) implements Check {
		}

		/**
		 * The credential did not match.
		 *
		 * @param why what failed, for the debug log; never the credential
		 */
		record Mismatch(String why) implements Check {
		}

		/**
		 * The credential could not be checked: what it is checked against is not loaded.
		 *
		 * @param why what is missing, for the debug log
		 */
		record Unavailable(String why) implements Check {
		}

	}

	/**
	 * A credential matched against {@code {id}}-prefixed secrets by the server's password
	 * encoder.
	 *
	 * @param id what identifies it: the token, or {@code user:password}
	 * @param raw what is matched against the secrets: the token, or the password
	 * @param secrets the secrets it may match
	 * @param decoy whether it fails whatever it matches (an unknown Basic user, checked
	 * all the same so it takes the time a known one does)
	 * @param matcher the server's encoder: whether a raw value matches a secret
	 */
	record SecretCredential(String id, String raw, List<String> secrets, boolean decoy,
			BiPredicate<String, String> matcher) implements Credential {

		@Override
		public boolean bounded() {
			return this.secrets.stream().anyMatch(secret -> !PasswordEncoders.cheap(secret));
		}

		@Override
		public Check check() {
			for (String secret : this.secrets) {
				if (this.matcher.test(this.raw, secret)) {
					return this.decoy ? new Check.Mismatch("unknown user") : Check.MATCH;
				}
			}
			return new Check.Mismatch("no secret matched");
		}

	}

}
