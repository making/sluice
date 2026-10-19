package am.ik.sluice.server.auth;

import java.util.List;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

/**
 * One way a route takes credentials ({@link RouteAuthenticator}): the header carrying
 * them, its challenge, and how a presented value is checked. Schemes sharing
 * {@code Authorization} are told apart by their {@link #authScheme()} token.
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
			public boolean matches() {
				return false;
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

		/** Checks the credential. */
		boolean matches();

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
		public boolean matches() {
			for (String secret : this.secrets) {
				if (this.matcher.test(this.raw, secret)) {
					return !this.decoy;
				}
			}
			return false;
		}

	}

}
