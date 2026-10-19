package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

/**
 * HTTP Basic (RFC 7617): {@code Authorization: Basic base64(user:password)}, the password
 * matched against the user's secret. A user the route does not know is checked against
 * another user's password, the outcome discarded.
 */
final class BasicScheme implements CredentialScheme {

	private final Map<String, String> users;

	private final BiPredicate<String, String> matcher;

	private BasicScheme(Map<String, String> users, BiPredicate<String, String> matcher) {
		this.users = new TreeMap<>(users);
		this.matcher = matcher;
	}

	/**
	 * The scheme of the route's {@code basic} users; null when it has none.
	 * @throws IllegalStateException for a user name that is empty or holds a colon, or a
	 * password {@link PasswordEncoders#validate} refuses
	 */
	static @Nullable BasicScheme of(String property, Map<String, String> users, BiPredicate<String, String> matcher) {
		if (users.isEmpty()) {
			return null;
		}
		users.forEach((user, password) -> {
			if (user.isEmpty() || user.indexOf(':') >= 0) {
				throw new IllegalStateException(property + ".basic: a user name is empty or holds a colon");
			}
			PasswordEncoders.validate(property + ".basic." + user, password);
		});
		return new BasicScheme(users, matcher);
	}

	@Override
	public String name() {
		return "basic";
	}

	@Override
	public String header() {
		return RouteAuthenticator.AUTHORIZATION;
	}

	@Override
	public String authScheme() {
		return "basic";
	}

	@Override
	public String challenge(String realm) {
		return "Basic " + realm + ", charset=\"UTF-8\"";
	}

	@Override
	public Credential credential(String value) {
		String decoded;
		try {
			decoded = new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
		}
		catch (IllegalArgumentException e) {
			return Credential.NONE;
		}
		int colon = decoded.indexOf(':');
		if (colon < 0) {
			return Credential.NONE;
		}
		String password = decoded.substring(colon + 1);
		String secret = this.users.get(decoded.substring(0, colon));
		if (secret == null) {
			return new SecretCredential(decoded, password, List.of(this.users.values().iterator().next()), true,
					this.matcher);
		}
		return new SecretCredential(decoded, password, List.of(secret), false, this.matcher);
	}

}
