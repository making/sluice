package am.ik.sluice.server.auth;

import java.util.List;
import java.util.Locale;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * A token in a header of its own ({@code X-API-Token} by default), its whole value: the
 * token is matched against each of the route's. Its challenge names the header as the
 * scheme.
 */
final class ApiTokenScheme implements CredentialScheme {

	/** An RFC 9110 token: a header name, an auth scheme. */
	private static final Pattern TOKEN = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	private final String headerName;

	private final List<String> tokens;

	private final BiPredicate<String, String> matcher;

	private ApiTokenScheme(String headerName, List<String> tokens, BiPredicate<String, String> matcher) {
		this.headerName = headerName;
		this.tokens = List.copyOf(tokens);
		this.matcher = matcher;
	}

	/**
	 * The scheme of the route's {@code api-token}s in {@code headerName}; null when it
	 * has none.
	 * @throws IllegalStateException for a header name that is no token or is
	 * {@code Authorization}, or a token {@link PasswordEncoders#validate} refuses
	 */
	static @Nullable ApiTokenScheme of(String property, String headerName, List<String> tokens,
			BiPredicate<String, String> matcher) {
		if (tokens.isEmpty()) {
			return null;
		}
		for (int i = 0; i < tokens.size(); i++) {
			PasswordEncoders.validate(property + ".api-token[" + i + "]", tokens.get(i));
		}
		if (!TOKEN.matcher(headerName).matches()
				|| headerName.toLowerCase(Locale.ROOT).equals(RouteAuthenticator.AUTHORIZATION)) {
			throw new IllegalStateException(property + ".api-token-header is no header name besides Authorization");
		}
		return new ApiTokenScheme(headerName, tokens, matcher);
	}

	@Override
	public String name() {
		return this.headerName;
	}

	@Override
	public String header() {
		return this.headerName.toLowerCase(Locale.ROOT);
	}

	@Override
	public @Nullable String authScheme() {
		return null;
	}

	@Override
	public String challenge(String realm) {
		return this.headerName + " " + realm;
	}

	@Override
	public Credential credential(String value) {
		return value.isEmpty() ? Credential.NONE : new SecretCredential(value, value, this.tokens, false, this.matcher);
	}

}
