package am.ik.sluice.server.auth;

import java.util.List;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

/**
 * {@code Authorization: Bearer <token>} (RFC 6750) with static tokens: the token is
 * matched against each of the route's. A refused token is answered
 * {@code error="invalid_token"}.
 */
final class BearerScheme implements CredentialScheme {

	private final List<String> tokens;

	private final BiPredicate<String, String> matcher;

	private BearerScheme(List<String> tokens, BiPredicate<String, String> matcher) {
		this.tokens = List.copyOf(tokens);
		this.matcher = matcher;
	}

	/**
	 * The scheme of the route's {@code bearer} tokens; null when it has none.
	 * @throws IllegalStateException for a token {@link PasswordEncoders#validate} refuses
	 */
	static @Nullable BearerScheme of(String property, List<String> tokens, BiPredicate<String, String> matcher) {
		if (tokens.isEmpty()) {
			return null;
		}
		for (int i = 0; i < tokens.size(); i++) {
			PasswordEncoders.validate(property + ".bearer[" + i + "]", tokens.get(i));
		}
		return new BearerScheme(tokens, matcher);
	}

	@Override
	public String name() {
		return "bearer";
	}

	@Override
	public String header() {
		return RouteAuthenticator.AUTHORIZATION;
	}

	@Override
	public String authScheme() {
		return "bearer";
	}

	@Override
	public String challenge(String realm) {
		return "Bearer " + realm;
	}

	@Override
	public String refusal(String realm) {
		return challenge(realm) + ", error=\"invalid_token\"";
	}

	@Override
	public Credential credential(String value) {
		return value.isEmpty() ? Credential.NONE : new SecretCredential(value, value, this.tokens, false, this.matcher);
	}

}
