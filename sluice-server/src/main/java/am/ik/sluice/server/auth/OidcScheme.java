package am.ik.sluice.server.auth;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import org.jspecify.annotations.Nullable;

import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * {@code Authorization: Bearer <JWT>} (RFC 6750) with the signed access tokens of an OIDC
 * provider (resource-server style): a token in JWS compact form is decoded by
 * {@link NimbusJwtDecoder} against the issuer's keys in memory ({@link IssuerKeys}), with
 * the signature algorithms allowed, and validated -- {@code exp} (required) and
 * {@code nbf} within the clock skew, {@code iss}, {@code aud}, the claims required. It
 * shares {@code bearer} with {@link BearerScheme}: it {@link #takes} the tokens in JWS
 * compact form, the static tokens the rest. A match is remembered until the token's
 * {@code exp} at most. A token naming a key not loaded, or of an issuer whose keys have
 * not loaded, is checked off the caller after a refresh of the keys, when one may run
 * ({@link IssuerKeys#mayRefresh}); with no keys loaded it is
 * {@link CredentialScheme.Check.Unavailable unavailable}. A refused token is answered
 * {@code error="invalid_token"}.
 */
final class OidcScheme implements CredentialScheme {

	private final IssuerKeys keys;

	private final NimbusJwtDecoder decoder;

	private final Clock clock;

	/** How long a check waits for the refresh of the keys. */
	private final Duration refreshWait;

	private OidcScheme(IssuerKeys keys, NimbusJwtDecoder decoder, Clock clock, Duration refreshWait) {
		this.keys = keys;
		this.decoder = decoder;
		this.clock = clock;
		this.refreshWait = refreshWait;
	}

	/**
	 * The scheme of the route's {@code oidc} provider; null when it has none.
	 * @throws IllegalStateException for an issuer that is no absolute http(s) URI, no
	 * audience, an algorithm that is no asymmetric JWS one, a required claim without a
	 * value, or a negative clock skew
	 */
	static @Nullable OidcScheme of(String property, SluiceServerProperties.Route.Auth.@Nullable Oidc oidc,
			OidcIssuers issuers) {
		if (oidc == null) {
			return null;
		}
		String prefix = property + ".oidc";
		String issuer = oidc.issuerUri();
		if (!isHttpUri(issuer)) {
			throw new IllegalStateException(prefix + ".issuer-uri is no absolute http(s) URI");
		}
		if (oidc.audiences().isEmpty() || oidc.audiences().stream().anyMatch(String::isBlank)) {
			throw new IllegalStateException(prefix + ".audiences names no audience, or a blank one");
		}
		Set<SignatureAlgorithm> algorithms = new LinkedHashSet<>();
		for (String name : oidc.jwsAlgorithms()) {
			SignatureAlgorithm algorithm = SignatureAlgorithm.from(name.strip());
			if (algorithm == null) {
				throw new IllegalStateException(prefix + ".jws-algorithms: " + name
						+ " is none of RS256, RS384, RS512, ES256, ES384, ES512, PS256, PS384, PS512");
			}
			algorithms.add(algorithm);
		}
		oidc.claims().forEach((claim, values) -> {
			if (values.isEmpty() || values.stream().anyMatch(String::isEmpty)) {
				throw new IllegalStateException(prefix + ".claims." + claim + " names no value, or an empty one");
			}
		});
		if (oidc.clockSkew().isNegative()) {
			throw new IllegalStateException(prefix + ".clock-skew is negative");
		}
		IssuerKeys keys = issuers.keys(issuer);
		NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSource(keys)
			.jwsAlgorithms(allowed -> allowed.addAll(algorithms))
			.build();
		decoder.setJwtValidator(validator(issuer, oidc, issuers.clock()));
		return new OidcScheme(keys, decoder, issuers.clock(), issuers.timing().fetchTimeout().multipliedBy(2));
	}

	private static OAuth2TokenValidator<Jwt> validator(String issuer, SluiceServerProperties.Route.Auth.Oidc oidc,
			Clock clock) {
		JwtTimestampValidator timestamps = new JwtTimestampValidator(oidc.clockSkew());
		timestamps.setClock(clock);
		timestamps.setAllowEmptyExpiryClaim(false);
		List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
		validators.add(timestamps);
		validators.add(new JwtIssuerValidator(issuer));
		List<String> audiences = List.copyOf(oidc.audiences());
		validators.add(new JwtClaimValidator<Collection<String>>(JwtClaimNames.AUD,
				aud -> aud.stream().anyMatch(audiences::contains)));
		for (Map.Entry<String, List<String>> claim : oidc.claims().entrySet()) {
			List<String> accepted = List.copyOf(claim.getValue());
			validators.add(new JwtClaimValidator<Object>(claim.getKey(), value -> holds(value, accepted)));
		}
		return new DelegatingOAuth2TokenValidator<>(validators);
	}

	/**
	 * Whether the claim's value, or one of a list claim's, matches one accepted; a claim
	 * absent fails in {@link JwtClaimValidator} itself.
	 */
	private static boolean holds(Object value, List<String> accepted) {
		if (value instanceof Collection<?> values) {
			return values.stream().anyMatch(each -> each != null && matchesAny(String.valueOf(each), accepted));
		}
		return matchesAny(String.valueOf(value), accepted);
	}

	private static boolean matchesAny(String value, List<String> accepted) {
		return accepted.stream().anyMatch(pattern -> glob(pattern, value));
	}

	/**
	 * Whether the value matches the pattern, {@code *} matching any run of characters.
	 */
	static boolean glob(String pattern, String value) {
		String[] parts = pattern.split("\\*", -1);
		if (parts.length == 1) {
			return pattern.equals(value);
		}
		if (!value.startsWith(parts[0])) {
			return false;
		}
		int at = parts[0].length();
		for (int i = 1; i < parts.length - 1; i++) {
			int found = value.indexOf(parts[i], at);
			if (found < 0) {
				return false;
			}
			at = found + parts[i].length();
		}
		String last = parts[parts.length - 1];
		return value.length() - last.length() >= at && value.endsWith(last);
	}

	private static boolean isHttpUri(String value) {
		try {
			URI uri = new URI(value);
			return uri.isAbsolute() && ("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
					&& uri.getHost() != null && uri.getRawQuery() == null && uri.getRawFragment() == null;
		}
		catch (URISyntaxException e) {
			return false;
		}
	}

	@Override
	public String name() {
		return "oidc";
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

	/** Takes a token in JWS compact form: three base64url parts, the first not empty. */
	@Override
	public boolean takes(String credential) {
		int dots = 0;
		for (int i = 0; i < credential.length(); i++) {
			char c = credential.charAt(i);
			if (c == '.') {
				if (++dots > 2 || i == 0) {
					return false;
				}
			}
			else if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_')) {
				return false;
			}
		}
		return dots == 2;
	}

	@Override
	public Credential credential(String value) {
		if (!takes(value)) {
			return Credential.NONE;
		}
		String keyId;
		boolean refresh;
		try {
			keyId = JWSHeader.parse(new Base64URL(value.substring(0, value.indexOf('.')))).getKeyID();
			refresh = !this.keys.knows(keyId) && this.keys.mayRefresh();
		}
		catch (ParseException e) {
			// no JWS header (alg none among them): the decoder refuses it, naming why
			refresh = false;
		}
		return new JwtCredential(value, refresh);
	}

	/** A token in JWS compact form. */
	private final class JwtCredential implements Credential {

		private final String token;

		/**
		 * Whether the keys load anew before it is checked: it names a key not loaded, or
		 * none are, and a load may run.
		 */
		private final boolean refresh;

		private JwtCredential(String token, boolean refresh) {
			this.token = token;
			this.refresh = refresh;
		}

		@Override
		public String id() {
			return this.token;
		}

		@Override
		public boolean bounded() {
			return this.refresh;
		}

		@Override
		public Check check() {
			return OidcScheme.this.check(this.token, this.refresh);
		}

	}

	private Check check(String token, boolean refresh) {
		if (refresh) {
			try {
				this.keys.refresh().get(this.refreshWait.toNanos(), TimeUnit.NANOSECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return new Check.Unavailable("interrupted while the keys of " + this.keys.issuer() + " loaded");
			}
			catch (ExecutionException | TimeoutException e) {
				// checked against the keys loaded, if any
			}
		}
		if (!this.keys.loaded()) {
			return new Check.Unavailable("the keys of " + this.keys.issuer() + " have not loaded");
		}
		Jwt jwt;
		try {
			jwt = this.decoder.decode(token);
		}
		catch (JwtValidationException e) {
			return new Check.Mismatch(
					e.getErrors().stream().map(OAuth2Error::getDescription).collect(Collectors.joining("; ")));
		}
		catch (JwtException e) {
			return new Check.Mismatch(String.valueOf(e.getMessage()));
		}
		Instant expiresAt = jwt.getExpiresAt();
		return new Check.Match(expiresAt == null ? Duration.ZERO : Duration.between(this.clock.instant(), expiresAt));
	}

}
