package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

/**
 * Per-route credential check of the data plane ({@code sluice.route.auth.<route key>}): a
 * request of a route with credentials passes with any one of them -- HTTP Basic,
 * {@code Authorization: Bearer}, or an API token header --, else it is unauthorized, with
 * the challenges of the route's schemes. A request carrying one of the route's credential
 * headers more than once is unauthorized whatever the values. With
 * {@code allow-preflight} a CORS preflight passes without credentials. Unless the route
 * forwards them, the headers of its schemes are removed from every request that passes.
 * <p>
 * Every secret is matched by the server's {@link PasswordEncoder} by its {@code {id}}
 * prefix; a secret without one, with an unknown id or malformed for its encoder fails the
 * start, as does a {@code tcp:<port>} key -- a tcp route carries no request to check --
 * and a route configuring no credential.
 * <p>
 * A credential that matched is remembered per route, by its SHA-256, for
 * {@link #CACHE_TTL} among the {@link #CACHE_SIZE} most recent ones: the client's later
 * requests skip the encoder. A credential that did not match is never remembered. Checks
 * against a deliberately slow hash (bcrypt, pbkdf2) run at most {@code maxVerifications}
 * at once; a request whose check would take one more is refused ({@link Verdict.Busy}). A
 * Basic user name the route does not know is checked against another user's password all
 * the same, so it takes the time a known one does.
 */
@Component
public class RouteAuthenticator {

	private static final Logger log = LoggerFactory.getLogger(RouteAuthenticator.class);

	/** How many matched credentials are remembered at most. */
	static final int CACHE_SIZE = 10_000;

	/** How long a matched credential is remembered. */
	static final Duration CACHE_TTL = Duration.ofMinutes(10);

	/** The {@code Retry-After} of a request refused for the verifications in flight. */
	static final Duration BUSY_RETRY_AFTER = Duration.ofSeconds(1);

	static final String AUTHORIZATION = "authorization";

	private static final String PREFLIGHT_METHOD = "access-control-request-method";

	/** An RFC 9110 token: a header name, an auth scheme. */
	private static final Pattern TOKEN = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	private final Map<String, Policy> policies;

	private final PasswordEncoder encoder;

	private final Semaphore verifications;

	private final LongSupplier clock;

	/** Matched credentials to the time they expire, least recently used first. */
	private final Map<String, Long> verified;

	@Autowired
	RouteAuthenticator(SluiceServerProperties properties, PasswordEncoder encoder) {
		this(properties, encoder, Math.max(2, Runtime.getRuntime().availableProcessors()), CACHE_SIZE,
				System::nanoTime);
	}

	/**
	 * @param maxVerifications checks against a slow hash at once
	 * @param cacheSize matched credentials remembered at most
	 * @param clock the time in nanoseconds, for the cache's expiry
	 */
	RouteAuthenticator(SluiceServerProperties properties, PasswordEncoder encoder, int maxVerifications, int cacheSize,
			LongSupplier clock) {
		this.verified = new LinkedHashMap<>(16, 0.75f, true) {

			@Override
			protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
				return size() > cacheSize;
			}

		};
		this.encoder = encoder;
		this.verifications = new Semaphore(maxVerifications);
		this.clock = clock;
		Map<String, Policy> policies = new TreeMap<>();
		properties.route().auth().forEach((key, auth) -> {
			String normalized = normalize(key);
			policies.put(normalized, policy(normalized, auth));
		});
		this.policies = Map.copyOf(policies);
		if (!policies.isEmpty() && properties.dataTlsBundle() == null) {
			log.warn(
					"routes {} require credentials, yet the data plane terminates no TLS (sluice.data-tls-bundle unset): "
							+ "their credentials cross it in cleartext unless a front end terminates TLS",
					policies.keySet());
		}
	}

	/**
	 * Checks the request of the route against the route's credentials;
	 * {@link Verdict#OPEN} for a route without any.
	 */
	public Verdict verify(Router.Route route, RequestHeaders headers) {
		Policy policy = this.policies.get(keyOf(route));
		if (policy == null) {
			return Verdict.OPEN;
		}
		if (policy.allowPreflight() && "OPTIONS".equals(headers.method())
				&& !headers.values(PREFLIGHT_METHOD).isEmpty()) {
			return policy.pass();
		}
		List<Presented> presented = new ArrayList<>(2);
		for (String header : policy.strip()) {
			List<String> values = headers.values(header);
			if (values.size() > 1) {
				return policy.unauthorized("invalid");
			}
			if (values.size() == 1) {
				presented.add(header.equals(AUTHORIZATION) ? policy.authorization(values.get(0))
						: new Presented("token", values.get(0).strip(), policy.apiToken(), false));
			}
		}
		if (presented.isEmpty()) {
			return policy.unauthorized("missing");
		}
		for (Presented credential : presented) {
			Verdict verdict = this.check(policy, credential);
			if (verdict != null) {
				return verdict;
			}
		}
		return policy.unauthorized("invalid");
	}

	/**
	 * Whether the route requires credentials: a connection to it the data plane cannot
	 * read a request of (TLS passthrough, a non-HTTP protocol) is refused.
	 */
	public boolean protects(Router.Route route) {
		return this.policies.containsKey(keyOf(route));
	}

	/**
	 * Whether the route key requires credentials.
	 * @param routeKey the route key: the domain or host pattern, {@code *} or empty for
	 * the catch-all
	 */
	public boolean protects(String routeKey) {
		return this.policies.containsKey(normalize(routeKey));
	}

	/**
	 * The schemes the route accepts -- {@code basic}, {@code bearer}, its API token
	 * header --; empty when it requires no credential.
	 * @param routeKey the route key: the domain or host pattern, {@code *} or empty for
	 * the catch-all
	 */
	public List<String> schemes(String routeKey) {
		Policy policy = this.policies.get(normalize(routeKey));
		return policy == null ? List.of() : policy.schemes();
	}

	/**
	 * The verdict on the credential: a pass when it matched, busy when checking it would
	 * take a verification past the bound; null when it did not match.
	 */
	private @Nullable Verdict check(Policy policy, Presented credential) {
		if (credential.value().isEmpty() || credential.secrets().isEmpty()) {
			return null;
		}
		String cacheKey = policy.key() + '\n' + credential.scheme() + '\n' + sha256(credential.value());
		if (this.remembered(cacheKey)) {
			return policy.pass();
		}
		boolean slow = credential.secrets().stream().anyMatch(secret -> !PasswordEncoders.cheap(secret));
		if (slow && !this.verifications.tryAcquire()) {
			return new Verdict.Busy(BUSY_RETRY_AFTER);
		}
		try {
			boolean matched = false;
			for (String secret : credential.secrets()) {
				if (this.matches(credential.raw(), secret)) {
					matched = true;
					break;
				}
			}
			if (!matched || credential.decoy()) {
				return null;
			}
		}
		finally {
			if (slow) {
				this.verifications.release();
			}
		}
		synchronized (this.verified) {
			this.verified.put(cacheKey, this.clock.getAsLong() + CACHE_TTL.toNanos());
		}
		return policy.pass();
	}

	private boolean remembered(String cacheKey) {
		synchronized (this.verified) {
			Long expiry = this.verified.get(cacheKey);
			if (expiry == null) {
				return false;
			}
			if (this.clock.getAsLong() - expiry >= 0) {
				this.verified.remove(cacheKey);
				return false;
			}
			return true;
		}
	}

	/**
	 * Whether the raw value matches the secret; an encoder failing on the pair is a
	 * mismatch.
	 */
	private boolean matches(String raw, String secret) {
		try {
			return this.encoder.matches(raw, secret);
		}
		catch (RuntimeException e) {
			log.debug("a credential did not match: {}", e.toString());
			return false;
		}
	}

	/** The route key of the route, as {@code sluice.route.auth} is keyed. */
	private static String keyOf(Router.Route route) {
		if (route.listenPort() > 0) {
			return "tcp:" + route.listenPort();
		}
		return route.domain().isEmpty() ? "*" : route.domain();
	}

	private static String normalize(String key) {
		return key == null || key.isBlank() ? "*" : key.strip();
	}

	private static Policy policy(String key, SluiceServerProperties.Route.Auth auth) {
		String property = "sluice.route.auth.[" + key + "]";
		if (key.startsWith("tcp:")) {
			throw new IllegalStateException(property + ": a tcp route carries no request whose credentials "
					+ "the data plane could check; only http routes take credentials");
		}
		if (auth.basic().isEmpty() && auth.bearer().isEmpty() && auth.apiToken().isEmpty()) {
			throw new IllegalStateException(property + " configures no credential (basic, bearer or api-token)");
		}
		auth.basic().forEach((user, password) -> {
			if (user.isEmpty() || user.indexOf(':') >= 0) {
				throw new IllegalStateException(property + ".basic: a user name is empty or holds a colon");
			}
			validate(property + ".basic." + user, password);
		});
		for (int i = 0; i < auth.bearer().size(); i++) {
			validate(property + ".bearer[" + i + "]", auth.bearer().get(i));
		}
		for (int i = 0; i < auth.apiToken().size(); i++) {
			validate(property + ".api-token[" + i + "]", auth.apiToken().get(i));
		}
		String apiTokenHeader = auth.apiTokenHeader().toLowerCase(Locale.ROOT);
		if (!TOKEN.matcher(apiTokenHeader).matches() || apiTokenHeader.equals(AUTHORIZATION)) {
			throw new IllegalStateException(property + ".api-token-header is no header name besides Authorization");
		}
		String realm = auth.realm().isEmpty() ? key : auth.realm();
		if (realm.chars().anyMatch(c -> c < 0x20 || c == 0x7f)) {
			throw new IllegalStateException(property + ".realm holds a control character");
		}
		String quotedRealm = "realm=\"" + realm.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
		List<String> schemes = new ArrayList<>();
		List<String> challenges = new ArrayList<>();
		Set<String> headers = new LinkedHashSet<>();
		if (!auth.basic().isEmpty()) {
			schemes.add("basic");
			challenges.add("Basic " + quotedRealm + ", charset=\"UTF-8\"");
			headers.add(AUTHORIZATION);
		}
		if (!auth.bearer().isEmpty()) {
			schemes.add("bearer");
			challenges.add("Bearer " + quotedRealm);
			headers.add(AUTHORIZATION);
		}
		if (!auth.apiToken().isEmpty()) {
			schemes.add(auth.apiTokenHeader());
			challenges.add(auth.apiTokenHeader() + " " + quotedRealm);
			headers.add(apiTokenHeader);
		}
		return new Policy(key, new TreeMap<>(auth.basic()), auth.bearer(), auth.apiToken(), Set.copyOf(headers),
				auth.forwardCredentials() ? Verdict.OPEN : new Verdict.Pass(headers), auth.allowPreflight(),
				List.copyOf(schemes), List.copyOf(challenges));
	}

	private static void validate(String property, String secret) {
		try {
			PasswordEncoders.validate(secret);
		}
		catch (IllegalArgumentException e) {
			throw new IllegalStateException(property + " " + e.getMessage(), e);
		}
	}

	private static String sha256(String value) {
		try {
			return HexFormat.of()
				.formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
		}
		catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * The credentials of one route.
	 *
	 * @param strip the lowercase names of the headers carrying them
	 * @param pass the verdict on a request that passes
	 */
	private record Policy(String key, Map<String, String> basic, List<String> bearer, List<String> apiToken,
			Set<String> strip, Verdict pass, boolean allowPreflight, List<String> schemes, List<String> challenges) {

		Verdict unauthorized(String reason) {
			return new Verdict.Unauthorized(reason, this.challenges);
		}

		/**
		 * The credential of an {@code Authorization} value: Basic or Bearer, as the route
		 * accepts it; one matching no secret otherwise (a scheme the route does not
		 * accept, a Basic value that does not decode or holds no colon, an empty token).
		 */
		Presented authorization(String value) {
			String trimmed = value.strip();
			int space = trimmed.indexOf(' ');
			String scheme = (space < 0 ? trimmed : trimmed.substring(0, space)).toLowerCase(Locale.ROOT);
			String credential = space < 0 ? "" : trimmed.substring(space + 1).strip();
			if (scheme.equals("bearer") && !this.bearer.isEmpty()) {
				return new Presented(scheme, credential, this.bearer, false);
			}
			if (!scheme.equals("basic") || this.basic.isEmpty()) {
				return Presented.NONE;
			}
			String decoded;
			try {
				decoded = new String(Base64.getDecoder().decode(credential), StandardCharsets.UTF_8);
			}
			catch (IllegalArgumentException e) {
				return Presented.NONE;
			}
			int colon = decoded.indexOf(':');
			if (colon < 0) {
				return Presented.NONE;
			}
			String user = decoded.substring(0, colon);
			String password = decoded.substring(colon + 1);
			String secret = this.basic.get(user);
			if (secret == null) {
				// checked against another user's password, the outcome discarded
				return new Presented(scheme, decoded, List.of(this.basic.values().iterator().next()), true, password);
			}
			return new Presented(scheme, decoded, List.of(secret), false, password);
		}

	}

	/**
	 * A credential a request presented.
	 *
	 * @param scheme what it was presented as
	 * @param value what identifies it in the cache: the token, or {@code user:password}
	 * @param secrets the secrets it may match
	 * @param decoy whether it fails whatever it matches (an unknown Basic user)
	 * @param raw what is matched against the secrets: the token, or the password
	 */
	private record Presented(String scheme, String value, List<String> secrets, boolean decoy, String raw) {

		static final Presented NONE = new Presented("none", "", List.of(), true, "");

		Presented(String scheme, String value, List<String> secrets, boolean decoy) {
			this(scheme, value, secrets, decoy, value);
		}

	}

}
