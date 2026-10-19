package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

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
 * Each scheme is a {@link CredentialScheme} ({@link BasicScheme}, {@link BearerScheme},
 * {@link ApiTokenScheme}); a header's value goes to the scheme of the header whose
 * auth-scheme token it carries.
 * <p>
 * Every secret is matched by the server's {@link PasswordEncoder} by its {@code {id}}
 * prefix; a secret without one, with an unknown id or malformed for its encoder fails the
 * start, as does a {@code tcp:<port>} key -- a tcp route carries no request to check --
 * and a route configuring no credential.
 * <p>
 * A credential that matched is remembered per route, by its SHA-256, for
 * {@link #CACHE_TTL} among the {@link #CACHE_SIZE} most recent ones: the client's later
 * requests skip the encoder. A credential that did not match is never remembered. A check
 * against a deliberately slow hash (bcrypt, pbkdf2) runs on a virtual thread of its own
 * ({@link #verifyAsync}), at most {@code maxVerifications} of them at once: a request
 * whose check would take one more is refused ({@link Verdict.Busy}), and a request
 * presenting a credential under check already waits for that check instead of starting
 * one. A Basic user name the route does not know is checked against another user's
 * password all the same, so it takes the time a known one does.
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

	private final Map<String, Policy> policies;

	private final PasswordEncoder encoder;

	private final Semaphore verifications;

	private final LongSupplier clock;

	/** Matched credentials to the time they expire, least recently used first. */
	private final Map<String, Long> verified;

	/**
	 * The slow checks in flight, by the key their credential is remembered under: whether
	 * it matched. With its own lock.
	 */
	private final Map<String, CompletableFuture<Boolean>> inFlight = new HashMap<>();

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
			policies.put(normalized, this.policy(normalized, auth));
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
	 * {@link Verdict#OPEN} for a route without any. Blocks while a slow check runs
	 * ({@link #verifyAsync}): for a caller on a thread of its own.
	 * @throws IllegalStateException interrupted while waiting for a slow check (the
	 * thread's interrupt status set again), or the check failed
	 */
	public Verdict verify(Router.Route route, RequestHeaders headers) {
		try {
			return verifyAsync(route, headers).get();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while checking a credential", e);
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof Error error) {
				throw error;
			}
			throw new IllegalStateException("the credential check failed", e.getCause());
		}
	}

	/**
	 * Checks the request of the route against the route's credentials without blocking:
	 * the verdict is complete at once unless a credential is to be checked against a slow
	 * hash and is not remembered; that check runs on a virtual thread of its own -- or is
	 * the one in flight for the same credential --, and the verdict completes with it, on
	 * that thread, or exceptionally with what the check threw. Cancelling the verdict
	 * leaves the check running to its end. {@link Verdict#OPEN} for a route without
	 * credentials.
	 */
	public CompletableFuture<Verdict> verifyAsync(Router.Route route, RequestHeaders headers) {
		Policy policy = this.policies.get(keyOf(route));
		if (policy == null) {
			return CompletableFuture.completedFuture(Verdict.OPEN);
		}
		if (policy.allowPreflight() && "OPTIONS".equals(headers.method())
				&& !headers.values(PREFLIGHT_METHOD).isEmpty()) {
			return CompletableFuture.completedFuture(policy.pass());
		}
		List<Presented> presented = new ArrayList<>(2);
		for (String header : policy.headers()) {
			List<String> values = headers.values(header);
			if (values.size() > 1) {
				return CompletableFuture.completedFuture(policy.unauthorized("invalid"));
			}
			if (values.size() == 1) {
				presented.add(policy.credential(header, values.get(0)));
			}
		}
		if (presented.isEmpty()) {
			return CompletableFuture.completedFuture(policy.unauthorized("missing"));
		}
		return this.decide(policy, presented, 0);
	}

	/**
	 * The verdict on the presented credentials from {@code from} on, in order: the first
	 * that matches passes, a check past the bound is busy, and none matching is
	 * unauthorized.
	 */
	private CompletableFuture<Verdict> decide(Policy policy, List<Presented> presented, int from) {
		for (int i = from; i < presented.size(); i++) {
			CompletableFuture<@Nullable Verdict> verdict = this.check(policy, presented.get(i));
			if (!verdict.isDone() || verdict.isCompletedExceptionally()) {
				int next = i + 1;
				return verdict.thenCompose(checked -> checked != null ? CompletableFuture.completedFuture(checked)
						: this.decide(policy, presented, next));
			}
			Verdict checked = verdict.join();
			if (checked != null) {
				return CompletableFuture.completedFuture(checked);
			}
		}
		return CompletableFuture.completedFuture(policy.unauthorized("invalid"));
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
		return policy == null ? List.of() : policy.names();
	}

	/**
	 * The verdict on the credential: a pass when it matched, busy when checking it would
	 * take a verification past the bound; null when it did not match. Complete at once
	 * but for a credential checked against a slow hash and not remembered
	 * ({@link #slowMatch}).
	 */
	private CompletableFuture<@Nullable Verdict> check(Policy policy, Presented presented) {
		CredentialScheme.Credential credential = presented.credential();
		if (credential.id().isEmpty()) {
			return CompletableFuture.completedFuture(null);
		}
		String cacheKey = policy.key() + '\n' + presented.scheme() + '\n' + sha256(credential.id());
		if (this.remembered(cacheKey)) {
			return CompletableFuture.completedFuture(policy.pass());
		}
		if (!credential.bounded()) {
			if (!credential.matches()) {
				return CompletableFuture.completedFuture(null);
			}
			this.remember(cacheKey);
			return CompletableFuture.completedFuture(policy.pass());
		}
		CompletableFuture<Boolean> matched = this.slowMatch(cacheKey, credential);
		if (matched == null) {
			return CompletableFuture.completedFuture(new Verdict.Busy(BUSY_RETRY_AFTER));
		}
		return matched.thenApply(match -> match ? policy.pass() : null);
	}

	/**
	 * Whether a credential checked against a slow hash matched: the check in flight for
	 * it, else a new one on a virtual thread of its own, holding one of the verifications
	 * until it ended -- null when none is left. A credential that matched is remembered
	 * before its check leaves the flight, so a request after it finds either.
	 */
	private @Nullable CompletableFuture<Boolean> slowMatch(String cacheKey, CredentialScheme.Credential credential) {
		CompletableFuture<Boolean> matched;
		synchronized (this.inFlight) {
			CompletableFuture<Boolean> running = this.inFlight.get(cacheKey);
			if (running != null) {
				return running;
			}
			if (!this.verifications.tryAcquire()) {
				return null;
			}
			matched = new CompletableFuture<>();
			this.inFlight.put(cacheKey, matched);
		}
		Runnable check = () -> {
			boolean match = false;
			Throwable failure = null;
			try {
				match = credential.matches();
				if (match) {
					this.remember(cacheKey);
				}
			}
			catch (Throwable e) {
				// delivered to every request waiting for the check
				failure = e;
			}
			finally {
				this.landed(cacheKey);
			}
			if (failure != null) {
				matched.completeExceptionally(failure);
			}
			else {
				matched.complete(match);
			}
		};
		try {
			Thread.ofVirtual().name("sluice-route-auth").start(check);
		}
		catch (RuntimeException | Error e) {
			this.landed(cacheKey);
			matched.completeExceptionally(e);
		}
		return matched;
	}

	/** A slow check ended: it leaves the flight and frees its verification. */
	private void landed(String cacheKey) {
		synchronized (this.inFlight) {
			this.inFlight.remove(cacheKey);
		}
		this.verifications.release();
	}

	private void remember(String cacheKey) {
		synchronized (this.verified) {
			this.verified.put(cacheKey, this.clock.getAsLong() + CACHE_TTL.toNanos());
		}
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

	private Policy policy(String key, SluiceServerProperties.Route.Auth auth) {
		String property = "sluice.route.auth.[" + key + "]";
		if (key.startsWith("tcp:")) {
			throw new IllegalStateException(property + ": a tcp route carries no request whose credentials "
					+ "the data plane could check; only http routes take credentials");
		}
		if (auth.basic().isEmpty() && auth.bearer().isEmpty() && auth.apiToken().isEmpty()) {
			throw new IllegalStateException(property + " configures no credential (basic, bearer or api-token)");
		}
		List<CredentialScheme> schemes = new ArrayList<>(3);
		Stream
			.of(BasicScheme.of(property, auth.basic(), this::matches),
					BearerScheme.of(property, auth.bearer(), this::matches),
					ApiTokenScheme.of(property, auth.apiTokenHeader(), auth.apiToken(), this::matches))
			.filter(Objects::nonNull)
			.forEach(schemes::add);
		String realm = auth.realm().isEmpty() ? key : auth.realm();
		if (realm.chars().anyMatch(c -> c < 0x20 || c == 0x7f)) {
			throw new IllegalStateException(property + ".realm holds a control character");
		}
		String quotedRealm = "realm=\"" + realm.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
		Set<String> headers = new LinkedHashSet<>();
		schemes.forEach(scheme -> headers.add(scheme.header()));
		return new Policy(key, List.copyOf(schemes), List.copyOf(headers),
				auth.forwardCredentials() ? Verdict.OPEN : new Verdict.Pass(headers), auth.allowPreflight(),
				schemes.stream().map(scheme -> scheme.challenge(quotedRealm)).toList());
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
	 * @param schemes the schemes it accepts, in the order its challenges are listed
	 * @param headers the lowercase names of the headers carrying them, each once
	 * @param pass the verdict on a request that passes
	 */
	private record Policy(String key, List<CredentialScheme> schemes, List<String> headers, Verdict pass,
			boolean allowPreflight, List<String> challenges) {

		Verdict unauthorized(String reason) {
			return new Verdict.Unauthorized(reason, this.challenges);
		}

		List<String> names() {
			return this.schemes.stream().map(CredentialScheme::name).toList();
		}

		/**
		 * The credential of the header's value, taken by the scheme of the header whose
		 * auth-scheme token it carries, or whose whole value it is;
		 * {@link CredentialScheme.Credential#NONE} when no scheme of the route takes it.
		 */
		Presented credential(String header, String value) {
			String trimmed = value.strip();
			int space = trimmed.indexOf(' ');
			String token = (space < 0 ? trimmed : trimmed.substring(0, space)).toLowerCase(Locale.ROOT);
			for (CredentialScheme scheme : this.schemes) {
				if (!scheme.header().equals(header)) {
					continue;
				}
				String authScheme = scheme.authScheme();
				if (authScheme == null) {
					return new Presented(scheme.name(), scheme.credential(trimmed));
				}
				if (authScheme.equals(token)) {
					return new Presented(scheme.name(),
							scheme.credential(space < 0 ? "" : trimmed.substring(space + 1).strip()));
				}
			}
			return new Presented("", CredentialScheme.Credential.NONE);
		}

	}

	/**
	 * A credential a request presented, with the name of the scheme that took it: a value
	 * remembered for one scheme passes no other.
	 */
	private record Presented(String scheme, CredentialScheme.Credential credential) {
	}

}
