package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

/**
 * Per-route credential check of the data plane ({@code sluice.route.auth.<route key>}): a
 * request of a route with credentials passes with any one of them -- HTTP Basic,
 * {@code Authorization: Bearer} with a static token or an OIDC provider's JWT, an API
 * token header, or the session cookie of the browser sign-in ({@link OidcSignIn}) --,
 * else it is unauthorized, with the challenges of the route's schemes; a browser's
 * navigation of a route signing browsers in is redirected to its provider instead, and a
 * signed-in user the route's claims do not admit is forbidden. The sign-in's callback and
 * sign-out paths are answered, never relayed; the challenge of a scheme whose credential
 * was refused names the error where the scheme has one. A request carrying one of the
 * route's credential headers more than once is unauthorized whatever the values. With
 * {@code allow-preflight} a CORS preflight passes without credentials. Unless the route
 * forwards them, the headers of its schemes are removed from every request that passes.
 * Each scheme is a {@link CredentialScheme} ({@link BasicScheme}, {@link OidcScheme},
 * {@link BearerScheme}, {@link ApiTokenScheme}); a header's value goes to the first
 * scheme of the header whose auth-scheme token it carries that
 * {@link CredentialScheme#takes takes} it. A request whose JWT could not be checked for
 * its provider's keys not loaded is {@link Verdict.Unavailable}.
 * <p>
 * Every secret is matched by the server's {@link PasswordEncoder} by its {@code {id}}
 * prefix; a secret without one, with an unknown id or malformed for its encoder fails the
 * start, as does a {@code tcp:<port>} key -- a tcp route carries no request to check --
 * and a route configuring no credential.
 * <p>
 * A credential that matched is remembered per route, by its SHA-256, for
 * {@link #CACHE_TTL} -- a JWT until its {@code exp} at most -- among the
 * {@link #CACHE_SIZE} most recent ones: the client's later requests skip the encoder. A
 * credential that did not match is never remembered. A check against a deliberately slow
 * hash (bcrypt, pbkdf2), or of a JWT waiting for its provider's keys to load, runs on a
 * virtual thread of its own ({@link #verifyAsync}) of the executor, at most
 * {@code maxVerifications} of them at once: a request whose check would take one more is
 * refused ({@link Verdict.Busy}), and a request presenting a credential under check
 * already waits for that check instead of starting one. A Basic user name the route does
 * not know is checked against another user's password all the same, so it takes the time
 * a known one does.
 */
@Component
public class RouteAuthenticator implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(RouteAuthenticator.class);

	/** How many matched credentials are remembered at most. */
	static final int CACHE_SIZE = 10_000;

	/** How long a matched credential is remembered. */
	static final Duration CACHE_TTL = Duration.ofMinutes(10);

	/** The {@code Retry-After} of a request refused for the verifications in flight. */
	static final Duration BUSY_RETRY_AFTER = Duration.ofSeconds(1);

	static final String AUTHORIZATION = "authorization";

	/** The path of the browser sign-in's callback on a route signing browsers in. */
	public static final String CALLBACK_PATH = OidcSignIn.CALLBACK_PATH;

	/** The path signing a browser out of a route signing browsers in. */
	public static final String SIGN_OUT_PATH = OidcSignIn.SIGN_OUT_PATH;

	private static final String PREFLIGHT_METHOD = "access-control-request-method";

	private final Map<String, Policy> policies;

	private final PasswordEncoder encoder;

	private final Semaphore verifications;

	private final LongSupplier clock;

	private final Executor executor;

	private final OidcIssuers issuers;

	/** The randomness of the browser sign-in: its states, and a session secret. */
	private final SecureRandom random = new SecureRandom();

	/** Matched credentials to the time they expire, least recently used first. */
	private final Map<String, Long> verified;

	/**
	 * The checks off the caller in flight, by the key their credential is remembered
	 * under: what they found. With its own lock.
	 */
	private final Map<String, CompletableFuture<CredentialScheme.Check>> inFlight = new HashMap<>();

	@Autowired
	RouteAuthenticator(SluiceServerProperties properties, PasswordEncoder encoder,
			@Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor, RestClient.Builder restClientBuilder) {
		this(properties, encoder, Math.max(2, Runtime.getRuntime().availableProcessors()), CACHE_SIZE, System::nanoTime,
				taskExecutor, new OidcIssuers(OidcIssuers.Timing.DEFAULT, Clock.systemUTC(), System::nanoTime,
						taskExecutor, restClientBuilder));
	}

	/**
	 * Builds every route's policy, then loads the keys of their OIDC issuers, waiting for
	 * the loads ({@link OidcIssuers#start}).
	 * @param maxVerifications checks off the caller at once
	 * @param cacheSize matched credentials remembered at most
	 * @param clock the time in nanoseconds, for the cache's expiry
	 * @param executor what the checks off the caller run on, a virtual thread each
	 * @param issuers the OIDC issuers, closed with the authenticator
	 */
	RouteAuthenticator(SluiceServerProperties properties, PasswordEncoder encoder, int maxVerifications, int cacheSize,
			LongSupplier clock, Executor executor, OidcIssuers issuers) {
		this.verified = new LinkedHashMap<>(16, 0.75f, true) {

			@Override
			protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
				return size() > cacheSize;
			}

		};
		this.encoder = encoder;
		this.verifications = new Semaphore(maxVerifications);
		this.clock = clock;
		this.executor = executor;
		this.issuers = issuers;
		Map<String, Policy> policies = new TreeMap<>();
		try {
			byte[] secret = this.sessionSecret(properties);
			properties.route().auth().forEach((key, auth) -> {
				String normalized = normalize(key);
				policies.put(normalized, this.policy(normalized, auth, secret));
			});
		}
		catch (RuntimeException e) {
			issuers.close();
			throw e;
		}
		this.policies = Map.copyOf(policies);
		if (!policies.isEmpty() && properties.dataTlsBundle() == null) {
			log.warn(
					"routes {} require credentials, yet the data plane terminates no TLS (sluice.data-tls-bundle unset): "
							+ "their credentials cross it in cleartext unless a front end terminates TLS",
					policies.keySet());
			List<String> signingIn = policies.values()
				.stream()
				.filter(policy -> policy.signIn() != null)
				.map(Policy::key)
				.toList();
			if (!signingIn.isEmpty() && properties.accessControl().trustedProxyCidrs().isEmpty()) {
				log.warn("routes {} sign browsers in, yet neither the data plane terminates TLS nor a trusted proxy "
						+ "(sluice.access-control.trusted-proxy-cidrs) states X-Forwarded-Proto: "
						+ "their sign-ins are refused 403 over plaintext", signingIn);
			}
		}
		issuers.start();
	}

	/**
	 * The secret the browser sign-in's cookies are sealed with: {@code session-secret},
	 * else -- on a single node -- a random one for this start; empty when no route signs
	 * browsers in.
	 * @throws IllegalStateException for a secret shorter than 32 characters, or none in
	 * cluster mode
	 */
	private byte[] sessionSecret(SluiceServerProperties properties) {
		List<String> signingIn = properties.route()
			.auth()
			.entrySet()
			.stream()
			.filter(entry -> entry.getValue().oidc() != null && entry.getValue().oidc().signsIn())
			.map(entry -> normalize(entry.getKey()))
			.sorted()
			.toList();
		if (signingIn.isEmpty()) {
			return new byte[0];
		}
		String secret = properties.route().sessionSecret();
		if (secret.isEmpty()) {
			if (properties.clusterEnabled()) {
				throw new IllegalStateException("routes " + signingIn + " sign browsers in: cluster mode requires "
						+ "sluice.route.session-secret, the same on every node");
			}
			log.warn("routes {} sign browsers in, yet sluice.route.session-secret is unset: their sessions are sealed "
					+ "with a random secret and end with this start", signingIn);
			byte[] random = new byte[32];
			this.random.nextBytes(random);
			return random;
		}
		if (secret.length() < 32) {
			throw new IllegalStateException("sluice.route.session-secret is shorter than 32 characters");
		}
		return secret.getBytes(StandardCharsets.UTF_8);
	}

	/** Stops the loads of the OIDC issuers' keys. */
	@Override
	public void close() {
		this.issuers.close();
	}

	/**
	 * Checks the request of the route against the route's credentials;
	 * {@link Verdict#OPEN} for a route without any. Blocks while a slow check runs
	 * ({@link #verifyAsync}): for a caller on a thread of its own.
	 * @param origin the origin the client addressed, which the browser sign-in returns to
	 * @throws IllegalStateException interrupted while waiting for a slow check (the
	 * thread's interrupt status set again), or the check failed
	 */
	public Verdict verify(Router.Route route, RequestHeaders headers, Origin origin) {
		try {
			return verifyAsync(route, headers, origin).get();
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
	 * that thread, or exceptionally with what the check threw; so does the code exchange
	 * of the browser sign-in's callback. Cancelling the verdict leaves the check running
	 * to its end. {@link Verdict#OPEN} for a route without credentials.
	 * <p>
	 * On a route signing browsers in, the callback and sign-out paths are answered; a
	 * session whose user the route admits passes, else the credentials presented decide;
	 * without any, a signed-in user is forbidden, and a browser's navigation is
	 * redirected to the provider.
	 * @param origin the origin the client addressed, which the browser sign-in returns to
	 */
	public CompletableFuture<Verdict> verifyAsync(Router.Route route, RequestHeaders headers, Origin origin) {
		Policy policy = this.policies.get(route.routeKey());
		if (policy == null) {
			return CompletableFuture.completedFuture(Verdict.OPEN);
		}
		OidcSignIn signIn = policy.signIn();
		if (signIn != null && signIn.reserved(headers.target())) {
			return signIn.answer(headers, origin, this::offCaller);
		}
		if (policy.allowPreflight() && "OPTIONS".equals(headers.method())
				&& !headers.values(PREFLIGHT_METHOD).isEmpty()) {
			return CompletableFuture.completedFuture(policy.pass(headers, null));
		}
		OidcSignIn.Session session = signIn == null ? null : signIn.session(headers);
		if (signIn != null && session != null && signIn.admits(session)) {
			return CompletableFuture.completedFuture(policy.pass(headers, session));
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
			if (session != null) {
				return CompletableFuture.completedFuture(new Verdict.Forbidden(Verdict.Forbidden.DENIED));
			}
			Verdict redirect = signIn == null ? null : signIn.signIn(headers, origin);
			return CompletableFuture.completedFuture(redirect != null ? redirect : policy.unauthorized("missing"));
		}
		return this.decide(policy, headers, presented, 0);
	}

	/**
	 * The verdict on the presented credentials from {@code from} on, in order: the first
	 * that matches passes, a check past the bound is busy, one whose keys have not loaded
	 * unavailable, and none matching is unauthorized, the challenges of their schemes
	 * naming the error.
	 */
	private CompletableFuture<Verdict> decide(Policy policy, RequestHeaders headers, List<Presented> presented,
			int from) {
		for (int i = from; i < presented.size(); i++) {
			CompletableFuture<@Nullable Verdict> verdict = this.check(policy, headers, presented.get(i));
			if (!verdict.isDone() || verdict.isCompletedExceptionally()) {
				int next = i + 1;
				return verdict.thenCompose(checked -> checked != null ? CompletableFuture.completedFuture(checked)
						: this.decide(policy, headers, presented, next));
			}
			Verdict checked = verdict.join();
			if (checked != null) {
				return CompletableFuture.completedFuture(checked);
			}
		}
		return CompletableFuture.completedFuture(policy.refused(presented));
	}

	/**
	 * Whether the route requires credentials: a connection to it the data plane cannot
	 * read a request of (TLS passthrough, a non-HTTP protocol) is refused.
	 */
	public boolean protects(Router.Route route) {
		return this.policies.containsKey(route.routeKey());
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
	 * The schemes the route accepts -- {@code basic}, {@code oidc}, {@code bearer}, its
	 * API token header, {@code oidc-login} --; empty when it requires no credential.
	 * @param routeKey the route key: the domain or host pattern, {@code *} or empty for
	 * the catch-all
	 */
	public List<String> schemes(String routeKey) {
		Policy policy = this.policies.get(normalize(routeKey));
		return policy == null ? List.of() : policy.names();
	}

	/**
	 * The verdict on the credential: a pass when it matched, busy when checking it would
	 * take a verification past the bound, unavailable when what it is checked against has
	 * not loaded; null when it did not match. Complete at once but for a bounded
	 * credential not remembered ({@link #slowMatch}).
	 */
	private CompletableFuture<@Nullable Verdict> check(Policy policy, RequestHeaders headers, Presented presented) {
		CredentialScheme.Credential credential = presented.credential();
		if (credential.id().isEmpty()) {
			return CompletableFuture.completedFuture(null);
		}
		String cacheKey = policy.key() + '\n' + presented.scheme() + '\n' + sha256(credential.id());
		if (this.remembered(cacheKey)) {
			return CompletableFuture.completedFuture(policy.pass(headers, null));
		}
		if (!credential.bounded()) {
			CredentialScheme.Check checked = credential.check();
			if (checked instanceof CredentialScheme.Check.Match match) {
				this.remember(cacheKey, match);
			}
			return CompletableFuture.completedFuture(this.verdictOf(policy, headers, presented.scheme(), checked));
		}
		CompletableFuture<CredentialScheme.Check> matched = this.slowMatch(cacheKey, credential);
		if (matched == null) {
			return CompletableFuture.completedFuture(new Verdict.Busy(BUSY_RETRY_AFTER));
		}
		return matched.thenApply(checked -> this.verdictOf(policy, headers, presented.scheme(), checked));
	}

	/**
	 * The verdict of a check: a pass, unavailable, or null for a mismatch -- whose cause
	 * is logged at debug, never the credential.
	 */
	private @Nullable Verdict verdictOf(Policy policy, RequestHeaders headers, String scheme,
			CredentialScheme.Check checked) {
		return switch (checked) {
			case CredentialScheme.Check.Match match -> policy.pass(headers, null);
			case CredentialScheme.Check.Unavailable unavailable -> {
				log.debug("route {}: a {} credential could not be checked: {}", policy.key(), scheme,
						unavailable.why());
				yield new Verdict.Unavailable(this.issuers.timing().minRefreshInterval());
			}
			case CredentialScheme.Check.Mismatch mismatch -> {
				log.debug("route {}: a {} credential did not match: {}", policy.key(), scheme, mismatch.why());
				yield null;
			}
		};
	}

	/**
	 * What checking a bounded credential found: the check in flight for it, else a new
	 * one on a virtual thread of its own of the executor, holding one of the
	 * verifications until it ended -- null when none is left. A credential that matched
	 * is remembered before its check leaves the flight, so a request after it finds
	 * either.
	 */
	private @Nullable CompletableFuture<CredentialScheme.Check> slowMatch(String cacheKey,
			CredentialScheme.Credential credential) {
		CompletableFuture<CredentialScheme.Check> matched;
		synchronized (this.inFlight) {
			CompletableFuture<CredentialScheme.Check> running = this.inFlight.get(cacheKey);
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
			CredentialScheme.Check checked = null;
			Throwable failure = null;
			try {
				checked = credential.check();
				if (checked instanceof CredentialScheme.Check.Match match) {
					this.remember(cacheKey, match);
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
				matched.complete(checked);
			}
		};
		try {
			this.executor.execute(check);
		}
		catch (RuntimeException | Error e) {
			this.landed(cacheKey);
			matched.completeExceptionally(e);
		}
		return matched;
	}

	/**
	 * Runs the work -- the code exchange of a sign-in's callback, which waits for the
	 * provider -- on a virtual thread of its own of the executor, holding one of the
	 * verifications until it ended: busy when none is left. The verdict completes with
	 * the work, on that thread, or exceptionally with what it threw.
	 */
	private CompletableFuture<Verdict> offCaller(Supplier<Verdict> work) {
		if (!this.verifications.tryAcquire()) {
			return CompletableFuture.completedFuture(new Verdict.Busy(BUSY_RETRY_AFTER));
		}
		CompletableFuture<Verdict> verdict = new CompletableFuture<>();
		try {
			this.executor.execute(() -> {
				Verdict answered = null;
				Throwable failure = null;
				try {
					answered = work.get();
				}
				catch (Throwable e) {
					failure = e;
				}
				finally {
					this.verifications.release();
				}
				if (failure != null) {
					verdict.completeExceptionally(failure);
				}
				else {
					verdict.complete(answered);
				}
			});
		}
		catch (RuntimeException | Error e) {
			this.verifications.release();
			verdict.completeExceptionally(e);
		}
		return verdict;
	}

	/** A slow check ended: it leaves the flight and frees its verification. */
	private void landed(String cacheKey) {
		synchronized (this.inFlight) {
			this.inFlight.remove(cacheKey);
		}
		this.verifications.release();
	}

	/**
	 * Remembers a match for {@link #CACHE_TTL}, or the match's own bound when shorter;
	 * not at all when that has passed.
	 */
	private void remember(String cacheKey, CredentialScheme.Check.Match match) {
		Duration ttl = match.ttl();
		long nanos = ttl == null ? CACHE_TTL.toNanos() : Math.min(CACHE_TTL.toNanos(), ttl.toNanos());
		if (nanos <= 0) {
			return;
		}
		synchronized (this.verified) {
			this.verified.put(cacheKey, this.clock.getAsLong() + nanos);
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

	private static String normalize(String key) {
		return key == null || key.isBlank() ? "*" : key.strip();
	}

	private Policy policy(String key, SluiceServerProperties.Route.Auth auth, byte[] sessionSecret) {
		String property = "sluice.route.auth.[" + key + "]";
		if (key.startsWith("tcp:")) {
			throw new IllegalStateException(property + ": a tcp route carries no request whose credentials "
					+ "the data plane could check; only http routes take credentials");
		}
		if (auth.basic().isEmpty() && auth.bearer().isEmpty() && auth.apiToken().isEmpty() && auth.oidc() == null) {
			throw new IllegalStateException(property + " configures no credential (basic, bearer, api-token or oidc)");
		}
		List<CredentialScheme> schemes = new ArrayList<>(4);
		// oidc before bearer: it takes the JWTs, the static tokens the rest
		Stream
			.of(BasicScheme.of(property, auth.basic(), this::matches),
					OidcScheme.of(property, auth.oidc(), this.issuers),
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
		// after the schemes: OidcScheme#of checked the issuer
		OidcSignIn signIn = OidcSignIn.of(property, key, auth.oidc(), this.issuers, sessionSecret, this.random,
				headers);
		return new Policy(key, List.copyOf(schemes), List.copyOf(headers),
				auth.forwardCredentials() ? (Verdict.Pass) Verdict.OPEN : new Verdict.Pass(headers),
				auth.forwardCredentials(), auth.allowPreflight(), quotedRealm, signIn);
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
	 * @param pass the verdict on a request that passes, but for the browser sign-in's
	 * @param forward whether the route forwards its credentials
	 * @param realm the quoted realm parameter of the challenges
	 * @param signIn the browser sign-in; null when the route signs no browser in
	 */
	private record Policy(String key, List<CredentialScheme> schemes, List<String> headers, Verdict.Pass pass,
			boolean forward, boolean allowPreflight, String realm, @Nullable OidcSignIn signIn) {

		/**
		 * The verdict on a request that passes: {@link #pass}, and on a route signing
		 * browsers in its sign-in's cookies and identity headers
		 * ({@link OidcSignIn#pass}).
		 * @param session the session it passed with; null when none
		 */
		Verdict.Pass pass(RequestHeaders request, OidcSignIn.@Nullable Session session) {
			OidcSignIn signIn = this.signIn;
			return signIn == null ? this.pass : signIn.pass(this.pass, request, session, this.forward);
		}

		/** Unauthorized for the reason, with each scheme's challenge. */
		Verdict unauthorized(String reason) {
			return this.challenging(reason, Set.of());
		}

		/**
		 * Unauthorized for the presented credentials refused: a scheme that took one
		 * answers its {@link CredentialScheme#refusal}.
		 */
		Verdict refused(List<Presented> presented) {
			Set<String> refused = new HashSet<>();
			presented.forEach(each -> refused.add(each.scheme()));
			return this.challenging("invalid", refused);
		}

		/**
		 * Unauthorized with a challenge per scheme, the refused schemes' refusals, the
		 * browser sign-in's last; schemes sharing a challenge (static tokens and OIDC on
		 * {@code Bearer}) answer it once, as a refusal when either refused.
		 */
		private Verdict challenging(String reason, Set<String> refused) {
			Map<String, String> challenges = new LinkedHashMap<>();
			for (CredentialScheme scheme : this.schemes) {
				String challenge = scheme.challenge(this.realm);
				if (refused.contains(scheme.name())) {
					challenges.put(challenge, scheme.refusal(this.realm));
				}
				else {
					challenges.putIfAbsent(challenge, challenge);
				}
			}
			OidcSignIn signIn = this.signIn;
			if (signIn != null) {
				String challenge = signIn.challenge(this.realm);
				challenges.putIfAbsent(challenge, challenge);
			}
			return new Verdict.Unauthorized(reason, List.copyOf(challenges.values()));
		}

		List<String> names() {
			List<String> names = new ArrayList<>(this.schemes.stream().map(CredentialScheme::name).toList());
			if (this.signIn != null) {
				names.add("oidc-login");
			}
			return List.copyOf(names);
		}

		/**
		 * The credential of the header's value, taken by the first scheme of the header
		 * whose auth-scheme token it carries, or whose whole value it is, that
		 * {@link CredentialScheme#takes takes} it; when none takes it, the first such
		 * scheme's {@link CredentialScheme.Credential#NONE}, and a scheme-less NONE when
		 * the route has no such scheme.
		 */
		Presented credential(String header, String value) {
			String trimmed = value.strip();
			int space = trimmed.indexOf(' ');
			String token = (space < 0 ? trimmed : trimmed.substring(0, space)).toLowerCase(Locale.ROOT);
			Presented none = new Presented("", CredentialScheme.Credential.NONE);
			for (CredentialScheme scheme : this.schemes) {
				if (!scheme.header().equals(header)) {
					continue;
				}
				String authScheme = scheme.authScheme();
				String credential;
				if (authScheme == null) {
					credential = trimmed;
				}
				else if (authScheme.equals(token)) {
					credential = space < 0 ? "" : trimmed.substring(space + 1).strip();
				}
				else {
					continue;
				}
				if (scheme.takes(credential)) {
					return new Presented(scheme.name(), scheme.credential(credential));
				}
				if (none.scheme().isEmpty()) {
					none = new Presented(scheme.name(), CredentialScheme.Credential.NONE);
				}
			}
			return none;
		}

	}

	/**
	 * A credential a request presented, with the name of the scheme that took it: a value
	 * remembered for one scheme passes no other.
	 */
	private record Presented(String scheme, CredentialScheme.Credential credential) {
	}

}
