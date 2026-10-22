package am.ik.sluice.server.auth;

import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jose.util.JSONObjectUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.web.client.RestClient;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * The browser sign-in of one route: the route is an OIDC client of its provider
 * (authorization code flow with PKCE, confidential with a client secret, public without),
 * and a browser that signed in carries a session cookie the data plane checks, no store
 * behind it.
 * <p>
 * A navigation of a browser ({@link #navigation}) without a session is redirected to the
 * provider's authorization endpoint, a state cookie of its own (state, nonce, PKCE
 * verifier, the page to return to) set for {@link #STATE_MAX_AGE}; the callback
 * ({@link #CALLBACK_PATH}, answered by the data plane, never relayed) checks the state
 * against that cookie, exchanges the code at the token endpoint off the caller, validates
 * the ID token (signature against the provider's keys, {@code iss}, {@code aud} holding
 * the client id, {@code azp} when present, {@code exp}, the nonce) and sets the session
 * cookie, then redirects back to the page the sign-in began at -- a path of the same
 * origin, never one the request names. The session cookie holds the ID token's claims the
 * route reads ({@code sub}, those of {@code claims} and {@code headers}) until
 * {@code session-max-age}; a session past it, altered or sealed under another key is no
 * session. A signed-in user whose claims the route does not admit is
 * {@link Verdict.Forbidden forbidden}, not redirected. {@link #SIGN_OUT_PATH} removes the
 * session cookie and redirects to the provider's end session endpoint (RP-initiated
 * logout) when it has one, else to the origin's root.
 * <p>
 * The cookies are {@code __Host-} prefixed, {@code Secure}, {@code HttpOnly},
 * {@code SameSite=Lax}, of the host only: a sign-in begins only over https (TLS
 * terminated by the data plane, or a trusted proxy's {@code X-Forwarded-Proto}), else it
 * is forbidden. Sealed by {@link SessionCodec}, so every node sharing the session secret
 * accepts them. The cookies are removed from the {@code Cookie} header of every request
 * relayed, the client's other cookies kept, unless the route forwards its credentials;
 * the identity headers are removed from every request relayed and set from the session.
 */
final class OidcSignIn {

	private static final Logger log = LoggerFactory.getLogger(RouteAuthenticator.class);

	/** The path of the provider's redirect back to the route. */
	static final String CALLBACK_PATH = "/.sluice/oauth2/callback";

	/** The path that signs a browser out. */
	static final String SIGN_OUT_PATH = "/.sluice/oauth2/sign-out";

	/** The prefix of every cookie of the sign-in. */
	static final String COOKIE_PREFIX = "__Host-sluice-";

	static final String SESSION_COOKIE = COOKIE_PREFIX + "session";

	/** The prefix of a state cookie, the start of its state following it. */
	static final String STATE_COOKIE = COOKIE_PREFIX + "state-";

	/** How long a sign-in may take from the redirect to the callback. */
	static final Duration STATE_MAX_AGE = Duration.ofMinutes(10);

	/** The longest session cookie set, name and value: browsers keep 4096 bytes. */
	static final int MAX_COOKIE = 4000;

	/** The characters of a state naming its cookie. */
	private static final int STATE_NAME_LENGTH = 12;

	private static final String COOKIE_ATTRIBUTES = "; Path=/; Secure; HttpOnly; SameSite=Lax";

	private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

	/** An RFC 9110 token: a header name. */
	private static final Pattern TOKEN = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	/** A scope token (RFC 6749 3.3). */
	private static final Pattern SCOPE = Pattern.compile("[\\x21\\x23-\\x5b\\x5d-\\x7e]+");

	/**
	 * A target a sign-in returns to: an origin-form path of visible ASCII that no browser
	 * reads as another host ({@code //host}, {@code /\host}).
	 */
	private static final Pattern RETURN_TARGET = Pattern.compile("/(?![/\\\\])[\\x21-\\x7e]*");

	/** Headers the identity headers must not replace. */
	private static final Set<String> RESERVED_HEADERS = Set.of("host", "cookie", RouteAuthenticator.AUTHORIZATION,
			"content-length", "content-type", "transfer-encoding", "connection", "upgrade", "te", "keep-alive",
			"proxy-connection", "expect", "x-forwarded-proto", "x-forwarded-host", "x-forwarded-port",
			"x-forwarded-for", "x-forwarded-prefix", "forwarded");

	private static final String SESSION = "session";

	private static final String STATE = "state";

	private final String routeKey;

	private final IssuerKeys issuer;

	private final RestClient http;

	private final NimbusJwtDecoder decoder;

	private final SessionCodec codec;

	private final SecureRandom random;

	private final Clock clock;

	private final String clientId;

	private final String clientSecret;

	/** The scopes, space separated. */
	private final String scope;

	private final Duration sessionMaxAge;

	/** The claims a signed-in user must carry: the claim name to the values accepted. */
	private final Map<String, List<String>> claims;

	/** The identity headers, lowercase names to the claims they carry. */
	private final Map<String, String> headers;

	/** The claims the session cookie keeps. */
	private final Set<String> kept;

	/** The {@code Retry-After} of a sign-in whose provider has not loaded. */
	private final Duration retryAfter;

	/** How long a callback waits for the refresh of the keys. */
	private final Duration refreshWait;

	private OidcSignIn(String routeKey, SluiceServerProperties.Route.Auth.Oidc oidc, OidcIssuers issuers,
			SessionCodec codec, SecureRandom random, Map<String, String> headers) {
		this.routeKey = routeKey;
		this.issuer = issuers.keys(oidc.issuerUri());
		this.http = issuers.http();
		this.codec = codec;
		this.random = random;
		this.clock = issuers.clock();
		this.clientId = oidc.clientId();
		this.clientSecret = oidc.clientSecret();
		this.scope = String.join(" ", new LinkedHashSet<>(oidc.scopes().stream().map(String::strip).toList()));
		this.sessionMaxAge = oidc.sessionMaxAge();
		this.claims = Map.copyOf(oidc.claims());
		this.headers = Map.copyOf(headers);
		Set<String> kept = new HashSet<>();
		kept.add(JwtClaimNames.SUB);
		kept.addAll(this.claims.keySet());
		kept.addAll(this.headers.values());
		this.kept = Set.copyOf(kept);
		this.retryAfter = issuers.timing().minRefreshInterval();
		this.refreshWait = issuers.timing().fetchTimeout().multipliedBy(2);
		this.decoder = NimbusJwtDecoder.withJwkSource(this.issuer)
			.jwsAlgorithms(allowed -> allowed.addAll(OidcScheme.algorithms("", oidc)))
			.build();
		String clientId = this.clientId;
		this.decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(OidcScheme.timestamps(oidc, this.clock),
				new JwtIssuerValidator(oidc.issuerUri()),
				new JwtClaimValidator<Collection<String>>(JwtClaimNames.AUD, aud -> aud.contains(clientId)), jwt -> {
					String azp = jwt.getClaimAsString("azp");
					return azp == null || azp.equals(clientId) ? OAuth2TokenValidatorResult.success()
							: OAuth2TokenValidatorResult
								.failure(new OAuth2Error("invalid_token", "The azp claim is not valid", null));
				}));
	}

	/**
	 * The browser sign-in of the route's {@code oidc} provider; null when it signs no
	 * browser in (no {@code client-id}). Its issuer is checked by {@link OidcScheme#of}.
	 * @param secret the session secret its cookies are sealed with ({@link SessionCodec})
	 * @param credentialHeaders the lowercase names of the headers of the route's
	 * credentials, which no identity header may name
	 * @throws IllegalStateException for scopes without {@code openid} or with a malformed
	 * one, a session max age not positive, or an identity header that is no header name,
	 * names a header the relay sets or a credential header, or names no claim
	 */
	static @Nullable OidcSignIn of(String property, String routeKey,
			SluiceServerProperties.Route.Auth.@Nullable Oidc oidc, OidcIssuers issuers, byte[] secret,
			SecureRandom random, Set<String> credentialHeaders) {
		if (oidc == null || !oidc.signsIn()) {
			return null;
		}
		String prefix = property + ".oidc";
		List<String> scopes = oidc.scopes().stream().map(String::strip).toList();
		if (!scopes.contains("openid") || scopes.stream().anyMatch(each -> !SCOPE.matcher(each).matches())) {
			throw new IllegalStateException(prefix + ".scopes " + scopes + " names no openid, or a malformed scope");
		}
		if (oidc.sessionMaxAge().isNegative() || oidc.sessionMaxAge().isZero()) {
			throw new IllegalStateException(prefix + ".session-max-age is not positive");
		}
		Map<String, String> headers = new LinkedHashMap<>();
		oidc.headers().forEach((header, claim) -> {
			String name = header.strip().toLowerCase(Locale.ROOT);
			if (!TOKEN.matcher(name).matches() || RESERVED_HEADERS.contains(name) || credentialHeaders.contains(name)) {
				throw new IllegalStateException(prefix + ".headers." + header
						+ " is no header name, or names one the relay sets or a credential header");
			}
			if (claim == null || claim.isBlank()) {
				throw new IllegalStateException(prefix + ".headers." + header + " names no claim");
			}
			headers.put(name, claim.strip());
		});
		return new OidcSignIn(routeKey, oidc, issuers, new SessionCodec(secret, routeKey, random), random, headers);
	}

	/** The {@code WWW-Authenticate} challenge of a non-browser request of the route. */
	String challenge(String realm) {
		return "OIDC " + realm;
	}

	/**
	 * Whether the request target is a path the sign-in answers itself: the callback or
	 * the sign-out.
	 */
	boolean reserved(String target) {
		String path = pathOf(target);
		return path.equals(CALLBACK_PATH) || path.equals(SIGN_OUT_PATH);
	}

	/**
	 * The answer to a request of a {@link #reserved} path: the sign-out's redirect, or
	 * the callback's -- a refusal at once, or the code's exchange, run by
	 * {@code offCaller}.
	 */
	CompletableFuture<Verdict> answer(RequestHeaders request, Origin origin,
			Function<Supplier<Verdict>, CompletableFuture<Verdict>> offCaller) {
		if (pathOf(request.target()).equals(SIGN_OUT_PATH)) {
			return CompletableFuture.completedFuture(this.signOut(origin));
		}
		Map<String, String> query = queryOf(request.target());
		if (query == null) {
			return CompletableFuture.completedFuture(failed("the callback's query is malformed"));
		}
		String state = query.get(STATE);
		if (state == null || state.length() < STATE_NAME_LENGTH) {
			return CompletableFuture.completedFuture(failed("the callback names no state"));
		}
		Map<String, Object> saved = this.state(request, state);
		if (saved == null) {
			return CompletableFuture.completedFuture(failed("no state cookie holds the callback's state"));
		}
		String error = query.get("error");
		if (error != null) {
			return CompletableFuture.completedFuture(failed("the provider answered " + printable(error)));
		}
		String code = query.get("code");
		if (code == null || code.isEmpty()) {
			return CompletableFuture.completedFuture(failed("the callback names no code"));
		}
		return offCaller.apply(() -> this.signedIn(saved, code));
	}

	/**
	 * The redirect beginning a sign-in for a browser's navigation without a session:
	 * forbidden over plaintext, unavailable while the provider's endpoints have not
	 * loaded; null for a request that is no navigation or names no well-formed host.
	 */
	@Nullable Verdict signIn(RequestHeaders request, Origin origin) {
		String base = origin.uri();
		if (!navigation(request) || base == null) {
			return null;
		}
		if (!origin.secure()) {
			return new Verdict.Forbidden(Verdict.Forbidden.INSECURE);
		}
		IssuerKeys.Endpoints endpoints = this.issuer.endpoints();
		URI authorization = endpoints == null ? null : endpoints.authorization();
		if (authorization == null) {
			log.debug("route {}: a sign-in could not begin: the endpoints of {} have not loaded", this.routeKey,
					this.issuer.issuer());
			return new Verdict.Unavailable(this.retryAfter);
		}
		String state = this.random();
		String nonce = this.random();
		String verifier = this.random();
		String target = request.target();
		if (!RETURN_TARGET.matcher(target).matches() || this.reserved(target)) {
			target = "/";
		}
		String redirectUri = base + CALLBACK_PATH;
		Map<String, Object> saved = new LinkedHashMap<>();
		saved.put(STATE, state);
		saved.put("nonce", nonce);
		saved.put("verifier", verifier);
		saved.put("origin", base);
		saved.put("target", target);
		saved.put("exp", this.clock.instant().plus(STATE_MAX_AGE).getEpochSecond());
		String cookie = STATE_COOKIE + state.substring(0, STATE_NAME_LENGTH) + "=" + this.codec.seal(STATE, saved)
				+ "; Max-Age=" + STATE_MAX_AGE.toSeconds() + COOKIE_ATTRIBUTES;
		String location = authorization + (authorization.getRawQuery() == null ? "?" : "&") + "response_type=code"
				+ "&client_id=" + encode(this.clientId) + "&redirect_uri=" + encode(redirectUri) + "&scope="
				+ encode(this.scope) + "&state=" + state + "&nonce=" + nonce + "&code_challenge="
				+ ENCODER.encodeToString(sha256(verifier)) + "&code_challenge_method=S256";
		return new Verdict.Redirect(location, List.of(cookie), Verdict.Redirect.SIGN_IN);
	}

	/**
	 * The session the request's cookie holds; null when it carries none that this route
	 * sealed and has not expired.
	 */
	@Nullable Session session(RequestHeaders request) {
		long now = this.clock.instant().getEpochSecond();
		for (Crumb crumb : crumbsOf(request)) {
			if (!crumb.name().equals(SESSION_COOKIE)) {
				continue;
			}
			Map<String, Object> opened = this.codec.open(SESSION, crumb.value());
			if (opened == null) {
				log.debug("route {}: a session cookie did not open", this.routeKey);
				continue;
			}
			try {
				if (JSONObjectUtils.getLong(opened, "exp") > now) {
					Map<String, Object> claims = JSONObjectUtils.getJSONObject(opened, "claims");
					return new Session(claims == null ? Map.of() : claims);
				}
				log.debug("route {}: a session cookie expired", this.routeKey);
			}
			catch (ParseException e) {
				log.debug("route {}: a session cookie is malformed: {}", this.routeKey, e.getMessage());
			}
		}
		return null;
	}

	/** Whether the route admits the signed-in user: each claim it requires matches. */
	boolean admits(Session session) {
		for (Map.Entry<String, List<String>> claim : this.claims.entrySet()) {
			Object value = session.claims().get(claim.getKey());
			if (value == null || !OidcScheme.holds(value, claim.getValue())) {
				log.debug("route {}: the signed-in {} lacks the claim {}", this.routeKey,
						session.claims().get(JwtClaimNames.SUB), claim.getKey());
				return false;
			}
		}
		return true;
	}

	/**
	 * The pass of a request of the route: {@code base}'s, the identity headers removed
	 * and set from the session if any, and unless the route forwards its credentials the
	 * sign-in's cookies removed from {@code Cookie}, the other cookies kept.
	 */
	Verdict.Pass pass(Verdict.Pass base, RequestHeaders request, @Nullable Session session, boolean forward) {
		Set<String> strip = new HashSet<>(base.strip());
		strip.addAll(this.headers.keySet());
		Map<String, String> add = new LinkedHashMap<>(base.add());
		if (!forward) {
			List<Crumb> crumbs = crumbsOf(request);
			if (crumbs.stream().anyMatch(crumb -> crumb.name().startsWith(COOKIE_PREFIX))) {
				strip.add("cookie");
				String others = crumbs.stream()
					.filter(crumb -> !crumb.name().startsWith(COOKIE_PREFIX))
					.map(Crumb::text)
					.collect(Collectors.joining("; "));
				if (!others.isEmpty()) {
					add.put("cookie", others);
				}
			}
		}
		if (session != null) {
			this.headers.forEach((header, claim) -> {
				String value = headerValue(session.claims().get(claim));
				if (value != null) {
					add.put(header, value);
				}
			});
		}
		return new Verdict.Pass(strip, add);
	}

	/**
	 * Whether the request is a browser's navigation, worth a redirect to the provider: a
	 * {@code GET} or {@code HEAD} accepting {@code text/html}, neither an XHR nor a fetch
	 * of a script ({@code Sec-Fetch-Mode} other than {@code navigate}).
	 */
	static boolean navigation(RequestHeaders request) {
		String method = request.method();
		if (!method.equals("GET") && !method.equals("HEAD")) {
			return false;
		}
		if (!request.values("x-requested-with").isEmpty()) {
			return false;
		}
		List<String> mode = request.values("sec-fetch-mode");
		if (!mode.isEmpty() && !mode.getFirst().strip().equalsIgnoreCase("navigate")) {
			return false;
		}
		return request.values("accept")
			.stream()
			.flatMap(accept -> java.util.Arrays.stream(accept.split(",")))
			.anyMatch(type -> type.split(";")[0].strip().equalsIgnoreCase("text/html"));
	}

	/** The redirect of the sign-out, removing the session cookie. */
	private Verdict signOut(Origin origin) {
		String base = origin.uri();
		String home = base == null ? "/" : base + "/";
		IssuerKeys.Endpoints endpoints = this.issuer.endpoints();
		URI endSession = endpoints == null ? null : endpoints.endSession();
		String location = home;
		if (endSession != null && base != null) {
			location = endSession + (endSession.getRawQuery() == null ? "?" : "&") + "client_id="
					+ encode(this.clientId) + "&post_logout_redirect_uri=" + encode(home);
		}
		return new Verdict.Redirect(location, List.of(SESSION_COOKIE + "=; Max-Age=0" + COOKIE_ATTRIBUTES),
				Verdict.Redirect.SIGNED_OUT);
	}

	/**
	 * The state the request's state cookie of {@code state} holds, unexpired and equal to
	 * it; null when it carries none.
	 */
	private @Nullable Map<String, Object> state(RequestHeaders request, String state) {
		String name = STATE_COOKIE + state.substring(0, STATE_NAME_LENGTH);
		long now = this.clock.instant().getEpochSecond();
		for (Crumb crumb : crumbsOf(request)) {
			if (!crumb.name().equals(name)) {
				continue;
			}
			Map<String, Object> saved = this.codec.open(STATE, crumb.value());
			if (saved == null) {
				continue;
			}
			try {
				String held = JSONObjectUtils.getString(saved, STATE);
				if (held != null
						&& MessageDigest.isEqual(held.getBytes(StandardCharsets.UTF_8),
								state.getBytes(StandardCharsets.UTF_8))
						&& JSONObjectUtils.getLong(saved, "exp") > now) {
					return saved;
				}
			}
			catch (ParseException e) {
				// no state of this sign-in
			}
		}
		return null;
	}

	/**
	 * Exchanges the code of a callback whose state held and answers the redirect back to
	 * the page the sign-in began at, the session cookie set and the state cookie removed;
	 * forbidden when the exchange or the ID token fails, unavailable while the provider's
	 * endpoints or keys have not loaded. Off the caller: it waits for the provider.
	 */
	private Verdict signedIn(Map<String, Object> saved, String code) {
		String state;
		String nonce;
		String verifier;
		String origin;
		String target;
		try {
			state = JSONObjectUtils.getString(saved, STATE);
			nonce = JSONObjectUtils.getString(saved, "nonce");
			verifier = JSONObjectUtils.getString(saved, "verifier");
			origin = JSONObjectUtils.getString(saved, "origin");
			target = JSONObjectUtils.getString(saved, "target");
		}
		catch (ParseException e) {
			return failed("the state cookie is malformed");
		}
		IssuerKeys.Endpoints endpoints = this.issuer.endpoints();
		URI token = endpoints == null ? null : endpoints.token();
		if (endpoints == null || token == null) {
			log.debug("route {}: a code could not be exchanged: the endpoints of {} have not loaded", this.routeKey,
					this.issuer.issuer());
			return new Verdict.Unavailable(this.retryAfter);
		}
		String idToken;
		try {
			idToken = this.redeem(token, endpoints, code, origin + CALLBACK_PATH, verifier);
		}
		catch (RuntimeException e) {
			return failed("the code exchange failed: " + e.getMessage());
		}
		Verdict unloaded = this.awaitKeys(idToken);
		if (unloaded != null) {
			return unloaded;
		}
		Jwt jwt;
		try {
			jwt = this.decoder.decode(idToken);
		}
		catch (JwtValidationException e) {
			return failed("the ID token is not valid: "
					+ e.getErrors().stream().map(OAuth2Error::getDescription).collect(Collectors.joining("; ")));
		}
		catch (JwtException e) {
			return failed("the ID token is not valid: " + e.getMessage());
		}
		String held = jwt.getClaimAsString("nonce");
		if (held == null || !MessageDigest.isEqual(held.getBytes(StandardCharsets.UTF_8),
				nonce.getBytes(StandardCharsets.UTF_8))) {
			return failed("the ID token's nonce is not the sign-in's");
		}
		Map<String, Object> claims = new LinkedHashMap<>();
		jwt.getClaims().forEach((name, value) -> {
			if (this.kept.contains(name)) {
				claims.put(name, value instanceof Collection<?> values ? new ArrayList<>(values) : value);
			}
		});
		Map<String, Object> session = new LinkedHashMap<>();
		session.put("claims", claims);
		session.put("exp", this.clock.instant().plus(this.sessionMaxAge).getEpochSecond());
		String cookie = SESSION_COOKIE + "=" + this.codec.seal(SESSION, session);
		if (cookie.length() > MAX_COOKIE) {
			log.warn("route {}: the session of {} takes {} bytes, more than a cookie holds; keep fewer claims "
					+ "(claims, headers)", this.routeKey, jwt.getSubject(), cookie.length());
			return failed("the session is too large for a cookie");
		}
		return new Verdict.Redirect(origin + target,
				List.of(cookie + "; Max-Age=" + this.sessionMaxAge.toSeconds() + COOKIE_ATTRIBUTES,
						STATE_COOKIE + state.substring(0, STATE_NAME_LENGTH) + "=; Max-Age=0" + COOKIE_ATTRIBUTES),
				Verdict.Redirect.SIGNED_IN);
	}

	/**
	 * Loads the keys anew, waiting for the load, when the ID token names a key not loaded
	 * and a load may run; unavailable when none have loaded, null otherwise.
	 */
	private @Nullable Verdict awaitKeys(String idToken) {
		try {
			String keyId = JWSHeader.parse(new Base64URL(idToken.substring(0, Math.max(0, idToken.indexOf('.')))))
				.getKeyID();
			if (!this.issuer.knows(keyId) && this.issuer.mayRefresh()) {
				this.issuer.refresh().get(this.refreshWait.toNanos(), TimeUnit.NANOSECONDS);
			}
		}
		catch (ParseException e) {
			// no JWS header: the decoder refuses it, naming why
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (ExecutionException | TimeoutException e) {
			// checked against the keys loaded, if any
		}
		if (!this.issuer.loaded()) {
			log.debug("route {}: an ID token could not be checked: the keys of {} have not loaded", this.routeKey,
					this.issuer.issuer());
			return new Verdict.Unavailable(this.retryAfter);
		}
		return null;
	}

	/**
	 * The ID token the token endpoint answers the code with: the client authenticates
	 * with {@code client_secret_basic} unless the endpoint supports only
	 * {@code client_secret_post}; a public client names itself only.
	 * @throws RuntimeException naming why when the endpoint does not answer one
	 */
	private String redeem(URI token, IssuerKeys.Endpoints endpoints, String code, String redirectUri, String verifier) {
		StringBuilder form = new StringBuilder("grant_type=authorization_code&code=").append(encode(code))
			.append("&redirect_uri=")
			.append(encode(redirectUri))
			.append("&code_verifier=")
			.append(encode(verifier));
		List<String> methods = endpoints.tokenAuthMethods();
		boolean basic = !this.clientSecret.isEmpty() && (methods.isEmpty() || methods.contains("client_secret_basic")
				|| !methods.contains("client_secret_post"));
		if (!basic) {
			form.append("&client_id=").append(encode(this.clientId));
			if (!this.clientSecret.isEmpty()) {
				form.append("&client_secret=").append(encode(this.clientSecret));
			}
		}
		return this.http.post()
			.uri(token)
			.contentType(MediaType.APPLICATION_FORM_URLENCODED)
			.accept(MediaType.APPLICATION_JSON)
			.headers(headers -> {
				if (basic) {
					headers.set(HttpHeaders.AUTHORIZATION,
							"Basic " + Base64.getEncoder()
								.encodeToString((encode(this.clientId) + ":" + encode(this.clientSecret))
									.getBytes(StandardCharsets.UTF_8)));
				}
			})
			.body(form.toString())
			.exchange((request, response) -> {
				String body;
				try (InputStream in = response.getBody()) {
					byte[] read = in.readNBytes(IssuerKeys.MAX_DOCUMENT + 1);
					if (read.length > IssuerKeys.MAX_DOCUMENT) {
						throw new IllegalStateException(
								token + " answered more than " + IssuerKeys.MAX_DOCUMENT + " bytes");
					}
					body = new String(read, StandardCharsets.UTF_8);
				}
				Map<String, Object> answer;
				try {
					answer = JSONObjectUtils.parse(body);
				}
				catch (ParseException e) {
					throw new IllegalStateException(
							token + " answered " + response.getStatusCode().value() + " with no JSON object");
				}
				if (!response.getStatusCode().isSameCodeAs(HttpStatus.OK)) {
					throw new IllegalStateException(token + " answered " + response.getStatusCode().value() + " "
							+ printable(answer.get("error")));
				}
				Object idToken = answer.get("id_token");
				if (!(idToken instanceof String value)) {
					throw new IllegalStateException(token + " answered no id_token");
				}
				return value;
			});
	}

	private Verdict failed(String why) {
		log.debug("route {}: a sign-in failed: {}", this.routeKey, why);
		return new Verdict.Forbidden(Verdict.Forbidden.FAILED);
	}

	/** 32 random bytes, base64url. */
	private String random() {
		byte[] bytes = new byte[32];
		this.random.nextBytes(bytes);
		return ENCODER.encodeToString(bytes);
	}

	/**
	 * A claim as a header value: a list claim's values joined by commas, the UTF-8 bytes
	 * of the text as they go on the wire, control characters dropped; null when nothing
	 * is left.
	 */
	private static @Nullable String headerValue(@Nullable Object claim) {
		if (claim == null) {
			return null;
		}
		String text = claim instanceof Collection<?> values
				? values.stream().map(String::valueOf).collect(Collectors.joining(",")) : String.valueOf(claim);
		StringBuilder value = new StringBuilder(text.length());
		for (byte b : text.getBytes(StandardCharsets.UTF_8)) {
			int c = b & 0xff;
			if (c >= 0x20 && c != 0x7f) {
				value.append((char) c);
			}
		}
		String stripped = value.toString().strip();
		return stripped.isEmpty() ? null : stripped;
	}

	/**
	 * An error code of the provider as the debug log tells it: itself when it is one (RFC
	 * 6749 5.2), else {@code (malformed)}.
	 */
	private static String printable(@Nullable Object error) {
		return error instanceof String code && SCOPE.matcher(code).matches() ? code : "(malformed)";
	}

	/** The path of a request target: up to its query. */
	private static String pathOf(String target) {
		int query = target.indexOf('?');
		return query < 0 ? target : target.substring(0, query);
	}

	/**
	 * The parameters of the target's query, decoded; null when one is malformed or comes
	 * twice (RFC 6749 3.1).
	 */
	private static @Nullable Map<String, String> queryOf(String target) {
		int start = target.indexOf('?');
		Map<String, String> parameters = new HashMap<>();
		if (start < 0) {
			return parameters;
		}
		for (String pair : target.substring(start + 1).split("&")) {
			if (pair.isEmpty()) {
				continue;
			}
			int equals = pair.indexOf('=');
			String name;
			String value;
			try {
				name = URLDecoder.decode(equals < 0 ? pair : pair.substring(0, equals), StandardCharsets.UTF_8);
				value = equals < 0 ? "" : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
			}
			catch (IllegalArgumentException e) {
				return null;
			}
			if (parameters.put(name, value) != null) {
				return null;
			}
		}
		return parameters;
	}

	/** The cookies of the request's {@code Cookie} headers, in order. */
	private static List<Crumb> crumbsOf(RequestHeaders request) {
		List<Crumb> crumbs = new ArrayList<>();
		for (String header : request.values("cookie")) {
			for (String part : header.split(";")) {
				String text = part.strip();
				if (text.isEmpty()) {
					continue;
				}
				int equals = text.indexOf('=');
				crumbs.add(new Crumb(equals < 0 ? text : text.substring(0, equals).strip(),
						equals < 0 ? "" : text.substring(equals + 1).strip(), text));
			}
		}
		return crumbs;
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	private static byte[] sha256(String value) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII));
		}
		catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * A signed-in browser's session.
	 *
	 * @param claims the ID token's claims the session keeps
	 */
	record Session(Map<String, Object> claims) {

		Session {
			claims = Collections.unmodifiableMap(new LinkedHashMap<>(claims));
		}

	}

	/**
	 * A cookie of a {@code Cookie} header.
	 *
	 * @param text the cookie as it came, {@code name=value}
	 */
	private record Crumb(String name, String value, String text) {
	}

}
