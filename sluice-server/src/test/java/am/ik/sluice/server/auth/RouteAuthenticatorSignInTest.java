package am.ik.sluice.server.auth;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.nimbusds.jwt.JWTClaimsSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import org.springframework.web.client.RestClient;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The browser sign-in of a route ({@code oidc.client-id}) against {@link MockIssuer}: the
 * redirect to the provider, the callback, the session cookie and what passes with it. The
 * time of the sessions follows {@link #now}.
 */
class RouteAuthenticatorSignInTest {

	private static final String ROUTE = "app.local";

	private static final String CLIENT_ID = "sluice-app";

	private static final String SECRET = "a-session-secret-of-32-characters!";

	private static final Origin HTTPS = new Origin(true, ROUTE);

	private static final String HTML = "text/html,application/xhtml+xml;q=0.9";

	/** Loads never due in a test but those it asks for. */
	private static final OidcIssuers.Timing TIMING = new OidcIssuers.Timing(Duration.ofSeconds(1), Duration.ofHours(1),
			Duration.ofSeconds(10));

	private final MockIssuer issuer = new MockIssuer();

	private final AtomicLong now = new AtomicLong();

	private final Instant epoch = Instant.now().truncatedTo(ChronoUnit.SECONDS);

	private final Clock clock = new Clock() {

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return RouteAuthenticatorSignInTest.this.epoch.plusNanos(RouteAuthenticatorSignInTest.this.now.get());
		}

	};

	private final List<RouteAuthenticator> authenticators = new ArrayList<>();

	private int codes;

	@AfterEach
	void close() {
		this.authenticators.forEach(RouteAuthenticator::close);
		this.issuer.close();
	}

	/**
	 * A browser's navigation is redirected to the provider with PKCE and a state cookie;
	 * the callback exchanges the code (the client authenticating with Basic) and returns
	 * to the page with the session cookie, which passes until its max age, its cookies
	 * and the client's identity headers removed and the identity set. Any node sharing
	 * the secret accepts it; another secret, another route or an altered cookie does not.
	 * A request that is no navigation is answered 401, and a bearer JWT still passes.
	 */
	@Test
	@Timeout(30)
	void aBrowserSignsInAndPassesWithItsSessionUntilItsMaxAge() throws Exception {
		RouteAuthenticator authenticator = this.authenticator(ROUTE,
				this.signIn()
					.audiences(List.of(MockIssuer.AUDIENCE))
					.headers(Map.of("X-Forwarded-Email", "email", "X-Forwarded-Groups", "groups"))
					.build(),
				SECRET);

		Verdict.Redirect redirect = (Verdict.Redirect) verify(authenticator,
				request("GET", "/page?x=1", "accept", HTML), HTTPS);
		assertThat(redirect.reason()).isEqualTo(Verdict.Redirect.SIGN_IN);
		assertThat(redirect.location()).startsWith(this.issuer.issuer() + "/authorize?tenant=t1&response_type=code&");
		Map<String, String> query = queryOf(redirect.location());
		assertThat(query).containsEntry("client_id", CLIENT_ID)
			.containsEntry("redirect_uri", "https://app.local/.sluice/oauth2/callback")
			.containsEntry("scope", "openid email")
			.containsEntry("code_challenge_method", "S256")
			.containsKeys("state", "nonce", "code_challenge");
		String stateCookie = redirect.cookies().getFirst();
		assertThat(stateCookie)
			.startsWith("__Host-sluice-state-" + requireNonNull(query.get("state")).substring(0, 12) + "=")
			.endsWith("; Max-Age=600; Path=/; Secure; HttpOnly; SameSite=Lax");

		for (RequestHeaders api : List.of(request("GET", "/page"), request("POST", "/page", "accept", HTML),
				request("GET", "/page", "accept", HTML, "x-requested-with", "XMLHttpRequest"),
				request("GET", "/page", "accept", HTML, "sec-fetch-mode", "cors"))) {
			assertThat(verify(authenticator, api, HTTPS)).as("no navigation")
				.isEqualTo(new Verdict.Unauthorized("missing",
						List.of("Bearer realm=\"app.local\"", "OIDC realm=\"app.local\"")));
		}

		String session = this.signedIn(authenticator, redirect, "/page?x=1",
				claims -> claims.claim("email", "alice@example.com").claim("groups", List.of("users", "admins")));
		Map<String, String> token = this.issuer.tokenRequest();
		assertThat(token).containsEntry("grant_type", "authorization_code")
			.containsEntry("redirect_uri", "https://app.local/.sluice/oauth2/callback")
			.containsEntry("-",
					"Basic " + Base64.getEncoder()
						.encodeToString((CLIENT_ID + ":client-secret").getBytes(StandardCharsets.UTF_8)))
			.doesNotContainKey("client_secret");
		assertThat(Base64.getUrlEncoder()
			.withoutPadding()
			.encodeToString(MessageDigest.getInstance("SHA-256")
				.digest(requireNonNull(token.get("code_verifier")).getBytes(StandardCharsets.US_ASCII))))
			.as("the verifier of the challenge")
			.isEqualTo(query.get("code_challenge"));

		RequestHeaders signedIn = request("GET", "/page", "cookie", "theme=dark; " + session, "cookie", "lang=ja",
				"x-forwarded-email", "mallory@example.com");
		assertThat(verify(authenticator, signedIn, HTTPS)).isEqualTo(
				new Verdict.Pass(java.util.Set.of("authorization", "cookie", "x-forwarded-email", "x-forwarded-groups"),
						Map.of("cookie", "theme=dark; lang=ja", "x-forwarded-email", "alice@example.com",
								"x-forwarded-groups", "users,admins")));
		assertThat(verify(authenticator,
				request("GET", "/ws", "upgrade", "websocket", "sec-fetch-mode", "websocket", "cookie", session), HTTPS))
			.as("a WebSocket opened from the page")
			.isInstanceOf(Verdict.Pass.class);
		assertThat(verify(authenticator,
				request("GET", "/ws", "upgrade", "websocket", "sec-fetch-mode", "websocket", "accept", HTML), HTTPS))
			.as("a WebSocket without a session is no navigation")
			.isInstanceOf(Verdict.Unauthorized.class);
		RouteAuthenticator otherNode = this.authenticator(ROUTE, this.signIn().build(), SECRET);
		assertThat(verify(otherNode, request("GET", "/", "cookie", session), HTTPS)).as("another node, one secret")
			.isInstanceOf(Verdict.Pass.class);
		RouteAuthenticator otherSecret = this.authenticator(ROUTE, this.signIn().build(), SECRET + "2");
		assertThat(verify(otherSecret, request("GET", "/", "accept", HTML, "cookie", session), HTTPS))
			.as("another secret")
			.isInstanceOf(Verdict.Redirect.class);
		RouteAuthenticator otherRoute = this.authenticator("other.local", this.signIn().build(), SECRET);
		assertThat(otherRoute.verify(route("other.local"), request("GET", "/", "cookie", session),
				new Origin(true, "other.local")))
			.as("another route")
			.isInstanceOf(Verdict.Unauthorized.class);
		String altered = session.substring(0, session.length() - 2)
				+ (session.charAt(session.length() - 2) == 'A' ? 'B' : 'A') + session.charAt(session.length() - 1);
		assertThat(verify(authenticator, request("GET", "/", "accept", HTML, "cookie", altered), HTTPS)).as("altered")
			.isInstanceOf(Verdict.Redirect.class);
		assertThat(verify(authenticator,
				request("GET", "/", "authorization", "Bearer " + this.issuer.token(this.clock.instant(), claims -> {
				})), HTTPS))
			.as("a bearer JWT besides")
			.isInstanceOf(Verdict.Pass.class);

		this.now.addAndGet(Duration.ofHours(1).toNanos());
		assertThat(verify(authenticator, request("GET", "/", "accept", HTML, "cookie", session), HTTPS))
			.as("past its max age")
			.isInstanceOf(Verdict.Redirect.class);
		assertThat(verify(authenticator, request("GET", "/", "cookie", session), HTTPS))
			.isInstanceOf(Verdict.Unauthorized.class);
	}

	/**
	 * A callback whose state does not hold -- no state cookie, another state, the
	 * provider's error, a state cookie past its max age -- or whose code or ID token does
	 * not -- a code the provider refuses, another nonce or audience, a token expired --
	 * fails 403. A signed-in user the claims do not admit is forbidden, not redirected; a
	 * sign-in over plaintext is forbidden, as the start warned; the sign-out removes the
	 * cookie at the provider's end session endpoint. Without a session secret a single
	 * node seals with a random one, warning that the sessions end with the start.
	 */
	@Test
	@Timeout(30)
	void aCallbackThatDoesNotHoldFailsAndAUserTheClaimsDoNotAdmitIsForbidden() throws Exception {
		ListAppender<ILoggingEvent> events = new ListAppender<>();
		Logger logger = (Logger) LoggerFactory.getLogger(RouteAuthenticator.class);
		events.start();
		logger.addAppender(events);
		RouteAuthenticator authenticator;
		try {
			authenticator = this.authenticator(ROUTE,
					this.signIn().claims(Map.of("email", List.of("*@example.com"))).build(), "");
		}
		finally {
			logger.detachAppender(events);
		}
		assertThat(events.list).extracting(ILoggingEvent::getFormattedMessage)
			.anySatisfy(line -> assertThat(line)
				.startsWith("routes [app.local] sign browsers in, yet sluice.route.session-secret is unset"))
			.anySatisfy(line -> assertThat(line).startsWith(
					"routes [app.local] sign browsers in, yet neither the data plane terminates TLS nor a trusted proxy"));
		Verdict.Redirect redirect = (Verdict.Redirect) verify(authenticator, request("GET", "/", "accept", HTML),
				HTTPS);
		String state = requireNonNull(queryOf(redirect.location()).get("state"));
		String stateCookie = cookieOf(redirect.cookies().getFirst());
		Verdict failed = new Verdict.Forbidden(Verdict.Forbidden.FAILED);

		assertThat(callback(authenticator, "code=c&state=" + state)).as("no state cookie").isEqualTo(failed);
		assertThat(callback(authenticator, "code=c&state=" + state.substring(0, 12) + "x", stateCookie))
			.as("another state")
			.isEqualTo(failed);
		assertThat(callback(authenticator, "code=c&code=d&state=" + state, stateCookie)).as("a parameter twice")
			.isEqualTo(failed);
		assertThat(callback(authenticator, "error=access_denied&state=" + state, stateCookie))
			.as("the provider's error")
			.isEqualTo(failed);
		assertThat(callback(authenticator, "code=unknown&state=" + state, stateCookie)).as("a code refused")
			.isEqualTo(failed);
		String nonce = requireNonNull(queryOf(redirect.location()).get("nonce"));
		Map<String, Consumer<JWTClaimsSet.Builder>> flawed = new LinkedHashMap<>();
		flawed.put("another nonce", claims -> claims.claim("nonce", "other"));
		flawed.put("another audience", claims -> claims.audience("other-client"));
		flawed.put("another azp", claims -> claims.claim("azp", "other-client"));
		flawed.put("expired",
				claims -> claims.expirationTime(java.util.Date.from(this.clock.instant().minusSeconds(120))));
		for (Map.Entry<String, Consumer<JWTClaimsSet.Builder>> flaw : flawed.entrySet()) {
			String code = "code-" + this.codes++;
			this.issuer.code(code, this.idToken(nonce, flaw.getValue()));
			assertThat(callback(authenticator, "code=" + code + "&state=" + state, stateCookie)).as(flaw.getKey())
				.isEqualTo(failed);
		}

		String outsider = this.signedIn(authenticator, redirect, "/",
				claims -> claims.claim("email", "mallory@example.org"));
		assertThat(verify(authenticator, request("GET", "/", "accept", HTML, "cookie", outsider), HTTPS))
			.as("signed in, not admitted")
			.isEqualTo(new Verdict.Forbidden(Verdict.Forbidden.DENIED));
		this.now.addAndGet(OidcSignIn.STATE_MAX_AGE.toNanos());
		assertThat(callback(authenticator, "code=c&state=" + state, stateCookie)).as("the state past its max age")
			.isEqualTo(failed);

		assertThat(verify(authenticator, request("GET", "/", "accept", HTML), new Origin(false, ROUTE))).as("plaintext")
			.isEqualTo(new Verdict.Forbidden(Verdict.Forbidden.INSECURE));
		assertThat(verify(authenticator, request("GET", "/.sluice/oauth2/sign-out", "cookie", outsider), HTTPS))
			.isEqualTo(new Verdict.Redirect(
					this.issuer.issuer() + "/logout?client_id=" + CLIENT_ID
							+ "&post_logout_redirect_uri=https%3A%2F%2Fapp.local%2F",
					List.of("__Host-sluice-session=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax"),
					Verdict.Redirect.SIGNED_OUT));
	}

	/**
	 * Follows the redirect's sign-in through the callback, the provider answering an ID
	 * token of the claims: the redirect back carries the session cookie and removes the
	 * state cookie.
	 * @param target the page the sign-in began at
	 * @return the session cookie, {@code name=value}
	 */
	private String signedIn(RouteAuthenticator authenticator, Verdict.Redirect redirect, String target,
			Consumer<JWTClaimsSet.Builder> claims) throws Exception {
		Map<String, String> query = queryOf(redirect.location());
		String code = "code-" + this.codes++;
		this.issuer.code(code, this.idToken(requireNonNull(query.get("nonce")), claims));
		Verdict.Redirect back = (Verdict.Redirect) callback(authenticator,
				"code=" + code + "&state=" + requireNonNull(query.get("state")),
				cookieOf(redirect.cookies().getFirst()));
		assertThat(back.reason()).isEqualTo(Verdict.Redirect.SIGNED_IN);
		assertThat(back.location()).isEqualTo("https://app.local" + target);
		assertThat(back.cookies()).hasSize(2);
		assertThat(back.cookies().getFirst()).startsWith("__Host-sluice-session=")
			.endsWith("; Max-Age=3600; Path=/; Secure; HttpOnly; SameSite=Lax");
		assertThat(back.cookies().get(1))
			.isEqualTo("__Host-sluice-state-" + requireNonNull(query.get("state")).substring(0, 12)
					+ "=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax");
		return cookieOf(back.cookies().getFirst());
	}

	private String idToken(String nonce, Consumer<JWTClaimsSet.Builder> claims) {
		return this.issuer.token(this.clock.instant(),
				((Consumer<JWTClaimsSet.Builder>) builder -> builder.audience(CLIENT_ID).claim("nonce", nonce))
					.andThen(claims));
	}

	private SluiceServerProperties.Route.Auth.Oidc.Builder signIn() {
		return SluiceServerProperties.Route.Auth.Oidc.builder()
			.issuerUri(this.issuer.issuer())
			.clientId(CLIENT_ID)
			.clientSecret("client-secret")
			.scopes(List.of("openid", "email"))
			.sessionMaxAge(Duration.ofHours(1));
	}

	private RouteAuthenticator authenticator(String key, SluiceServerProperties.Route.Auth.Oidc oidc, String secret) {
		Map<String, SluiceServerProperties.Route.Auth> auth = Map.of(key,
				SluiceServerProperties.Route.Auth.builder().oidc(oidc).build());
		RouteAuthenticator authenticator = new RouteAuthenticator(
				SluiceServerProperties.builder()
					.route(SluiceServerProperties.Route.builder().auth(auth).sessionSecret(secret).build())
					.build(),
				PasswordEncoders.delegating(), 2, RouteAuthenticator.CACHE_SIZE, this.now::get,
				RouteAuthenticatorTest.VIRTUAL, new OidcIssuers(TIMING, this.clock, this.now::get,
						RouteAuthenticatorTest.VIRTUAL, RestClient.builder()));
		this.authenticators.add(authenticator);
		return authenticator;
	}

	private static Verdict verify(RouteAuthenticator authenticator, RequestHeaders request, Origin origin)
			throws Exception {
		return authenticator.verifyAsync(route(ROUTE), request, origin).get(10, TimeUnit.SECONDS);
	}

	private static Verdict callback(RouteAuthenticator authenticator, String query, String... cookies)
			throws Exception {
		List<String> headers = new ArrayList<>();
		for (String cookie : cookies) {
			headers.add("cookie");
			headers.add(cookie);
		}
		return verify(authenticator,
				request("GET", "/.sluice/oauth2/callback?" + query, headers.toArray(String[]::new)), HTTPS);
	}

	private static Router.Route route(String domain) {
		return Router.Route.builder().clientId("c").domain(domain).address("127.0.0.1:1").build();
	}

	/** A request of the method and target with the headers, as name / value pairs. */
	private static RequestHeaders request(String method, String target, String... headers) {
		return new RequestHeaders() {

			@Override
			public String method() {
				return method;
			}

			@Override
			public String target() {
				return target;
			}

			@Override
			public List<String> values(String name) {
				List<String> values = new ArrayList<>();
				for (int i = 0; i < headers.length; i += 2) {
					if (headers[i].equals(name)) {
						values.add(headers[i + 1]);
					}
				}
				return values;
			}

		};
	}

	/** The {@code name=value} of a {@code Set-Cookie}. */
	private static String cookieOf(String setCookie) {
		return setCookie.substring(0, setCookie.indexOf(';'));
	}

	private static Map<String, String> queryOf(String location) {
		Map<String, String> query = new LinkedHashMap<>();
		for (String pair : URI.create(location).getRawQuery().split("&")) {
			int equals = pair.indexOf('=');
			query.put(pair.substring(0, equals), URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
		}
		return query;
	}

}
