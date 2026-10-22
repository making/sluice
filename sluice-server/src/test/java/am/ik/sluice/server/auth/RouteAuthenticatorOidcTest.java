package am.ik.sluice.server.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

import org.springframework.web.client.RestClient;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Route credentials of an OIDC provider ({@code oidc}) against {@link MockIssuer}: the
 * time of the tokens and of the bound on the key loads follows {@link #now}.
 */
class RouteAuthenticatorOidcTest {

	private static final String ROUTE = "api.local";

	/** Loads never due in a test but those it asks for. */
	private static final OidcIssuers.Timing TIMING = new OidcIssuers.Timing(Duration.ofSeconds(1), Duration.ofHours(1),
			Duration.ofSeconds(10));

	private static final Verdict.Unauthorized REFUSED = new Verdict.Unauthorized("invalid",
			List.of("Bearer realm=\"api.local\", error=\"invalid_token\""));

	private final MockIssuer issuer = new MockIssuer();

	/** Nanoseconds past {@link #epoch}: the nano clock, and the wall clock from it. */
	private final AtomicLong now = new AtomicLong();

	private final Instant epoch = Instant.now().truncatedTo(ChronoUnit.SECONDS);

	/** Counts the wall clock's reads: a token decoded reads it. */
	private final AtomicInteger clockReads = new AtomicInteger();

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
			RouteAuthenticatorOidcTest.this.clockReads.incrementAndGet();
			return RouteAuthenticatorOidcTest.this.epoch.plusNanos(RouteAuthenticatorOidcTest.this.now.get());
		}

	};

	private final List<RouteAuthenticator> authenticators = new ArrayList<>();

	@AfterEach
	void close() {
		this.authenticators.forEach(RouteAuthenticator::close);
		this.issuer.close();
	}

	/**
	 * A route taking static tokens and the provider's, with required claims: a token
	 * passes and is remembered until its {@code exp}, static tokens still pass, and every
	 * flawed token is refused {@code invalid_token}, the debug log telling why without
	 * the token.
	 */
	@Test
	void aTokenOfTheIssuerPassesUntilItsExpAndEveryFlawedOneIsRefused() {
		JWK ec = MockIssuer.ec("ec-1");
		this.issuer.publish(ec);
		RouteAuthenticator authenticator = this.authenticator(SluiceServerProperties.Route.Auth.builder()
			.bearer(List.of("{noop}static-token"))
			.oidc(this.oidc()
				.claims(Map.of("email", List.of("*@example.com"), "groups", List.of("admins")))
				.clockSkew(Duration.ZERO)
				.build())
			.build(), TIMING);
		Instant at = this.clock.instant();
		Consumer<JWTClaimsSet.Builder> member = claims -> claims.claim("email", "alice@example.com")
			.claim("groups", List.of("users", "admins"));
		String token = this.issuer.token(at,
				member.andThen(claims -> claims.expirationTime(Date.from(at.plusSeconds(30)))));

		assertThat(verify(authenticator, "Bearer " + token)).isEqualTo(new Verdict.Pass(Set.of("authorization")));
		int reads = this.clockReads.get();
		assertThat(verify(authenticator, "Bearer " + token)).isInstanceOf(Verdict.Pass.class);
		assertThat(this.clockReads).as("the repeat is remembered, not decoded").hasValue(reads);
		assertThat(verify(authenticator, "Bearer static-token")).as("a static token goes to the static ones")
			.isInstanceOf(Verdict.Pass.class);
		assertThat(authenticator.schemes(ROUTE)).containsExactly("oidc", "bearer");
		assertThat(verify(authenticator)).as("one challenge for both bearer schemes")
			.isEqualTo(new Verdict.Unauthorized("missing", List.of("Bearer realm=\"api.local\"")));

		this.now.addAndGet(Duration.ofSeconds(30).toNanos());
		reads = this.clockReads.get();
		assertThat(verify(authenticator, "Bearer " + token)).as("at its exp, still valid (the validator's bound)")
			.isInstanceOf(Verdict.Pass.class);
		assertThat(this.clockReads.get()).as("remembered until its exp only, not the cache's 10m: decoded again")
			.isGreaterThan(reads);
		this.now.addAndGet(Duration.ofSeconds(1).toNanos());
		assertThat(verify(authenticator, "Bearer " + token)).as("past its exp").isEqualTo(REFUSED);

		Instant later = this.clock.instant();
		Map<String, String> flawed = new LinkedHashMap<>();
		flawed.put("expired",
				this.issuer.token(later, member.andThen(claims -> claims.issueTime(Date.from(later.minusSeconds(60)))
					.expirationTime(Date.from(later.minusSeconds(1))))));
		flawed.put("no exp", this.issuer.token(later, member.andThen(claims -> claims.expirationTime(null))));
		flawed.put("not yet valid", this.issuer.token(later,
				member.andThen(claims -> claims.notBeforeTime(Date.from(later.plusSeconds(60))))));
		flawed.put("audience", this.issuer.token(later, member.andThen(claims -> claims.audience("api://unrelated"))));
		flawed.put("issuer",
				this.issuer.token(later, member.andThen(claims -> claims.issuer("http://elsewhere.example"))));
		flawed.put("signature", this.issuer.token(MockIssuer.rsa("key-1"), later, member));
		flawed.put("unknown key", this.issuer.token(MockIssuer.rsa("key-9"), later, member));
		flawed.put("algorithm not allowed", this.issuer.token(ec, later, member));
		flawed.put("hmac", this.issuer.hmacToken(new byte[32], later, member));
		flawed.put("alg none",
				new PlainJWT(new JWTClaimsSet.Builder().issuer(this.issuer.issuer())
					.audience(MockIssuer.AUDIENCE)
					.expirationTime(Date.from(later.plusSeconds(60)))
					.claim("email", "alice@example.com")
					.claim("groups", List.of("admins"))
					.build()).serialize());
		flawed.put("claim value",
				this.issuer.token(later, member.andThen(claims -> claims.claim("email", "alice@example.org"))));
		flawed.put("claim missing", this.issuer.token(later, member.andThen(claims -> claims.claim("groups", null))));
		flawed.put("jws-shaped", "a.b.c");
		flawed.put("static mismatch", "other-token");

		ListAppender<ILoggingEvent> events = new ListAppender<>();
		Logger logger = (Logger) LoggerFactory.getLogger(RouteAuthenticator.class);
		Level level = logger.getLevel();
		events.start();
		logger.addAppender(events);
		logger.setLevel(Level.DEBUG);
		try {
			flawed.forEach(
					(flaw, value) -> assertThat(verify(authenticator, "Bearer " + value)).as(flaw).isEqualTo(REFUSED));
		}
		finally {
			logger.detachAppender(events);
			logger.setLevel(level);
		}
		List<String> lines = events.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
		assertThat(lines).hasSize(flawed.size())
			.allSatisfy(line -> assertThat(line).startsWith("route api.local: a ").contains(" did not match: "))
			.anySatisfy(line -> assertThat(line).contains("expired"))
			.anySatisfy(line -> assertThat(line).contains("aud"))
			.anySatisfy(line -> assertThat(line).contains("iss"))
			.anySatisfy(line -> assertThat(line).contains("Invalid signature"))
			.anySatisfy(line -> assertThat(line).contains("none"));
		for (String value : flawed.values()) {
			assertThat(lines).as("never the token").noneSatisfy(line -> assertThat(line).contains(value));
		}
	}

	/**
	 * A token naming a key not loaded loads the keys anew off the caller, at most once
	 * per {@code minRefreshInterval}; the refresh due loads a rotated key by itself, and
	 * stops with the authenticator.
	 */
	@Test
	@Timeout(30)
	void aRotatedKeyLoadsOffTheCallerOnTheFirstTokenNamingItOrWithTheRefreshDue() throws Exception {
		RouteAuthenticator authenticator = this.authenticator(oidcRoute(), TIMING);
		assertThat(this.issuer.jwksFetches()).as("loaded at the start").isEqualTo(1);
		this.issuer.rotate("key-2");
		String rotated = this.issuer.token(this.clock.instant(), claims -> {
		});

		assertThat(verify(authenticator, "Bearer " + rotated)).as("no load within the min interval").isEqualTo(REFUSED);
		assertThat(this.issuer.jwksFetches()).isEqualTo(1);
		this.now.addAndGet(TIMING.minRefreshInterval().toNanos());
		CountDownLatch held = this.issuer.hold();
		CompletableFuture<Verdict> loading = verifyAsync(authenticator, "Bearer " + rotated);
		assertThat(loading).as("the check waits for the load off the caller").isNotDone();
		held.countDown();
		assertThat(loading.get(10, TimeUnit.SECONDS)).isInstanceOf(Verdict.Pass.class);
		assertThat(this.issuer.jwksFetches()).as("one load for the key").isEqualTo(2);
		assertThat(verify(authenticator,
				"Bearer " + this.issuer.token(MockIssuer.rsa("key-3"), this.clock.instant(), claims -> {
				})))
			.as("a key still unknown right after")
			.isEqualTo(REFUSED);
		assertThat(this.issuer.jwksFetches()).isEqualTo(2);

		Duration refreshInterval = Duration.ofMillis(200);
		RouteAuthenticator refreshing = this.authenticator(oidcRoute(),
				new OidcIssuers.Timing(Duration.ofSeconds(1), refreshInterval, Duration.ofSeconds(10)));
		this.issuer.rotate("key-4");
		String fourth = this.issuer.token(this.clock.instant(), claims -> {
		});
		Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
			// within the min interval: the token loads nothing, only the refresh due does
			CompletableFuture<Verdict> verdict = verifyAsync(refreshing, "Bearer " + fourth);
			assertThat(verdict).as("checked on the caller").isDone();
			assertThat(verdict.join()).isInstanceOf(Verdict.Pass.class);
		});

		refreshing.close();
		int fetches = this.issuer.jwksFetches();
		Thread.sleep(refreshInterval.multipliedBy(5).toMillis());
		assertThat(this.issuer.jwksFetches()).as("no load after the close").isEqualTo(fetches);
	}

	/**
	 * A load that fails keeps the keys loaded before, and the next one runs
	 * {@code minRefreshInterval} after it in the background.
	 */
	@Test
	@Timeout(30)
	void aFailedLoadKeepsTheKeysAndIsRetriedInTheBackground() throws Exception {
		Duration retry = Duration.ofMillis(300);
		RouteAuthenticator authenticator = this.authenticator(oidcRoute(),
				new OidcIssuers.Timing(Duration.ofSeconds(1), Duration.ofHours(1), retry));
		String first = this.issuer.token(this.clock.instant(), claims -> {
		});
		this.issuer.fault(MockIssuer.Fault.UNAVAILABLE);
		JWK second = MockIssuer.rsa("key-2");
		ListAppender<ILoggingEvent> events = new ListAppender<>();
		Logger logger = (Logger) LoggerFactory.getLogger(IssuerKeys.class);
		events.start();
		logger.addAppender(events);
		try {
			this.now.addAndGet(retry.toNanos());
			assertThat(verify(authenticator, "Bearer " + this.issuer.token(second, this.clock.instant(), claims -> {
			}))).as("a key the failed load could not bring").isEqualTo(REFUSED);
		}
		finally {
			logger.detachAppender(events);
		}
		assertThat(this.issuer.jwksFetches()).as("precondition: a load failed").isEqualTo(2);
		assertThat(events.list).singleElement()
			.extracting(ILoggingEvent::getFormattedMessage)
			.asString()
			.contains("the keys did not refresh, the ones loaded are kept");
		assertThat(verify(authenticator, "Bearer " + first)).as("the keys loaded before")
			.isInstanceOf(Verdict.Pass.class);

		this.issuer.fault(MockIssuer.Fault.NONE);
		this.issuer.publish(second);
		String later = this.issuer.token(second, this.clock.instant(), claims -> claims.subject("bob"));
		Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
			// the nano clock stands still: no token may load the keys, only the retry due
			CompletableFuture<Verdict> verdict = verifyAsync(authenticator, "Bearer " + later);
			assertThat(verdict).as("checked on the caller").isDone();
			assertThat(verdict.join()).isInstanceOf(Verdict.Pass.class);
		});
	}

	/**
	 * An issuer whose keys do not load at the start -- down, hanging, oversized,
	 * malformed, or naming another issuer -- leaves its tokens unavailable ({@code 503}),
	 * never passed, until a later load succeeds.
	 */
	@ParameterizedTest
	@EnumSource(names = { "UNAVAILABLE", "HANGING", "OVERSIZED", "MALFORMED", "FOREIGN" })
	@Timeout(30)
	void anIssuerWhoseKeysDoNotLoadIsUnavailableUntilTheyDo(MockIssuer.Fault fault) throws Exception {
		this.issuer.fault(fault);
		ListAppender<ILoggingEvent> events = new ListAppender<>();
		Logger logger = (Logger) LoggerFactory.getLogger(IssuerKeys.class);
		events.start();
		logger.addAppender(events);
		RouteAuthenticator authenticator;
		try {
			authenticator = this.authenticator(oidcRoute(), TIMING);
		}
		finally {
			logger.detachAppender(events);
		}
		assertThat(events.list).singleElement()
			.extracting(ILoggingEvent::getFormattedMessage)
			.asString()
			.startsWith("oidc issuer " + this.issuer.issuer() + ": the keys did not load");
		String token = this.issuer.token(this.clock.instant(), claims -> {
		});
		Verdict.Unavailable unavailable = new Verdict.Unavailable(TIMING.minRefreshInterval());

		assertThat(verify(authenticator, "Bearer " + token)).isEqualTo(unavailable);
		this.issuer.fault(MockIssuer.Fault.NONE);
		assertThat(verify(authenticator, "Bearer " + token)).as("no load within the min interval")
			.isEqualTo(unavailable);
		this.now.addAndGet(TIMING.minRefreshInterval().toNanos());
		assertThat(verifyAsync(authenticator, "Bearer " + token).get(10, TimeUnit.SECONDS))
			.isInstanceOf(Verdict.Pass.class);
	}

	@Test
	void anOidcProviderMisconfiguredFailsTheStart() {
		Map<SluiceServerProperties.Route.Auth.Oidc, String> misconfigured = new LinkedHashMap<>();
		misconfigured.put(this.oidc().issuerUri("").build(), "issuer-uri is no absolute http(s) URI");
		misconfigured.put(this.oidc().issuerUri("ftp://idp.example").build(), "issuer-uri is no absolute http(s) URI");
		misconfigured.put(this.oidc().issuerUri("https://idp.example?x=1").build(),
				"issuer-uri is no absolute http(s) URI");
		misconfigured.put(this.oidc().audiences(List.of()).build(), "audiences names no audience, or a blank one");
		misconfigured.put(this.oidc().jwsAlgorithms(List.of("RS256", "HS256")).build(),
				"jws-algorithms: HS256 is none of RS256, RS384, RS512, ES256, ES384, ES512, PS256, PS384, PS512");
		misconfigured.put(this.oidc().claims(Map.of("groups", List.of())).build(),
				"claims.groups names no value, or an empty one");
		misconfigured.put(this.oidc().clockSkew(Duration.ofSeconds(-1)).build(), "clock-skew is negative");
		misconfigured.put(this.oidc().clientId("app").scopes(List.of("email")).build(),
				"scopes [email] names no openid, or a malformed scope");
		misconfigured.put(this.oidc().clientId("app").sessionMaxAge(Duration.ZERO).build(),
				"session-max-age is not positive");
		for (String header : List.of("Cookie", "X-Forwarded-For", "Authorization", "X User")) {
			misconfigured.put(this.oidc().clientId("app").headers(Map.of(header, "email")).build(),
					"headers." + header + " is no header name, or names one the relay sets or a credential header");
		}
		misconfigured.put(this.oidc().clientId("app").headers(Map.of("X-User", " ")).build(),
				"headers.X-User names no claim");
		misconfigured.forEach((oidc, message) -> assertThatIllegalStateException()
			.isThrownBy(
					() -> this.authenticator(SluiceServerProperties.Route.Auth.builder().oidc(oidc).build(), TIMING))
			.withMessage("sluice.route.auth.[api.local].oidc." + message));
		SluiceServerProperties.Route.Auth signIn = SluiceServerProperties.Route.Auth.builder()
			.oidc(this.oidc().audiences(List.of()).clientId("app").build())
			.build();
		assertThatIllegalStateException().isThrownBy(() -> this.authenticator(SluiceServerProperties.builder()
			.route(SluiceServerProperties.Route.builder()
				.auth(Map.of(ROUTE, signIn))
				.sessionSecret("x".repeat(31))
				.build())
			.build(), TIMING)).withMessage("sluice.route.session-secret is shorter than 32 characters");
		assertThatIllegalStateException()
			.isThrownBy(() -> this.authenticator(SluiceServerProperties.builder()
				.cluster(SluiceServerProperties.Cluster.builder().nodes(List.of("a", "b")).build())
				.route(SluiceServerProperties.Route.builder().auth(Map.of(ROUTE, signIn)).build())
				.build(), TIMING))
			.withMessage("routes [api.local] sign browsers in: cluster mode requires sluice.route.session-secret, "
					+ "the same on every node");
		assertThat(this.issuer.jwksFetches()).as("no load for a start that failed").isZero();
	}

	@Test
	void aGlobStarMatchesAnyRun() {
		assertThat(OidcScheme.glob("*@example.com", "alice@example.com")).isTrue();
		assertThat(OidcScheme.glob("*@example.com", "alice@example.com.evil")).isFalse();
		assertThat(OidcScheme.glob("a*b*c", "abc")).isTrue();
		assertThat(OidcScheme.glob("a*b*c", "aXbYc")).isTrue();
		assertThat(OidcScheme.glob("ab*b", "ab")).as("the parts do not overlap").isFalse();
		assertThat(OidcScheme.glob("admins", "admins")).isTrue();
		assertThat(OidcScheme.glob("admins", "admins2")).isFalse();
	}

	private SluiceServerProperties.Route.Auth.Oidc.Builder oidc() {
		return SluiceServerProperties.Route.Auth.Oidc.builder()
			.issuerUri(this.issuer.issuer())
			.audiences(List.of("api://other", MockIssuer.AUDIENCE));
	}

	private SluiceServerProperties.Route.Auth oidcRoute() {
		return SluiceServerProperties.Route.Auth.builder().oidc(this.oidc().build()).build();
	}

	private RouteAuthenticator authenticator(SluiceServerProperties.Route.Auth auth, OidcIssuers.Timing timing) {
		return this.authenticator(SluiceServerProperties.builder()
			.route(SluiceServerProperties.Route.builder().auth(Map.of(ROUTE, auth)).build())
			.build(), timing);
	}

	private RouteAuthenticator authenticator(SluiceServerProperties properties, OidcIssuers.Timing timing) {
		RouteAuthenticator authenticator = new RouteAuthenticator(properties, PasswordEncoders.delegating(), 2,
				RouteAuthenticator.CACHE_SIZE, this.now::get, RouteAuthenticatorTest.VIRTUAL, new OidcIssuers(timing,
						this.clock, this.now::get, RouteAuthenticatorTest.VIRTUAL, RestClient.builder()));
		this.authenticators.add(authenticator);
		return authenticator;
	}

	private static Verdict verify(RouteAuthenticator authenticator, String... authorization) {
		return verifyAsync(authenticator, authorization).join();
	}

	private static CompletableFuture<Verdict> verifyAsync(RouteAuthenticator authenticator, String... authorization) {
		return authenticator.verifyAsync(
				Router.Route.builder().clientId("c").domain(ROUTE).address("127.0.0.1:1").build(),
				new RequestHeaders() {

					@Override
					public String method() {
						return "GET";
					}

					@Override
					public String target() {
						return "/";
					}

					@Override
					public List<String> values(String name) {
						return name.equals("authorization") ? List.of(authorization) : List.of();
					}

				}, Origin.UNKNOWN);
	}

}
