package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class RouteAuthenticatorTest {

	private static final String ROUTE = "api.local";

	private static final Set<String> AUTHORIZATION = Set.of("authorization");

	private static final List<String> CHALLENGES = List.of("Basic realm=\"api.local\", charset=\"UTF-8\"",
			"Bearer realm=\"api.local\"", "X-API-Token realm=\"api.local\"");

	/** bcrypt at the lowest cost: the tests check the matching, not the cost. */
	private static final BCryptPasswordEncoder BCRYPT = new BCryptPasswordEncoder(4);

	private final AtomicInteger matches = new AtomicInteger();

	private final AtomicLong now = new AtomicLong();

	/** Counts every match the authenticator asks of the server's encoder. */
	private final PasswordEncoder encoder = new PasswordEncoder() {

		private final PasswordEncoder delegate = PasswordEncoders.delegating();

		@Override
		public @Nullable String encode(@Nullable CharSequence rawPassword) {
			return this.delegate.encode(rawPassword);
		}

		@Override
		public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
			RouteAuthenticatorTest.this.matches.incrementAndGet();
			return this.delegate.matches(rawPassword, encodedPassword);
		}

	};

	@ParameterizedTest
	@ValueSource(strings = { "noop", "bcrypt" })
	void eachSchemeAcceptsItsCredentialAndRefusesAnyOther(String id) {
		RouteAuthenticator authenticator = authenticator(SluiceServerProperties.Route.Auth.builder()
			.basic(Map.of("alice", secret(id, "wonderland")))
			.bearer(List.of(secret(id, "other-token"), secret(id, "bearer-token")))
			.apiToken(List.of(secret(id, "api-token")))
			.build());

		assertThat(verify(authenticator, "authorization", basic("alice:wonderland")))
			.isEqualTo(new Verdict.Pass(Set.of("authorization", "x-api-token")));
		assertThat(verify(authenticator, "authorization", "Bearer bearer-token")).isInstanceOf(Verdict.Pass.class);
		assertThat(verify(authenticator, "authorization", "bearer  bearer-token ")).as("scheme case, extra spaces")
			.isInstanceOf(Verdict.Pass.class);
		assertThat(verify(authenticator, "x-api-token", "api-token")).isInstanceOf(Verdict.Pass.class);

		assertThat(verify(authenticator)).isEqualTo(new Verdict.Unauthorized("missing", CHALLENGES));
		for (String[] refused : new String[][] { { "authorization", basic("alice:mad-hatter") },
				{ "authorization", basic("bob:wonderland") }, { "authorization", basic("alice") },
				{ "authorization", "Basic not*base64" }, { "authorization", "Bearer wrong-token" },
				{ "authorization", "Bearer " }, { "authorization", "Bearer" }, { "authorization", "Token api-token" },
				{ "authorization", "Bearer api-token" }, { "x-api-token", "wrong-token" }, { "x-api-token", " " },
				{ "x-api-token", "bearer-token" } }) {
			assertThat(verify(authenticator, refused)).as("%s: %s", refused[0], refused[1])
				.isEqualTo(new Verdict.Unauthorized("invalid", CHALLENGES));
		}
		assertThat(verify(authenticator, "authorization", "Bearer bearer-token", "authorization", "Bearer x"))
			.as("a credential header twice is refused whatever the values")
			.isEqualTo(new Verdict.Unauthorized("invalid", CHALLENGES));
		assertThat(verify(authenticator, "x-api-token", "api-token", "x-api-token", "api-token"))
			.isEqualTo(new Verdict.Unauthorized("invalid", CHALLENGES));
	}

	@Test
	void theCacheSkipsTheEncoderForAMatchedCredentialUntilItExpiresOrIsCrowdedOut() {
		RouteAuthenticator authenticator = authenticator(SluiceServerProperties.Route.Auth.builder()
			.basic(Map.of("alice", secret("bcrypt", "wonderland"), "bob", secret("bcrypt", "builder"), "carol",
					secret("bcrypt", "singer")))
			.build());

		assertThat(verify(authenticator, "authorization", basic("alice:wonderland"))).isInstanceOf(Verdict.Pass.class);
		assertThat(this.matches).as("the first check matches").hasValue(1);
		assertThat(verify(authenticator, "authorization", basic("alice:wonderland"))).isInstanceOf(Verdict.Pass.class);
		assertThat(this.matches).as("the repeat is remembered").hasValue(1);

		assertThat(verify(authenticator, "authorization", basic("alice:mad-hatter")))
			.isInstanceOf(Verdict.Unauthorized.class);
		assertThat(verify(authenticator, "authorization", basic("alice:mad-hatter")))
			.isInstanceOf(Verdict.Unauthorized.class);
		assertThat(this.matches).as("a mismatch is never remembered").hasValue(3);

		assertThat(verify(authenticator, "authorization", basic("mallory:wonderland")))
			.isInstanceOf(Verdict.Unauthorized.class);
		assertThat(this.matches).as("an unknown user is matched against another user's password all the same")
			.hasValue(4);

		this.now.addAndGet(RouteAuthenticator.CACHE_TTL.toNanos());
		assertThat(verify(authenticator, "authorization", basic("alice:wonderland"))).isInstanceOf(Verdict.Pass.class);
		assertThat(this.matches).as("expired: matched again").hasValue(5);

		// the cache holds two: a third credential crowds out the least recently used
		verify(authenticator, "authorization", basic("bob:builder"));
		verify(authenticator, "authorization", basic("alice:wonderland"));
		assertThat(this.matches).as("precondition: alice and bob remembered").hasValue(6);
		verify(authenticator, "authorization", basic("carol:singer"));
		verify(authenticator, "authorization", basic("alice:wonderland"));
		assertThat(this.matches).as("alice still remembered").hasValue(7);
		assertThat(verify(authenticator, "authorization", basic("bob:builder"))).isInstanceOf(Verdict.Pass.class);
		assertThat(this.matches).as("bob crowded out").hasValue(8);
	}

	@Test
	@Timeout(10)
	void aCheckPastTheBoundOfSlowChecksInFlightIsBusy() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		PasswordEncoder holding = new PasswordEncoder() {

			@Override
			public @Nullable String encode(@Nullable CharSequence rawPassword) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
				if (encodedPassword != null && encodedPassword.startsWith("{bcrypt}")) {
					entered.countDown();
					try {
						release.await();
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				return RouteAuthenticatorTest.this.encoder.matches(rawPassword, encodedPassword);
			}

		};
		RouteAuthenticator authenticator = new RouteAuthenticator(properties(SluiceServerProperties.Route.Auth.builder()
			.bearer(List.of(secret("bcrypt", "slow-token")))
			.apiToken(List.of(secret("sha256", "fast-token")))
			.build()), holding, 1, RouteAuthenticator.CACHE_SIZE, this.now::get);

		CompletableFuture<Verdict> inFlight = CompletableFuture
			.supplyAsync(() -> verify(authenticator, "authorization", "Bearer slow-token"));
		assertThat(entered.await(5, TimeUnit.SECONDS)).as("precondition: a slow check in flight").isTrue();

		assertThat(verify(authenticator, "authorization", "Bearer slow-token"))
			.isEqualTo(new Verdict.Busy(RouteAuthenticator.BUSY_RETRY_AFTER));
		assertThat(verify(authenticator, "x-api-token", "fast-token")).as("a cheap check is not bounded")
			.isInstanceOf(Verdict.Pass.class);

		release.countDown();
		assertThat(inFlight.get(5, TimeUnit.SECONDS)).isInstanceOf(Verdict.Pass.class);
		assertThat(verify(authenticator, "authorization", "Bearer slow-token")).as("the bound frees up")
			.isInstanceOf(Verdict.Pass.class);
	}

	@Test
	void everyEncoderIdMatchesItsSecretsAndAnEncoderFailingIsAMismatch() {
		String sha256 = HexFormat.of().formatHex(sha256("sha-token"));
		SluiceServerProperties.Route.Auth auth = SluiceServerProperties.Route.Auth.builder()
			.bearer(List.of("{sha256}" + sha256.toUpperCase(Locale.ROOT),
					"{pbkdf2}" + Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_5().encode("pbkdf2-token"),
					"{pbkdf2@SpringSecurity_v5_8}"
							+ Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_8().encode("pbkdf2-v58-token"),
					secret("bcrypt", "bcrypt-token")))
			.build();
		RouteAuthenticator authenticator = authenticator(auth);

		for (String token : List.of("sha-token", "pbkdf2-token", "pbkdf2-v58-token", "bcrypt-token")) {
			assertThat(verify(authenticator, "authorization", "Bearer " + token)).as(token)
				.isInstanceOf(Verdict.Pass.class);
		}
		PasswordEncoder failing = new PasswordEncoder() {

			@Override
			public @Nullable String encode(@Nullable CharSequence rawPassword) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
				throw new IllegalArgumentException("unreadable");
			}

		};
		assertThat(new RouteAuthenticator(properties(auth), failing, 2, RouteAuthenticator.CACHE_SIZE, this.now::get)
			.verify(route(ROUTE), headers("GET", "authorization", "Bearer sha-token")))
			.isInstanceOf(Verdict.Unauthorized.class);
	}

	@Test
	void aRouteWithoutCredentialsIsOpenAndForwardingStripsNothing() {
		RouteAuthenticator authenticator = authenticator(SluiceServerProperties.Route.Auth.builder()
			.bearer(List.of(secret("noop", "token")))
			.forwardCredentials(true)
			.build());

		assertThat(verify(authenticator, "authorization", "Bearer token")).isEqualTo(Verdict.OPEN);
		assertThat(authenticator.verify(route("other.local"), headers("GET"))).isEqualTo(Verdict.OPEN);
		assertThat(authenticator.protects(route(ROUTE))).isTrue();
		assertThat(authenticator.protects(route("other.local"))).isFalse();
		assertThat(authenticator.schemes(ROUTE)).containsExactly("bearer");
	}

	@Test
	void aPreflightPassesWithoutCredentialsOnlyWhenAllowed() {
		SluiceServerProperties.Route.Auth.Builder auth = SluiceServerProperties.Route.Auth.builder()
			.bearer(List.of(secret("noop", "token")));
		RequestHeaders preflight = headers("OPTIONS", "access-control-request-method", "POST");

		assertThat(authenticator(auth.build()).verify(route(ROUTE), preflight))
			.isInstanceOf(Verdict.Unauthorized.class);
		RouteAuthenticator allowing = authenticator(auth.allowPreflight(true).build());
		assertThat(allowing.verify(route(ROUTE), preflight)).isEqualTo(new Verdict.Pass(AUTHORIZATION));
		assertThat(allowing.verify(route(ROUTE), headers("OPTIONS"))).as("an OPTIONS that is no preflight")
			.isInstanceOf(Verdict.Unauthorized.class);
	}

	@Test
	void aRealmAndAnApiTokenHeaderOfItsOwnNameTheChallenges() {
		RouteAuthenticator authenticator = authenticator(SluiceServerProperties.Route.Auth.builder()
			.apiToken(List.of(secret("noop", "token")))
			.apiTokenHeader("X-Key")
			.realm("my \"api\"")
			.build());

		assertThat(verify(authenticator, "x-api-token", "token"))
			.isEqualTo(new Verdict.Unauthorized("missing", List.of("X-Key realm=\"my \\\"api\\\"\"")));
		assertThat(verify(authenticator, "x-key", "token")).isEqualTo(new Verdict.Pass(Set.of("x-key")));
	}

	@Test
	void aSecretWithoutAKnownWellFormedPrefixOrARouteThatCannotBeCheckedFailsTheStart() {
		for (String secret : List.of("s3cr3t", "{}s3cr3t", "{scrypt}s3cr3t", "{bcrypt}s3cr3t",
				"{bcrypt}" + BCRYPT.encode("s3cr3t").replace("$04$", "$99$"), "{sha256}s3cr3t", "{noop}")) {
			assertThatIllegalStateException().as(secret)
				.isThrownBy(() -> authenticator(
						SluiceServerProperties.Route.Auth.builder().bearer(List.of("{noop}fine", secret)).build()))
				.withMessageStartingWith("sluice.route.auth.[api.local].bearer[1] ")
				.withMessageNotContaining("s3cr3t");
		}
		assertThatIllegalStateException()
			.isThrownBy(() -> new RouteAuthenticator(SluiceServerProperties.builder()
				.route(SluiceServerProperties.Route.builder()
					.auth(Map.of("tcp:9000",
							SluiceServerProperties.Route.Auth.builder().bearer(List.of("{noop}token")).build()))
					.build())
				.build(), this.encoder))
			.withMessageStartingWith("sluice.route.auth.[tcp:9000]: a tcp route");
		assertThatIllegalStateException()
			.isThrownBy(() -> authenticator(SluiceServerProperties.Route.Auth.builder().build()))
			.withMessage("sluice.route.auth.[api.local] configures no credential (basic, bearer or api-token)");
		assertThatIllegalStateException()
			.isThrownBy(() -> authenticator(
					SluiceServerProperties.Route.Auth.builder().basic(Map.of("a:b", "{noop}secret")).build()))
			.withMessage("sluice.route.auth.[api.local].basic: a user name is empty or holds a colon");
	}

	@Test
	void routesWithCredentialsOnADataPlaneWithoutTlsAreWarnedAbout() {
		ListAppender<ILoggingEvent> events = new ListAppender<>();
		Logger logger = (Logger) LoggerFactory.getLogger(RouteAuthenticator.class);
		events.start();
		logger.addAppender(events);
		try {
			authenticator(SluiceServerProperties.Route.Auth.builder().bearer(List.of("{noop}token")).build());
			new RouteAuthenticator(SluiceServerProperties.builder()
				.dataTlsBundle("data")
				.route(SluiceServerProperties.Route.builder()
					.auth(Map.of(ROUTE,
							SluiceServerProperties.Route.Auth.builder().bearer(List.of("{noop}token")).build()))
					.build())
				.build(), this.encoder);
		}
		finally {
			logger.detachAppender(events);
		}
		assertThat(events.list).singleElement()
			.extracting(ILoggingEvent::getFormattedMessage)
			.asString()
			.startsWith("routes [api.local] require credentials, yet the data plane terminates no TLS");
	}

	private RouteAuthenticator authenticator(SluiceServerProperties.Route.Auth auth) {
		return new RouteAuthenticator(properties(auth), this.encoder, 2, 2, this.now::get);
	}

	private static SluiceServerProperties properties(SluiceServerProperties.Route.Auth auth) {
		return SluiceServerProperties.builder()
			.route(SluiceServerProperties.Route.builder().auth(Map.of(ROUTE, auth)).build())
			.build();
	}

	private static Verdict verify(RouteAuthenticator authenticator, String... headers) {
		return authenticator.verify(route(ROUTE), headers("GET", headers));
	}

	private static Router.Route route(String domain) {
		return Router.Route.builder().clientId("c").domain(domain).address("127.0.0.1:1").build();
	}

	/** A request of the method with the headers, as name / value pairs. */
	private static RequestHeaders headers(String method, String... headers) {
		return new RequestHeaders() {

			@Override
			public String method() {
				return method;
			}

			@Override
			public List<String> values(String name) {
				List<String> values = new java.util.ArrayList<>();
				for (int i = 0; i < headers.length; i += 2) {
					if (headers[i].equals(name)) {
						values.add(headers[i + 1]);
					}
				}
				return values;
			}

		};
	}

	private static String secret(String id, String value) {
		return switch (id) {
			case "noop" -> "{noop}" + value;
			case "bcrypt" -> "{bcrypt}" + BCRYPT.encode(value);
			case "sha256" -> "{sha256}" + HexFormat.of().formatHex(sha256(value));
			default -> throw new IllegalArgumentException(id);
		};
	}

	private static String basic(String credentials) {
		return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
	}

	private static byte[] sha256(String value) {
		try {
			return java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
		}
		catch (java.security.NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

}
