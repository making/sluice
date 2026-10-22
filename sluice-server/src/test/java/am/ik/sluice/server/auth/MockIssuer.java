package am.ik.sluice.server.auth;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * An OIDC issuer for the tests: its discovery document and JWKS over HTTP on the
 * loopback, the signed access tokens of its keys, and a token endpoint answering the
 * codes it was told of ({@link #code}) with their ID tokens. It counts the fetches and
 * can fail them in the ways an issuer does ({@link Fault}).
 */
final class MockIssuer implements AutoCloseable {

	/** The audience of {@link #token}s. */
	static final String AUDIENCE = "api://sluice";

	/** How the issuer answers the fetches. */
	enum Fault {

		/** It answers. */
		NONE,

		/** Every document is a {@code 503}. */
		UNAVAILABLE,

		/** The JWKS never answers. */
		HANGING,

		/** The JWKS is larger than {@link IssuerKeys#MAX_DOCUMENT}. */
		OVERSIZED,

		/** The JWKS is no JSON. */
		MALFORMED,

		/** The discovery document names another issuer. */
		FOREIGN

	}

	private final HttpServer server;

	private final AtomicInteger jwksFetches = new AtomicInteger();

	private final CountDownLatch closed = new CountDownLatch(1);

	private volatile List<JWK> published;

	private volatile JWK signing;

	private volatile Fault fault = Fault.NONE;

	/** Held, the JWKS answers once it is counted down. */
	private volatile CountDownLatch held = new CountDownLatch(0);

	/** The ID tokens the token endpoint answers, by code. */
	private final Map<String, String> codes = new java.util.concurrent.ConcurrentHashMap<>();

	/** The last token request: its form, and its Authorization header under {@code -}. */
	private volatile Map<String, String> tokenRequest = Map.of();

	MockIssuer() {
		this.signing = rsa("key-1");
		this.published = List.of(this.signing);
		try {
			this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		this.server.createContext("/.well-known/openid-configuration", this::discovery);
		this.server.createContext("/jwks", this::jwks);
		this.server.createContext("/token", this::token);
		this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
		this.server.start();
	}

	@Override
	public void close() {
		this.closed.countDown();
		this.held.countDown();
		this.server.stop(0);
	}

	String issuer() {
		return "http://127.0.0.1:" + this.server.getAddress().getPort();
	}

	int jwksFetches() {
		return this.jwksFetches.get();
	}

	void fault(Fault fault) {
		this.fault = fault;
	}

	/** The token endpoint answers the code with the ID token, once. */
	void code(String code, String idToken) {
		this.codes.put(code, idToken);
	}

	/**
	 * The last token request's form parameters, decoded, and its Authorization header
	 * under {@code -}.
	 */
	Map<String, String> tokenRequest() {
		return this.tokenRequest;
	}

	/**
	 * Holds the JWKS answers until the latch returned is counted down.
	 */
	CountDownLatch hold() {
		CountDownLatch latch = new CountDownLatch(1);
		this.held = latch;
		return latch;
	}

	/**
	 * Signs with a new RSA key from now on, published in place of the ones before.
	 * @return the new key
	 */
	JWK rotate(String keyId) {
		JWK key = rsa(keyId);
		this.signing = key;
		this.published = List.of(key);
		return key;
	}

	/** Publishes the key besides the ones published. */
	void publish(JWK key) {
		List<JWK> keys = new ArrayList<>(this.published);
		keys.add(key);
		this.published = List.copyOf(keys);
	}

	static JWK rsa(String keyId) {
		try {
			return new RSAKeyGenerator(2048).keyID(keyId).generate();
		}
		catch (JOSEException e) {
			throw new IllegalStateException(e);
		}
	}

	static JWK ec(String keyId) {
		try {
			return new ECKeyGenerator(Curve.P_256).keyID(keyId).generate();
		}
		catch (JOSEException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * A token of the signing key: issued now for {@link #AUDIENCE}, expiring in five
	 * minutes, with the claims as the customizer leaves them.
	 */
	String token(Instant now, Consumer<JWTClaimsSet.Builder> claims) {
		return token(this.signing, now, claims);
	}

	/** A token of the key, as {@link #token(Instant, Consumer)}. */
	String token(JWK key, Instant now, Consumer<JWTClaimsSet.Builder> claims) {
		JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder().issuer(issuer())
			.audience(List.of(AUDIENCE))
			.subject("alice")
			.issueTime(java.util.Date.from(now))
			.expirationTime(java.util.Date.from(now.plusSeconds(300)));
		claims.accept(builder);
		try {
			if (key instanceof ECKey ec) {
				SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(key.getKeyID()).build(),
						builder.build());
				jwt.sign(new ECDSASigner(ec));
				return jwt.serialize();
			}
			SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(),
					builder.build());
			jwt.sign(new RSASSASigner((RSAKey) key));
			return jwt.serialize();
		}
		catch (JOSEException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * A token of the claims {@link #token} would carry, signed HS256 with the secret
	 * under the signing key's id.
	 */
	String hmacToken(byte[] secret, Instant now, Consumer<JWTClaimsSet.Builder> claims) {
		JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder().issuer(issuer())
			.audience(List.of(AUDIENCE))
			.subject("alice")
			.issueTime(java.util.Date.from(now))
			.expirationTime(java.util.Date.from(now.plusSeconds(300)));
		claims.accept(builder);
		try {
			SignedJWT jwt = new SignedJWT(
					new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(this.signing.getKeyID()).build(), builder.build());
			jwt.sign(new MACSigner(secret));
			return jwt.serialize();
		}
		catch (JOSEException e) {
			throw new IllegalStateException(e);
		}
	}

	private void discovery(HttpExchange exchange) throws IOException {
		if (this.fault == Fault.UNAVAILABLE) {
			answer(exchange, 503, "{}");
			return;
		}
		String issuer = this.fault == Fault.FOREIGN ? "http://elsewhere.example" : issuer();
		answer(exchange, 200,
				JSONObjectUtils.toJSONString(Map.of("issuer", issuer, "jwks_uri", issuer() + "/jwks",
						"authorization_endpoint", issuer() + "/authorize?tenant=t1", "token_endpoint",
						issuer() + "/token", "end_session_endpoint", issuer() + "/logout")));
	}

	/**
	 * Answers a code it was told of with its ID token, once; any other with
	 * {@code invalid_grant}.
	 */
	private void token(HttpExchange exchange) throws IOException {
		Map<String, String> form = new java.util.LinkedHashMap<>();
		for (String pair : new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).split("&")) {
			int equals = pair.indexOf('=');
			form.put(java.net.URLDecoder.decode(pair.substring(0, equals), StandardCharsets.UTF_8),
					java.net.URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
		}
		String authorization = exchange.getRequestHeaders().getFirst("Authorization");
		if (authorization != null) {
			form.put("-", authorization);
		}
		this.tokenRequest = Map.copyOf(form);
		String idToken = this.codes.remove(form.getOrDefault("code", ""));
		if (idToken == null) {
			answer(exchange, 400, "{\"error\":\"invalid_grant\"}");
			return;
		}
		answer(exchange, 200, JSONObjectUtils.toJSONString(
				Map.of("access_token", "opaque", "token_type", "Bearer", "expires_in", 300, "id_token", idToken)));
	}

	private void jwks(HttpExchange exchange) throws IOException {
		this.jwksFetches.incrementAndGet();
		try {
			this.held.await();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		switch (this.fault) {
			case UNAVAILABLE -> answer(exchange, 503, "{}");
			case HANGING -> {
				try {
					this.closed.await();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				exchange.close();
			}
			case OVERSIZED ->
				answer(exchange, 200, "{\"keys\":[],\"padding\":\"" + "x".repeat(IssuerKeys.MAX_DOCUMENT) + "\"}");
			case MALFORMED -> answer(exchange, 200, "<html>");
			default -> answer(exchange, 200, new JWKSet(this.published).toString(true));
		}
	}

	private static void answer(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(bytes);
		}
	}

}
