package am.ik.sluice.it;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Minimal OIDC provider backed by the JDK {@link HttpServer}. It implements just enough
 * of the authorization code flow for Spring Security's {@code oauth2Login}: discovery,
 * authorization (immediate consent redirect), token (an RS256-signed id token) and JWKS,
 * plus optionally RP-initiated logout (an end session endpoint that redirects straight
 * back).
 */
class MockOidcServer implements AutoCloseable {

	private static final Base64.Encoder URL_ENCODER = Base64.getUrlEncoder().withoutPadding();

	private final HttpServer server;

	private final RSAPrivateKey privateKey;

	private final String keyId;

	private final String clientId;

	private final boolean endSession;

	private volatile @org.jspecify.annotations.Nullable String nonce;

	private volatile Map<String, String> lastLogout = Map.of();

	MockOidcServer(String clientId) {
		this(clientId, true);
	}

	/**
	 * @param endSession whether discovery advertises an end session endpoint (some
	 * providers, Google among them, do not)
	 */
	MockOidcServer(String clientId, boolean endSession) {
		this.clientId = clientId;
		this.endSession = endSession;
		try {
			this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		KeyPairGenerator generator;
		try {
			generator = KeyPairGenerator.getInstance("RSA");
		}
		catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
		generator.initialize(2048);
		KeyPair keyPair = generator.generateKeyPair();
		this.privateKey = (RSAPrivateKey) keyPair.getPrivate();
		this.keyId = "it-key";
		this.server.createContext("/.well-known/openid-configuration", exchange -> json(exchange, discovery()));
		this.server.createContext("/oauth2/jwks", exchange -> json(exchange, jwks()));
		this.server.createContext("/oauth2/authorize", this::authorize);
		this.server.createContext("/oauth2/token", this::token);
		this.server.createContext("/oauth2/logout", this::logout);
		this.server.setExecutor(Executors.newSingleThreadExecutor());
		this.server.start();
	}

	@Override
	public void close() {
		this.server.stop(0);
	}

	String issuer() {
		return "http://127.0.0.1:" + this.server.getAddress().getPort();
	}

	/** Decoded query parameters of the last end session request, empty if none. */
	Map<String, String> lastLogout() {
		return this.lastLogout;
	}

	private String discovery() {
		Map<String, Object> claims = new LinkedHashMap<>();
		claims.put("issuer", issuer());
		claims.put("authorization_endpoint", issuer() + "/oauth2/authorize");
		claims.put("token_endpoint", issuer() + "/oauth2/token");
		claims.put("jwks_uri", issuer() + "/oauth2/jwks");
		claims.put("response_types_supported", List.of("code"));
		claims.put("subject_types_supported", List.of("public"));
		claims.put("id_token_signing_alg_values_supported", List.of("RS256"));
		claims.put("scopes_supported", List.of("openid", "email"));
		claims.put("token_endpoint_auth_methods_supported", List.of("client_secret_basic"));
		if (this.endSession) {
			claims.put("end_session_endpoint", issuer() + "/oauth2/logout");
		}
		return toJson(claims);
	}

	private String jwks() {
		var crtKey = (java.security.interfaces.RSAPrivateCrtKey) this.privateKey;
		String n = URL_ENCODER.encodeToString(toUnsignedByteArray(crtKey.getModulus()));
		String e = URL_ENCODER.encodeToString(toUnsignedByteArray(crtKey.getPublicExponent()));
		return toJson(Map.of("keys",
				List.of(Map.of("kty", "RSA", "kid", this.keyId, "alg", "RS256", "use", "sig", "n", n, "e", e))));
	}

	/**
	 * Simulates an instant user consent: redirects straight back to the client's redirect
	 * URI with an authorization code and the echoed state.
	 */
	private void authorize(HttpExchange exchange) throws IOException {
		Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
		String redirectUri = query.get("redirect_uri");
		String state = query.getOrDefault("state", "");
		// Spring Security validates the nonce claim of the id token itself
		this.nonce = query.get("nonce");
		exchange.getResponseHeaders().set("Location", redirectUri + "?code=mock-code&state=" + state);
		exchange.sendResponseHeaders(302, -1);
	}

	/**
	 * Ends the (implicit) provider session and redirects to the requested post logout
	 * redirect URI.
	 */
	private void logout(HttpExchange exchange) throws IOException {
		Map<String, String> query = new LinkedHashMap<>();
		parseQuery(exchange.getRequestURI().getRawQuery())
			.forEach((key, value) -> query.put(key, URLDecoder.decode(value, StandardCharsets.UTF_8)));
		this.lastLogout = Map.copyOf(query);
		String redirectUri = query.get("post_logout_redirect_uri");
		if (redirectUri == null) {
			exchange.sendResponseHeaders(204, -1);
			return;
		}
		exchange.getResponseHeaders().set("Location", redirectUri);
		exchange.sendResponseHeaders(302, -1);
	}

	private void token(HttpExchange exchange) throws IOException {
		Map<String, Object> claims = new LinkedHashMap<>();
		claims.put("iss", issuer());
		claims.put("sub", "console-user");
		claims.put("aud", List.of(this.clientId));
		claims.put("email", "console-user@example.com");
		if (this.nonce != null) {
			claims.put("nonce", this.nonce);
		}
		long now = System.currentTimeMillis() / 1000;
		claims.put("iat", now);
		claims.put("exp", now + 300);
		Map<String, Object> response = new LinkedHashMap<>();
		response.put("access_token", "mock-access-token");
		response.put("token_type", "Bearer");
		response.put("expires_in", 300);
		response.put("id_token", idToken(claims));
		json(exchange, toJson(response));
	}

	private String idToken(Map<String, Object> claims) {
		String header = URL_ENCODER.encodeToString(
				toJson(Map.of("alg", "RS256", "kid", this.keyId, "typ", "JWT")).getBytes(StandardCharsets.UTF_8));
		String payload = URL_ENCODER.encodeToString(toJson(claims).getBytes(StandardCharsets.UTF_8));
		String signingInput = header + "." + payload;
		try {
			Signature signature = Signature.getInstance("SHA256withRSA");
			signature.initSign(this.privateKey);
			signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
			return signingInput + "." + URL_ENCODER.encodeToString(signature.sign());
		}
		catch (NoSuchAlgorithmException | SignatureException | java.security.InvalidKeyException e) {
			throw new IllegalStateException("Failed to sign id token", e);
		}
	}

	private static void json(HttpExchange exchange, String body) throws IOException {
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(200, bytes.length);
		try (OutputStream os = exchange.getResponseBody()) {
			os.write(bytes);
		}
	}

	private static Map<String, String> parseQuery(String query) {
		Map<String, String> params = new LinkedHashMap<>();
		if (query != null && !query.isEmpty()) {
			for (String pair : query.split("&")) {
				String[] keyValue = pair.split("=", 2);
				if (keyValue.length == 2) {
					params.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return params;
	}

	private static String toJson(Map<String, Object> map) {
		StringBuilder builder = new StringBuilder("{");
		BiFunction<String, Object, String> entry = (key, value) -> quote(key) + ":" + toJsonValue(value);
		builder.append(map.entrySet()
			.stream()
			.map(e -> entry.apply(e.getKey(), e.getValue()))
			.reduce((a, b) -> a + "," + b)
			.orElse(""));
		return builder.append("}").toString();
	}

	private static String toJsonValue(Object value) {
		if (value instanceof String string) {
			return quote(string);
		}
		if (value instanceof Map<?, ?> map) {
			StringBuilder builder = new StringBuilder("{");
			builder.append(map.entrySet()
				.stream()
				.map(e -> quote(String.valueOf(e.getKey())) + ":" + toJsonValue(e.getValue()))
				.reduce((a, b) -> a + "," + b)
				.orElse(""));
			return builder.append("}").toString();
		}
		if (value instanceof List<?> list) {
			StringBuilder builder = new StringBuilder("[");
			builder.append(list.stream().map(MockOidcServer::toJsonValue).reduce((a, b) -> a + "," + b).orElse(""));
			return builder.append("]").toString();
		}
		return String.valueOf(value);
	}

	private static String quote(String value) {
		return "\"" + value + "\"";
	}

	private static byte[] toUnsignedByteArray(java.math.BigInteger value) {
		byte[] bytes = value.toByteArray();
		// strip the sign byte of a positive BigInteger when present
		return bytes[0] == 0 ? java.util.Arrays.copyOfRange(bytes, 1, bytes.length) : bytes;
	}

}
