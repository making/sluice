package am.ik.sluice.server.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;

/**
 * The password encoders of the server, picked by the {@code {id}} prefix a stored secret
 * carries: the console's user password and the data plane's route credentials share them.
 * {@code bcrypt}, {@code pbkdf2} (Spring Security 5.5 defaults),
 * {@code pbkdf2@SpringSecurity_v5_8}, {@code noop} (the value itself, compared in
 * constant time) and {@code sha256} (the lowercase or uppercase hex SHA-256 of the UTF-8
 * value, unsalted, compared in constant time: fit for high-entropy tokens, not for
 * passwords people choose). scrypt and argon2 need Bouncy Castle, which the server does
 * not carry: their ids are unknown.
 */
public final class PasswordEncoders {

	/** The id of the encoder new secrets are encoded with. */
	static final String BCRYPT = "bcrypt";

	/**
	 * A bcrypt hash {@link BCryptPasswordEncoder} matches: a version it knows, a cost
	 * from 4 to 31.
	 */
	private static final Pattern BCRYPT_HASH = Pattern
		.compile("\\$2[aby]?\\$(?:0[4-9]|[12]\\d|3[01])\\$[./0-9A-Za-z]{53}");

	private static final Pattern HEX = Pattern.compile("(?:[0-9a-fA-F]{2})+");

	private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-fA-F]{64}");

	/** The encoders by id, with what a well-formed secret of each looks like. */
	private static final Map<String, Encoder> ENCODERS = encoders();

	@SuppressWarnings("deprecation")
	private static Map<String, Encoder> encoders() {
		Map<String, Encoder> encoders = new LinkedHashMap<>();
		// {pbkdf2} keeps the meaning PasswordEncoderFactories gives it: the 5.5 defaults
		encoders.put(BCRYPT, new Encoder(new BCryptPasswordEncoder(), false, BCRYPT_HASH.asMatchPredicate()));
		encoders.put("pbkdf2",
				new Encoder(Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_5(), false, HEX.asMatchPredicate()));
		encoders.put("pbkdf2@SpringSecurity_v5_8",
				new Encoder(Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_8(), false, HEX.asMatchPredicate()));
		encoders.put("noop", new Encoder(new NoOpEncoder(), true, value -> !value.isEmpty()));
		encoders.put("sha256", new Encoder(new Sha256Encoder(), true, SHA256_HEX.asMatchPredicate()));
		return Map.copyOf(encoders);
	}

	private PasswordEncoders() {
	}

	/**
	 * The encoder that matches a secret by its {@code {id}} prefix and encodes new ones
	 * with bcrypt; a secret without a prefix, or with an id none of the encoders has,
	 * does not match (an {@link IllegalArgumentException}).
	 */
	public static PasswordEncoder delegating() {
		Map<String, PasswordEncoder> encoders = new LinkedHashMap<>();
		ENCODERS.forEach((id, encoder) -> encoders.put(id, encoder.encoder()));
		return new DelegatingPasswordEncoder(BCRYPT, encoders);
	}

	/**
	 * Checks that the secret carries the prefix of a known encoder and is well formed for
	 * it.
	 * @throws IllegalArgumentException naming what is wrong, never the secret
	 */
	public static void validate(String secret) {
		String id = idOf(secret);
		if (id == null) {
			throw new IllegalArgumentException("has no {id} prefix (one of " + String.join(", ", ids()) + ")");
		}
		Encoder encoder = ENCODERS.get(id);
		if (encoder == null) {
			throw new IllegalArgumentException(
					"has an unknown id {" + id + "} (one of " + String.join(", ", ids()) + ")");
		}
		if (!encoder.wellFormed().test(secret.substring(id.length() + 2))) {
			throw new IllegalArgumentException("is no well-formed {" + id + "} value");
		}
	}

	/**
	 * Whether matching against the secret is cheap -- {@code noop}, {@code sha256} --
	 * rather than a deliberately slow hash.
	 */
	static boolean cheap(String secret) {
		String id = idOf(secret);
		Encoder encoder = id == null ? null : ENCODERS.get(id);
		return encoder != null && encoder.cheap();
	}

	/** The ids, sorted. */
	static List<String> ids() {
		return ENCODERS.keySet().stream().sorted().toList();
	}

	/** The id of the secret's {@code {id}} prefix; null when it has none. */
	private static @Nullable String idOf(String secret) {
		int end = secret.indexOf('}');
		if (!secret.startsWith("{") || end < 2) {
			return null;
		}
		return secret.substring(1, end);
	}

	private record Encoder(PasswordEncoder encoder, boolean cheap, Predicate<String> wellFormed) {
	}

	/** The value itself, compared in constant time for values of one length. */
	private static final class NoOpEncoder implements PasswordEncoder {

		@Override
		public String encode(@Nullable CharSequence rawPassword) {
			return rawPassword == null ? "" : rawPassword.toString();
		}

		@Override
		public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
			if (rawPassword == null || encodedPassword == null || encodedPassword.isEmpty()) {
				return false;
			}
			return MessageDigest.isEqual(rawPassword.toString().getBytes(StandardCharsets.UTF_8),
					encodedPassword.getBytes(StandardCharsets.UTF_8));
		}

	}

	/** The hex SHA-256 of the UTF-8 value, unsalted, compared in constant time. */
	private static final class Sha256Encoder implements PasswordEncoder {

		@Override
		public String encode(@Nullable CharSequence rawPassword) {
			return HexFormat.of().formatHex(digest(rawPassword == null ? "" : rawPassword.toString()));
		}

		@Override
		public boolean matches(@Nullable CharSequence rawPassword, @Nullable String encodedPassword) {
			if (rawPassword == null || encodedPassword == null || !SHA256_HEX.matcher(encodedPassword).matches()) {
				return false;
			}
			return MessageDigest.isEqual(digest(rawPassword.toString()), HexFormat.of().parseHex(encodedPassword));
		}

		private static byte[] digest(String value) {
			try {
				return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			}
			catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException(e);
			}
		}

	}

}
