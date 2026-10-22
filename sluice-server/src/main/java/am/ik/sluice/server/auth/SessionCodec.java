package am.ik.sluice.server.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Base64;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.KDF;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.HKDFParameterSpec;

import com.nimbusds.jose.util.JSONObjectUtils;
import org.jspecify.annotations.Nullable;

/**
 * Seals the browser sign-in's cookies of one route: a JSON object encrypted and
 * authenticated with AES-256-GCM, under a key derived from the server's session secret
 * and the route key (HKDF-SHA256), so every node holding the secret opens what any node
 * sealed, and a cookie of one route opens on no other. The purpose (session, state) is
 * the associated data: a cookie of one purpose opens as no other. A sealed value is
 * base64url: a version byte, the 12-byte nonce, the ciphertext with its tag.
 */
final class SessionCodec {

	private static final byte VERSION = 1;

	private static final int NONCE_LENGTH = 12;

	private static final int TAG_BITS = 128;

	private static final byte[] SALT = "sluice route session".getBytes(StandardCharsets.UTF_8);

	private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

	private final SecretKey key;

	private final SecureRandom random;

	SessionCodec(byte[] secret, String routeKey, SecureRandom random) {
		try {
			this.key = KDF.getInstance("HKDF-SHA256")
				.deriveKey("AES",
						HKDFParameterSpec.ofExtract()
							.addIKM(secret)
							.addSalt(SALT)
							.thenExpand(routeKey.getBytes(StandardCharsets.UTF_8), 32));
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("deriving the session key failed", e);
		}
		this.random = random;
	}

	/** The object sealed for the purpose. */
	String seal(String purpose, Map<String, Object> payload) {
		byte[] nonce = new byte[NONCE_LENGTH];
		this.random.nextBytes(nonce);
		byte[] sealed;
		try {
			Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
			cipher.init(Cipher.ENCRYPT_MODE, this.key, new GCMParameterSpec(TAG_BITS, nonce));
			cipher.updateAAD(purpose.getBytes(StandardCharsets.UTF_8));
			sealed = cipher.doFinal(JSONObjectUtils.toJSONString(payload).getBytes(StandardCharsets.UTF_8));
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("sealing a cookie failed", e);
		}
		return ENCODER.encodeToString(
				ByteBuffer.allocate(1 + NONCE_LENGTH + sealed.length).put(VERSION).put(nonce).put(sealed).array());
	}

	/**
	 * The object sealed for the purpose; null for a value this codec did not seal for it:
	 * malformed, of another key or purpose, or altered.
	 */
	@Nullable Map<String, Object> open(String purpose, String value) {
		byte[] bytes;
		try {
			bytes = Base64.getUrlDecoder().decode(value);
		}
		catch (IllegalArgumentException e) {
			return null;
		}
		if (bytes.length < 1 + NONCE_LENGTH + TAG_BITS / 8 || bytes[0] != VERSION) {
			return null;
		}
		try {
			Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, this.key, new GCMParameterSpec(TAG_BITS, bytes, 1, NONCE_LENGTH));
			cipher.updateAAD(purpose.getBytes(StandardCharsets.UTF_8));
			byte[] opened = cipher.doFinal(bytes, 1 + NONCE_LENGTH, bytes.length - 1 - NONCE_LENGTH);
			return JSONObjectUtils.parse(new String(opened, StandardCharsets.UTF_8));
		}
		catch (GeneralSecurityException | ParseException e) {
			return null;
		}
	}

}
