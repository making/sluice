package am.ik.sluice.server.auth;

import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

import am.ik.sluice.server.config.SluiceServerProperties;

import org.springframework.stereotype.Component;

/**
 * Validates the {@code Authorization: Bearer <token>} header using a constant-time
 * comparison. An empty configured token disables authentication (the behavior of the
 * original implementation).
 */
@Component
public class TokenValidator {

	private final byte[] token;

	public TokenValidator(SluiceServerProperties properties) {
		String token = resolveToken(properties);
		this.token = ("Bearer " + token).getBytes(StandardCharsets.UTF_8);
	}

	private static String resolveToken(SluiceServerProperties properties) {
		if (properties.tokenFile() != null && !properties.tokenFile().isBlank()) {
			try {
				String content = java.nio.file.Files.readString(java.nio.file.Path.of(properties.tokenFile()),
						StandardCharsets.UTF_8);
				// trailing new-lines are stripped to keep the setup foolproof
				return content.stripTrailing();
			}
			catch (Exception e) {
				throw new IllegalStateException("unable to load token file: " + properties.tokenFile(), e);
			}
		}
		return properties.token() == null ? "" : properties.token();
	}

	/**
	 * Returns {@code true} when the given header value is acceptable.
	 */
	public boolean isValid(@Nullable String authorizationHeader) {
		if (this.token.length == "Bearer ".length()) { // empty token configured
			return true;
		}
		byte[] given = authorizationHeader == null ? new byte[0] : authorizationHeader.getBytes(StandardCharsets.UTF_8);
		return MessageDigest.isEqual(this.token, given);
	}

}
