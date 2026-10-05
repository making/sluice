package am.ik.sluice.server.auth;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;

import am.ik.sluice.server.config.SluiceServerProperties;

import org.springframework.stereotype.Component;

/**
 * Validates the {@code Authorization: Bearer <token>} header using a constant-time
 * comparison. When neither {@code sluice.token} nor {@code sluice.token-file} is
 * configured, a random token is generated, written to a temporary file and its path is
 * logged.
 */
@Component
public class TokenValidator {

	private static final Logger log = LoggerFactory.getLogger(TokenValidator.class);

	private final byte[] token;

	public TokenValidator(SluiceServerProperties properties) {
		String token = resolveToken(properties);
		this.token = ("Bearer " + token).getBytes(StandardCharsets.UTF_8);
	}

	private static String resolveToken(SluiceServerProperties properties) {
		if (properties.tokenFile() != null && !properties.tokenFile().isBlank()) {
			try {
				String content = Files.readString(Path.of(properties.tokenFile()), StandardCharsets.UTF_8);
				// trailing new-lines are stripped to keep the setup foolproof
				return content.stripTrailing();
			}
			catch (Exception e) {
				throw new IllegalStateException("unable to load token file: " + properties.tokenFile(), e);
			}
		}
		String token = properties.token();
		if (token == null || token.isBlank()) {
			return generateToken();
		}
		return token;
	}

	/**
	 * Generates a random token, persists it to a temporary file and logs the file path so
	 * clients can pick it up.
	 */
	private static String generateToken() {
		byte[] raw = new byte[16];
		new SecureRandom().nextBytes(raw);
		String token = HexFormat.of().formatHex(raw);
		try {
			Path file = Files.createTempFile("sluice-token-", ".txt");
			Files.writeString(file, token, StandardCharsets.UTF_8);
			log.info(
					"No token configured. Generated token is written to: {}. Reuse it by starting with --sluice.token-file={}",
					file.toAbsolutePath(), file.toAbsolutePath());
		}
		catch (Exception e) {
			log.warn("Failed to write the generated token to a temporary file", e);
		}
		log.trace("Generated sluice token: {}", token);
		return token;
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
