package am.ik.sluice.server.auth;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import am.ik.sluice.server.config.SluiceServerProperties;

import org.springframework.stereotype.Component;

/**
 * Validates the {@code Authorization: Bearer <token>} header against each configured
 * token using a constant-time comparison. When neither {@code sluice.token} nor
 * {@code sluice.token-file} is configured, a random token is generated, written to a
 * temporary file and its path is logged.
 */
@Component
public class TokenValidator {

	private static final Logger log = LoggerFactory.getLogger(TokenValidator.class);

	/** The acceptable header values; empty = a header of any value passes. */
	private final List<byte[]> bearerValues;

	public TokenValidator(SluiceServerProperties properties) {
		List<String> tokens = resolveTokens(properties);
		this.bearerValues = tokens.stream().map(token -> ("Bearer " + token).getBytes(StandardCharsets.UTF_8)).toList();
	}

	private static List<String> resolveTokens(SluiceServerProperties properties) {
		if (properties.tokenFile() != null && !properties.tokenFile().isBlank()) {
			return readTokenFile(properties.tokenFile());
		}
		List<String> tokens = new ArrayList<>();
		for (String token : properties.token()) {
			if (!token.isBlank()) {
				tokens.add(token);
			}
		}
		if (tokens.isEmpty()) {
			if (properties.clusterEnabled()) {
				// per-node random tokens would break clients connected to every node
				throw new IllegalStateException("cluster mode requires an explicit sluice.token / sluice.token-file");
			}
			return List.of(generateToken());
		}
		// a fixed order, so every node of a cluster sends the same one to its peers
		return tokens.stream().sorted().toList();
	}

	/**
	 * Reads one token per line; an empty file leaves the tunnel open, as a blank token
	 * did.
	 */
	private static List<String> readTokenFile(String path) {
		List<String> tokens = new ArrayList<>();
		try {
			for (String line : Files.readAllLines(Path.of(path), StandardCharsets.UTF_8)) {
				// surrounding blanks are stripped to keep the setup foolproof
				String token = line.strip();
				if (!token.isEmpty()) {
					tokens.add(token);
				}
			}
		}
		catch (Exception e) {
			throw new IllegalStateException("unable to load token file: " + path, e);
		}
		return tokens.stream().sorted().toList();
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
			log.warn(
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
		if (this.bearerValues.isEmpty()) { // empty token configured
			return true;
		}
		byte[] given = authorizationHeader == null ? new byte[0] : authorizationHeader.getBytes(StandardCharsets.UTF_8);
		// every one is compared, so the time taken tells nothing of which matched
		boolean valid = false;
		for (byte[] bearerValue : this.bearerValues) {
			valid |= MessageDigest.isEqual(bearerValue, given);
		}
		return valid;
	}

	/**
	 * The {@code authorization} header value that passes {@link #isValid}: the first of
	 * the configured tokens, for peer to peer calls (the route gate propagation).
	 */
	public String authorizationHeader() {
		return this.bearerValues.isEmpty() ? "Bearer " : new String(this.bearerValues.get(0), StandardCharsets.UTF_8);
	}

}
