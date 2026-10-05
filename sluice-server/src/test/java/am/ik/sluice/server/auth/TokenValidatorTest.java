package am.ik.sluice.server.auth;

import am.ik.sluice.server.config.SluiceServerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TokenValidatorTest {

	@Test
	void validTokenAccepted() {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().token("abcdefg").build());
		assertThat(validator.isValid("Bearer abcdefg")).isTrue();
	}

	@Test
	void invalidTokenRejected() {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().token("abcdefg").build());
		assertThat(validator.isValid("Bearer tuvwxyz")).isFalse();
	}

	@Test
	void missingHeaderRejectedWhenTokenConfigured() {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().token("abcdefg").build());
		assertThat(validator.isValid(null)).isFalse();
	}

	@Test
	void unsetTokenIsGeneratedAndWrittenToTemporaryFile() throws Exception {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().build());
		String logged = slugOfCapturedTokenFile();
		assertThat(logged).isNotEmpty();
		String token = java.nio.file.Files.readString(java.nio.file.Path.of(logged)).stripTrailing();
		assertThat(token).isNotBlank();
		assertThat(validator.isValid("Bearer " + token)).isTrue();
		assertThat(validator.isValid("Bearer unexpected")).isFalse();
	}

	private static String slugOfCapturedTokenFile() {
		// the generated token file path is logged; find the most recent sluice-token temp
		// file
		try (var files = java.nio.file.Files.list(java.nio.file.Path.of(System.getProperty("java.io.tmpdir")))) {
			return files.filter(p -> p.getFileName().toString().startsWith("sluice-token-"))
				.sorted(java.util.Comparator.comparingLong(p -> p.toFile().lastModified()))
				.reduce((a, b) -> b)
				.map(java.nio.file.Path::toString)
				.orElse("");
		}
		catch (Exception e) {
			return "";
		}
	}

}
