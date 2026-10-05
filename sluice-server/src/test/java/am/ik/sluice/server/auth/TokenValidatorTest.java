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
	void emptyTokenMeansNoAuth() {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().token("").build());
		assertThat(validator.isValid(null)).isTrue();
		assertThat(validator.isValid("anything")).isTrue();
	}

	@Test
	void missingHeaderRejectedWhenTokenConfigured() {
		TokenValidator validator = new TokenValidator(SluiceServerProperties.builder().token("abcdefg").build());
		assertThat(validator.isValid(null)).isFalse();
	}

}
