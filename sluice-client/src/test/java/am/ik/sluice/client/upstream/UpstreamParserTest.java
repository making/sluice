package am.ik.sluice.client.upstream;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class UpstreamParserTest {

	@Test
	void parsesKeyValuePairs() {
		assertThat(UpstreamParser.parse("example.com=http://127.0.0.1:3000")).containsEntry("example.com",
				"http://127.0.0.1:3000");
	}

	@Test
	void parsesMultipleEntriesPreservingOrder() {
		Map<String, String> result = UpstreamParser.parse("a.com=http://1.1.1.1,b.com=https://2.2.2.2");
		assertThat(result).containsExactly(entry("a.com", "http://1.1.1.1"), entry("b.com", "https://2.2.2.2"));
	}

	@Test
	void completesMissingScheme() {
		assertThat(UpstreamParser.parse("a.com=1.2.3.4:8080")).containsEntry("a.com", "http://1.2.3.4:8080");
		assertThat(UpstreamParser.parse("a.com=https://1.2.3.4")).containsEntry("a.com", "https://1.2.3.4");
	}

	@Test
	void supportsCatchAllEntry() {
		assertThat(UpstreamParser.parse("http://1.2.3.4")).containsEntry("", "http://1.2.3.4");
	}

	@Test
	void trimsWhitespace() {
		assertThat(UpstreamParser.parse(" a.com = http://1.2.3.4 ")).containsEntry("a.com", "http://1.2.3.4");
	}

	@Test
	void blankInputYieldsEmptyMap() {
		assertThat(UpstreamParser.parse(null)).isEmpty();
		assertThat(UpstreamParser.parse("")).isEmpty();
	}

}
