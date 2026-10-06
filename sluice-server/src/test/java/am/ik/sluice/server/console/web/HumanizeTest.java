package am.ik.sluice.server.console.web;

import java.time.Duration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class HumanizeTest {

	@ParameterizedTest
	@CsvSource({ "0, 0 B", "1023, 1023 B", "1024, 1.0 KiB", "1536, 1.5 KiB", "104857600, 100 MiB",
			"1073741824, 1.0 GiB" })
	void bytes(long bytes, String expected) {
		assertThat(Humanize.bytes(bytes)).isEqualTo(expected);
	}

	@ParameterizedTest
	@CsvSource({ "0, 0s", "45, 45s", "723, 12m 03s", "11520, 3h 12m", "187200, 2d 04h", "-5, 0s" })
	void duration(long seconds, String expected) {
		assertThat(Humanize.duration(Duration.ofSeconds(seconds))).isEqualTo(expected);
	}

}
