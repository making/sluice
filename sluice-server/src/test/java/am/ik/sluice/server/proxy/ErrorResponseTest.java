package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;

import com.samskivert.mustache.Mustache;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorResponseTest {

	private final ErrorResponse errorResponse = new ErrorResponse(
			Mustache.compiler().withLoader(name -> new StringReader("")));

	@Test
	void theConnectionLimitIsAPrefaceAndAGoAwayProcessingNoStream() {
		byte[] debug = "connection limit".getBytes(StandardCharsets.US_ASCII);
		String expected =
				// SETTINGS: empty, no flags, stream 0
				"000000" + "04" + "00" + "00000000"
				// GOAWAY: 8 + 16 bytes, stream 0; last stream id 0, NO_ERROR, the
				// debug data
						+ "000018" + "07" + "00" + "00000000" + "00000000" + "00000000"
						+ HexFormat.of().formatHex(debug);
		assertThat(HexFormat.of().formatHex(this.errorResponse.connectionLimit())).isEqualTo(expected);
	}

	@Test
	void theBusyAnswerIsA503WithRetryAfterInWholeSeconds() {
		ConnectionHeadParser.Head head = head("GET / HTTP/1.1\r\nHost: a.local\r\n\r\n");
		assertThat(new String(this.errorResponse.busy(head, Duration.ofMillis(7900)), StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=utf-8\r\n"
					+ "Content-Length: 0\r\nCache-Control: no-store\r\nRetry-After: 7\r\nConnection: close\r\n\r\n");
	}

	@Test
	void aHeadThatIsNoHttpRequestGetsNoBusyAnswer() {
		ConnectionHeadParser.Head head = head("*1\r\n$4\r\nPING\r\n");
		assertThat(this.errorResponse.busy(head, Duration.ofSeconds(7))).isEmpty();
	}

	private static ConnectionHeadParser.Head head(String bytes) {
		return new ConnectionHeadParser(64 * 1024)
			.parse(new ByteArrayInputStream(bytes.getBytes(StandardCharsets.US_ASCII)))
			.orElseThrow();
	}

}
