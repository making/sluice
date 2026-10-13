package am.ik.sluice.server.proxy;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
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

}
