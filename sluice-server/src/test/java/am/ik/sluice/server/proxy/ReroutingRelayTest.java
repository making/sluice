package am.ik.sluice.server.proxy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReroutingRelay#closesConnection}: a request or response head ends
 * its connection with a {@code close} connection option, or as HTTP/1.0 without
 * {@code keep-alive}.
 */
class ReroutingRelayTest {

	@ParameterizedTest
	@CsvSource(delimiter = '|', textBlock = """
			GET / HTTP/1.1\\r\\nHost: a\\r\\n\\r\\n                                 | false
			GET / HTTP/1.1\\r\\nHost: a\\r\\nConnection: close\\r\\n\\r\\n          | true
			GET / HTTP/1.1\\r\\nConnection: Upgrade, Close\\r\\n\\r\\n              | true
			GET / HTTP/1.0\\r\\nHost: a\\r\\n\\r\\n                                 | true
			GET / HTTP/1.0\\r\\nConnection: keep-alive\\r\\n\\r\\n                  | false
			HTTP/1.1 200 OK\\r\\nContent-Length: 0\\r\\n\\r\\n                      | false
			HTTP/1.1 200 OK\\r\\nConnection: close\\r\\n\\r\\n                      | true
			HTTP/1.0 200 OK\\r\\nContent-Length: 0\\r\\n\\r\\n                      | true
			HTTP/1.0 200 OK\\r\\nConnection: Keep-Alive\\r\\n\\r\\n                 | false
			GET /HTTP/1.0 HTTP/1.1\\r\\nX-Connection: close\\r\\n\\r\\n             | false
			""")
	void aHeadClosesItsConnectionWithACloseOptionOrAsHttp10WithoutKeepAlive(String head, boolean closes) {
		byte[] bytes = head.replace("\\r\\n", "\r\n").getBytes(StandardCharsets.ISO_8859_1);
		assertThat(ReroutingRelay.closesConnection(bytes)).isEqualTo(closes);
	}

}
