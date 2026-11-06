package am.ik.sluice.client.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The header edits of a route, from its args over those of the filter's definition, and
 * the ones refused at the start.
 */
class HeadersFilterTest {

	private static final String SECRET = "sk-secret";

	private static UpstreamFilter filter(Map<String, String> defaults, Map<String, String> args) {
		return Filters.builder()
			.definitions(Map.of(HeadersFilter.NAME, FilterConfig.builder().args(defaults).build()))
			.meters(new SimpleMeterRegistry())
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", List.of(FilterConfig.builder().name(HeadersFilter.NAME).args(args).build()));
	}

	@Test
	void everyExchangeOfTheRouteCarriesTheEdits() throws Exception {
		UpstreamFilter filter = filter(Map.of("response.remove.server", "", "response.remove.x-powered-by", "true"),
				Map.of("request.set.x-api-key", SECRET, "request.add.X-Trace", "1", "request.remove.cookie", "true",
						"response.set.Strict-Transport-Security", "max-age=63072000", "response.remove.x-powered-by",
						"false", "response.add.vary", "origin"));
		String request = "GET / HTTP/1.1\r\nHost: a\r\nX-API-Key: client\r\nx-trace: 0\r\nCookie: c\r\n\r\n";
		String response = "HTTP/1.1 200 OK\r\nServer: s\r\nX-Powered-By: p\r\nVary: accept\r\nContent-Length: 1\r\n\r\nx";

		FilterTests.Exchanged exchanged = FilterTests.exchange(filter, request + request,
				(response + response).getBytes(StandardCharsets.US_ASCII), 7);

		String edited = "GET / HTTP/1.1\r\nHost: a\r\nx-trace: 0\r\nx-api-key: " + SECRET + "\r\nX-Trace: 1\r\n\r\n";
		assertThat(exchanged.upstream()).isEqualTo(edited + edited);
		String answered = "HTTP/1.1 200 OK\r\nX-Powered-By: p\r\nVary: accept\r\nContent-Length: 1\r\n"
				+ "Strict-Transport-Security: max-age=63072000\r\nvary: origin\r\n\r\nx";
		assertThat(exchanged.relayed()).isEqualTo(answered + answered);
	}

	@Test
	void anEditAHeadCannotTakeStopsTheStart() {
		List<Map<String, String>> refused = List.of(Map.of("request.set.content-length", "1"),
				Map.of("response.remove.transfer-encoding", "true"), Map.of("request.add.connection", "close"),
				Map.of("request.set.Host", "b"), Map.of("response.set.x-api-key", SECRET + "\r\nx: y"),
				Map.of("request.set.x-api-key", SECRET + "あ"), Map.of("request.set.x api", SECRET),
				// an environment variable's X_API_KEY
				Map.of("request.set.x.api.key", SECRET), Map.of("request.put.x-api-key", SECRET),
				Map.of("x-api-key", SECRET), Map.of("request.set.", SECRET), Map.of("response.remove.server", "yes"),
				Map.of("request.set.X-Api-Key", SECRET, "request.set.x-api-key", SECRET));
		for (Map<String, String> args : refused) {
			assertThatThrownBy(() -> filter(Map.of(), args)).as(args.keySet().toString())
				.isInstanceOf(IllegalArgumentException.class)
				.message()
				.doesNotContain(SECRET);
		}
	}

}
