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
 * What a browser's preflights and requests get from an allowed origin and from another,
 * and the settings refused at the start.
 */
class CorsFilterTest {

	private static final String UPSTREAM = "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nVary: accept\r\n"
			+ "Content-Length: 0\r\n\r\n";

	private static UpstreamFilter filter(Map<String, String> args) {
		return Filters.builder()
			.meters(new SimpleMeterRegistry())
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", List.of(FilterConfig.builder().name(CorsFilter.NAME).args(args).build()));
	}

	private static FilterTests.Exchanged exchange(UpstreamFilter filter, String method, String origin, String... lines)
			throws Exception {
		String request = method + " /api HTTP/1.1\r\nHost: api.test\r\nOrigin: " + origin + "\r\n"
				+ String.join("", List.of(lines).stream().map(line -> line + "\r\n").toList()) + "\r\n";
		return FilterTests.exchange(filter, request, UPSTREAM.getBytes(StandardCharsets.US_ASCII), 7);
	}

	@Test
	void anAllowedOriginIsAnsweredAndAnotherPassesAsItIs() throws Exception {
		UpstreamFilter filter = filter(Map.of("allowed-origins", "https://app.test, http://localhost:5173",
				"allowed-methods", "GET,PUT", "allowed-headers", "Content-Type,X-Trace", "exposed-headers", "X-Total",
				"allow-credentials", "true", "max-age", "30m"));
		String vary = "Vary: Origin, Access-Control-Request-Method, Access-Control-Request-Headers\r\n";

		FilterTests.Exchanged preflight = exchange(filter, "OPTIONS", "https://APP.test",
				"Access-Control-Request-Method: PUT", "Access-Control-Request-Headers: content-type,x-trace");
		assertThat(preflight.upstream()).isEmpty();
		assertThat(preflight.relayed()).isEqualTo("HTTP/1.1 204 No Content\r\n" + vary
				+ "Access-Control-Allow-Origin: https://APP.test\r\nAccess-Control-Allow-Methods: GET, PUT\r\n"
				+ "Access-Control-Allow-Headers: content-type, x-trace\r\nAccess-Control-Allow-Credentials: true\r\n"
				+ "Access-Control-Max-Age: 1800\r\nConnection: close\r\n\r\n");
		FilterTests.Exchanged refused = exchange(filter, "OPTIONS", "https://app.test",
				"Access-Control-Request-Method: DELETE");
		assertThat(refused.upstream()).isEmpty();
		assertThat(refused.relayed())
			.isEqualTo("HTTP/1.1 403 Forbidden\r\n" + vary + "Content-Length: 0\r\nConnection: close\r\n\r\n");
		String passed = "OPTIONS /api HTTP/1.1\r\nHost: api.test\r\nOrigin: https://evil.test\r\n"
				+ "Access-Control-Request-Method: PUT\r\n\r\n";
		FilterTests.Exchanged another = FilterTests.exchange(filter, passed,
				UPSTREAM.getBytes(StandardCharsets.US_ASCII), 7);
		assertThat(another.upstream()).isEqualTo(passed);
		assertThat(another.relayed()).isEqualTo(UPSTREAM.replace("\r\n\r\n", "\r\nVary: Origin\r\n\r\n"));

		FilterTests.Exchanged request = exchange(filter, "PUT", "http://localhost:5173", "Content-Length: 0");
		assertThat(request.relayed()).isEqualTo("HTTP/1.1 200 OK\r\nVary: accept\r\nContent-Length: 0\r\n"
				+ "Access-Control-Allow-Origin: http://localhost:5173\r\nAccess-Control-Allow-Credentials: true\r\n"
				+ "Access-Control-Expose-Headers: X-Total\r\nVary: Origin\r\n\r\n");
		assertThat(exchange(filter, "GET", "https://evil.test").relayed())
			.isEqualTo(UPSTREAM.replace("\r\n\r\n", "\r\nVary: Origin\r\n\r\n"));
	}

	@Test
	void anyOriginWithoutCredentialsIsAnsweredWithTheWildcard() throws Exception {
		UpstreamFilter filter = filter(Map.of("allowed-origins", "*", "allowed-methods", "*", "allowed-headers", "*"));

		assertThat(exchange(filter, "OPTIONS", "https://any.test", "Access-Control-Request-Method: PATCH",
				"Access-Control-Request-Headers: X-Anything")
			.relayed())
			.contains("Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: PATCH\r\n"
					+ "Access-Control-Allow-Headers: X-Anything\r\nConnection: close\r\n\r\n");
		assertThat(exchange(filter, "GET", "https://any.test").relayed())
			.isEqualTo("HTTP/1.1 200 OK\r\nVary: accept\r\nContent-Length: 0\r\nAccess-Control-Allow-Origin: *\r\n"
					+ "Vary: Origin\r\n\r\n");
	}

	@Test
	void whatBrowsersRejectStopsTheStart() {
		List<Map<String, String>> refused = List.of(Map.of(),
				Map.of("allowed-origins", "*", "allow-credentials", "true"),
				Map.of("allowed-origins", "https://app.test/"), Map.of("allowed-origins", "app.test"),
				Map.of("allowed-origins", "*,https://app.test"),
				Map.of("allowed-origins", "https://app.test", "origins", "x"),
				Map.of("allowed-origins", "https://app.test", "allow-credentials", "yes"),
				Map.of("allowed-origins", "https://app.test", "allowed-headers", "x trace"),
				Map.of("allowed-origins", "https://app.test", "exposed-headers", "*", "allow-credentials", "true"),
				Map.of("allowed-origins", "https://app.test", "max-age", "-1"));
		for (Map<String, String> args : refused) {
			assertThatThrownBy(() -> filter(args)).as(args.toString()).isInstanceOf(IllegalArgumentException.class);
		}
	}

}
