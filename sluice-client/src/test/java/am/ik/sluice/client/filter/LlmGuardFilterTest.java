package am.ik.sluice.client.filter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.util.unit.DataSize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The models and output tokens a request of an LLM API may ask for: what is past them
 * never reaches the upstream, whatever its framing and split.
 */
class LlmGuardFilterTest {

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	private static final Map<String, String> LIMITS = Map.of("models", "claude-sonnet-*,gpt-4o", "max-output-tokens",
			"1024");

	private record Refused(String path, String body, int status, String answer, String outcome) {
	}

	private static String anthropic(String type, String message) {
		return "{\"type\":\"error\",\"error\":{\"type\":\"" + type + "\",\"message\":\"" + message + "\"}}";
	}

	private static String openAi(@Nullable String param, String message) {
		return "{\"error\":{\"message\":\"" + message + "\",\"type\":\"invalid_request_error\",\"param\":"
				+ (param == null ? "null" : "\"" + param + "\"") + ",\"code\":null}}";
	}

	private static List<Refused> refusedCases() {
		return List
			.of(new Refused("/v1/messages", "{\"model\":\"claude-opus-4-1\",\"max_tokens\":10,\"stream\":true}", 400,
					anthropic("invalid_request_error", "model claude-opus-4-1 is not allowed on this route"), "model"),
					new Refused("/v1/chat/completions", "{\"model\":\"gpt-4o-mini\",\"max_completion_tokens\":10}", 400,
							openAi("model", "model gpt-4o-mini is not allowed on this route"), "model"),
					new Refused("/v1/messages", "{\"max_tokens\":10}", 400,
							anthropic("invalid_request_error", "model is required on this route"), "model"),
					new Refused("/v1/messages", "{\"model\":\"claude-sonnet-4\",\"max_tokens\":4096,\"stream\":true}",
							400, anthropic("invalid_request_error", "max_tokens 4096 is past this route's 1024"),
							"max_tokens"),
					new Refused("/v1/chat/completions",
							"{\"model\":\"gpt-4o\",\"max_tokens\":10,\"max_completion_tokens\":2e3,\"stream\":true}",
							400, openAi("max_completion_tokens", "max_completion_tokens 2e3 is past this route's 1024"),
							"max_tokens"),
					new Refused("/v1/responses", "{\"model\":\"gpt-4o\",\"max_output_tokens\":null}", 400,
							openAi("max_output_tokens", "max_output_tokens up to 1024 is required on this route"),
							"max_tokens"),
					new Refused("/v1/completions", "{\"model\":\"gpt-4o\",\"max_tokens\":\"10\"}", 400,
							openAi("max_tokens", "max_tokens must be a number on this route"), "max_tokens"),
					new Refused("/v1/messages",
							"{\"model\":\"claude-sonnet-4\",\"model\":\"claude-opus-4-1\",\"max_tokens\":10}", 400,
							anthropic("invalid_request_error",
									"the request body is not a JSON object this route can read"),
							"unreadable"),
					new Refused("/v1/messages", "{\"model\":\"claude-sonnet-4\",\"max_tokens\":10} {}", 400,
							anthropic("invalid_request_error",
									"the request body is not a JSON object this route can read"),
							"unreadable"),
					new Refused("/v1/messages", "{\"model\":\"claude-sonnet-4\",\"max_tokens\":10", 400,
							anthropic("invalid_request_error",
									"the request body is not a JSON object this route can read"),
							"unreadable"),
					// the spellings an upstream may route as the API
					new Refused("/V1//messages/;x?beta=true", "{\"model\":\"claude-opus-4-1\",\"max_tokens\":10}", 400,
							anthropic("invalid_request_error", "model claude-opus-4-1 is not allowed on this route"),
							"model"),
					new Refused("/v1/x/../%6Dessages", "{\"model\":\"claude-opus-4-1\",\"max_tokens\":10}", 400,
							anthropic("invalid_request_error", "model claude-opus-4-1 is not allowed on this route"),
							"model"));
	}

	@Test
	void aRequestPastTheLimitsNeverReachesTheUpstream() throws Exception {
		for (Refused refused : refusedCases()) {
			for (boolean chunked : new boolean[] { false, true }) {
				for (int segment : new int[] { 1, 7, 64 * 1024 }) {
					SimpleMeterRegistry registry = new SimpleMeterRegistry();
					String request = chunked ? chunked(refused.path(), refused.body())
							: sized(refused.path(), refused.body());

					FilterTests.Exchanged exchanged = FilterTests.exchange(guard(registry, LIMITS), request,
							OK.getBytes(StandardCharsets.US_ASCII), segment);

					String as = refused.path() + " " + refused.body() + (chunked ? " chunked" : "") + " in " + segment;
					assertThat(exchanged.upstream()).as(as).isEmpty();
					assertThat(exchanged.relayed()).as(as).isEqualTo(answer(refused.status(), refused.answer()));
					assertThat(FilterTests.counters(registry)).as(as)
						.isEqualTo(Map.of("llm.guard.requests " + api(refused.path()) + " " + refused.outcome(), 1L));
				}
			}
		}
	}

	@Test
	void whatTheRouteCannotReadIsRefused() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		String unreadable = anthropic("invalid_request_error",
				"the request body is not a JSON object this route can read");
		String body = "{\"model\":\"claude-sonnet-4\",\"max_tokens\":10}";
		UpstreamFilter limited = guard(registry, LIMITS, DataSize.ofBytes(body.length() - 1), TunnelBudget.unlimited());

		Map<String, String> requests = Map.of(
				"POST /v1/messages HTTP/1.1\r\nHost: a\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\n\r\n{}",
				answer(400, anthropic("invalid_request_error", "the request body is encoded")),
				"POST /v1/messages HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n", answer(400, unreadable),
				sized("/v1/messages", body), answer(413, anthropic("request_too_large",
						"the request body is past the " + (body.length() - 1) + " bytes this route holds")));
		for (Map.Entry<String, String> request : requests.entrySet()) {
			FilterTests.Exchanged exchanged = FilterTests.exchange(limited, request.getKey(),
					OK.getBytes(StandardCharsets.US_ASCII), 7);

			assertThat(exchanged.upstream()).as(request.getKey()).isEmpty();
			assertThat(exchanged.relayed()).as(request.getKey()).isEqualTo(request.getValue());
		}
		// past the budget as past the limit
		FilterTests.Exchanged exchanged = FilterTests.exchange(guard(registry, LIMITS, null, new TunnelBudget(4)),
				sized("/v1/messages", body), OK.getBytes(StandardCharsets.US_ASCII), 7);

		assertThat(exchanged.upstream()).isEmpty();
		assertThat(exchanged.relayed()).startsWith("HTTP/1.1 413 ");
		assertThat(FilterTests.counters(registry)).isEqualTo(Map.of("llm.guard.requests anthropic unreadable", 4L));
	}

	@Test
	void aRequestWithinTheLimitsReachesTheUpstreamAsItCame() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Map<String, String> bodies = Map.of("/v1/messages",
				"{ \"model\" : \"claude-sonnet-4-5\", \"max_tokens\": 1024, \"messages\":[{\"model\":\"x\",\"max_tokens\":9}], \"stream\":true }",
				"/v1/chat/completions", "{\"model\":\"gpt-4\\u006f\",\"max_tokens\":1,\"max_completion_tokens\":1.0E3}",
				"/v1/responses", "{\"model\":\"gpt-4o\",\"max_output_tokens\":7,\"input\":\"\\u00e9 あ\"}");
		for (Map.Entry<String, String> body : bodies.entrySet()) {
			for (boolean chunked : new boolean[] { false, true }) {
				String request = FilterTests
					.latin1(chunked ? chunked(body.getKey(), body.getValue()) : sized(body.getKey(), body.getValue()));

				FilterTests.Exchanged exchanged = FilterTests.exchange(guard(registry, LIMITS), request,
						OK.getBytes(StandardCharsets.US_ASCII), 7);

				assertThat(exchanged.upstream()).as(request)
					.isEqualTo(FilterTests.latin1(sized(body.getKey(), body.getValue())));
				assertThat(exchanged.relayed()).isEqualTo(OK);
			}
		}
		// other paths and methods are not the APIs'
		String others = "GET /v1/messages HTTP/1.1\r\nHost: a\r\n\r\n" + sized("/v1/messages/count_tokens", "{}");
		assertThat(
				FilterTests.exchange(guard(registry, LIMITS), others, (OK + OK).getBytes(StandardCharsets.US_ASCII), 7)
					.upstream())
			.isEqualTo(others);
		assertThat(FilterTests.counters(registry))
			.isEqualTo(Map.of("llm.guard.requests anthropic allowed", 2L, "llm.guard.requests openai allowed", 4L));
	}

	private record Lowered(String path, String body, String lowered) {
	}

	@Test
	void lowerRewritesTheLimitsAloneToTheCeiling() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Map<String, String> lower = new HashMap<>(LIMITS);
		lower.put("over-max-output-tokens", "lower");
		List<Lowered> cases = List.of(
				new Lowered("/v1/messages", "{\"model\":\"claude-sonnet-4\",\"max_tokens\": 64000 ,\"stream\":true}",
						"{\"model\":\"claude-sonnet-4\",\"max_tokens\": 1024 ,\"stream\":true}"),
				new Lowered("/v1/chat/completions",
						"{\"model\":\"gpt-4o\",\"max_tokens\":-1e9,\"max_completion_tokens\":null}",
						"{\"model\":\"gpt-4o\",\"max_tokens\":-1e9,\"max_completion_tokens\":1024}"),
				new Lowered("/v1/chat/completions", "{\"model\":\"gpt-4o\",\"messages\":[]}",
						"{\"model\":\"gpt-4o\",\"messages\":[],\"max_completion_tokens\":1024}"));
		for (Lowered lowered : cases) {
			for (boolean chunked : new boolean[] { false, true }) {
				String request = chunked ? chunked(lowered.path(), lowered.body())
						: sized(lowered.path(), lowered.body());

				String upstream = FilterTests
					.exchange(guard(registry, lower), request, OK.getBytes(StandardCharsets.US_ASCII), 7)
					.upstream();

				assertThat(upstream).as(request).isEqualTo(sized(lowered.path(), lowered.lowered()));
			}
		}
		assertThat(FilterTests.counters(registry))
			.isEqualTo(Map.of("llm.guard.requests anthropic lowered", 2L, "llm.guard.requests openai lowered", 4L));

		// a model outside the list is refused all the same
		assertThat(FilterTests
			.exchange(guard(registry, lower),
					sized("/v1/messages", "{\"model\":\"claude-opus-4-1\",\"max_tokens\":64000}"),
					OK.getBytes(StandardCharsets.US_ASCII), 7)
			.upstream()).isEmpty();
	}

	@Test
	void otherPathsSetRefuseTheRest() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Map<String, String> only = new HashMap<>(LIMITS);
		only.put("other-paths", "/v1/models*,/v1/messages/count_tokens");
		String passing = "GET /v1/models/claude HTTP/1.1\r\nHost: a\r\n\r\n" + sized("/v1/messages/count_tokens", "{}")
				+ sized("/v1/messages", "{\"model\":\"claude-sonnet-4\",\"max_tokens\":1}");

		assertThat(FilterTests
			.exchange(guard(registry, only), passing, (OK + OK + OK).getBytes(StandardCharsets.US_ASCII), 7)
			.upstream()).isEqualTo(passing);

		FilterTests.Exchanged batch = FilterTests.exchange(guard(registry, only),
				sized("/v1/messages/batches", "{\"requests\":[]}"), OK.getBytes(StandardCharsets.US_ASCII), 7);

		assertThat(batch.upstream()).isEmpty();
		assertThat(batch.relayed())
			.isEqualTo(answer(403, anthropic("permission_error", "/v1/messages/batches is not allowed on this route")));
		assertThat(FilterTests.counters(registry))
			.isEqualTo(Map.of("llm.guard.requests anthropic allowed", 1L, "llm.guard.requests none path", 1L));
	}

	@Test
	void expectedBodiesWaitForTheDecisionToo() throws Exception {
		String request = sized("/v1/messages", "{\"model\":\"claude-opus-4-1\",\"max_tokens\":1000}")
			.replace("Host: a\r\n", "Host: a\r\nExpect: 100-continue\r\n");

		FilterTests.Exchanged exchanged = FilterTests.exchange(guard(new SimpleMeterRegistry(), LIMITS), request,
				"HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.US_ASCII), 7);

		// the head goes at once, as its client waits for the upstream's 100; the body
		// never
		assertThat(exchanged.upstream()).doesNotContain("claude");
		assertThat(exchanged.relayed()).endsWith(
				answer(400, anthropic("invalid_request_error", "model claude-opus-4-1 is not allowed on this route")));
	}

	@Test
	void argumentsAreCheckedAtTheStart() {
		Map<Map<String, String>, String> invalid = Map.of(Map.of(),
				"filter llm-guard needs models or max-output-tokens", Map.of("max-output-tokens", "0"),
				"filter llm-guard takes a positive max-output-tokens, not 0",
				Map.of("models", "a", "over-max-output-tokens", "lower"),
				"filter llm-guard lowers nothing without max-output-tokens",
				Map.of("max-output-tokens", "1", "over-max-output-tokens", "cut"),
				"filter llm-guard takes reject or lower for over-max-output-tokens, not cut",
				Map.of("models", "a", "model", "b"), "filter llm-guard takes no argument model");
		invalid.forEach(
				(args, message) -> assertThatThrownBy(() -> guard(new SimpleMeterRegistry(), args)).as(args.toString())
					.isInstanceOf(IllegalArgumentException.class)
					.hasMessage(message));
	}

	private static UpstreamFilter guard(SimpleMeterRegistry registry, Map<String, String> args) {
		return guard(registry, args, null, TunnelBudget.unlimited());
	}

	private static UpstreamFilter guard(SimpleMeterRegistry registry, Map<String, String> args,
			@Nullable DataSize maxBody, TunnelBudget budget) {
		FilterConfig.Builder use = FilterConfig.builder().name(LlmGuardFilter.NAME).args(args);
		if (maxBody != null) {
			use.maxBody(maxBody);
		}
		return Filters.builder().meters(registry).budget(budget).build().forRoute("route.test", List.of(use.build()));
	}

	private static String api(String path) {
		return java.util.Objects.requireNonNull(LlmApi.ofLoosePath(path)).tag;
	}

	private static String answer(int status, String body) {
		String reason = switch (status) {
			case 400 -> "Bad Request";
			case 403 -> "Forbidden";
			default -> "Content Too Large";
		};
		return "HTTP/1.1 " + status + " " + reason + "\r\nContent-Type: application/json\r\nContent-Length: "
				+ body.length() + "\r\nConnection: close\r\n\r\n" + body;
	}

	private static String sized(String path, String body) {
		return "POST " + path + " HTTP/1.1\r\nHost: a\r\nContent-Length: "
				+ body.getBytes(StandardCharsets.UTF_8).length + "\r\n\r\n" + body;
	}

	private static String chunked(String path, String body) {
		return "POST " + path + " HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"
				+ Integer.toHexString(body.getBytes(StandardCharsets.UTF_8).length) + "\r\n" + body + "\r\n0\r\n\r\n";
	}

}
