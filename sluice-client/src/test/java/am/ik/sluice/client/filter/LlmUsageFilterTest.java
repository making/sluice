package am.ik.sluice.client.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.util.unit.DataSize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The token usage of each API read as the body passes, however the upstream's bytes are
 * split, and the option an OpenAI compatible stream needs to report any.
 */
class LlmUsageFilterTest {

	private static final String CHAT_DELTA = "data: {\"id\":\"c\",\"model\":\"gpt\",\"choices\":[{\"delta\":{\"content\":\"\\\"usage\\\" ひとつ\"}}],\"usage\":null}\n\n";

	private static final String ANTHROPIC_START = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude\",\"content\":[],\"usage\":{\"input_tokens\":2679,\"cache_creation_input_tokens\":7,\"cache_read_input_tokens\":11,\"output_tokens\":3}}}\n\n";

	private static final String ANTHROPIC_TEXT = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"\\\"usage\\\"\"}}\n\n";

	private static final String ANTHROPIC_DELTA = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":10682,\"output_tokens\":510}}\n\n";

	private static final String ANTHROPIC_STOP = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

	private record Usage(String target, String response, Map<String, Long> expected) {
	}

	private static UpstreamFilter filter(MeterRegistry registry, FilterConfig... uses) {
		return Filters.builder()
			.meters(registry)
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", List.of(uses));
	}

	private static FilterConfig.Builder usage() {
		return FilterConfig.builder().name(LlmUsageFilter.NAME);
	}

	private static String json(String body) {
		return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
				+ body.getBytes(StandardCharsets.UTF_8).length + "\r\n\r\n" + body;
	}

	private static String sse(String... events) {
		StringBuilder chunks = new StringBuilder(
				"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
		for (String event : events) {
			chunks.append(Integer.toHexString(event.getBytes(StandardCharsets.UTF_8).length))
				.append("\r\n")
				.append(event)
				.append("\r\n");
		}
		return chunks.append("0\r\n\r\n").toString();
	}

	private static Map<String, Long> tokens(String api, String model, long input, long output, long cacheRead,
			long cacheCreation, String stream, String outcome) {
		Map<String, Long> expected = new TreeMap<>();
		expected.put("llm.requests " + api + " " + model + " " + outcome + " " + stream, 1L);
		if (input > 0) {
			expected.put("llm.tokens " + api + " " + model + " input", input);
		}
		if (output > 0) {
			expected.put("llm.tokens " + api + " " + model + " output", output);
		}
		if (cacheRead > 0) {
			expected.put("llm.tokens " + api + " " + model + " cache_read", cacheRead);
		}
		if (cacheCreation > 0) {
			expected.put("llm.tokens " + api + " " + model + " cache_creation", cacheCreation);
		}
		return expected;
	}

	private static List<Usage> usageCases() {
		return List.of(new Usage("/v1/chat/completions", json(
				"{\"model\":\"gpt\",\"choices\":[{\"message\":{\"content\":\"\\\"usage\\\":{}\"}}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":30}}}"),
				tokens("openai", "gpt", 70, 20, 30, 0, "false", "complete")),
				new Usage("/v1/chat/completions?x=1", sse(CHAT_DELTA, CHAT_DELTA,
						"data: {\"model\":\"gpt\",\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n"),
						tokens("openai", "gpt", 9, 4, 0, 0, "true", "complete")),
				new Usage("/v1/chat/completions", sse(CHAT_DELTA, "data: [DONE]\n\n"),
						tokens("openai", "gpt", 0, 0, 0, 0, "true", "no_usage")),
				new Usage("/v1/responses", json(
						"{\"model\":\"gpt\",\"usage\":{\"input_tokens\":5,\"output_tokens\":6,\"input_tokens_details\":{\"cached_tokens\":2,\"cache_write_tokens\":1}}}"),
						tokens("openai", "gpt", 2, 6, 2, 1, "false", "complete")),
				new Usage("/v1/responses", sse(
						"data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt\",\"usage\":null}}\n\n",
						"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
						"data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt\",\"usage\":{\"input_tokens\":5,\"output_tokens\":6}}}\n\n"),
						tokens("openai", "gpt", 5, 6, 0, 0, "true", "complete")),
				new Usage("/v1/messages", json(
						"{\"model\":\"claude\",\"content\":[{\"type\":\"text\",\"text\":\"\\\"usage\\\"\"}],\"usage\":{\"input_tokens\":12,\"output_tokens\":34,\"cache_read_input_tokens\":56,\"cache_creation_input_tokens\":78}}"),
						tokens("anthropic", "claude", 12, 34, 56, 78, "false", "complete")),
				// message_delta overwrites: a server tool raises the input, and the
				// values are cumulative
				new Usage("/v1/messages", sse(ANTHROPIC_START, ANTHROPIC_TEXT, ANTHROPIC_DELTA, ANTHROPIC_STOP),
						tokens("anthropic", "claude", 10682, 510, 11, 7, "true", "complete")),
				new Usage("/v1/messages", sse(ANTHROPIC_START, ANTHROPIC_TEXT),
						tokens("anthropic", "claude", 2679, 3, 11, 7, "true", "cut")),
				new Usage("/v1/messages", sse(ANTHROPIC_START,
						"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"),
						tokens("anthropic", "claude", 2679, 3, 11, 7, "true", "error")),
				new Usage("/v1/messages/count_tokens", json("{\"input_tokens\":3}"), Map.of()),
				new Usage("/v1/messages",
						"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
						Map.of("llm.requests anthropic unknown status_4 unknown", 1L)),
				new Usage("/v1/messages",
						"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\n\r\n{}",
						Map.of("llm.requests anthropic unknown encoded unknown", 1L)));
	}

	@Test
	void usageOfEachApiReadAsTheBodyPassesUnchanged() throws Exception {
		for (Usage usage : usageCases()) {
			for (int segment : new int[] { 1, 7, 64 * 1024 }) {
				SimpleMeterRegistry registry = new SimpleMeterRegistry();

				String relayed = FilterTests.relayed(filter(registry, usage().build()), "POST", usage.target(),
						usage.response(), segment);

				assertThat(relayed).as(usage.target()).isEqualTo(FilterTests.latin1(usage.response()));
				assertThat(FilterTests.counters(registry)).as(usage.target() + " in segments of " + segment)
					.isEqualTo(usage.expected());
			}
		}
	}

	/** A body held by nothing here: it may be of any size. */
	@Test
	void aJsonBodyPastAnySizeIsRead() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		String padding = "x".repeat(8 * 1024 * 1024);

		FilterTests.relayed(filter(registry, usage().build()), "POST", "/v1/messages", json(
				"{\"model\":\"m\",\"content\":\"" + padding + "\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}"),
				64 * 1024);

		assertThat(FilterTests.counters(registry)).isEqualTo(tokens("anthropic", "m", 1, 2, 0, 0, "false", "complete"));
	}

	/** Prometheus takes a counter's series only if they have the same tag keys. */
	@Test
	void everySeriesOfACounterHasTheSameTags() throws Exception {
		PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
		for (Usage usage : usageCases()) {
			FilterTests.relayed(filter(registry, usage().build()), "POST", usage.target(), usage.response(), 1024);
		}

		assertThat(registry.scrape()).contains("outcome=\"status_4\"").contains("outcome=\"complete\"");
	}

	@Test
	void aModelFromAResponseIsBoundedAsATag() throws Exception {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		UpstreamFilter filter = filter(registry, usage().build());
		for (int i = 0; i < 300; i++) {
			FilterTests.relayed(filter, "POST", "/v1/messages", json("{\"model\":\"m" + i + "\"}"), 1024);
		}

		assertThat(FilterTests.counters(registry)).hasSize(256 + 1)
			.containsEntry("llm.requests anthropic other no_usage false", 300L - 256);
	}

	@Test
	void aCompressedBodyIsAvoidedOnTheApisOnlyOrDecodedInFront() throws Exception {
		String response = "HTTP/1.1 204 No Content\r\n\r\n";
		String requests = "POST /v1/messages HTTP/1.1\r\nHost: a\r\nAccept-Encoding: gzip\r\nContent-Length: 0\r\n\r\n";
		String other = "GET /health HTTP/1.1\r\nHost: a\r\nAccept-Encoding: gzip\r\n\r\n";

		String upstream = FilterTests
			.exchange(filter(new SimpleMeterRegistry(), usage().build()), requests + other,
					(response + response).getBytes(StandardCharsets.US_ASCII), 1024)
			.upstream();

		assertThat(upstream).isEqualTo("POST /v1/messages HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n" + other);

		// compression in front decodes it for the filter, and encodes it again
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		byte[] body = "{\"model\":\"claude\",\"usage\":{\"input_tokens\":12,\"output_tokens\":34}}"
			.getBytes(StandardCharsets.US_ASCII);
		byte[] gzip = FilterTests.gzip(body);
		String coded = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: "
				+ gzip.length + "\r\n\r\n" + new String(gzip, StandardCharsets.ISO_8859_1);
		FilterTests.Exchanged exchanged = FilterTests.exchange(
				filter(registry, FilterConfig.builder().name(CompressionFilter.NAME).build(), usage().build()),
				requests, coded.getBytes(StandardCharsets.ISO_8859_1), 7);

		assertThat(exchanged.upstream()).isEqualTo(requests.replace("Accept-Encoding: gzip\r\nContent-Length: 0",
				"Content-Length: 0\r\nAccept-Encoding: gzip"));
		assertThat(FilterTests.gunzip(FilterTests.Chunked.of(exchanged.relayed()).body())).isEqualTo(body);
		assertThat(FilterTests.counters(registry))
			.isEqualTo(tokens("anthropic", "claude", 12, 34, 0, 0, "false", "complete"));
	}

	@Test
	void includeUsageAddsTheOptionToAStreamThatDoesNotSetIt() throws Exception {
		FilterConfig include = usage().args(Map.of("include-usage", "true")).build();
		String stream = "{\"model\":\"m\",\"messages\":[{\"stream\":false}],\"stream\":true}";
		String added = "{\"model\":\"m\",\"messages\":[{\"stream\":false}],\"stream\":true,\"stream_options\":{\"include_usage\":true}}";
		for (int segment : new int[] { 1, 7, 64 * 1024 }) {
			assertThat(upstreamOf(include, "/v1/chat/completions", stream, segment)).isEqualTo(sized(added));
		}
		// left as it came: not a stream, the option set, another API
		for (String body : List.of("{\"stream\":false}", "{\"messages\":[]}",
				"{\"stream\":true,\"stream_options\":{\"include_usage\":false}}")) {
			assertThat(upstreamOf(include, "/v1/chat/completions", body, 1024)).isEqualTo(sized(body));
		}
		assertThat(upstreamOf(include, "/v1/messages", stream, 1024)).isEqualTo(sized(stream));
		// not set: the body is not even held
		assertThat(upstreamOf(usage().build(), "/v1/chat/completions", stream, 1024)).isEqualTo(sized(stream));
	}

	@Test
	void includeUsageLeavesABodyPastItsLimitAsItWas() throws Exception {
		FilterConfig include = usage().args(Map.of("include-usage", "true")).maxBody(DataSize.ofBytes(10)).build();
		String stream = "{\"stream\":true,\"padding\":\"past the limit\"}";

		assertThat(upstreamOf(include, "/v1/chat/completions", stream, 7)).isEqualTo(sized(stream));

		// a chunked request held to its end gets its length
		String chunked = "POST /v1/chat/completions HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"
				+ "f\r\n{\"stream\":true}\r\n0\r\n\r\n";
		String added = "{\"stream\":true,\"stream_options\":{\"include_usage\":true}}";
		FilterConfig generous = usage().args(Map.of("include-usage", "true")).build();
		assertThat(FilterTests
			.exchange(filter(new SimpleMeterRegistry(), generous), chunked,
					"HTTP/1.1 204 No Content\r\n\r\n".getBytes(StandardCharsets.US_ASCII), 1024)
			.upstream())
			.isEqualTo("POST /v1/chat/completions HTTP/1.1\r\nHost: a\r\nContent-Length: " + added.length() + "\r\n\r\n"
					+ added);
	}

	@Test
	void unknownArgumentsAreRefused() {
		assertThatThrownBy(
				() -> filter(new SimpleMeterRegistry(), usage().args(Map.of("include_usage", "true")).build()))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("filter llm-usage takes no argument include_usage");
	}

	private static String sized(String body) {
		return "POST /v1/x HTTP/1.1\r\nHost: a\r\nContent-Length: " + body.length() + "\r\n\r\n" + body;
	}

	private static String upstreamOf(FilterConfig use, String path, String body, int segment) throws Exception {
		String request = "POST " + path + " HTTP/1.1\r\nHost: a\r\nContent-Length: " + body.length() + "\r\n\r\n"
				+ body;
		String upstream = FilterTests
			.exchange(filter(new SimpleMeterRegistry(), use), request,
					"HTTP/1.1 204 No Content\r\n\r\n".getBytes(StandardCharsets.US_ASCII), segment)
			.upstream();
		return upstream.replace(path, "/v1/x");
	}

}
