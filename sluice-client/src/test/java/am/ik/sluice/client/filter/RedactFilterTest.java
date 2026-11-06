package am.ik.sluice.client.filter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * What a route's bodies become whatever their reads' boundaries, what passes as it came,
 * a stream as it comes, and the rules refused at the start.
 */
class RedactFilterTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final String SECRET = "sk-ant-api03-SECRET";

	private static final Map<String, String> RULES = Map.of("key.literal", SECRET, "email.regex",
			"[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,255}\\.[A-Za-z]{2,24}", "email.replacement", "<email>",
			"name.literal", "山田", "name.replacement", "**", "word.regex", "\\bpass\\b", "word.replacement", "****");

	private static final String BODY = "{\"note\":\"pass passport alice@example.com " + SECRET
			+ " 山田\",\"x\":\"pass\"}\n";

	/** Literals alone, found without a regex: at a place, the first rule by name. */
	private static final Map<String, String> LITERALS = Map.of("key.literal", SECRET, "name.literal", "山田",
			"name.replacement", "**", "port.literal", "passport", "port.replacement", "[port]", "word.literal", "pass",
			"word.replacement", "[pass]");

	private static final String REDACTED = "{\"note\":\"**** passport <email> [REDACTED] **\",\"x\":\"****\"}\n";

	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

	private UpstreamFilter filter(Map<String, String> args, String... before) {
		List<FilterConfig> uses = new ArrayList<>();
		for (String name : before) {
			uses.add(FilterConfig.builder().name(name).build());
		}
		uses.add(FilterConfig.builder().name(RedactFilter.NAME).args(args).build());
		return Filters.builder()
			.meters(this.registry)
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", uses);
	}

	/** The args and these keys and values. */
	private static Map<String, String> with(Map<String, String> args, String... more) {
		Map<String, String> all = new HashMap<>(args);
		for (int i = 0; i < more.length; i += 2) {
			all.put(more[i], more[i + 1]);
		}
		return all;
	}

	private static byte[] response(String head, byte[] body) {
		byte[] start = (head + "Content-Length: " + body.length + "\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);
		byte[] whole = new byte[start.length + body.length];
		System.arraycopy(start, 0, whole, 0, start.length);
		System.arraycopy(body, 0, whole, start.length, body.length);
		return whole;
	}

	private static byte[] utf8(String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}

	@Test
	void everyMatchIsReplacedWhereverTheReadsEnd() throws Exception {
		// the start of a body only, wherever a read begins
		Map<String, String> args = with(RULES, "side", "both", "start.regex", "\\A\\{\"note\"", "start.replacement",
				"{\"memo\"");
		assertRedacted(filter(args), "{\"memo\"" + REDACTED.substring("{\"note\"".length()), REDACTED);
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.ofEntries(
				Map.entry("redact.bodies redacted request", 5L), Map.entry("redact.bodies redacted response", 5L),
				Map.entry("redact.replacements email response", 200L),
				Map.entry("redact.replacements key response", 200L),
				Map.entry("redact.replacements name response", 200L),
				Map.entry("redact.replacements word response", 400L),
				Map.entry("redact.replacements start response", 5L), Map.entry("redact.replacements email request", 5L),
				Map.entry("redact.replacements key request", 5L), Map.entry("redact.replacements name request", 5L),
				Map.entry("redact.replacements word request", 10L),
				Map.entry("redact.replacements start request", 5L)));

		String literals = "{\"note\":\"[pass] [port] alice@example.com [REDACTED] **\",\"x\":\"[pass]\"}\n";
		assertRedacted(filter(with(LITERALS, "side", "both")), literals, literals);
	}

	/** The request's body, then 40 of them as a response, in reads of every size. */
	private static void assertRedacted(UpstreamFilter filter, String first, String rest) throws Exception {
		String json = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: \"v1\"\r\n";
		byte[] body = utf8(BODY.repeat(40));
		String request = "POST /api HTTP/1.1\r\nHost: a\r\nAccept-Encoding: gzip\r\nRange: bytes=0-9\r\n"
				+ "Content-Type: application/json\r\nContent-Length: " + utf8(BODY).length + "\r\n\r\n"
				+ FilterTests.latin1(BODY);
		for (int segment : new int[] { 1, 2, 5, 13, 64 * 1024 }) {
			FilterTests.Exchanged exchanged = FilterTests.exchange(filter, request, response(json, body), segment);
			FilterTests.Chunked relayed = FilterTests.Chunked.of(exchanged.relayed());
			assertThat(relayed.head()).as("segment " + segment)
				.isEqualTo("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: W/\"v1\"\r\n"
						+ "Transfer-Encoding: chunked\r\n\r\n");
			assertThat(new String(relayed.body(), StandardCharsets.UTF_8)).as("segment " + segment)
				.isEqualTo(first + rest.repeat(39));
			// the request's body, of a length no longer its own
			FilterTests.Chunked upstream = FilterTests.Chunked.of(exchanged.upstream());
			assertThat(upstream.head()).isEqualTo(
					"POST /api HTTP/1.1\r\nHost: a\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n");
			assertThat(new String(upstream.body(), StandardCharsets.UTF_8)).isEqualTo(first);
		}
	}

	@Test
	void whatHasNoMatchOrIsNotReadPassesAsItCame() throws Exception {
		UpstreamFilter filter = filter(RULES);
		String request = "GET /api HTTP/1.1\r\nHost: a\r\nAccept-Encoding: gzip\r\n\r\n";
		// no match, though it begins one at a read's end
		byte[] clean = utf8("{\"note\":\"passport sk-ant-api03 alice@\",\"x\":\"山\"}".repeat(30));
		FilterTests.Chunked relayed = FilterTests.Chunked.of(FilterTests
			.exchange(filter, request, response("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n", clean), 7)
			.relayed());
		assertThat(relayed.body()).isEqualTo(clean);

		// not of its types, coded
		byte[] secret = utf8(BODY);
		List<byte[]> unread = List.of(response("HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n", secret),
				response("HTTP/1.1 500 Oops\r\n", secret),
				response("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Encoding: gzip\r\n",
						FilterTests.gzip(secret)));
		for (byte[] response : unread) {
			assertThat(FilterTests.exchange(filter, request, response, 7).relayed())
				.isEqualTo(new String(response, StandardCharsets.ISO_8859_1));
		}
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("redact.bodies clean response", 1L,
				"redact.bodies other_type response", 2L, "redact.bodies encoded response", 1L));

		// decoded by compression, coded again
		FilterTests.Exchanged decoded = FilterTests.exchange(filter(RULES, CompressionFilter.NAME), request,
				response("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Encoding: gzip\r\n",
						FilterTests.gzip(secret)),
				7);
		assertThat(decoded.upstream()).isEqualTo(request);
		assertThat(new String(FilterTests.gunzip(FilterTests.Chunked.of(decoded.relayed()).body()),
				StandardCharsets.UTF_8))
			.isEqualTo(REDACTED);
	}

	/** Each event goes out as it came; only what may begin a match waits for the next. */
	@Test
	void aStreamStaysLive() throws Exception {
		for (Map<String, String> rules : List.of(RULES, LITERALS)) {
			assertLive(filter(rules));
		}
	}

	private static void assertLive(UpstreamFilter filter) throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, filter)) {
			pipe.sink().write("GET /events HTTP/1.1\r\nHost: a\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
			upstream.answer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
			InputStream relayed = new BufferedInputStream(pipe.source());
			assertTimeoutPreemptively(TIMEOUT, () -> {
				String head = readHead(relayed);
				assertThat(head).doesNotContain("Content-Length");
				for (String[] event : new String[][] { { "data: {\"n\":1}\n\n", "data: {\"n\":1}\n\n" },
						{ "data: " + SECRET + "\n\n", "data: [REDACTED]\n\n" }, { "data: sk-ant", "data: " },
						{ "-api03-SECRET!\n\n", "[REDACTED]!\n\n" } }) {
					upstream.answer(Integer.toHexString(event[0].length()) + "\r\n" + event[0] + "\r\n");
					assertThat(readChunk(relayed)).isEqualTo(event[1]);
				}
				upstream.answer("0\r\n\r\n");
				assertThat(readChunk(relayed)).isEmpty();
			});
		}
	}

	private static String readHead(InputStream in) throws IOException {
		StringBuilder head = new StringBuilder();
		while (!head.toString().endsWith("\r\n\r\n")) {
			head.append((char) in.read());
		}
		return head.toString();
	}

	private static String readChunk(InputStream in) throws IOException {
		StringBuilder size = new StringBuilder();
		while (!size.toString().endsWith("\r\n")) {
			size.append((char) in.read());
		}
		byte[] chunk = in.readNBytes(Integer.parseInt(size.toString().trim(), 16));
		in.readNBytes(2);
		return new String(chunk, StandardCharsets.ISO_8859_1);
	}

	@Test
	void aRuleItCannotBoundStopsTheStart() {
		List<Map<String, String>> refused = List.of(Map.of(), Map.of("key.literal", ""),
				Map.of("key.literal", SECRET, "key.regex", "a"), Map.of("key.replacement", SECRET),
				Map.of("key.pattern", SECRET), Map.of("key.literal", SECRET, "side", "upstream"),
				Map.of("key.literal", SECRET, "types", "json"), Map.of("key.literal", SECRET.repeat(300)),
				Map.of("key.regex", "sk-[a-z]+"), Map.of("key.regex", "sk-.*"), Map.of("key.regex", "sk-a{2,}"),
				Map.of("key.regex", "sk-[a-z]{1,5000}"), Map.of("key.regex", "(sk-[a-z]{1,100}){1,100}"),
				Map.of("key.regex", "a?"), Map.of("key.regex", "(?=sk)sk"), Map.of("key.regex", "(?<!x)sk"),
				Map.of("key.regex", "(a)\\1"), Map.of("key.regex", "(?x)sk # key"), Map.of("key.regex", "山田"),
				Map.of("key.regex", "sk-[a-z"), Map.of("key.regex", "\\Qsk"));
		for (Map<String, String> args : refused) {
			assertThatThrownBy(() -> filter(args)).as(args.toString())
				.isInstanceOf(IllegalArgumentException.class)
				.message()
				.doesNotContain(SECRET);
		}
		// bounded, though it looks otherwise
		filter(Map.of("key.regex", "(?i:sk-)(?<id>[\\w\\-]{8,40}|\\Q+*\\E)[\\[\\]a-z&&[^q]]{0,4}\\b$"));
	}

}
