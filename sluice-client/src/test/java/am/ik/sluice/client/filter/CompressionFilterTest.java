package am.ik.sluice.client.filter;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * What reaches the client compressed, decoded or as it came, a stream as it comes, and
 * what does not decode.
 */
class CompressionFilterTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final String TEXT = "<p>" + "sluice relays raw bytes over one gRPC stream. ".repeat(60) + "</p>";

	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

	private UpstreamFilter filter(Map<String, String> args) {
		return Filters.builder()
			.meters(this.registry)
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", List.of(FilterConfig.builder().name(CompressionFilter.NAME).args(args).build()));
	}

	private static byte[] response(String head, byte[] body) {
		byte[] start = (head + "Content-Length: " + body.length + "\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);
		byte[] whole = new byte[start.length + body.length];
		System.arraycopy(start, 0, whole, 0, start.length);
		System.arraycopy(body, 0, whole, start.length, body.length);
		return whole;
	}

	private static String request(@Nullable String acceptEncoding) {
		return "GET /page HTTP/1.1\r\nHost: a\r\n" + (acceptEncoding == null ? "" : acceptEncoding + "\r\n") + "\r\n";
	}

	@Test
	void whatTheClientTakesComesCompressedAndTheRestAsItCame() throws Exception {
		UpstreamFilter filter = filter(Map.of());
		byte[] text = TEXT.getBytes(StandardCharsets.US_ASCII);
		String html = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nETag: \"v1\"\r\n";

		FilterTests.Exchanged compressed = FilterTests.exchange(filter, request("Accept-Encoding: br, gzip, deflate"),
				response(html, text), 7);
		assertThat(compressed.upstream()).isEqualTo("GET /page HTTP/1.1\r\nHost: a\r\nAccept-Encoding: gzip\r\n\r\n");
		FilterTests.Chunked relayed = FilterTests.Chunked.of(compressed.relayed());
		assertThat(relayed.head()).isEqualTo("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
				+ "Vary: Accept-Encoding\r\nContent-Encoding: gzip\r\nETag: W/\"v1\"\r\nTransfer-Encoding: chunked\r\n\r\n");
		assertThat(FilterTests.gunzip(relayed.body())).isEqualTo(text);
		assertThat(relayed.body().length).isLessThan(text.length / 10);

		// the upstream's gzip, unseen, passes as it came; to a client not taking it,
		// decoded
		byte[] coded = response(html + "Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n", FilterTests.gzip(text));
		assertThat(FilterTests.exchange(filter, request("Accept-Encoding: gzip"), coded, 7).relayed())
			.isEqualTo(new String(coded, StandardCharsets.ISO_8859_1));
		FilterTests.Chunked decoded = FilterTests.Chunked.of(
				FilterTests
					.exchange(filter, request("Accept-Encoding: gzip;q=0, br"),
							response(html + "Content-Encoding: x-gzip\r\n", FilterTests.gzip(text)), 7)
					.relayed());
		assertThat(decoded.head()).isEqualTo("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
				+ "Vary: Accept-Encoding\r\nETag: W/\"v1\"\r\nTransfer-Encoding: chunked\r\n\r\n");
		assertThat(decoded.body()).isEqualTo(text);
		// deflate in zlib's format and raw, to gzip
		for (boolean raw : new boolean[] { false, true }) {
			ByteArrayOutputStream deflated = new ByteArrayOutputStream();
			try (DeflaterOutputStream out = new DeflaterOutputStream(deflated, new Deflater(6, raw))) {
				out.write(text);
			}
			FilterTests.Chunked recoded = FilterTests.Chunked.of(
					FilterTests
						.exchange(filter, request("Accept-Encoding: gzip"),
								response(html + "Content-Encoding: deflate\r\n", deflated.toByteArray()), 5)
						.relayed());
			assertThat(FilterTests.gunzip(recoded.body())).as("raw " + raw).isEqualTo(text);
		}

		// as it came: compressed already, not to be transformed, a range, an unknown
		// coding, too small
		List<byte[]> responses = List.of(response("HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n", text),
				response(html + "Cache-Control: public, no-transform\r\n", text),
				response("HTTP/1.1 206 Partial Content\r\nContent-Type: text/html\r\n", text),
				response(html + "Content-Encoding: br\r\n", text), response(html, new byte[1023]));
		for (byte[] response : responses) {
			String as = new String(response, StandardCharsets.ISO_8859_1);
			assertThat(FilterTests.exchange(filter, request("Accept-Encoding: gzip"), response, 1024).relayed())
				.isEqualTo(as);
		}
		// without Accept-Encoding: told it varies
		FilterTests.Exchanged told = FilterTests.exchange(filter, request(null), response(html, text), 1024);
		assertThat(told.upstream()).isEqualTo(request(null));
		assertThat(told.relayed()).isEqualTo(new String(response(html, text), StandardCharsets.ISO_8859_1)
			.replace("\r\n\r\n", "\r\nVary: Accept-Encoding\r\n\r\n"));
		assertThat(FilterTests.counters(this.registry)).isEmpty();
	}

	/** Each event goes out as it came, decodable at once. */
	@Test
	void aStreamStaysLive() throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, filter(Map.of()))) {
			pipe.sink().write(request("Accept-Encoding: gzip").getBytes(StandardCharsets.US_ASCII));
			upstream.answer("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n");
			Inflater inflater = new Inflater(true);
			ByteArrayOutputStream events = new ByteArrayOutputStream();
			// read as the relay does, not a byte at a time, which the pipe would read too
			InputStream relayed = new BufferedInputStream(pipe.source());
			assertTimeoutPreemptively(TIMEOUT, () -> {
				String head = readHead(relayed);
				assertThat(head).contains("Content-Encoding: gzip\r\n");
				for (int i = 0; i < 3; i++) {
					String event = "data: {\"n\":" + i + "}\n\n";
					upstream.answer(Integer.toHexString(event.length()) + "\r\n" + event + "\r\n");
					byte[] chunk = readChunk(relayed);
					// the first after gzip's header
					int start = i == 0 ? 10 : 0;
					inflater.setInput(chunk, start, chunk.length - start);
					byte[] out = new byte[1024];
					int n = inflater.inflate(out);
					events.write(out, 0, n);
					assertThat(events.toString(StandardCharsets.US_ASCII)).endsWith(event);
				}
				upstream.answer("0\r\n\r\n");
				byte[] last = readChunk(relayed);
				inflater.setInput(last, 0, last.length - 8);
				assertThat(inflater.inflate(new byte[1024])).isZero();
				assertThat(inflater.finished()).isTrue();
			});
			inflater.end();
		}
	}

	private static String readHead(InputStream in) throws IOException {
		StringBuilder head = new StringBuilder();
		while (!head.toString().endsWith("\r\n\r\n")) {
			head.append((char) in.read());
		}
		return head.toString();
	}

	private static byte[] readChunk(InputStream in) throws IOException {
		StringBuilder size = new StringBuilder();
		while (!size.toString().endsWith("\r\n")) {
			size.append((char) in.read());
		}
		byte[] chunk = in.readNBytes(Integer.parseInt(size.toString().trim(), 16));
		in.readNBytes(2);
		return chunk;
	}

	@Test
	void aBodyThatDoesNotDecodeFailsAndBadSettingsStopTheStart() throws Exception {
		UpstreamFilter filter = filter(Map.of("max-ratio", "10"));
		String html = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Encoding: gzip\r\n";
		byte[] text = TEXT.getBytes(StandardCharsets.US_ASCII);
		byte[] coded = FilterTests.gzip(text);
		byte[] cut = Arrays.copyOf(coded, coded.length - 4);
		byte[] bomb = FilterTests.gzip(new byte[64 * 1024 * 1024]);
		for (byte[] body : List.of(text, cut, bomb)) {
			assertThatThrownBy(() -> FilterTests.exchange(filter, request("Accept-Encoding: identity"),
					response(html, body), 64 * 1024))
				.isInstanceOfAny(IOException.class, UncheckedIOException.class);
		}
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures compression body", 3L));

		for (Map<String, String> args : List.of(Map.of("level", "10"), Map.of("types", "json"),
				Map.of("max-ratio", "0"), Map.of("min_length", "1"))) {
			assertThatThrownBy(() -> filter(args)).as(args.toString()).isInstanceOf(IllegalArgumentException.class);
		}
	}

}
