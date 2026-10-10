package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.junit.jupiter.api.Test;

import am.ik.sluice.server.proxy.Http1RequestParser.Listener;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Http1RequestParser}: every request head of the connection is
 * emitted with its Host (keep-alive and pipelined), bodies pass through per their
 * framing, and the relayed bytes reconstruct the input exactly however the stream is
 * chunked.
 */
class Http1RequestParserTest {

	private final ByteArrayOutputStream passthrough = new ByteArrayOutputStream();

	private final List<String> heads = new ArrayList<>();

	private final List<@Nullable String> hosts = new ArrayList<>();

	private final Listener listener = new Listener() {

		@Override
		public void head(byte[] head, @Nullable String host) {
			Http1RequestParserTest.this.heads.add(new String(head, StandardCharsets.US_ASCII));
			Http1RequestParserTest.this.hosts.add(host);
			passthrough.writeBytes(head);
		}

		@Override
		public void data(byte[] bytes, int offset, int length) {
			passthrough.write(bytes, offset, length);
		}
	};

	private String parseAll(String... chunks) {
		Http1RequestParser parser = new Http1RequestParser(this.listener);
		for (String chunk : chunks) {
			byte[] bytes = chunk.getBytes(StandardCharsets.UTF_8);
			parser.feed(bytes, 0, bytes.length);
		}
		parser.finish();
		return this.passthrough.toString(StandardCharsets.UTF_8);
	}

	@Test
	void emitsEveryHeadOfPipelinedRequests() {
		String first = request("GET /one", null);
		String second = request("GET /two", null);
		String output = parseAll(first + second);
		assertThat(this.heads).containsExactly(first, second);
		assertThat(this.hosts).containsExactly("demo.local", "demo.local");
		assertThat(output).isEqualTo(first + second);
	}

	@Test
	void emitsBodyThenNextHeadArbitraryChunking() {
		String post = request("POST /submit", "Content-Length: 5\r\n") + "hello";
		String get = request("GET /next", null);
		String all = post + get;
		String output = parseAll(all.substring(0, 13), all.substring(13, 40), all.substring(40));
		assertThat(this.heads).containsExactly(request("POST /submit", "Content-Length: 5\r\n"),
				request("GET /next", null));
		assertThat(output).isEqualTo(all);
	}

	@Test
	void chunkedBodyWithTrailersPassesThrough() {
		String post = request("POST /upload", "Transfer-Encoding: chunked\r\n")
				+ "4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Tr: 1\r\n\r\n";
		String get = request("GET /next", null);
		String output = parseAll(post, get);
		assertThat(this.heads).containsExactly(request("POST /upload", "Transfer-Encoding: chunked\r\n"),
				request("GET /next", null));
		assertThat(output).isEqualTo(post + get);
	}

	@Test
	void upgradedConnectionEmitsOneHeadThenVerbatimFrames() {
		String upgrade = request("GET /ws", "Upgrade: websocket\r\nConnection: Upgrade\r\n");
		String frames = "\0binary websocket frames \r\n\r\n inside";
		String output = parseAll(upgrade + frames);
		assertThat(this.heads).containsExactly(upgrade);
		assertThat(output).isEqualTo(upgrade + frames);
	}

	@Test
	void truncatedHeadIsFlushedVerbatimAtEndOfInput() {
		String partial = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Trailing";
		assertThat(parseAll(partial)).isEqualTo(partial);
		assertThat(this.heads).isEmpty();
	}

	@Test
	void nonAsciiHeadDegradesToPassthrough() {
		String head = new String("GET / HTTP/1.1\r\nHost: ほげ\r\n\r\n".getBytes(StandardCharsets.UTF_8),
				StandardCharsets.UTF_8);
		String rest = "GET /next HTTP/1.1\r\nHost: demo.local\r\n\r\n";
		String output = parseAll(head, rest);
		assertThat(output).isEqualTo(head + rest);
		assertThat(this.heads).isEmpty();
	}

	@Test
	void headWithoutHostIsEmittedWithNullHost() {
		String head = "GET / HTTP/1.1\r\nX-Any: 1\r\n\r\n";
		assertThat(parseAll(head)).isEqualTo(head);
		assertThat(this.hosts).containsExactly((String) null);
	}

	private static String request(String requestLine, @Nullable String extraHeader) {
		String headers = "Host: demo.local\r\n" + (extraHeader == null ? "" : extraHeader);
		return requestLine + " HTTP/1.1\r\n" + headers + "\r\n";
	}

}
