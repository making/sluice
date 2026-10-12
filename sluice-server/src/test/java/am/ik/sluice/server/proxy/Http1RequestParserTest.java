package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import am.ik.sluice.server.proxy.Http1RequestParser.Listener;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Http1RequestParser}: every request head of the connection is
 * emitted with its Host (keep-alive and pipelined), bodies pass through per their
 * framing, and the relayed bytes reconstruct the input exactly however the stream is
 * chunked; field values may carry obs-text, and a malformed request stops the parse
 * without relaying any of it.
 */
class Http1RequestParserTest {

	private final ByteArrayOutputStream passthrough = new ByteArrayOutputStream();

	private final List<String> heads = new ArrayList<>();

	private final List<@Nullable String> hosts = new ArrayList<>();

	private final Listener listener = new Listener() {

		@Override
		public void head(byte[] head, @Nullable String host) {
			Http1RequestParserTest.this.heads.add(new String(head, StandardCharsets.UTF_8));
			Http1RequestParserTest.this.hosts.add(host);
			passthrough.writeBytes(head);
		}

		@Override
		public void data(byte[] bytes, int offset, int length) {
			passthrough.write(bytes, offset, length);
		}
	};

	private final Http1RequestParser parser = new Http1RequestParser(this.listener);

	private String parseAll(String... chunks) {
		feed(chunks);
		this.parser.finish();
		return output();
	}

	private void feed(String... chunks) {
		for (String chunk : chunks) {
			byte[] bytes = chunk.getBytes(StandardCharsets.UTF_8);
			this.parser.feed(bytes, 0, bytes.length);
		}
	}

	private String output() {
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
	void anUpgradeHoldsTheBytesAfterItUntilItsAnswer() {
		String upgrade = request("GET /ws", "Upgrade: websocket\r\nConnection: Upgrade\r\n");
		String frames = "\0binary websocket frames \r\n\r\n inside";
		feed(upgrade + frames.substring(0, 10), frames.substring(10));
		assertThat(this.heads).containsExactly(upgrade);
		assertThat(output()).isEqualTo(upgrade);
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.AWAITING_UPGRADE);
	}

	@Test
	void aSwitchedUpgradeRelaysEverythingAfterItVerbatim() {
		String upgrade = request("GET /ws", "Upgrade: websocket\r\nConnection: Upgrade\r\n");
		String frames = "\0binary websocket frames \r\n\r\n inside";
		String later = request("GET /not-a-request", null);
		feed(upgrade + frames);
		this.parser.upgradeAnswered(true);
		feed(later);
		assertThat(this.heads).containsExactly(upgrade);
		assertThat(output()).isEqualTo(upgrade + frames + later);
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.PASSTHROUGH);
	}

	@Test
	void aDeclinedUpgradeResumesParsingWithTheBytesHeld() {
		String upgrade = request("GET /ws", "Upgrade: h2c\r\nConnection: Upgrade, HTTP2-Settings\r\n");
		String next = request("GET /next", null);
		String partial = "GET /third HTTP/1.1\r\n";
		feed(upgrade + next + partial);
		assertThat(this.heads).containsExactly(upgrade);
		this.parser.upgradeAnswered(false);
		assertThat(this.heads).containsExactly(upgrade, next);
		assertThat(output()).isEqualTo(upgrade + next);
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.IN_HEAD);
	}

	@Test
	void anUpgradeWithABodyAwaitsItsAnswerAfterTheBody() {
		String upgrade = request("POST /up", "Upgrade: h2c\r\nConnection: Upgrade\r\nContent-Length: 4\r\n");
		feed(upgrade + "bo");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.IN_BODY);
		feed("dy" + "after");
		assertThat(output()).isEqualTo(upgrade + "body");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.AWAITING_UPGRADE);
	}

	@Test
	void aConnectAwaitsItsAnswer() {
		String connect = "CONNECT upstream.local:443 HTTP/1.1\r\nHost: upstream.local:443\r\n\r\n";
		feed(connect + "\u0016tls");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.AWAITING_UPGRADE);
		this.parser.upgradeAnswered(true);
		assertThat(output()).isEqualTo(connect + "\u0016tls");
	}

	@Test
	void heldBytesAreFlushedVerbatimAtEndOfInput() {
		String upgrade = request("GET /ws", "Upgrade: websocket\r\nConnection: Upgrade\r\n");
		assertThat(parseAll(upgrade + "early")).isEqualTo(upgrade + "early");
	}

	@Test
	void thePositionFollowsTheRequestStream() {
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.BETWEEN_REQUESTS);
		feed("POST / HTTP/1.1\r\n");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.IN_HEAD);
		feed("Host: demo.local\r\nContent-Length: 2\r\n\r\n");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.IN_BODY);
		feed("ok");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.BETWEEN_REQUESTS);
	}

	@Test
	void truncatedHeadIsFlushedVerbatimAtEndOfInput() {
		String partial = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Trailing";
		assertThat(parseAll(partial)).isEqualTo(partial);
		assertThat(this.heads).isEmpty();
	}

	@Test
	void aFieldValueWithObsTextIsParsedLikeAnyOther() {
		String head = request("GET /", "X-Name: \u00e9\u307b\r\n");
		String rest = request("GET /next", null);
		String output = parseAll(head, rest);
		assertThat(this.heads).containsExactly(head, rest);
		assertThat(this.hosts).containsExactly("demo.local", "demo.local");
		assertThat(output).isEqualTo(head + rest);
	}

	@Test
	void anObsTextHostIsReadAsIso88591() {
		byte[] head = "GET / HTTP/1.1\r\nHost: \u00e9\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
		this.parser.feed(head, 0, head.length);
		assertThat(this.hosts).containsExactly("\u00e9");
	}

	@Test
	void anEmptyLineBeforeARequestLineIsIgnored() {
		String head = request("GET /", null);
		String output = parseAll("\r\n" + head);
		assertThat(this.heads).containsExactly(head);
		assertThat(output).isEqualTo(head);
	}

	@Test
	void validFramingVariantsAreFollowed() {
		String identical = request("POST /a", "Content-Length: 2, 2\r\n") + "ok";
		String gzip = request("POST /b", "Transfer-Encoding: gzip, chunked\r\n") + "A;ext=\"v\"\r\n0123456789\r\n"
				+ "1 ;x\r\n!\r\n0\r\n\r\n";
		String next = request("GET /c", null);
		String output = parseAll(identical + gzip + next);
		assertThat(this.heads).hasSize(3);
		assertThat(output).isEqualTo(identical + gzip + next);
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.BETWEEN_REQUESTS);
	}

	@ParameterizedTest
	@ValueSource(strings = { "GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length : 5\r\n\r\nhello",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nX-A: 1\r\n 2\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: abc\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: -1\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: +1\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length:\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 5, 6\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 1234567890123456789\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nTransfer-Encoding: chunked, gzip\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nTransfer-Encoding: chunked, chunked\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nHost: other.local\r\n\r\n",
			"GET / HTTP/1.1\nHost: demo.local\r\n\r\n", "GET / HTTP/1.1\r\nHost: demo.local\r\nX-A: a\rb\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nX-A: a\u0000b\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nX-A: a\u007fb\r\n\r\n",
			"GET  / HTTP/1.1\r\nHost: demo.local\r\n\r\n", "GET / HTTP/1\r\nHost: demo.local\r\n\r\n",
			"GET / HTTP/1.1 x\r\nHost: demo.local\r\n\r\n", "G\u00e9T / HTTP/1.1\r\nHost: demo.local\r\n\r\n",
			"GET /\u0001 HTTP/1.1\r\nHost: demo.local\r\n\r\n", "GET / HTTP/1.1\r\nHost: demo.local\r\nGarbage\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: demo.local\r\nX\u00e9: 1\r\n\r\n" })
	void aMalformedHeadStopsTheParseUnrelayed(String malformed) {
		String first = request("GET /first", null);
		String output = parseAll(first + malformed + request("GET /after", null));
		assertThat(this.heads).containsExactly(first);
		assertThat(output).isEqualTo(first);
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(this.parser.malformedInBody()).isFalse();
		String malformedHead = new String(this.parser.malformedHead(), StandardCharsets.UTF_8);
		assertThat(malformedHead).as("what was read of it").startsWith("G");
		assertThat(malformed).startsWith(malformedHead);
	}

	@ParameterizedTest
	@ValueSource(strings = { "zz\r\n", "+5\r\nhello\r\n0\r\n\r\n", "5 5\r\nhello\r\n0\r\n\r\n", "\r\n",
			"5\nhello\r\n0\r\n\r\n", "5\r\nhelloXX0\r\n\r\n", "5\r\nhello\n0\r\n\r\n", "0\r\nnot a trailer\r\n\r\n",
			"0\r\nX-T: a\u0000\r\n\r\n", "fffffffffffffffff\r\n", "5;ext\u0001\r\nhello\r\n0\r\n\r\n" })
	void aMalformedChunkedBodyStopsTheParse(String body) {
		String head = request("POST /upload", "Transfer-Encoding: chunked\r\n");
		feed(head + body + request("GET /after", null));
		assertThat(this.heads).containsExactly(head);
		assertThat(output()).as("the head and the valid part of the body only")
			.isEqualTo(head + validPrefix(body))
			.doesNotContain("GET /after");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(this.parser.malformedInBody()).isTrue();
		assertThat(this.parser.malformedHead()).isEmpty();
		this.parser.finish();
		assertThat(output()).as("nothing is flushed at the end").isEqualTo(head + validPrefix(body));
	}

	/** The part of each malformed chunked body the parser relays before the error. */
	private static String validPrefix(String body) {
		return switch (body) {
			case "5\r\nhelloXX0\r\n\r\n", "5\r\nhello\n0\r\n\r\n" -> "5\r\nhello";
			case "0\r\nnot a trailer\r\n\r\n", "0\r\nX-T: a\u0000\r\n\r\n" -> "0\r\n";
			default -> "";
		};
	}

	@Test
	void aHeadOverTheLimitIsMalformedWithoutWaitingForItsEnd() {
		String start = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Long: ";
		feed(start + "a".repeat(Http1RequestParser.MAX_HEAD - start.length()));
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.IN_HEAD);
		feed("a");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(output()).isEmpty();
	}

	@Test
	void aLineEndedByABareLfIsMalformedAtOnce() {
		// never ended by CRLFCRLF: without this, it would wait for the head timeout
		feed("GET / HTTP/1.1\nHost: demo.local\n\n");
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(output()).isEmpty();
	}

	@Test
	void aHeadOfManyShortLinesOverTheLimitIsMalformed() {
		feed("GET / HTTP/1.1\r\n" + "A: b\r\n".repeat(Http1RequestParser.MAX_HEAD / 6 + 1));
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(output()).isEmpty();
	}

	@Test
	void aChunkSizeLineOverTheLimitIsMalformed() {
		String head = request("POST /upload", "Transfer-Encoding: chunked\r\n");
		feed(head + "5;" + "x".repeat(Http1RequestParser.MAX_HEAD));
		assertThat(this.parser.position()).isEqualTo(Http1RequestParser.Position.MALFORMED);
		assertThat(output()).isEqualTo(head);
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
