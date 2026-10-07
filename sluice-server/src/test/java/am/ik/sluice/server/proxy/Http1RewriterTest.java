package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Http1Rewriter}: every request head of the connection is rewritten
 * (keep-alive and pipelined), bodies pass through per their framing, and anything the
 * machine cannot reproduce degrades to verbatim passthrough.
 */
class Http1RewriterTest {

	private static final String AUTHORITY = "127.0.0.1:3128";

	@Test
	void rewritesHostOfTwoPipelinedRequests() {
		String first = request("GET /one", null);
		String second = request("GET /two", null);
		String output = rewriteAll(first + second);
		assertThat(output).isEqualTo(rewritten("GET /one") + rewritten("GET /two"));
	}

	@Test
	void contentLengthBodyPassesThroughAndNextRequestIsRewritten() {
		String post = request("POST /submit", "Content-Length: 5\r\n") + "hello";
		String get = request("GET /next", null);
		// the stream is split at arbitrary boundaries
		String all = post + get;
		String output = rewriteAll(all.substring(0, 13), all.substring(13, 40), all.substring(40));
		assertThat(output).isEqualTo("""
				POST /submit HTTP/1.1\r
				Host: 127.0.0.1:3128\r
				Content-Length: 5\r
				\r
				hello""" + rewritten("GET /next"));
	}

	@Test
	void chunkedBodyWithTrailersPassesThrough() {
		String post = request("POST /upload", "Transfer-Encoding: chunked\r\n")
				+ "4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Tr: 1\r\n\r\n";
		String get = request("GET /next", null);
		String output = rewriteAll(post, get);
		assertThat(output).isEqualTo("""
				POST /upload HTTP/1.1\r
				Host: 127.0.0.1:3128\r
				Transfer-Encoding: chunked\r
				\r
				4\r
				Wiki\r
				5\r
				pedia\r
				0\r
				X-Tr: 1\r
				\r
				""" + rewritten("GET /next"));
	}

	@Test
	void upgradedConnectionIsRewrittenOnceThenRelayedVerbatim() {
		String upgrade = request("GET /ws", "Upgrade: websocket\r\nConnection: Upgrade\r\n");
		String frames = "\0binary websocket frames \r\n\r\n inside";
		String output = rewriteAll(upgrade + frames);
		assertThat(output).isEqualTo("""
				GET /ws HTTP/1.1\r
				Host: 127.0.0.1:3128\r
				Upgrade: websocket\r
				Connection: Upgrade\r
				\r
				""" + frames);
	}

	@Test
	void truncatedHeadIsFlushedVerbatimAtEndOfInput() {
		String partial = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Trailing";
		assertThat(rewriteAll(partial)).isEqualTo(partial);
	}

	@Test
	void nonAsciiHeadDegradesToPassthrough() {
		String head = new String("GET / HTTP/1.1\r\nHost: ほげ\r\n\r\n".getBytes(StandardCharsets.UTF_8),
				StandardCharsets.UTF_8);
		String rest = "GET /next HTTP/1.1\r\nHost: demo.local\r\n\r\n";
		String output = rewriteAll(head, rest);
		assertThat(output).isEqualTo(head + rest);
	}

	@Test
	void headWithoutHostIsRelayedVerbatim() {
		String head = "GET / HTTP/1.1\r\nX-Any: 1\r\n\r\n";
		assertThat(rewriteAll(head)).isEqualTo(head);
	}

	private static String rewriteAll(String... chunks) {
		Http1Rewriter rewriter = new Http1Rewriter(AUTHORITY);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		HostRewritingPipe.Sink sink = (bytes, offset, length) -> out.write(bytes, offset, length);
		for (String chunk : chunks) {
			byte[] bytes = chunk.getBytes(StandardCharsets.UTF_8);
			rewriter.rewrite(bytes, 0, bytes.length, sink);
		}
		rewriter.finish(sink);
		return out.toString(StandardCharsets.UTF_8);
	}

	private static String request(String requestLine, @Nullable String extraHeader) {
		String headers = "Host: demo.local\r\n" + (extraHeader == null ? "" : extraHeader);
		return requestLine + " HTTP/1.1\r\n" + headers + "\r\n";
	}

	private static String rewritten(String requestLine) {
		return requestLine + " HTTP/1.1\r\nHost: 127.0.0.1:3128\r\n\r\n";
	}

}
