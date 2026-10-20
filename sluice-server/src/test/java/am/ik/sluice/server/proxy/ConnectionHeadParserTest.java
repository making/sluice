package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import am.ik.sluice.server.auth.RequestHeaders;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionHeadParserTest {

	private static final ConnectionHeadParser PARSER = new ConnectionHeadParser(64 * 1024);

	private static byte[] h2Head(byte[] headerBlock, boolean endHeaders) {
		return h2Head(1, new byte[][] { headerBlock }, new boolean[] { endHeaders });
	}

	/** Builds preface + SETTINGS + HEADERS/CONTINUATION frames around raw HPACK bytes. */
	private static byte[] h2Head(int streamId, byte[][] blocks, boolean[] endFlags) {
		java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		out.writeBytes(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
		out.writeBytes(frame(0x4, 0x0, 0, new byte[0])); // SETTINGS
		for (int i = 0; i < blocks.length; i++) {
			int type = i == 0 ? 0x1 : 0x9; // HEADERS then CONTINUATION
			out.writeBytes(frame(type, endFlags[i] ? 0x4 : 0x0, streamId, blocks[i]));
		}
		return out.toByteArray();
	}

	private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		out.write((payload.length >>> 16) & 0xff);
		out.write((payload.length >>> 8) & 0xff);
		out.write(payload.length & 0xff);
		out.write(type);
		out.write(flags);
		out.write((streamId >>> 24) & 0x7f);
		out.write((streamId >>> 16) & 0xff);
		out.write((streamId >>> 8) & 0xff);
		out.write(streamId & 0xff);
		out.writeBytes(payload);
		return out.toByteArray();
	}

	/**
	 * HPACK literal without indexing, no huffman: 0x00, name-len, name, value-len, value.
	 */
	private static byte[] literal(String name, String value) {
		java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
		byte[] valueBytes = value.getBytes(StandardCharsets.US_ASCII);
		out.write(0x00);
		out.write(nameBytes.length);
		out.writeBytes(nameBytes);
		out.write(valueBytes.length);
		out.writeBytes(valueBytes);
		return out.toByteArray();
	}

	@Test
	void http1HostIsExtracted() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.host()).isEqualTo("demo.local");
		assertThat(parsed.bytes()).isEqualTo(head);
	}

	@Test
	void http1RequestLineIsExtracted() throws IOException {
		byte[] head = "POST /api?id=1 HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head.Request request = PARSER.parse(new ByteArrayInputStream(head))
			.orElseThrow()
			.request();
		assertThat(request).isNotNull();
		assertThat(request.method()).isEqualTo("POST");
		assertThat(request.path()).isEqualTo("/api?id=1");
		assertThat(request.version()).isEqualTo("1.1");
	}

	@Test
	void http2MethodAndPathAreExtracted() throws IOException {
		java.io.ByteArrayOutputStream block = new java.io.ByteArrayOutputStream();
		block.writeBytes(literal(":authority", "demo.local"));
		block.writeBytes(literal(":method", "GET"));
		block.writeBytes(literal(":path", "/index.html"));
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(h2Head(block.toByteArray(), true)))
			.orElseThrow();
		assertThat(parsed.request()).isNotNull();
		assertThat(parsed.request().method()).isEqualTo("GET");
		assertThat(parsed.request().path()).isEqualTo("/index.html");
		assertThat(parsed.request().version()).isEqualTo("2");
	}

	@Test
	void anErrorIsProblemDetailsUnlessTheAcceptNamesTextHtml() {
		String[][] h1 = { { "Accept: text/html", "false" }, { "Accept: text/html,application/json", "false" },
				{ "Accept: text/html;q=0.9,*/*;q=0.8", "false" }, { "Accept: application/json", "true" },
				{ "Accept: text/plain, application/json;q=0.9", "true" }, { "Accept: */*", "true" },
				{ "Content-Type: application/json", "true" }, { "Content-Type: text/html", "true" }, {}, };
		for (String[] header : h1) {
			ConnectionHeadParser.Head parsed = PARSER
				.parse(new ByteArrayInputStream(("GET / HTTP/1.1\r\nHost: a.local\r\n" + String.join("\r\n", header)
						+ (header.length > 0 ? "\r\n" : "") + "\r\n")
					.getBytes(java.nio.charset.StandardCharsets.US_ASCII)))
				.orElseThrow();
			String name = header.length > 0 ? header[0] : "no headers";
			assertThat(json(parsed)).as(name).isEqualTo(header.length == 0 || Boolean.parseBoolean(header[1]));
		}
		assertThat(json(parseH2(literal(":method", "GET"), literal(":path", "/"), literal(":authority", "a.local"),
				literal("accept", "text/html"))))
			.isFalse();
		assertThat(json(parseH2(literal(":method", "GET"), literal(":path", "/"), literal(":authority", "a.local"),
				literal("accept", "application/json"))))
			.isTrue();
		assertThat(json(parseH2(literal(":method", "GET"), literal(":path", "/"), literal(":authority", "a.local"))))
			.isTrue();
	}

	@Test
	void http2ConnectTakesItsAuthorityAsThePath() {
		ConnectionHeadParser.Head parsed = parseH2(literal(":method", "CONNECT"),
				literal(":authority", "tunnel.local:443"));
		assertThat(parsed.request())
			.isEqualTo(new ConnectionHeadParser.Head.Request("CONNECT", "tunnel.local:443", "2"));
		assertThat(parsed.host()).isEqualTo("tunnel.local:443");
		assertThat(parsed.http2Connection()).isTrue();
	}

	@Test
	void http2ExtendedConnectKeepsItsPath() {
		ConnectionHeadParser.Head parsed = parseH2(literal(":method", "CONNECT"), literal(":protocol", "websocket"),
				literal(":scheme", "http"), literal(":authority", "ws.local"), literal(":path", "/chat"));
		assertThat(parsed.request()).isEqualTo(new ConnectionHeadParser.Head.Request("CONNECT", "/chat", "2"));
	}

	@Test
	void http2HeadsThatAreNoRequestYieldNoneYetOpenAnHttp2Connection() {
		byte[][][] blocks = { { literal(":method", "CONNECT"), literal("host", "tunnel.local") },
				{ literal(":method", "CONNECT"), literal(":protocol", "websocket"), literal(":authority", "ws.local") },
				{ literal(":authority", "demo.local"), literal(":path", "/") },
				{ literal(":method", "GET"), literal(":authority", "demo.local") },
				{ literal(":method", "GET"), literal(":authority", "demo.local"), literal(":path", "") } };
		for (byte[][] block : blocks) {
			ConnectionHeadParser.Head parsed = parseH2(block);
			assertThat(parsed.request()).isNull();
			assertThat(parsed.http2Connection()).isTrue();
		}
	}

	@Test
	void http2HeadWhoseBlockDoesNotDecodeIsUndecodable() {
		// index 0 is no HPACK field (RFC 7541 6.1)
		ConnectionHeadParser.Head parsed = parseH2(literal(":authority", "demo.local"), literal(":method", "GET"),
				literal(":path", "/"), new byte[] { (byte) 0x80 });
		assertThat(parsed.undecodable()).isTrue();
		assertThat(parsed.host()).isNull();
		assertThat(parsed.request()).isNull();
		assertThat(parsed.http2Connection()).isTrue();
	}

	@Test
	void http2HeadWithAFieldTheValidationRejectsIsDecodable() {
		ConnectionHeadParser.Head parsed = parseH2(literal(":authority", "demo.local"), literal(":method", "GET"),
				literal(":path", "/"), literal("connection", "close"));
		assertThat(parsed.undecodable()).isFalse();
		assertThat(parsed.request()).isNull();
	}

	@Test
	void http1AndTlsHeadsOpenNoHttp2Connection() {
		byte[] http1 = "GET / HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(http1)).orElseThrow().http2Connection()).isFalse();
		assertThat(ConnectionHeadParser.Head.encrypted(new byte[] { 0x16, 0x03, 0x01 }, "tls.local").http2Connection())
			.isFalse();
	}

	private static boolean json(ConnectionHeadParser.Head head) {
		ConnectionHeadParser.Head.Request request = head.request();
		assertThat(request).isNotNull();
		return request != null && request.json();
	}

	private static ConnectionHeadParser.Head parseH2(byte[]... literals) {
		java.io.ByteArrayOutputStream block = new java.io.ByteArrayOutputStream();
		for (byte[] literal : literals) {
			block.writeBytes(literal);
		}
		return PARSER.parse(new ByteArrayInputStream(h2Head(block.toByteArray(), true))).orElseThrow();
	}

	@Test
	void respHeadHasNoRequest() {
		byte[] head = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().request()).isNull();
	}

	@Test
	void http1HostIsCaseInsensitiveAndTrimmed() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nhost:  demo.local:8080 \r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().host()).isEqualTo("demo.local:8080");
	}

	@Test
	void http1WithoutHostYieldsNullHost() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().host()).isNull();
	}

	@Test
	void http1ForwardedForIsExtractedCaseInsensitively() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-FORWARDED-FOR:  198.51.100.7 \r\n\r\n"
			.getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().forwardedFor()).isEqualTo("198.51.100.7");
	}

	@Test
	void http1WithoutForwardedForYieldsNullWhateverTheBodyHolds() throws IOException {
		byte[] head = "POST / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 31\r\n\r\nX-Forwarded-For: 198.51.100.7\r\n"
			.getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.bytes()).as("precondition: the body was read with the head").isEqualTo(head);
		assertThat(parsed.forwardedFor()).isNull();
	}

	@Test
	void http1HeadersAreReadWithEveryValueInOrder() {
		byte[] head = "GET / HTTP/1.1\r\nAuthorization: Bearer a\r\nHost: demo.local\r\nauthorization:Bearer b \r\n\r\n"
			.getBytes(StandardCharsets.US_ASCII);
		RequestHeaders headers = ConnectionHeadParser.http1Headers(head);
		assertThat(headers.method()).isEqualTo("GET");
		assertThat(headers.values("authorization")).containsExactly("Bearer a", "Bearer b");
		assertThat(headers.values("x-api-token")).isEmpty();
	}

	@Test
	void http2ForwardedForIsExtracted() throws IOException {
		java.io.ByteArrayOutputStream block = new java.io.ByteArrayOutputStream();
		block.writeBytes(literal(":authority", "demo.local"));
		block.writeBytes(literal(":method", "GET"));
		block.writeBytes(literal(":path", "/"));
		block.writeBytes(literal("x-forwarded-for", "198.51.100.7, 203.0.113.1"));
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(h2Head(block.toByteArray(), true)))
			.orElseThrow();
		assertThat(parsed.forwardedFor()).isEqualTo("198.51.100.7, 203.0.113.1");
	}

	@Test
	void respHeadHasNoForwardedFor() {
		byte[] head = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().forwardedFor()).isNull();
	}

	@Test
	void http1ForwardedIsExtractedCaseInsensitively() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\nFORWARDED: for=198.51.100.7;by=10.1.2.3\r\n\r\n"
			.getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().forwarded())
			.isEqualTo("for=198.51.100.7;by=10.1.2.3");
	}

	@Test
	void http1WithoutForwardedYieldsNull() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().forwarded()).isNull();
	}

	@Test
	void http2ForwardedIsExtracted() throws IOException {
		java.io.ByteArrayOutputStream block = new java.io.ByteArrayOutputStream();
		block.writeBytes(literal(":authority", "demo.local"));
		block.writeBytes(literal(":method", "GET"));
		block.writeBytes(literal(":path", "/"));
		block.writeBytes(literal("forwarded", "for=198.51.100.7"));
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(h2Head(block.toByteArray(), true)))
			.orElseThrow();
		assertThat(parsed.forwarded()).isEqualTo("for=198.51.100.7");
	}

	@Test
	void respHeadHasNoForwarded() {
		byte[] head = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().forwarded()).isNull();
	}

	@Test
	void http2AuthorityIsExtracted() throws IOException {
		byte[] head = h2Head(literal(":authority", "demo.local"), true);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.host()).isEqualTo("demo.local");
		assertThat(parsed.bytes()).startsWith(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
	}

	@Test
	void http2FallsBackToHostHeader() throws IOException {
		byte[] head = h2Head(literal("host", "fallback.local"), true);
		assertThat(PARSER.parse(new ByteArrayInputStream(head)).orElseThrow().host()).isEqualTo("fallback.local");
	}

	@Test
	void http2HeadersAcrossContinuationWithPadding() throws IOException {
		byte[] first = literal(":authority", "split.local");
		byte[] second = literal("user-agent", "test/1.0");
		// pad the CONTINUATION payload: pad-length prefix + data + padding zeros
		byte[] padded = new byte[1 + second.length + 3];
		padded[0] = 3;
		System.arraycopy(second, 0, padded, 1, second.length);
		java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		out.writeBytes(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
		out.writeBytes(frame(0x4, 0x0, 0, new byte[0])); // SETTINGS
		out.writeBytes(frame(0x1, 0x0, 7, first)); // HEADERS, no END_HEADERS
		out.writeBytes(frame(0x9, 0x4 | 0x8, 7, padded)); // CONTINUATION,
															// END_HEADERS|PADDED
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(out.toByteArray())).orElseThrow();
		assertThat(parsed.host()).isEqualTo("split.local");
	}

	@Test
	void http2HeadIsIncompleteUntilEndHeaders() throws IOException {
		byte[] head = h2Head(literal(":authority", "x"), false); // no END_HEADERS, no
																	// CONTINUATION
		assertThat(PARSER.parse(new ByteArrayInputStream(head))).isEmpty();
		byte[] rest = frame(0x9, 0x4, 1, new byte[0]);
		byte[] complete = new java.io.ByteArrayOutputStream() {

			{
				this.writeBytes(head);
				this.writeBytes(rest);
			}
		}.toByteArray();
		assertThat(PARSER.parse(new ByteArrayInputStream(complete)).orElseThrow().host()).isEqualTo("x");
	}

	@Test
	void http2FramesBeforeHeadersAreSkipped() throws IOException {
		java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		out.writeBytes(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
		out.writeBytes(frame(0x4, 0x0, 0, new byte[0])); // SETTINGS
		out.writeBytes(frame(0x8, 0x0, 1, new byte[4])); // WINDOW_UPDATE
		out.writeBytes(frame(0x1, 0x4, 3, literal(":authority", "skip.local")));
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(out.toByteArray())).orElseThrow();
		assertThat(parsed.host()).isEqualTo("skip.local");
	}

	@Test
	void http1HeadIsCompleteWhenBodyFollowsInTheSameSegment() throws IOException {
		// a POST whose headers and body arrive in one read: the head ends at the blank
		// line, the body bytes that follow are consumed but must be preserved
		byte[] head = "POST / HTTP/1.1\r\nHost: demo.local\r\nContent-Length: 7\r\n\r\na=1&b=2"
			.getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.host()).isEqualTo("demo.local");
		assertThat(parsed.request()).isNotNull();
		assertThat(parsed.request().method()).isEqualTo("POST");
		assertThat(parsed.bytes()).isEqualTo(head);
	}

	@Test
	void partialHttp1HeadIsNotComplete() throws IOException {
		byte[] head = "GET / HTTP/1.1\r\nHost: a".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head))).isEmpty();
	}

	@Test
	void headIsTruncatedBeyondLimit() throws IOException {
		byte[] head = "GET /aaaaaaaaaaaaaaaa HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		// delivers 5 bytes per read so the head spans multiple reads
		java.io.InputStream chunked = new java.io.InputStream() {

			int pos;

			@Override
			public int read() {
				return this.pos < head.length ? head[this.pos++] & 0xff : -1;
			}

			@Override
			public int read(byte[] b, int off, int len) {
				if (this.pos >= head.length) {
					return -1;
				}
				int n = Math.min(5, b.length - off);
				System.arraycopy(head, this.pos, b, off, n);
				this.pos += n;
				return n;
			}

		};
		ConnectionHeadParser tiny = new ConnectionHeadParser(30);
		assertThat(tiny.parse(chunked)).isEmpty(); // 35-byte head exceeds the 30-byte cap
	}

	@Test
	void respCommandArrayIsARoutableHeadlessHead() {
		byte[] head = "*2\r\n$3\r\nSET\r\n$5\r\nsluice\r\n".getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.host()).isNull(); // routed via the catch-all route
		assertThat(parsed.encrypted()).isFalse();
		assertThat(parsed.bytes()).isEqualTo(head);
	}

	@Test
	void respHeadKeepsPipelinedBytesBeyondTheFirstArray() {
		byte[] head = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head parsed = PARSER.parse(new ByteArrayInputStream(head)).orElseThrow();
		assertThat(parsed.host()).isNull();
		assertThat(parsed.bytes()).isEqualTo(head); // pipelined bytes must not be dropped
	}

	@Test
	void respArrayIsIncompleteUntilTheLastBulkEnds() {
		byte[] head = "*2\r\n$3\r\nSET\r\n$3\r\nke".getBytes(StandardCharsets.US_ASCII);
		assertThat(PARSER.parse(new ByteArrayInputStream(head))).isEmpty();
	}

	@Test
	void emptyStreamYieldsEmpty() {
		assertThat(PARSER.parse(new ByteArrayInputStream(new byte[0]))).isEmpty();
	}

	@Test
	void isHttp2DetectsPrefacePrefixes() {
		assertThat(ConnectionHeadParser.isHttp2("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII)))
			.isTrue();
		assertThat(ConnectionHeadParser.isHttp2("PRI * HTT".getBytes(StandardCharsets.US_ASCII))).isTrue();
		assertThat(ConnectionHeadParser.isHttp2("GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII)))
			.isFalse();
		assertThat(ConnectionHeadParser.isHttp2("GET".getBytes(StandardCharsets.US_ASCII))).isFalse();
	}

}
