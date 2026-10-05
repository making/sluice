package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

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
