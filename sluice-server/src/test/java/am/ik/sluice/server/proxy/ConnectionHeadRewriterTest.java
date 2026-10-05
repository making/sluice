package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConnectionHeadRewriter}: HTTP/1.1 Host line replacement and h2
 * HEADERS re-encoding, both verifiable by re-parsing with the production
 * {@link ConnectionHeadParser}.
 */
class ConnectionHeadRewriterTest {

	private static final String AUTHORITY = "127.0.0.1:3128";

	@Test
	void rewritesHostHeaderLineOfHttp1Head() {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Any: 1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		ConnectionHeadParser.Head head0 = parse(head);
		byte[] rewritten = ConnectionHeadRewriter.rewrite(head0, AUTHORITY);
		assertThat(new String(rewritten, StandardCharsets.US_ASCII))
			.isEqualTo("GET / HTTP/1.1\r\nHost: " + AUTHORITY + "\r\nX-Any: 1\r\n\r\n");
		assertThat(parse(rewritten).host()).isEqualTo(AUTHORITY);
	}

	@Test
	void http1HeadWithoutHostIsReturnedVerbatim() {
		byte[] head = "GET / HTTP/1.1\r\nX-Any: 1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		byte[] rewritten = ConnectionHeadRewriter.rewrite(parse(head), AUTHORITY);
		assertThat(rewritten).isEqualTo(head);
	}

	@Test
	void blankAuthorityReturnsHeadVerbatim() {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		byte[] rewritten = ConnectionHeadRewriter.rewrite(parse(head), " ");
		assertThat(rewritten).isEqualTo(head);
	}

	@Test
	void rewritesAuthorityOfHttp2Head() {
		byte[] head = h2Head(":authority=demo.local", ":method=GET", ":path=/");
		ConnectionHeadParser.Head parsed = parse(head);
		assertThat(parsed.host()).isEqualTo("demo.local");
		assertThat(parsed.h2()).isNotNull();
		byte[] rewritten = ConnectionHeadRewriter.rewrite(parsed, AUTHORITY);
		ConnectionHeadParser.Head reparsed = parse(rewritten);
		assertThat(reparsed.host()).isEqualTo(AUTHORITY);
		// the preface and SETTINGS frame are preserved
		byte[] prefix = ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII);
		assertThat(rewritten).startsWith(prefix);
	}

	@Test
	void http2HeadWithContinuationFallsBackToVerbatim() {
		byte[] head = h2HeadWithContinuation(":authority=demo.local", ":method=GET");
		ConnectionHeadParser.Head parsed = parse(head);
		assertThat(parsed.h2()).isNull();
		byte[] rewritten = ConnectionHeadRewriter.rewrite(parsed, AUTHORITY);
		assertThat(rewritten).isEqualTo(head);
	}

	private static ConnectionHeadParser.Head parse(byte[] head) {
		return new ConnectionHeadParser(64 * 1024).parse(new ByteArrayInputStream(head)).orElseThrow();
	}

	private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		return ByteBuffer.allocate(9 + payload.length)
			.put((byte) ((payload.length >>> 16) & 0xff))
			.put((byte) ((payload.length >>> 8) & 0xff))
			.put((byte) (payload.length & 0xff))
			.put((byte) type)
			.put((byte) flags)
			.put((byte) ((streamId >>> 24) & 0x7f))
			.put((byte) ((streamId >>> 16) & 0xff))
			.put((byte) ((streamId >>> 8) & 0xff))
			.put((byte) (streamId & 0xff))
			.put(payload)
			.array();
	}

	private static byte[] literalBlock(String... headers) {
		java.io.ByteArrayOutputStream block = new java.io.ByteArrayOutputStream();
		for (String header : headers) {
			int eq = header.indexOf('=');
			byte[] name = header.substring(0, eq).getBytes(StandardCharsets.US_ASCII);
			byte[] value = header.substring(eq + 1).getBytes(StandardCharsets.US_ASCII);
			block.write(0x00);
			block.write(name.length);
			block.writeBytes(name);
			block.write(value.length);
			block.writeBytes(value);
		}
		return block.toByteArray();
	}

	private static byte[] h2Head(String... headerList) {
		byte[] preface = ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII);
		byte[] settings = frame(0x4, 0x0, 0, new byte[0]);
		byte[] headers = frame(0x1, 0x4 | 0x1, 1, literalBlock(headerList));
		return concat(concat(preface, settings), headers);
	}

	/** HEADERS without END_HEADERS followed by a CONTINUATION frame carrying the rest. */
	private static byte[] h2HeadWithContinuation(String... headers) {
		byte[] all = literalBlock(headers);
		byte[] first = new byte[1];
		byte[] rest = new byte[all.length - 1];
		System.arraycopy(all, 0, first, 0, 1);
		System.arraycopy(all, 1, rest, 0, rest.length);
		byte[] preface = ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII);
		byte[] settings = frame(0x4, 0x0, 0, new byte[0]);
		byte[] headersFrame = frame(0x1, 0x0, 1, first);
		byte[] continuation = frame(0x9, 0x4, 1, rest);
		return concat(concat(concat(preface, settings), headersFrame), continuation);
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] out = new byte[a.length + b.length];
		System.arraycopy(a, 0, out, 0, a.length);
		System.arraycopy(b, 0, out, a.length, b.length);
		return out;
	}

}