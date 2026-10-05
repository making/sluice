package am.ik.sluice.server.proxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import am.ik.sluice.server.proxy.ConnectionHeadParser.Head;
import am.ik.sluice.server.proxy.ConnectionHeadParser.H2Head;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;

/**
 * Rewrites the connection head so the request reaches the upstream with the target's
 * authority: the HTTP/1.1 {@code Host} header line is replaced, and for HTTP/2 the first
 * HEADERS block is decoded and re-encoded with the new {@code :authority}. Rewriting is
 * best-effort; any structure the rewriter cannot reproduce (CONTINUATION frames, non
 * ASCII bytes, oversized blocks, decode/encode failures) falls back to the verbatim head.
 */
final class ConnectionHeadRewriter {

	private static final int FRAME_HEADER_LENGTH = 9;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	private ConnectionHeadRewriter() {
	}

	/**
	 * Returns the head bytes with the authority rewritten to the given
	 * {@code host[:port]}; the original bytes when there is nothing to rewrite.
	 */
	static byte[] rewrite(Head head, String authority) {
		if (authority == null || authority.isBlank()) {
			return head.bytes();
		}
		H2Head h2 = head.h2();
		return h2 == null ? rewriteHttp1(head.bytes(), authority) : rewriteHttp2(h2, authority, head.bytes());
	}

	private static byte[] rewriteHttp1(byte[] head, String authority) {
		if (!isAscii(head)) {
			return head;
		}
		String[] lines = new String(head, StandardCharsets.US_ASCII).split("\r\n", -1);
		boolean replaced = false;
		for (int i = 1; i < lines.length - 1; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if ("host".equalsIgnoreCase(lines[i].substring(0, colon))) {
				lines[i] = "Host: " + authority;
				replaced = true;
			}
		}
		if (!replaced) {
			return head;
		}
		return String.join("\r\n", lines).getBytes(StandardCharsets.US_ASCII);
	}

	private static byte[] rewriteHttp2(H2Head h2, String authority, byte[] original) {
		try {
			Http2Headers headers = new DefaultHttp2HeadersDecoder(true).decodeHeaders(h2.streamId(),
					Unpooled.wrappedBuffer(h2.headerBlock()));
			headers.authority(AsciiString.of(authority));
			ByteBuf encoded = Unpooled.buffer(256);
			new DefaultHttp2HeadersEncoder().encodeHeaders(h2.streamId(), headers, encoded);
			int length = encoded.readableBytes();
			if (length <= 0 || length >= (1 << 24)) {
				return original; // does not fit a single HEADERS frame
			}
			byte[] block = new byte[length];
			encoded.readBytes(block);
			ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_LENGTH + length);
			putUnsigned24(frame, length);
			frame.put((byte) FRAME_HEADERS);
			frame.put((byte) FLAG_END_HEADERS);
			frame.putInt(h2.streamId());
			frame.put(block);
			byte[] bytes = new byte[h2.prefix().length + frame.position() + h2.suffix().length];
			System.arraycopy(h2.prefix(), 0, bytes, 0, h2.prefix().length);
			System.arraycopy(frame.array(), 0, bytes, h2.prefix().length, frame.position());
			System.arraycopy(h2.suffix(), 0, bytes, h2.prefix().length + frame.position(), h2.suffix().length);
			return bytes;
		}
		catch (Exception e) {
			return original;
		}
	}

	private static void putUnsigned24(ByteBuffer buf, int value) {
		buf.put((byte) ((value >>> 16) & 0xff));
		buf.put((byte) ((value >>> 8) & 0xff));
		buf.put((byte) (value & 0xff));
	}

	private static boolean isAscii(byte[] bytes) {
		for (byte b : bytes) {
			if (b < 0) {
				return false;
			}
		}
		return true;
	}

}
