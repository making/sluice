package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;

/**
 * Reads the head of an incoming connection far enough to route it: the HTTP/1.1 request
 * head (Host header) or the HTTP/2 client preface through the first HEADERS block
 * ({@code :authority}). The consumed bytes are returned verbatim so they can be replayed
 * to the upstream as the beginning of the relayed stream.
 */
final class ConnectionHeadParser {

	/** HTTP/2 client connection preface (RFC 9113 3.5). */
	static final String H2_MAGIC = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

	private static final int FRAME_HEADER_LENGTH = 9;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FRAME_CONTINUATION = 0x9;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int FLAG_PADDED = 0x8;

	private static final int FLAG_PRIORITY = 0x20;

	private final int maxHeadSize;

	private final DefaultHttp2HeadersDecoder headersDecoder = new DefaultHttp2HeadersDecoder(true);

	ConnectionHeadParser(int maxHeadSize) {
		if (maxHeadSize <= 0) {
			throw new IllegalArgumentException("maxHeadSize must be positive");
		}
		this.maxHeadSize = maxHeadSize;
	}

	record Head(@Nullable String host, byte[] bytes) {

		static Head of(byte[] bytes, @Nullable String host) {
			return new Head(host, bytes);
		}

	}

	/**
	 * Reads from the stream until a routable head (HTTP/1.1 or HTTP/2) has been consumed;
	 * empty on EOF, error, or when no complete head arrives within the size limit.
	 */
	Optional<Head> parse(InputStream in) {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);
		byte[] chunk = new byte[1024];
		try {
			int n;
			while (buffer.size() < this.maxHeadSize && (n = in.read(chunk)) > 0) {
				buffer.write(chunk, 0, n);
				byte[] bytes = buffer.toByteArray();
				if (isHttp2(bytes)) {
					Optional<Head> head = this.parseHttp2(bytes);
					if (head.isPresent()) {
						return head;
					}
				}
				else if (endsWithDoubleCrlf(bytes)) {
					return Optional.of(Head.of(bytes, http1HostOf(bytes)));
				}
			}
		}
		catch (Exception e) {
			return Optional.empty();
		}
		return Optional.empty();
	}

	/**
	 * Returns {@code true} when the accumulated bytes begin with (or are a prefix of) the
	 * HTTP/2 connection preface.
	 */
	static boolean isHttp2(byte[] bytes) {
		if (bytes.length >= H2_MAGIC.length()) {
			return H2_MAGIC.equals(new String(bytes, 0, H2_MAGIC.length(), StandardCharsets.US_ASCII));
		}
		return H2_MAGIC.startsWith(new String(bytes, StandardCharsets.US_ASCII));
	}

	/**
	 * Extracts the Host header value from an HTTP/1.1 request head; {@code null} when
	 * absent.
	 */
	static @Nullable String http1HostOf(byte[] head) {
		String text = new String(head, StandardCharsets.US_ASCII);
		String[] lines = text.split("\r\n");
		for (int i = 1; i < lines.length; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if ("host".equalsIgnoreCase(lines[i].substring(0, colon))) {
				String value = lines[i].substring(colon + 1).trim();
				return value.isBlank() ? null : value;
			}
		}
		return null;
	}

	/**
	 * Walks the HTTP/2 frames of an accumulated buffer and resolves the route host from
	 * the first END_HEADERS-terminated HEADERS block; empty while the head is still
	 * incomplete.
	 */
	private Optional<Head> parseHttp2(byte[] bytes) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		buf.position(H2_MAGIC.length());
		ByteArrayOutputStream headerBlock = new ByteArrayOutputStream(256);
		boolean headersEnded = false;
		int headersStreamId = 1;
		while (buf.remaining() >= FRAME_HEADER_LENGTH && !headersEnded) {
			int length = unsigned24(buf);
			if (length < 0 || buf.remaining() < length) {
				break; // frame not fully buffered yet
			}
			int type = buf.get() & 0xff;
			int flags = buf.get() & 0xff;
			int streamId = buf.getInt() & 0x7fffffff;
			if (type == FRAME_HEADERS) {
				headersStreamId = streamId;
			}
			if (type != FRAME_HEADERS && type != FRAME_CONTINUATION) {
				skip(buf, length);
				continue;
			}
			int start = buf.position();
			int payloadLength = length;
			if ((flags & FLAG_PADDED) != 0 && length > 0) {
				int padLength = buf.get() & 0xff;
				payloadLength -= 1 + padLength;
			}
			if ((flags & FLAG_PRIORITY) != 0 && type == FRAME_HEADERS && payloadLength > 0) {
				buf.position(buf.position() + 5);
				payloadLength -= 5;
			}
			for (int i = 0; i < payloadLength; i++) {
				headerBlock.write(buf.get());
			}
			buf.position(start + length);
			if ((flags & FLAG_END_HEADERS) != 0) {
				headersEnded = true;
			}
		}
		if (!headersEnded) {
			return Optional.empty();
		}
		String host = this.authorityOf(headerBlock.toByteArray(), headersStreamId);
		return Optional.of(Head.of(bytes, host));
	}

	private static int unsigned24(ByteBuffer buf) {
		int length = ((buf.get() & 0xff) << 16) | ((buf.get() & 0xff) << 8) | (buf.get() & 0xff);
		return length;
	}

	private static void skip(ByteBuffer buf, int length) {
		int target = buf.position() + length;
		buf.position(target);
	}

	private @Nullable String authorityOf(byte[] headerBlock, int streamId) {
		try {
			Http2Headers headers = this.headersDecoder.decodeHeaders(streamId, Unpooled.wrappedBuffer(headerBlock));
			CharSequence authority = headers.authority();
			if (authority != null && !authority.isEmpty()) {
				return authority.toString();
			}
			CharSequence hostHeader = headers.get(AsciiString.cached("host"));
			if (hostHeader != null && !hostHeader.isEmpty()) {
				return hostHeader.toString();
			}
		}
		catch (Exception e) {
			// fall through: unroutable head
		}
		return null;
	}

	private static boolean endsWithDoubleCrlf(byte[] bytes) {
		if (bytes == null || bytes.length < 4) {
			return false;
		}
		int length = bytes.length;
		return bytes[length - 4] == '\r' && bytes[length - 3] == '\n' && bytes[length - 2] == '\r'
				&& bytes[length - 1] == '\n';
	}

}
