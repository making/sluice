package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;

import am.ik.sluice.server.auth.RequestHeaders;

/**
 * Reads the head of an incoming connection far enough to route it: the HTTP/1.1 request
 * head (Host header) or the HTTP/2 client preface through the first HEADERS block
 * ({@code :authority}). The consumed bytes are returned verbatim so they can be replayed
 * to the upstream as the beginning of the relayed stream.
 */
public final class ConnectionHeadParser {

	/** HTTP/2 client connection preface (RFC 9113 3.5). */
	static final String H2_MAGIC = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

	/**
	 * First byte of a RESP array (a Redis command), e.g. {@code *1\r\n$4\r\nPING\r\n}.
	 */
	static final byte RESP_ARRAY = '*';

	private static final AsciiString CONNECT = AsciiString.cached("CONNECT");

	private static final String USER_AGENT = "user-agent";

	/** The pseudo-header of an extended CONNECT (RFC 8441 4). */
	private static final AsciiString PROTOCOL = AsciiString.cached(":protocol");

	private static final int FRAME_HEADER_LENGTH = 9;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FRAME_CONTINUATION = 0x9;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int FLAG_PADDED = 0x8;

	private static final int FLAG_PRIORITY = 0x20;

	private final int maxHeadSize;

	private final DefaultHttp2HeadersDecoder headersDecoder = new DefaultHttp2HeadersDecoder(true);

	public ConnectionHeadParser(int maxHeadSize) {
		if (maxHeadSize <= 0) {
			throw new IllegalArgumentException("maxHeadSize must be positive");
		}
		this.maxHeadSize = maxHeadSize;
	}

	/**
	 * The HTTP/2 parts of the head needed to re-encode the first HEADERS block: the bytes
	 * before the HEADERS frame, the (padding/priority-stripped) header block, the bytes
	 * after it, and the stream id. Present only when the head ends with a single HEADERS
	 * frame (no CONTINUATION).
	 */
	public record H2Head(byte[] prefix, byte[] headerBlock, byte[] suffix, int streamId) {
	}

	/**
	 * A connection head. {@code undecodable} marks an HTTP/2 head whose first header
	 * block failed to decode, a connection error {@code COMPRESSION_ERROR} (RFC 9113
	 * 4.3): the HPACK state is unknown from there on, so nothing of the connection may be
	 * routed.
	 */
	public record Head(@Nullable String host, byte[] bytes, @Nullable H2Head h2, boolean encrypted,
			@Nullable Request request, @Nullable String forwardedFor, @Nullable String forwarded, boolean undecodable) {

		/** The forwarded-for header the access control trusts; literal addresses only. */
		static final String FORWARDED_FOR = "x-forwarded-for";

		/**
		 * The RFC 7239 {@code Forwarded} header; preferred over {@link #FORWARDED_FOR}.
		 */
		static final String FORWARDED = "forwarded";

		/**
		 * The request of the head: the HTTP/1.1 request-line parts, or the method/path of
		 * the first HEADERS block for HTTP/2 ({@code version} is {@code 2}; a CONNECT's
		 * path is its {@code :authority}). {@code json} tells that the request targets a
		 * JSON API, see {@link ConnectionHeadParser#prefersJson}: its error answers are
		 * problem details instead of pages. {@code userAgent} is the first
		 * {@code User-Agent} value for the access log; {@code null} when absent or blank.
		 */
		public record Request(String method, String path, String version, boolean json, @Nullable String userAgent) {

			public Request(String method, String path, String version) {
				this(method, path, version, false);
			}

			public Request(String method, String path, String version, boolean json) {
				this(method, path, version, json, null);
			}

		}

		/**
		 * Whether the head opens an HTTP/2 connection (the client preface), whether or
		 * not its first HEADERS block is a well-formed request.
		 */
		public boolean http2Connection() {
			return !this.encrypted && isHttp2(this.bytes);
		}

		/** A copy of this head with the route host replaced. */
		public Head withHost(@Nullable String host) {
			return new Head(host, this.bytes, this.h2, this.encrypted, this.request, this.forwardedFor, this.forwarded,
					this.undecodable);
		}

		public static Head http1(byte[] bytes, @Nullable String host) {
			return new Head(host, bytes, null, false, http1RequestOf(bytes), http1HeaderOf(bytes, FORWARDED_FOR),
					http1HeaderOf(bytes, FORWARDED), false);
		}

		public static Head http2(byte[] bytes, @Nullable String host, @Nullable H2Head h2, @Nullable Request request,
				@Nullable String forwardedFor, @Nullable String forwarded) {
			return new Head(host, bytes, h2, false, request, forwardedFor, forwarded, false);
		}

		/**
		 * An HTTP/2 head whose first header block failed to decode: nothing to route by.
		 */
		public static Head undecodableHttp2(byte[] bytes) {
			return new Head(null, bytes, null, false, null, null, null, true);
		}

		/**
		 * A TLS passthrough head: {@code host} is the SNI value and the bytes are relayed
		 * verbatim (the rewriter must not touch TLS records).
		 */
		public static Head encrypted(byte[] bytes, @Nullable String host) {
			return new Head(host, bytes, null, true, null, null, null, false);
		}

		/**
		 * A hostless protocol head such as a RESP command array: routable only via the
		 * catch-all route, relayed verbatim.
		 */
		public static Head hostless(byte[] bytes) {
			return new Head(null, bytes, null, false, null, null, null, false);
		}

	}

	/**
	 * Reads from the stream until a routable head (HTTP/1.1 or HTTP/2) has been consumed;
	 * empty on EOF, error, or when no complete head arrives within the size limit.
	 */
	public Optional<Head> parse(InputStream in) {
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
				else if (bytes[0] == RESP_ARRAY) {
					if (respArrayEnd(bytes) >= 0) {
						// the whole buffer is the head: bytes beyond the first array are
						// pipelined commands that must reach the upstream too
						return Optional.of(Head.hostless(bytes));
					}
				}
				else if (headerEnd(bytes) >= 0) {
					// the head ends at the blank line; consumed bytes beyond it (a body
					// or
					// pipelined request arriving in the same segment) are preserved: they
					// are
					// replayed with the head and never re-read from the stream
					return Optional.of(Head.http1(bytes, http1HostOf(bytes)));
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
	public static boolean isHttp2(byte[] bytes) {
		if (bytes.length >= H2_MAGIC.length()) {
			return H2_MAGIC.equals(new String(bytes, 0, H2_MAGIC.length(), StandardCharsets.US_ASCII));
		}
		return H2_MAGIC.startsWith(new String(bytes, StandardCharsets.US_ASCII));
	}

	/**
	 * Extracts the request line ({@code method SP request-target SP HTTP-version}) from
	 * an HTTP/1.1 request head; {@code null} when the first line is not a well-formed
	 * request-line.
	 */
	static Head.@Nullable Request http1RequestOf(byte[] head) {
		String text = new String(head, StandardCharsets.ISO_8859_1);
		int eol = text.indexOf("\r\n");
		String line = eol < 0 ? text : text.substring(0, eol);
		String[] parts = line.split(" ");
		if (parts.length != 3 || !parts[2].startsWith("HTTP/")) {
			return null;
		}
		return new Head.Request(parts[0], parts[1], parts[2].substring("HTTP/".length()),
				prefersJson(http1HeaderValues(head, "content-type"), http1HeaderValues(head, "accept")),
				http1HeaderOf(head, USER_AGENT));
	}

	/**
	 * Extracts the Host header value from an HTTP/1.1 request head; {@code null} when
	 * absent.
	 */
	public static @Nullable String http1HostOf(byte[] head) {
		return http1HeaderOf(head, "host");
	}

	/**
	 * Extracts the value of the first header with the given name from an HTTP/1.1 request
	 * head; {@code null} when absent or blank.
	 */
	static @Nullable String http1HeaderOf(byte[] head, String name) {
		List<String> values = http1HeaderValues(head, name);
		if (values.isEmpty()) {
			return null;
		}
		String value = values.get(0);
		return value.isBlank() ? null : value;
	}

	/**
	 * Every value of the headers with the given name of an HTTP/1.1 request head,
	 * trimmed, in order; the bytes past the head's blank line (a body, a pipelined
	 * request) are not read.
	 */
	static List<String> http1HeaderValues(byte[] head, String name) {
		int end = headerEnd(head);
		String text = new String(head, 0, end < 0 ? head.length : end, StandardCharsets.ISO_8859_1);
		String[] lines = text.split("\r\n");
		List<String> values = new ArrayList<>(1);
		for (int i = 1; i < lines.length; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if (name.equalsIgnoreCase(lines[i].substring(0, colon))) {
				values.add(lines[i].substring(colon + 1).trim());
			}
		}
		return values;
	}

	/**
	 * The method and headers of an HTTP/1.1 request head, as the route's credential check
	 * reads them.
	 */
	static RequestHeaders http1Headers(byte[] head) {
		return new RequestHeaders() {

			@Override
			public String method() {
				Head.Request request = http1RequestOf(head);
				return request == null ? "" : request.method();
			}

			@Override
			public List<String> values(String name) {
				return http1HeaderValues(head, name);
			}

		};
	}

	/**
	 * The method and headers of an HTTP/2 request header block, as the route's credential
	 * check reads them.
	 */
	static RequestHeaders http2Headers(Http2Headers headers) {
		CharSequence method = headers.method();
		return new RequestHeaders() {

			@Override
			public String method() {
				return method == null ? "" : method.toString();
			}

			@Override
			public List<String> values(String name) {
				return headers.getAll(AsciiString.of(name)).stream().map(CharSequence::toString).toList();
			}

		};
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
		boolean continuationSeen = false;
		int headersFrameStart = -1;
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
				headersFrameStart = buf.position() - FRAME_HEADER_LENGTH;
			}
			if (type == FRAME_CONTINUATION) {
				continuationSeen = true;
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
		byte[] block = headerBlock.toByteArray();
		Decoded decoded = decode(block, headersStreamId);
		if (decoded == null) {
			return Optional.of(Head.undecodableHttp2(bytes));
		}
		if (continuationSeen || headersFrameStart < 0) {
			return Optional.of(Head.http2(bytes, decoded.host(), null, decoded.request(), decoded.forwardedFor(),
					decoded.forwarded()));
		}
		byte[] prefix = Arrays.copyOfRange(bytes, 0, headersFrameStart);
		byte[] suffix = Arrays.copyOfRange(bytes, buf.position(), bytes.length);
		return Optional.of(Head.http2(bytes, decoded.host(), new H2Head(prefix, block, suffix, headersStreamId),
				decoded.request(), decoded.forwardedFor(), decoded.forwarded()));
	}

	private static int unsigned24(ByteBuffer buf) {
		int length = ((buf.get() & 0xff) << 16) | ((buf.get() & 0xff) << 8) | (buf.get() & 0xff);
		return length;
	}

	private static void skip(ByteBuffer buf, int length) {
		int target = buf.position() + length;
		buf.position(target);
	}

	private record Decoded(@Nullable String host, Head.@Nullable Request request, @Nullable String forwardedFor,
			@Nullable String forwarded) {
	}

	/**
	 * Decodes the first header block; {@code null} when it does not decode (a connection
	 * error). A stream error -- a field the validation rejects, the block decoded in full
	 * -- leaves the head unroutable instead.
	 */
	private @Nullable Decoded decode(byte[] headerBlock, int streamId) {
		try {
			Http2Headers headers = this.headersDecoder.decodeHeaders(streamId, Unpooled.wrappedBuffer(headerBlock));
			String host = headerOf(headers.authority(), headers.get(AsciiString.cached("host")));
			Head.Request request = http2RequestOf(headers);
			String forwardedFor = headerOf(null, headers.get(AsciiString.of(Head.FORWARDED_FOR)));
			String forwarded = headerOf(null, headers.get(AsciiString.of(Head.FORWARDED)));
			return new Decoded(host, request, forwardedFor, forwarded);
		}
		catch (Http2Exception.StreamException e) {
			// the decoder defers field validation past the whole block: its state holds
			return new Decoded(null, null, null, null);
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * The request of an HTTP/2 request head: its {@code :method} and {@code :path}, or
	 * for a CONNECT (RFC 9113 8.5) its {@code :authority} as the path, like the
	 * authority-form target of an HTTP/1.1 CONNECT. {@code null} for a head that is no
	 * well-formed request in these terms: no {@code :method}, no {@code :path} but for a
	 * CONNECT, a CONNECT without {@code :authority}, or an extended CONNECT (RFC 8441,
	 * {@code :protocol}) without {@code :path}.
	 */
	static Head.@Nullable Request http2RequestOf(Http2Headers headers) {
		CharSequence method = headers.method();
		if (method == null) {
			return null;
		}
		CharSequence path = headers.path();
		if (path == null && CONNECT.contentEquals(method) && !headers.contains(PROTOCOL)) {
			path = headers.authority();
		}
		if (path == null || path.isEmpty()) {
			return null;
		}
		return new Head.Request(method.toString(), path.toString(), "2",
				prefersJson(values(headers, "content-type"), values(headers, "accept")), http2UserAgentOf(headers));
	}

	/**
	 * The first {@code User-Agent} value of an HTTP/2 header block; {@code null} when
	 * absent or blank.
	 */
	static @Nullable String http2UserAgentOf(Http2Headers headers) {
		CharSequence value = headers.get(AsciiString.of(USER_AGENT));
		return value == null || value.toString().isBlank() ? null : value.toString();
	}

	private static List<String> values(Http2Headers headers, String name) {
		return headers.getAll(AsciiString.of(name)).stream().map(CharSequence::toString).toList();
	}

	/**
	 * Whether a request targets a JSON API: its {@code Content-Type} is JSON
	 * ({@code application/json} or a {@code +json} type), or its {@code Accept} lists a
	 * JSON type and no {@code text/html} (a browser's navigation, which lists both, gets
	 * the page).
	 */
	static boolean prefersJson(List<String> contentTypes, List<String> accepts) {
		if (contentTypes.stream().anyMatch(ConnectionHeadParser::isJsonType)) {
			return true;
		}
		List<String> types = accepts.stream()
			.flatMap(accept -> Arrays.stream(accept.split(",")))
			.map(type -> type.split(";")[0].trim().toLowerCase(Locale.ROOT))
			.toList();
		return !types.contains("text/html") && types.stream().anyMatch(ConnectionHeadParser::isJsonType);
	}

	private static boolean isJsonType(String mediaType) {
		String type = mediaType.split(";")[0].trim().toLowerCase(Locale.ROOT);
		return type.equals("application/json") || type.startsWith("application/") && type.endsWith("+json");
	}

	private static @Nullable String headerOf(@Nullable CharSequence first, @Nullable CharSequence second) {
		if (first != null && !first.isEmpty()) {
			return first.toString();
		}
		if (second != null && !second.isEmpty()) {
			return second.toString();
		}
		return null;
	}

	/**
	 * The index just past the blank line that terminates an HTTP/1.1 header section
	 * ({@code \r\n\r\n}) anywhere in the buffer; {@code -1} while the head is still
	 * incomplete.
	 */
	static int headerEnd(byte[] bytes) {
		for (int i = 0; i + 3 < bytes.length; i++) {
			if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n') {
				return i + 4;
			}
		}
		return -1;
	}

	/**
	 * Returns the length of the first complete RESP array in the buffer (a {@code *N}
	 * header followed by N bulk strings {@code $len\r\n<bytes>\r\n}), or {@code -1} while
	 * the array is still incomplete or malformed.
	 */
	static int respArrayEnd(byte[] bytes) {
		int pos = 0;
		int crlfAt = indexOfCrlf(bytes, pos);
		if (crlfAt < 0 || crlfAt == pos + 1) {
			return -1; // array header not fully buffered, or no count
		}
		int count;
		try {
			count = Integer.parseInt(new String(bytes, pos + 1, crlfAt - pos - 1, StandardCharsets.US_ASCII));
		}
		catch (NumberFormatException e) {
			return -1;
		}
		if (count < 0) {
			return -1;
		}
		pos = crlfAt + 2;
		for (int i = 0; i < count; i++) {
			if (pos >= bytes.length || bytes[pos] != '$') {
				return -1;
			}
			crlfAt = indexOfCrlf(bytes, pos);
			if (crlfAt < 0 || crlfAt == pos + 1) {
				return -1; // bulk header not fully buffered, or no length
			}
			int length;
			try {
				length = Integer.parseInt(new String(bytes, pos + 1, crlfAt - pos - 1, StandardCharsets.US_ASCII));
			}
			catch (NumberFormatException e) {
				return -1;
			}
			if (length < 0) {
				return -1;
			}
			pos = crlfAt + 2 + length + 2; // header CRLF + data + trailing CRLF
			if (pos > bytes.length) {
				return -1;
			}
		}
		return pos;
	}

	private static int indexOfCrlf(byte[] bytes, int from) {
		for (int i = from; i + 1 < bytes.length; i++) {
			if (bytes[i] == '\r' && bytes[i + 1] == '\n') {
				return i;
			}
		}
		return -1;
	}

}
