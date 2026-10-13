package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;

import org.jspecify.annotations.Nullable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;

import org.springframework.stereotype.Component;

/**
 * The data plane's error answers, encoded in the protocol the request arrived in
 * (HTTP/1.1, or HTTP/2 for prior knowledge and ALPN h2), after which the connection
 * closes: the 503 no-route page for a request no tunnel session can serve, the 403
 * forbidden page for a peer address the access control excludes, a bare HTTP/1.1 400 for
 * a malformed request, the HTTP/2 502 page for a stream its upstream lost, a bodiless
 * HTTP/2 421 for a stream of a route reached over HTTP/1.1 only, a GOAWAY
 * {@code COMPRESSION_ERROR} for an HTTP/2 head whose header block does not decode, a
 * GOAWAY processing no stream for an HTTP/2 connection whose relay does not fit, and the
 * 503 busy page with {@code Retry-After} for a request past the connection limit.
 */
@Component
public class ErrorResponse {

	private static final String CONTENT_TYPE = "text/html; charset=utf-8";

	/** The minimum SETTINGS_MAX_FRAME_SIZE every HTTP/2 peer accepts (RFC 9113 4.2). */
	private static final int MAX_FRAME_SIZE = 16_384;

	/** The HTTP/2 error code of a header block that does not decode (RFC 9113 7). */
	private static final int COMPRESSION_ERROR = 0x9;

	/** The debug data of the GOAWAY of {@link #connectionLimit()}. */
	static final String CONNECTION_LIMIT = "connection limit";

	private static final byte[] BAD_REQUEST = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
		.getBytes(StandardCharsets.US_ASCII);

	private final Page noRoute;

	private final Page forbidden;

	private final Page badGateway;

	private final Page busy;

	ErrorResponse(Mustache.Compiler compiler) {
		this.noRoute = new Page(compiler.loadTemplate("proxy/no-route"), "503 Service Unavailable");
		this.forbidden = new Page(compiler.loadTemplate("proxy/forbidden"), "403 Forbidden");
		this.badGateway = new Page(compiler.loadTemplate("proxy/bad-gateway"), "502 Bad Gateway");
		this.busy = new Page(compiler.loadTemplate("proxy/busy"), "503 Service Unavailable");
	}

	/**
	 * The answer to a request past the connection limit: the busy page, a {@code 503}
	 * with a {@code Retry-After} of {@code retryAfter} in whole seconds, in the protocol
	 * of {@code head} (HTTP/2: on the request's stream, a GOAWAY naming it as the last
	 * processed after it); empty for a head that is no HTTP request.
	 */
	public byte[] busy(ConnectionHeadParser.Head head, Duration retryAfter) {
		if (head.request() == null && !head.http2Connection()) {
			return new byte[0];
		}
		return this.render(head, this.busy.retryingAfter(retryAfter.toSeconds()));
	}

	/**
	 * The no-route response bytes for the given connection head.
	 */
	public byte[] noRoute(ConnectionHeadParser.Head head) {
		return this.render(head, this.noRoute);
	}

	/**
	 * The forbidden response bytes for the given connection head.
	 */
	public byte[] forbidden(ConnectionHeadParser.Head head) {
		return this.render(head, this.forbidden);
	}

	/**
	 * The HTTP/1.1 response to a malformed request, after which the connection closes.
	 */
	public byte[] badRequest() {
		return BAD_REQUEST.clone();
	}

	/**
	 * The HTTP/2 answer to a connection head whose first header block does not decode:
	 * the server preface (SETTINGS) and a GOAWAY {@code COMPRESSION_ERROR} (RFC 9113 4.3)
	 * naming no stream as processed.
	 */
	public byte[] compressionError() {
		Http2Frames frames = new Http2Frames(0);
		frames.write(Http2Frame.SETTINGS, new byte[0]);
		frames.write(Http2Frame.GOAWAY, ByteBuffer.allocate(8).putInt(0).putInt(COMPRESSION_ERROR).array());
		return frames.toByteArray();
	}

	/**
	 * The HTTP/2 answer to a connection the server cannot relay as h2 for want of the
	 * bytes its relay reserves: the server preface (SETTINGS) and a GOAWAY
	 * {@code NO_ERROR} naming no stream as processed, so the client may retry its
	 * requests on another connection (RFC 9113 6.8), with {@code connection limit} as its
	 * debug data.
	 */
	public byte[] connectionLimit() {
		Http2Frames frames = new Http2Frames(0);
		frames.write(Http2Frame.SETTINGS, new byte[0]);
		byte[] debug = CONNECTION_LIMIT.getBytes(StandardCharsets.US_ASCII);
		frames.write(Http2Frame.GOAWAY, ByteBuffer.allocate(8 + debug.length).putInt(0).putInt(0).put(debug).array());
		return frames.toByteArray();
	}

	/**
	 * The response on a single stream of a living HTTP/2 connection that this connection
	 * cannot serve -- its route's upstream is reached over HTTP/1.1 only: the head of a
	 * {@code 421} without a body (RFC 9110 15.5.20), so it takes no flow-control window,
	 * and the client may retry the request on another connection; for a relay whose
	 * HTTP/2 codec frames it.
	 */
	public Http2Headers misdirectedHeaders() {
		return new DefaultHttp2Headers().status("421").set("content-length", "0").set("cache-control", "no-store");
	}

	/**
	 * The no-route page on a single stream, for a relay whose HTTP/2 codec frames it.
	 */
	public StreamPage noRoutePage(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request) {
		return this.streamPage(this.noRoute, host, request);
	}

	/**
	 * The forbidden page on a single stream, for a relay whose HTTP/2 codec frames it.
	 */
	public StreamPage forbiddenPage(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request) {
		return this.streamPage(this.forbidden, host, request);
	}

	/**
	 * The bad-gateway page on a single stream its upstream lost before answering, for a
	 * relay whose HTTP/2 codec frames it and holds its body to the client's windows.
	 */
	public StreamPage badGatewayPage(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request) {
		return this.streamPage(this.badGateway, host, request);
	}

	private StreamPage streamPage(Page page, @Nullable String host,
			ConnectionHeadParser.Head.@Nullable Request request) {
		boolean withBody = request != null && !isHead(request);
		byte[] body = this.page(host, request, page);
		Http2Headers headers = new DefaultHttp2Headers().status(page.status())
			.set("content-type", CONTENT_TYPE)
			.set("content-length", String.valueOf(body.length))
			.set("cache-control", "no-store");
		return new StreamPage(headers, withBody ? body : new byte[0]);
	}

	/**
	 * An error page on a single HTTP/2 stream: its head and the body sent after it, empty
	 * when none is (a {@code HEAD} request, or no request), the head's
	 * {@code content-length} naming the page's length all the same.
	 */
	public record StreamPage(Http2Headers headers, byte[] body) {
	}

	private byte[] page(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request, Page page) {
		// an empty value skips its section (emptyStringIsFalse)
		Map<String, String> context = Map.of("host", host == null ? "" : host, "method",
				request == null ? "" : request.method(), "path", request == null ? "" : request.path());
		return page.page().execute(context).getBytes(StandardCharsets.UTF_8);
	}

	private byte[] render(ConnectionHeadParser.Head head, Page page) {
		if (head.encrypted()) {
			return new byte[0];
		}
		if (ConnectionHeadParser.isHttp2(head.bytes())) {
			return this.http2(head, page);
		}
		if (head.request() == null) {
			return bare(page);
		}
		return this.http1(head, head.request(), page);
	}

	private byte[] http1(ConnectionHeadParser.Head head, ConnectionHeadParser.Head.Request request, Page page) {
		byte[] body = this.page(head, page);
		Long retryAfter = page.retryAfter();
		String responseHead = "HTTP/1.1 " + page.reason() + "\r\n" + "Content-Type: " + CONTENT_TYPE + "\r\n"
				+ "Content-Length: " + body.length + "\r\n" + "Cache-Control: no-store\r\n"
				+ (retryAfter == null ? "" : "Retry-After: " + retryAfter + "\r\n") + "Connection: close\r\n\r\n";
		ByteArrayOutputStream out = new ByteArrayOutputStream(responseHead.length() + body.length);
		out.writeBytes(responseHead.getBytes(StandardCharsets.US_ASCII));
		if (!isHead(request)) {
			out.writeBytes(body);
		}
		return out.toByteArray();
	}

	/**
	 * The server preface (SETTINGS) and the acknowledgement of the client's SETTINGS
	 * consumed with the head, the response on the request's stream, and a GOAWAY that
	 * refuses any further stream. The page fits the default 64 KiB flow-control window.
	 */
	private byte[] http2(ConnectionHeadParser.Head head, Page page) {
		ConnectionHeadParser.Head.Request request = head.request();
		int streamId = head.h2() == null ? 1 : head.h2().streamId();
		boolean withBody = request != null && !isHead(request);
		byte[] body = request == null ? new byte[0] : this.page(head, page);
		Http2Headers headers = new DefaultHttp2Headers().status(page.status())
			.set("content-type", CONTENT_TYPE)
			.set("content-length", String.valueOf(body.length))
			.set("cache-control", "no-store");
		Long retryAfter = page.retryAfter();
		if (retryAfter != null) {
			headers.set("retry-after", String.valueOf(retryAfter));
		}
		Http2Frames frames = new Http2Frames(streamId);
		frames.write(Http2Frame.SETTINGS, new byte[0]);
		frames.write(Http2Frame.SETTINGS_ACK, new byte[0]);
		frames.write(withBody ? Http2Frame.HEADERS : Http2Frame.HEADERS_END_STREAM, headerBlock(streamId, headers));
		if (withBody) {
			for (int offset = 0; offset < body.length; offset += MAX_FRAME_SIZE) {
				int end = Math.min(offset + MAX_FRAME_SIZE, body.length);
				frames.write(end == body.length ? Http2Frame.DATA_END_STREAM : Http2Frame.DATA,
						Arrays.copyOfRange(body, offset, end));
			}
		}
		// last stream id, then error code NO_ERROR (0)
		frames.write(Http2Frame.GOAWAY, ByteBuffer.allocate(8).putInt(streamId).array());
		return frames.toByteArray();
	}

	private byte[] page(ConnectionHeadParser.Head head, Page page) {
		ConnectionHeadParser.Head.Request request = head.request();
		// an empty value skips its section (emptyStringIsFalse)
		Map<String, String> context = Map.of("host", head.host() == null ? "" : head.host(), "method",
				request == null ? "" : request.method(), "path", request == null ? "" : request.path());
		return page.page().execute(context).getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] bare(Page page) {
		return ("HTTP/1.1 " + page.reason() + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			.getBytes(StandardCharsets.US_ASCII);
	}

	private static boolean isHead(ConnectionHeadParser.Head.Request request) {
		return "HEAD".equals(request.method());
	}

	/** An error page: its template and its status. */
	private record Page(Template page, Status line) {

		private Page(Template page, String reason) {
			this(page, new Status(reason, null));
		}

		/** The same page with a {@code Retry-After} of {@code seconds}. */
		private Page retryingAfter(long seconds) {
			return new Page(this.page, new Status(this.line.reason(), seconds));
		}

		private String reason() {
			return this.line.reason();
		}

		/** The bare status code: the leading token of the reason. */
		private String status() {
			String reason = this.line.reason();
			int end = reason.indexOf(' ');
			return end < 0 ? reason : reason.substring(0, end);
		}

		private @Nullable Long retryAfter() {
			return this.line.retryAfter();
		}

	}

	/**
	 * An HTTP status line, and the {@code Retry-After} in seconds the response carries;
	 * null = none.
	 */
	private record Status(String reason, @Nullable Long retryAfter) {
	}

	/**
	 * The frames this response emits: type, flags, and whether it is connection-level.
	 */
	private enum Http2Frame {

		DATA(0x0, 0x0), DATA_END_STREAM(0x0, 0x1), HEADERS(0x1, 0x4), HEADERS_END_STREAM(0x1, 0x4 | 0x1),
		SETTINGS(0x4, 0x0), SETTINGS_ACK(0x4, 0x1), GOAWAY(0x7, 0x0);

		private final int type;

		private final int flags;

		Http2Frame(int type, int flags) {
			this.type = type;
			this.flags = flags;
		}

		private boolean connectionLevel() {
			return this == SETTINGS || this == SETTINGS_ACK || this == GOAWAY;
		}

	}

	private static final class Http2Frames {

		private final ByteArrayOutputStream out = new ByteArrayOutputStream(16 * 1024);

		private final int streamId;

		private Http2Frames(int streamId) {
			this.streamId = streamId;
		}

		private void write(Http2Frame frame, byte[] payload) {
			int streamId = frame.connectionLevel() ? 0 : this.streamId;
			this.out.write(payload.length >>> 16 & 0xff);
			this.out.write(payload.length >>> 8 & 0xff);
			this.out.write(payload.length & 0xff);
			this.out.write(frame.type);
			this.out.write(frame.flags);
			this.out.writeBytes(ByteBuffer.allocate(4).putInt(streamId & 0x7fffffff).array());
			this.out.writeBytes(payload);
		}

		private byte[] toByteArray() {
			return this.out.toByteArray();
		}

	}

	/**
	 * HPACK-encodes the headers as never-indexed literals, so the block leaves the peer's
	 * dynamic table untouched whatever SETTINGS_HEADER_TABLE_SIZE it announced.
	 */
	private static byte[] headerBlock(int streamId, Http2Headers headers) {
		ByteBuf buf = Unpooled.buffer(128);
		try {
			new DefaultHttp2HeadersEncoder((name, value) -> true).encodeHeaders(streamId, headers, buf);
			return ByteBufUtil.getBytes(buf);
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(Http2Error.INTERNAL_ERROR.name(), e);
		}
		finally {
			buf.release();
		}
	}

}
