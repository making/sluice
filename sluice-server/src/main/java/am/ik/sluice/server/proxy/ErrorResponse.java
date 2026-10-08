package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
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
 * closes: the 503 no-route page for a request no tunnel session can serve, and the 403
 * forbidden page for a peer address the access control excludes.
 */
@Component
public class ErrorResponse {

	private static final String CONTENT_TYPE = "text/html; charset=utf-8";

	/** The minimum SETTINGS_MAX_FRAME_SIZE every HTTP/2 peer accepts (RFC 9113 4.2). */
	private static final int MAX_FRAME_SIZE = 16_384;

	private final Page noRoute;

	private final Page forbidden;

	ErrorResponse(Mustache.Compiler compiler) {
		this.noRoute = new Page(compiler.loadTemplate("proxy/no-route"), "503 Service Unavailable");
		this.forbidden = new Page(compiler.loadTemplate("proxy/forbidden"), "403 Forbidden");
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
		String responseHead = "HTTP/1.1 " + page.reason() + "\r\n" + "Content-Type: " + CONTENT_TYPE + "\r\n"
				+ "Content-Length: " + body.length + "\r\n" + "Cache-Control: no-store\r\n"
				+ "Connection: close\r\n\r\n";
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

	/**
	 * An error page: its template and its HTTP status line (the bare status code is the
	 * leading token of the reason).
	 */
	private record Page(Template page, String reason) {

		private String status() {
			int end = this.reason.indexOf(' ');
			return end < 0 ? this.reason : this.reason.substring(0, end);
		}

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
