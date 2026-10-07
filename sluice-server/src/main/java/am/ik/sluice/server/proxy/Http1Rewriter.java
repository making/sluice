package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

/**
 * Per-request HTTP/1.1 Host rewriter: a byte level state machine that replaces the
 * {@code Host} header of every request head and relays the body untouched, tracking the
 * body extent (Content-Length or chunked framing) to find the next head. Upgraded
 * connections (WebSocket, h2c, CONNECT) are rewritten up to the switch and relayed
 * verbatim afterwards.
 */
final class Http1Rewriter implements HostRewritingPipe.Rewriter {

	private static final int MAX_BUFFER = 64 * 1024;

	private enum Mode {

		HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, PASSTHROUGH

	}

	private final String authority;

	private final ByteArrayOutputStream headBuffer = new ByteArrayOutputStream(512);

	private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(64);

	private Mode mode = Mode.HEAD;

	private long remaining;

	Http1Rewriter(String authority) {
		this.authority = authority;
	}

	@Override
	public void rewrite(byte[] bytes, int offset, int length, HostRewritingPipe.Sink sink) {
		for (int i = 0; i < length; i++) {
			this.accept(bytes[offset + i], sink);
		}
	}

	private void accept(byte value, HostRewritingPipe.Sink sink) {
		switch (this.mode) {
			case HEAD -> {
				this.headBuffer.write(value);
				if (value == '\n') {
					this.onHeadByte(sink);
				}
			}
			case BODY -> {
				sink.accept(value);
				if (--this.remaining == 0) {
					this.mode = Mode.HEAD;
				}
			}
			case CHUNK_SIZE -> this.onLine(value, sink, () -> {
				int size = this.chunkSize();
				this.lineBuffer.reset();
				if (size < 0) {
					this.mode = Mode.PASSTHROUGH;
				}
				else if (size == 0) {
					this.mode = Mode.TRAILERS;
				}
				else {
					this.remaining = size;
					this.mode = Mode.CHUNK_DATA;
				}
			});
			case CHUNK_DATA -> {
				sink.accept(value);
				if (--this.remaining == 0) {
					this.mode = Mode.CHUNK_CRLF;
				}
			}
			case CHUNK_CRLF -> {
				sink.accept(value);
				if (value == '\n') {
					this.mode = Mode.CHUNK_SIZE;
				}
			}
			case TRAILERS -> this.onLine(value, sink, () -> {
				byte[] line = this.lineBuffer.toByteArray();
				this.lineBuffer.reset();
				if (this.isBlankLine(line)) {
					this.mode = Mode.HEAD;
				}
			});
			case PASSTHROUGH -> sink.accept(value);
		}
	}

	private interface LineAction {

		void run();

	}

	/**
	 * Accumulates a CRLF terminated line and runs the action once it completes; overlong
	 * or non ASCII lines degrade the connection to passthrough.
	 */
	private void onLine(byte value, HostRewritingPipe.Sink sink, LineAction action) {
		this.lineBuffer.write(value);
		if (value != '\n') {
			if (this.lineBuffer.size() > MAX_BUFFER || value < 0) {
				this.emit(this.lineBuffer, sink);
				this.lineBuffer.reset();
				this.mode = Mode.PASSTHROUGH;
			}
			return;
		}
		// the framing line itself is part of the stream
		this.emit(this.lineBuffer, sink);
		action.run();
	}

	private void onHeadByte(HostRewritingPipe.Sink sink) {
		byte[] bytes = this.headBuffer.toByteArray();
		int end = ConnectionHeadParser.headerEnd(bytes);
		if (end < 0) {
			if (bytes.length > MAX_BUFFER || !isAsciiSoFar(bytes)) {
				this.emit(this.headBuffer, sink);
				this.headBuffer.reset();
				this.mode = Mode.PASSTHROUGH;
			}
			return;
		}
		this.headBuffer.reset();
		byte[] head = Arrays.copyOf(bytes, end);
		if (!ConnectionHeadRewriter.isAscii(head)) {
			sink.accept(head, 0, head.length);
			this.mode = Mode.PASSTHROUGH;
			return;
		}
		byte[] rewritten = ConnectionHeadRewriter.rewriteHttp1(head, this.authority);
		sink.accept(rewritten, 0, rewritten.length);
		if (this.isUpgrade(head)) {
			this.mode = Mode.PASSTHROUGH;
		}
		else if (this.isChunked(head)) {
			this.mode = Mode.CHUNK_SIZE;
		}
		else {
			long contentLength = this.contentLength(head);
			if (contentLength > 0) {
				this.remaining = contentLength;
				this.mode = Mode.BODY;
			}
			else {
				this.mode = Mode.HEAD;
			}
		}
	}

	private boolean isUpgrade(byte[] head) {
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http1RequestOf(head);
		if (request != null && "CONNECT".equals(request.method())) {
			return true;
		}
		String connection = this.header(head, "connection");
		return connection != null && connection.toLowerCase(java.util.Locale.ROOT).contains("upgrade")
				|| this.header(head, "upgrade") != null;
	}

	private boolean isChunked(byte[] head) {
		String transferEncoding = this.header(head, "transfer-encoding");
		return transferEncoding != null && transferEncoding.toLowerCase(java.util.Locale.ROOT).contains("chunked");
	}

	private long contentLength(byte[] head) {
		String value = this.header(head, "content-length");
		if (value == null) {
			return -1;
		}
		try {
			return Long.parseLong(value.trim());
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}

	private @Nullable String header(byte[] head, String name) {
		String[] lines = new String(head, StandardCharsets.US_ASCII).split("\r\n");
		for (int i = 1; i < lines.length; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if (name.equalsIgnoreCase(lines[i].substring(0, colon).trim())) {
				return lines[i].substring(colon + 1).trim();
			}
		}
		return null;
	}

	private int chunkSize() {
		String line = this.lineBuffer.toString(StandardCharsets.US_ASCII);
		int semicolon = line.indexOf(';');
		String hex = (semicolon < 0 ? line : line.substring(0, semicolon)).trim();
		try {
			return Integer.parseUnsignedInt(hex, 16);
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}

	private boolean isBlankLine(byte[] line) {
		return line.length == 2 && line[0] == '\r' && line[1] == '\n';
	}

	private void emit(ByteArrayOutputStream buffer, HostRewritingPipe.Sink sink) {
		byte[] bytes = buffer.toByteArray();
		sink.accept(bytes, 0, bytes.length);
	}

	private static boolean isAsciiSoFar(byte[] bytes) {
		for (byte b : bytes) {
			if (b < 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void finish(HostRewritingPipe.Sink sink) {
		// a truncated head or chunk line is flushed verbatim; body bytes already went out
		this.emit(this.headBuffer, sink);
		this.emit(this.lineBuffer, sink);
	}

}
