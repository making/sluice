package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

/**
 * Push parser for the request side of a relayed HTTP/1.1 connection: every complete
 * request head is emitted with its Host header, body bytes in between are passed through,
 * and the body extent (Content-Length or chunked framing) is tracked to find the next
 * head. A request asking for an upgrade (WebSocket, h2c) or a CONNECT holds the parse
 * after its message: the bytes that follow are kept until the caller reports the answer
 * ({@link #upgradeAnswered(boolean)}) -- switched, they are passed through verbatim from
 * then on; declined, they are parsed as the next request. Any structure the parser cannot
 * follow degrades the remainder to verbatim passthrough.
 */
final class Http1RequestParser {

	private static final int MAX_LINE = 64 * 1024;

	/** Receives the parsed events, in stream order. */
	interface Listener {

		/**
		 * A complete request head, terminator included, with the value of its Host header
		 * ({@code null} when absent).
		 */
		void head(byte[] head, @Nullable String host);

		/** Request bytes that are relayed unchanged. */
		void data(byte[] bytes, int offset, int length);

	}

	/** Where the parser stands in the request stream. */
	enum Position {

		/** At a request boundary with nothing of the next request read. */
		BETWEEN_REQUESTS,

		/** Part of a request head read. */
		IN_HEAD,

		/** Inside a request body. */
		IN_BODY,

		/** After a request asking for an upgrade, until its answer is reported. */
		AWAITING_UPGRADE,

		/** Left HTTP/1.1 parsing: an upgrade, CONNECT, or unparseable bytes. */
		PASSTHROUGH

	}

	private enum Mode {

		HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, AWAIT_UPGRADE, PASSTHROUGH

	}

	private final Listener listener;

	private final ByteArrayOutputStream head = new ByteArrayOutputStream(512);

	private final ByteArrayOutputStream line = new ByteArrayOutputStream(64);

	/** The bytes fed while an upgrade's answer is awaited. */
	private final ByteArrayOutputStream held = new ByteArrayOutputStream(0);

	private Mode mode = Mode.HEAD;

	/** The message being parsed asked for an upgrade. */
	private boolean upgrading;

	private long remaining;

	Http1RequestParser(Listener listener) {
		this.listener = listener;
	}

	/**
	 * Feeds the next bytes of the request stream; a single chunk may span several
	 * requests.
	 */
	void feed(byte[] bytes, int offset, int length) {
		int i = 0;
		while (i < length) {
			switch (this.mode) {
				case HEAD -> {
					this.head.write(bytes[offset + i]);
					boolean lineEnded = bytes[offset + i] == '\n';
					i++;
					if (lineEnded) {
						this.onHeadByte();
					}
				}
				case BODY -> i += this.passBody(bytes, offset + i, length - i);
				case CHUNK_SIZE -> {
					this.line.write(bytes[offset + i]);
					boolean lineEnded = bytes[offset + i] == '\n';
					i++;
					if (lineEnded) {
						this.onChunkSize();
					}
				}
				case CHUNK_DATA -> i += this.passChunk(bytes, offset + i, length - i);
				case CHUNK_CRLF -> {
					this.listener.data(bytes, offset + i, 1);
					boolean lineEnded = bytes[offset + i] == '\n';
					i++;
					if (lineEnded) {
						this.mode = Mode.CHUNK_SIZE;
					}
				}
				case TRAILERS -> {
					this.line.write(bytes[offset + i]);
					boolean lineEnded = bytes[offset + i] == '\n';
					i++;
					if (lineEnded && this.onTrailerLine()) {
						this.endMessage();
					}
				}
				case AWAIT_UPGRADE -> {
					this.held.write(bytes, offset + i, length - i);
					return;
				}
				case PASSTHROUGH -> {
					this.listener.data(bytes, offset + i, length - i);
					return;
				}
			}
		}
	}

	private void onHeadByte() {
		byte[] bytes = this.head.toByteArray();
		int end = ConnectionHeadParser.headerEnd(bytes);
		if (end < 0) {
			if (bytes.length > MAX_LINE || !ConnectionHeadRewriter.isAscii(bytes)) {
				this.emitVerbatim(this.head);
				this.mode = Mode.PASSTHROUGH;
			}
			return;
		}
		this.head.reset();
		byte[] head = Arrays.copyOf(bytes, end);
		if (!ConnectionHeadRewriter.isAscii(head)) {
			this.listener.data(head, 0, head.length);
			this.mode = Mode.PASSTHROUGH;
			return;
		}
		this.listener.head(head, ConnectionHeadParser.http1HostOf(head));
		this.upgrading = isUpgrade(head);
		if (this.isChunked(head)) {
			this.mode = Mode.CHUNK_SIZE;
		}
		else {
			long contentLength = contentLength(head);
			if (contentLength > 0) {
				this.remaining = contentLength;
				this.mode = Mode.BODY;
			}
			else {
				this.endMessage();
			}
		}
	}

	/** The request message ended: the next head follows, or an upgrade's answer. */
	private void endMessage() {
		this.mode = this.upgrading ? Mode.AWAIT_UPGRADE : Mode.HEAD;
		this.upgrading = false;
	}

	/**
	 * Reports the answer to the request asking for an upgrade: {@code switched}, the
	 * bytes held and every byte after them pass through verbatim; declined, they are
	 * parsed as the next request. Ignored unless the parser awaits an answer.
	 */
	void upgradeAnswered(boolean switched) {
		if (this.mode != Mode.AWAIT_UPGRADE) {
			return;
		}
		byte[] held = this.held.toByteArray();
		this.held.reset();
		if (switched) {
			this.mode = Mode.PASSTHROUGH;
			if (held.length > 0) {
				this.listener.data(held, 0, held.length);
			}
		}
		else {
			this.mode = Mode.HEAD;
			this.feed(held, 0, held.length);
		}
	}

	private int passBody(byte[] bytes, int offset, int length) {
		int n = (int) Math.min(this.remaining, length);
		this.listener.data(bytes, offset, n);
		this.remaining -= n;
		if (this.remaining == 0) {
			this.endMessage();
		}
		return n;
	}

	private int passChunk(byte[] bytes, int offset, int length) {
		int n = (int) Math.min(this.remaining, length);
		this.listener.data(bytes, offset, n);
		this.remaining -= n;
		if (this.remaining == 0) {
			this.mode = Mode.CHUNK_CRLF;
		}
		return n;
	}

	private void onChunkSize() {
		byte[] line = this.line.toByteArray();
		this.line.reset();
		this.listener.data(line, 0, line.length); // the framing line is part of the
													// stream
		int size = chunkSize(line);
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
	}

	/**
	 * Completes a trailer line; the line (terminator included) is relayed verbatim and
	 * {@code true} returned when the blank line closed the trailers.
	 */
	private boolean onTrailerLine() {
		byte[] line = this.line.toByteArray();
		this.line.reset();
		this.listener.data(line, 0, line.length);
		return line.length == 2 && line[0] == '\r' && line[1] == '\n';
	}

	/** Where the parser stands after the bytes fed so far. */
	Position position() {
		return switch (this.mode) {
			case HEAD -> this.head.size() == 0 ? Position.BETWEEN_REQUESTS : Position.IN_HEAD;
			case BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS -> Position.IN_BODY;
			case AWAIT_UPGRADE -> Position.AWAITING_UPGRADE;
			case PASSTHROUGH -> Position.PASSTHROUGH;
		};
	}

	/**
	 * Flushes a partially consumed head or line, or held bytes, verbatim at end of input.
	 */
	void finish() {
		this.emitVerbatim(this.head);
		this.emitVerbatim(this.line);
		this.emitVerbatim(this.held);
	}

	private void emitVerbatim(ByteArrayOutputStream buffer) {
		byte[] bytes = buffer.toByteArray();
		buffer.reset();
		if (bytes.length > 0) {
			this.listener.data(bytes, 0, bytes.length);
		}
	}

	/** Whether the request head asks for an upgrade or is a CONNECT. */
	static boolean isUpgrade(byte[] head) {
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http1RequestOf(head);
		if (request != null && "CONNECT".equals(request.method())) {
			return true;
		}
		String connection = header(head, "connection");
		return connection != null && connection.toLowerCase(java.util.Locale.ROOT).contains("upgrade")
				|| header(head, "upgrade") != null;
	}

	private boolean isChunked(byte[] head) {
		String transferEncoding = header(head, "transfer-encoding");
		return transferEncoding != null && transferEncoding.toLowerCase(java.util.Locale.ROOT).contains("chunked");
	}

	private static long contentLength(byte[] head) {
		String value = header(head, "content-length");
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

	private static @Nullable String header(byte[] head, String name) {
		return ConnectionHeadParser.http1HeaderOf(head, name);
	}

	private static int chunkSize(byte[] line) {
		String text = new String(line, StandardCharsets.US_ASCII);
		int semicolon = text.indexOf(';');
		String hex = (semicolon < 0 ? text : text.substring(0, semicolon)).trim();
		try {
			return Integer.parseUnsignedInt(hex, 16);
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}

}
