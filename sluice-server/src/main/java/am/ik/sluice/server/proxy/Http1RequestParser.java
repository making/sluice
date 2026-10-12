package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.jspecify.annotations.Nullable;

/**
 * Push parser for the request side of a relayed HTTP/1.1 connection: every complete
 * request head is emitted with its Host header, body bytes in between are passed through,
 * and the body extent (Content-Length or chunked framing) is tracked to find the next
 * head. Heads are read as ISO-8859-1: a field value may carry obs-text (RFC 9110 5.5),
 * which is relayed as is. A request asking for an upgrade (WebSocket, h2c) or a CONNECT
 * holds the parse after its message: the bytes that follow are kept until the caller
 * reports the answer ({@link #upgradeAnswered(boolean)}) -- switched, they are passed
 * through verbatim from then on; declined, they are parsed as the next request.
 * <p>
 * Input the parser cannot follow is a malformed request (RFC 9112 asks a server to answer
 * most of these 400 and close): a head over {@link #MAX_HEAD}, a line not ended by CRLF,
 * a bare CR, a NUL, a malformed request line, a field line folded, whose name is not a
 * token (whitespace before the colon included) or whose value holds a control character
 * other than HTAB, more than one Host, a Content-Length that is not one decimal number, a
 * Transfer-Encoding whose final coding is not chunked or that comes with a
 * Content-Length, and a chunk-size line, chunk terminator or trailer section out of the
 * chunked syntax. The parser then stops at {@link Position#MALFORMED}: the malformed
 * bytes are not relayed, and neither is anything after them.
 */
final class Http1RequestParser {

	/** The most a request head, a chunk-size line or a trailer section may take. */
	static final int MAX_HEAD = 64 * 1024;

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

		/** Left HTTP/1.1 parsing: a switched upgrade or CONNECT. */
		PASSTHROUGH,

		/** Met a malformed request: nothing more is parsed or relayed. */
		MALFORMED

	}

	private enum Mode {

		HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, AWAIT_UPGRADE, PASSTHROUGH, MALFORMED

	}

	private final Listener listener;

	/** The request head, or the chunk-size line or trailer section, being read. */
	private final ByteArrayOutputStream line = new ByteArrayOutputStream(512);

	/** The bytes fed while an upgrade's answer is awaited. */
	private final ByteArrayOutputStream held = new ByteArrayOutputStream(0);

	private Mode mode = Mode.HEAD;

	/** Where the line being read starts in {@link #line}. */
	private int lineStart;

	/** The last byte written to {@link #line}. */
	private byte lastByte;

	/** The message being parsed asked for an upgrade. */
	private boolean upgrading;

	private long remaining;

	/** The bytes of a chunk terminator (CRLF) read so far. */
	private int terminatorRead;

	/** The malformed request's head had been relayed: the body was malformed. */
	private boolean malformedInBody;

	/** The part of the malformed request's head read; empty when it was relayed. */
	private byte[] malformedHead = new byte[0];

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
				case HEAD, CHUNK_SIZE, TRAILERS -> i += this.readLine(bytes, offset + i, length - i);
				case BODY -> i += this.passBody(bytes, offset + i, length - i);
				case CHUNK_DATA -> i += this.passChunk(bytes, offset + i, length - i);
				case CHUNK_CRLF -> {
					byte b = bytes[offset + i];
					if (b != (this.terminatorRead == 0 ? '\r' : '\n')) {
						this.malformed();
						return;
					}
					this.listener.data(bytes, offset + i, 1);
					i++;
					if (++this.terminatorRead == 2) {
						this.mode = Mode.CHUNK_SIZE;
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
				case MALFORMED -> {
					return;
				}
			}
		}
	}

	/**
	 * Reads into {@link #line} up to and including the next LF, then handles the line;
	 * returns the bytes consumed.
	 */
	private int readLine(byte[] bytes, int offset, int length) {
		int lf = -1;
		for (int j = 0; j < length; j++) {
			if (bytes[offset + j] == '\n') {
				lf = j;
				break;
			}
		}
		int n = lf < 0 ? length : lf + 1;
		boolean crBeforeLf = lf > 0 ? bytes[offset + lf - 1] == '\r' : lf == 0 && this.lastByte == '\r';
		this.line.write(bytes, offset, n);
		this.lastByte = bytes[offset + n - 1];
		if (this.line.size() > MAX_HEAD) {
			this.malformed();
			return n;
		}
		if (lf >= 0) {
			if (!crBeforeLf) {
				this.malformed(); // a bare LF ends no line here: framing ambiguity
				return n;
			}
			this.onLine(this.line.size() - this.lineStart == 2);
		}
		return n;
	}

	/** A line ended with CRLF in {@link #line}; {@code empty} when it is just CRLF. */
	private void onLine(boolean empty) {
		switch (this.mode) {
			case HEAD -> {
				if (!empty) {
					this.lineStart = this.line.size();
				}
				else if (this.lineStart == 0) {
					// an empty line before a request line is ignored (RFC 9112 2.2)
					this.resetLine();
				}
				else {
					this.onHead(this.takeLine());
				}
			}
			case CHUNK_SIZE -> this.onChunkSize(this.takeLine());
			case TRAILERS -> {
				if (!empty) {
					this.lineStart = this.line.size();
				}
				else {
					byte[] trailers = this.takeLine();
					if (!validTrailers(trailers)) {
						this.malformed();
						return;
					}
					this.listener.data(trailers, 0, trailers.length);
					this.endMessage();
				}
			}
			default -> throw new IllegalStateException("no line is read in " + this.mode);
		}
	}

	private byte[] takeLine() {
		byte[] bytes = this.line.toByteArray();
		this.resetLine();
		return bytes;
	}

	private void resetLine() {
		this.line.reset();
		this.lineStart = 0;
		this.lastByte = 0;
	}

	private void onHead(byte[] head) {
		Framing framing = framing(head);
		if (framing == null) {
			this.malformedHead = head;
			this.malformed();
			return;
		}
		this.listener.head(head, ConnectionHeadParser.http1HostOf(head));
		this.upgrading = isUpgrade(head);
		if (framing.chunked()) {
			this.mode = Mode.CHUNK_SIZE;
		}
		else if (framing.length() > 0) {
			this.remaining = framing.length();
			this.mode = Mode.BODY;
		}
		else {
			this.endMessage();
		}
	}

	/** The request message ended: the next head follows, or an upgrade's answer. */
	private void endMessage() {
		this.mode = this.upgrading ? Mode.AWAIT_UPGRADE : Mode.HEAD;
		this.upgrading = false;
	}

	/** Stops at a malformed request; what was read of it is dropped. */
	private void malformed() {
		this.malformedInBody = this.mode != Mode.HEAD;
		if (this.mode == Mode.HEAD && this.malformedHead.length == 0) {
			this.malformedHead = this.line.toByteArray();
		}
		this.resetLine();
		this.mode = Mode.MALFORMED;
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
			this.terminatorRead = 0;
			this.mode = Mode.CHUNK_CRLF;
		}
		return n;
	}

	private void onChunkSize(byte[] line) {
		long size = chunkSize(line);
		if (size < 0) {
			this.malformed();
			return;
		}
		// the framing line is part of the stream
		this.listener.data(line, 0, line.length);
		if (size == 0) {
			this.mode = Mode.TRAILERS;
		}
		else {
			this.remaining = size;
			this.mode = Mode.CHUNK_DATA;
		}
	}

	/** Where the parser stands after the bytes fed so far. */
	Position position() {
		return switch (this.mode) {
			case HEAD -> this.line.size() == 0 ? Position.BETWEEN_REQUESTS : Position.IN_HEAD;
			case BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS -> Position.IN_BODY;
			case AWAIT_UPGRADE -> Position.AWAITING_UPGRADE;
			case PASSTHROUGH -> Position.PASSTHROUGH;
			case MALFORMED -> Position.MALFORMED;
		};
	}

	/**
	 * Whether the malformed request's head had been relayed, its body being malformed;
	 * meaningful at {@link Position#MALFORMED}.
	 */
	boolean malformedInBody() {
		return this.malformedInBody;
	}

	/**
	 * What was read of the malformed request's head; empty when the head had been
	 * relayed.
	 */
	byte[] malformedHead() {
		return this.malformedHead.clone();
	}

	/**
	 * Flushes a partially consumed head or line, or held bytes, verbatim at end of input.
	 */
	void finish() {
		if (this.mode == Mode.MALFORMED) {
			return;
		}
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
		String connection = ConnectionHeadParser.http1HeaderOf(head, "connection");
		return connection != null && connection.toLowerCase(Locale.ROOT).contains("upgrade")
				|| ConnectionHeadParser.http1HeaderOf(head, "upgrade") != null;
	}

	/** How a request head frames its body. */
	private record Framing(boolean chunked, long length) {
	}

	/**
	 * The body framing of a complete request head (terminator included); {@code null}
	 * when the head is malformed.
	 */
	private static @Nullable Framing framing(byte[] head) {
		List<int[]> lines = lines(head, head.length - 2);
		if (lines == null || lines.isEmpty() || !validRequestLine(head, lines.get(0)[0], lines.get(0)[1])) {
			return null;
		}
		int hosts = 0;
		List<String> contentLengths = new ArrayList<>();
		List<String> transferCodings = new ArrayList<>();
		for (int i = 1; i < lines.size(); i++) {
			int start = lines.get(i)[0];
			int end = lines.get(i)[1];
			int colon = fieldNameEnd(head, start, end);
			if (colon < 0) {
				return null;
			}
			String name = latin1(head, start, colon).toLowerCase(Locale.ROOT);
			String value = latin1(head, colon + 1, end).strip();
			switch (name) {
				case "host" -> hosts++;
				case "content-length" -> contentLengths.addAll(members(value));
				case "transfer-encoding" -> transferCodings.addAll(members(value));
				default -> {
				}
			}
		}
		if (hosts > 1) {
			return null;
		}
		if (!transferCodings.isEmpty()) {
			boolean chunkedLast = "chunked".equalsIgnoreCase(transferCodings.getLast());
			boolean chunkedOnce = transferCodings.stream().filter("chunked"::equalsIgnoreCase).count() == 1;
			return contentLengths.isEmpty() && chunkedLast && chunkedOnce ? new Framing(true, 0) : null;
		}
		if (contentLengths.isEmpty()) {
			return new Framing(false, 0);
		}
		String length = contentLengths.getFirst();
		if (!length.matches("[0-9]{1,18}") || contentLengths.stream().anyMatch(other -> !other.equals(length))) {
			return null;
		}
		return new Framing(false, Long.parseLong(length));
	}

	/**
	 * The {@code [start, end)} ranges of the CRLF-ended lines of {@code bytes} up to
	 * {@code limit}, terminators excluded; {@code null} when a CR is not followed by LF,
	 * an LF not preceded by CR, or a NUL occurs.
	 */
	private static @Nullable List<int[]> lines(byte[] bytes, int limit) {
		List<int[]> lines = new ArrayList<>();
		int start = 0;
		for (int i = 0; i < limit; i++) {
			byte b = bytes[i];
			if (b == 0 || b == '\n') {
				return null;
			}
			if (b == '\r') {
				if (i + 1 >= limit || bytes[i + 1] != '\n') {
					return null;
				}
				lines.add(new int[] { start, i });
				start = i + 2;
				i++;
			}
		}
		return start == limit ? lines : null;
	}

	/**
	 * Whether {@code [start, end)} is a request line: a token method, a request target of
	 * visible ASCII or obs-text, and {@code HTTP/<digit>.<digit>}, single spaces apart.
	 */
	private static boolean validRequestLine(byte[] bytes, int start, int end) {
		int sp1 = indexOf(bytes, start, end, (byte) ' ');
		int sp2 = sp1 < 0 ? -1 : indexOf(bytes, sp1 + 1, end, (byte) ' ');
		if (sp1 <= start || sp2 <= sp1 + 1 || indexOf(bytes, sp2 + 1, end, (byte) ' ') >= 0) {
			return false;
		}
		for (int i = start; i < sp1; i++) {
			if (!isTchar(bytes[i])) {
				return false;
			}
		}
		for (int i = sp1 + 1; i < sp2; i++) {
			int b = bytes[i] & 0xff;
			if (b <= 0x20 || b == 0x7f) {
				return false;
			}
		}
		return latin1(bytes, sp2 + 1, end).matches("HTTP/[0-9]\\.[0-9]");
	}

	/**
	 * The index of the colon ending the field name of the field line
	 * {@code [start, end)}; {@code -1} when the name is not a token (a folded line, a
	 * space before the colon) or the value holds a control character other than HTAB.
	 */
	private static int fieldNameEnd(byte[] bytes, int start, int end) {
		int colon = indexOf(bytes, start, end, (byte) ':');
		if (colon <= start) {
			return -1;
		}
		for (int i = start; i < colon; i++) {
			if (!isTchar(bytes[i])) {
				return -1;
			}
		}
		for (int i = colon + 1; i < end; i++) {
			int b = bytes[i] & 0xff;
			if (b < 0x20 && b != '\t' || b == 0x7f) {
				return -1;
			}
		}
		return colon;
	}

	/** Whether the trailer section (ended by its blank line) is a run of field lines. */
	private static boolean validTrailers(byte[] trailers) {
		List<int[]> lines = lines(trailers, trailers.length - 2);
		if (lines == null) {
			return false;
		}
		for (int[] line : lines) {
			if (fieldNameEnd(trailers, line[0], line[1]) < 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * The size of a chunk-size line (CRLF included): {@code 1*HEXDIG}, then nothing or
	 * chunk extensions (optional whitespace, then {@code ;}); {@code -1} when malformed
	 * or too large.
	 */
	private static long chunkSize(byte[] line) {
		int end = line.length - 2;
		if (lines(line, end + 2) == null) {
			return -1;
		}
		long size = 0;
		int i = 0;
		for (; i < end && Character.digit(line[i], 16) >= 0; i++) {
			if (size > (Long.MAX_VALUE >> 4)) {
				return -1;
			}
			size = (size << 4) + Character.digit(line[i], 16);
		}
		if (i == 0) {
			return -1;
		}
		while (i < end && (line[i] == ' ' || line[i] == '\t')) {
			i++;
		}
		if (i < end && line[i] != ';') {
			return -1;
		}
		for (; i < end; i++) {
			int b = line[i] & 0xff;
			if (b < 0x20 && b != '\t' || b == 0x7f) {
				return -1;
			}
		}
		return size;
	}

	/** The non-empty members of a comma-separated field value, trimmed. */
	private static List<String> members(String value) {
		List<String> members = new ArrayList<>();
		for (String member : value.split(",")) {
			String trimmed = member.strip();
			if (!trimmed.isEmpty()) {
				members.add(trimmed);
			}
		}
		if (members.isEmpty()) {
			// an empty value is a member that matches nothing valid
			members.add("");
		}
		return members;
	}

	private static boolean isTchar(byte b) {
		return b > 0x20 && b < 0x7f && "\"(),/:;<=>?@[\\]{}".indexOf(b) < 0;
	}

	private static int indexOf(byte[] bytes, int from, int to, byte target) {
		for (int i = from; i < to; i++) {
			if (bytes[i] == target) {
				return i;
			}
		}
		return -1;
	}

	private static String latin1(byte[] bytes, int from, int to) {
		return new String(bytes, from, to - from, StandardCharsets.ISO_8859_1);
	}

}
