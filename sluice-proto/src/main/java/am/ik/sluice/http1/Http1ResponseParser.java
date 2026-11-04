package am.ik.sluice.http1;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Push parser for the response side of a relayed HTTP/1.1 connection: a response is
 * complete at the end of its body (Content-Length or last chunk), at its head when it has
 * no body (a response to HEAD, 204, 304), at the end of an unframed body, or when it
 * switches to a tunneled mode (101, a 2xx to CONNECT). Heads are read as ISO-8859-1. A
 * response the parser cannot follow -- a head over {@link #MAX_HEAD} or without a status
 * code, an invalid Content-Length or chunk-size line, a chunk-size or trailer line over
 * {@link #MAX_HEAD} -- leaves the parse: it ends when the upstream closes, and its bytes
 * are reported as they come.
 */
public final class Http1ResponseParser {

	/** The most a response head, a chunk-size line or a trailer line may take. */
	public static final int MAX_HEAD = Http1RequestParser.MAX_HEAD;

	private static final long NO_LENGTH = -1;

	private static final long INVALID_LENGTH = -2;

	/** What follows a final response head. */
	public enum Follows {

		/** A tunneled mode: 101, or a 2xx to CONNECT. */
		SWITCH,

		/** No body, whatever its framing headers say (RFC 9112 6.3). */
		NOTHING,

		/** A body, framed as the head tells. */
		BODY

	}

	/** Receives the parsed events, in stream order. */
	public interface Listener {

		/** An interim head (1xx but 101), terminator included: another head follows. */
		default void interimHead(byte[] head) {
		}

		/**
		 * A final response head, terminator included.
		 * @param status its status code
		 * @return what follows it
		 */
		Follows finalHead(byte[] head, int status);

		/** Payload of a body, without any framing. */
		default void payload(byte[] bytes, int offset, int length) {
		}

		/** A chunk-size line but the last one's, or bytes of a chunk's terminator. */
		default void framing(byte[] bytes, int offset, int length) {
		}

		/** The size line of the last chunk; trailer lines follow. */
		default void lastChunk(byte[] line) {
		}

		/** A trailer line; the blank line ending the section is the last. */
		default void trailer(byte[] line) {
		}

		/** A response ended; the next head, if any, belongs to the next one. */
		void completed();

		/**
		 * The parser cannot follow what comes from here: {@code raw} is what it read of
		 * it. Everything after arrives as {@link #verbatim}, until {@link #cut()}.
		 */
		void untracked(byte[] raw);

		/** Bytes past a switch or of a response untracked. */
		default void verbatim(byte[] bytes, int offset, int length) {
		}

		/**
		 * The stream ended inside a response: a head begun (reported as
		 * {@link #verbatim}), a body not ended, or a response untracked.
		 */
		void cut();

	}

	private enum Mode {

		HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, UNFRAMED, UNTRACKED, PASS

	}

	private final Listener listener;

	private final ByteArrayOutputStream head = new ByteArrayOutputStream(512);

	private final ByteArrayOutputStream line = new ByteArrayOutputStream(64);

	private Mode mode = Mode.HEAD;

	private long remaining;

	/** The bytes of the blank line ending a head matched so far. */
	private int headEnd;

	public Http1ResponseParser(Listener listener) {
		this.listener = listener;
	}

	/** Feeds the next bytes of the response stream. */
	public void feed(byte[] bytes, int offset, int length) {
		int i = offset;
		int end = offset + length;
		while (i < end) {
			switch (this.mode) {
				case HEAD -> onHeadByte(bytes[i++]);
				case BODY -> {
					int n = (int) Math.min(this.remaining, end - i);
					this.listener.payload(bytes, i, n);
					i += n;
					this.remaining -= n;
					if (this.remaining == 0) {
						complete(Mode.HEAD);
					}
				}
				case CHUNK_SIZE -> {
					byte b = bytes[i++];
					this.line.write(b);
					if (this.line.size() > MAX_HEAD) {
						untrack(takeLine());
					}
					else if (b == '\n') {
						onChunkSize(takeLine());
					}
				}
				case CHUNK_DATA -> {
					int n = (int) Math.min(this.remaining, end - i);
					this.listener.payload(bytes, i, n);
					i += n;
					this.remaining -= n;
					if (this.remaining == 0) {
						this.mode = Mode.CHUNK_CRLF;
					}
				}
				case CHUNK_CRLF -> {
					byte b = bytes[i];
					this.listener.framing(bytes, i, 1);
					i++;
					if (b == '\n') {
						this.mode = Mode.CHUNK_SIZE;
					}
				}
				case TRAILERS -> {
					byte b = bytes[i++];
					this.line.write(b);
					if (this.line.size() > MAX_HEAD) {
						untrack(takeLine());
					}
					else if (b == '\n') {
						byte[] trailer = takeLine();
						this.listener.trailer(trailer);
						if (trailer.length == 2 && trailer[0] == '\r') {
							complete(Mode.HEAD);
						}
					}
				}
				case UNFRAMED -> {
					this.listener.payload(bytes, i, end - i);
					i = end;
				}
				case UNTRACKED, PASS -> {
					this.listener.verbatim(bytes, i, end - i);
					i = end;
				}
			}
		}
	}

	/**
	 * At the start of a response: none of the next head arrived, and no response is in
	 * progress or untracked. An interim response relayed counts as begun only while its
	 * head is incomplete.
	 */
	public boolean atResponseStart() {
		return this.mode == Mode.HEAD && this.head.size() == 0;
	}

	/**
	 * The stream ended: an unframed body ends here, a head begun or a body not ended is
	 * cut; at a response start the parser stays there, for the request owed to be
	 * answered otherwise.
	 */
	public void finish() {
		if (this.mode == Mode.PASS || atResponseStart()) {
			return;
		}
		if (this.mode == Mode.UNFRAMED) {
			complete(Mode.PASS);
			return;
		}
		byte[] partial = this.head.toByteArray();
		this.head.reset();
		if (partial.length > 0) {
			this.listener.verbatim(partial, 0, partial.length);
		}
		this.mode = Mode.PASS;
		this.listener.cut();
	}

	/** Whether the head's framing can be followed: its Content-Length is valid. */
	public static boolean followable(byte[] head) {
		return contentLength(head) != INVALID_LENGTH;
	}

	private void onHeadByte(byte b) {
		this.head.write(b);
		// the blank line matched so far: CR LF CR LF
		if (b == '\r') {
			this.headEnd = this.headEnd == 2 ? 3 : 1;
		}
		else if (b == '\n' && (this.headEnd == 1 || this.headEnd == 3)) {
			this.headEnd++;
		}
		else {
			this.headEnd = 0;
		}
		if (this.head.size() > MAX_HEAD) {
			byte[] raw = this.head.toByteArray();
			this.head.reset();
			this.headEnd = 0;
			untrack(raw);
		}
		else if (this.headEnd == 4) {
			byte[] bytes = this.head.toByteArray();
			this.head.reset();
			this.headEnd = 0;
			onHead(bytes);
		}
	}

	private void onHead(byte[] bytes) {
		int status = Http1Heads.status(bytes);
		if (status < 0) {
			untrack(bytes);
			return;
		}
		if (status >= 100 && status < 200 && status != 101) {
			this.listener.interimHead(bytes);
			return;
		}
		switch (this.listener.finalHead(bytes, status)) {
			case SWITCH -> complete(Mode.PASS);
			case NOTHING -> complete(Mode.HEAD);
			case BODY -> onBodyFraming(bytes);
		}
	}

	private void onBodyFraming(byte[] bytes) {
		long length = contentLength(bytes);
		if (length == INVALID_LENGTH) {
			untrack(new byte[0]);
		}
		else if (length == 0) {
			complete(Mode.HEAD);
		}
		else if (length > 0) {
			this.remaining = length;
			this.mode = Mode.BODY;
		}
		else {
			String coding = Http1Heads.first(bytes, "transfer-encoding");
			this.mode = coding != null && coding.toLowerCase(Locale.ROOT).contains("chunked") ? Mode.CHUNK_SIZE
					: Mode.UNFRAMED;
		}
	}

	private static long contentLength(byte[] head) {
		String value = Http1Heads.first(head, "content-length");
		if (value == null) {
			return NO_LENGTH;
		}
		try {
			long length = Long.parseLong(value.trim());
			return length < 0 ? INVALID_LENGTH : length;
		}
		catch (NumberFormatException e) {
			return INVALID_LENGTH;
		}
	}

	private void onChunkSize(byte[] bytes) {
		long size = chunkSize(bytes);
		if (size < 0) {
			untrack(bytes);
		}
		else if (size == 0) {
			this.listener.lastChunk(bytes);
			this.mode = Mode.TRAILERS;
		}
		else {
			this.listener.framing(bytes, 0, bytes.length);
			this.remaining = size;
			this.mode = Mode.CHUNK_DATA;
		}
	}

	/** The size of a chunk-size line, extensions ignored; {@code -1} when invalid. */
	private static long chunkSize(byte[] bytes) {
		String text = new String(bytes, StandardCharsets.ISO_8859_1);
		int semicolon = text.indexOf(';');
		String hex = (semicolon < 0 ? text : text.substring(0, semicolon)).trim();
		if (hex.isEmpty() || Character.digit(hex.charAt(0), 16) < 0) {
			return -1;
		}
		try {
			return Long.parseLong(hex, 16);
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}

	private byte[] takeLine() {
		byte[] bytes = this.line.toByteArray();
		this.line.reset();
		return bytes;
	}

	private void untrack(byte[] raw) {
		this.mode = Mode.UNTRACKED;
		this.listener.untracked(raw);
	}

	private void complete(Mode next) {
		this.mode = next;
		this.listener.completed();
	}

}
