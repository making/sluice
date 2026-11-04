package am.ik.sluice.client.filter;

import java.nio.charset.StandardCharsets;

/**
 * Picks fields out of JSON as it streams, holding nothing of it but the fields asked for:
 * a body of any size, split anywhere, costs a fixed few hundred bytes. It reads what is
 * well formed and is merely wrong on what is not; it never fails. Fields are found by
 * their path of object keys, {@code a.b.c}: nothing inside an array is reported, nor past
 * {@link #MAX_DEPTH}. A captured value keeps an escape's character without its backslash:
 * enough for what is asked of it.
 */
final class JsonFields {

	/** Told about the fields of one document. */
	interface Sink {

		/**
		 * Whether the path is of interest: a field to report, or an object to look into.
		 */
		boolean wants(String path);

		/**
		 * A value at a path it wanted: a string's content, cut at {@link #MAX_TEXT}, or a
		 * number or literal as written. An object or array is reported with an empty
		 * text.
		 */
		void field(String path, String text);

		/** The outermost object closed. */
		void documentEnd();

	}

	private static final int MAX_DEPTH = 6;

	private static final int MAX_TEXT = 128;

	private final Sink sink;

	/** What precedes a key at each depth of open objects. */
	private final String[] prefixes = new String[MAX_DEPTH + 1];

	private final byte[] text = new byte[MAX_TEXT];

	private int textLength;

	/** The objects open. */
	private int depth;

	/** The containers open inside what is skipped; 0 when none is. */
	private int skipping;

	private boolean inString;

	private boolean escaped;

	private boolean expectingKey;

	/** The string being read is a key. */
	private boolean isKey;

	/** A number or literal is being read. */
	private boolean inLiteral;

	/** The path of the value that follows. */
	private String path = "";

	/** The value that follows is reported. */
	private boolean wanted;

	JsonFields(Sink sink) {
		this.sink = sink;
		this.prefixes[0] = "";
	}

	/** Back to the start of a document, whatever was read of one. */
	void reset() {
		this.depth = 0;
		this.skipping = 0;
		this.inString = false;
		this.escaped = false;
		this.expectingKey = false;
		this.inLiteral = false;
		this.wanted = false;
		this.textLength = 0;
	}

	void feed(byte[] bytes, int offset, int length) {
		int end = offset + length;
		for (int i = offset; i < end; i++) {
			byte b = bytes[i];
			if (this.inString) {
				inString(b);
			}
			else if (this.skipping > 0) {
				skipped(b);
			}
			else {
				structure(b);
			}
		}
	}

	private void inString(byte b) {
		if (this.escaped) {
			this.escaped = false;
			capture(b);
		}
		else if (b == '\\') {
			this.escaped = true;
		}
		else if (b == '"') {
			this.inString = false;
			endString();
		}
		else {
			capture(b);
		}
	}

	private void capture(byte b) {
		if (this.textLength < MAX_TEXT) {
			this.text[this.textLength++] = b;
		}
	}

	private void endString() {
		if (this.skipping > 0) {
			return;
		}
		if (this.isKey) {
			this.path = this.prefixes[this.depth] + captured();
			this.wanted = this.sink.wants(this.path);
			this.expectingKey = false;
		}
		else if (this.wanted) {
			this.wanted = false;
			this.sink.field(this.path, captured());
		}
	}

	private String captured() {
		String captured = new String(this.text, 0, this.textLength, StandardCharsets.UTF_8);
		this.textLength = 0;
		return captured;
	}

	private void skipped(byte b) {
		switch (b) {
			case '"' -> {
				this.inString = true;
				this.isKey = false;
				this.textLength = 0;
			}
			case '{', '[' -> this.skipping++;
			case '}', ']' -> this.skipping--;
			default -> {
			}
		}
	}

	private void structure(byte b) {
		switch (b) {
			case ' ', '\t', '\r', '\n', ':' -> endLiteral();
			case '"' -> {
				endLiteral();
				this.inString = true;
				this.isKey = this.expectingKey;
				this.textLength = 0;
			}
			case '{' -> open();
			case '[' -> {
				container();
				this.skipping = 1;
			}
			case '}' -> close();
			case ',' -> {
				endLiteral();
				this.expectingKey = this.depth > 0;
			}
			default -> {
				if (!this.inLiteral) {
					this.inLiteral = true;
					this.textLength = 0;
				}
				capture(b);
			}
		}
	}

	private void open() {
		if (this.depth == 0) {
			this.depth = 1;
			this.prefixes[1] = "";
			this.expectingKey = true;
			return;
		}
		boolean inside = this.wanted && this.depth < MAX_DEPTH;
		container();
		if (inside) {
			this.prefixes[this.depth + 1] = this.path + ".";
			this.depth++;
			this.expectingKey = true;
		}
		else {
			this.skipping = 1;
		}
	}

	/** An object or array is a value too: its presence is reported. */
	private void container() {
		if (this.wanted) {
			this.wanted = false;
			this.sink.field(this.path, "");
		}
	}

	private void close() {
		endLiteral();
		if (this.depth > 0) {
			this.depth--;
			this.expectingKey = false;
			if (this.depth == 0) {
				this.sink.documentEnd();
			}
		}
	}

	private void endLiteral() {
		if (this.inLiteral) {
			this.inLiteral = false;
			if (this.wanted) {
				this.wanted = false;
				this.sink.field(this.path, captured());
			}
			this.textLength = 0;
		}
	}

}
