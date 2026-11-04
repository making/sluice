package am.ik.sluice.http1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.jspecify.annotations.Nullable;

/**
 * Reads the head of an HTTP/1.1 message, as ISO-8859-1: a field value may carry obs-text
 * (RFC 9110 5.5).
 */
public final class Http1Heads {

	private Http1Heads() {
	}

	/**
	 * The parts of a request line.
	 *
	 * @param method the method
	 * @param target the request target, as sent
	 * @param version the version without {@code HTTP/}
	 */
	public record RequestLine(String method, String target, String version) {
	}

	/**
	 * The index just past the blank line that terminates a header section ({@code
	 * \r\n\r\n}) anywhere in the buffer; {@code -1} while the head is still incomplete.
	 */
	public static int end(byte[] bytes) {
		for (int i = 0; i + 3 < bytes.length; i++) {
			if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n') {
				return i + 4;
			}
		}
		return -1;
	}

	/**
	 * Every value of the headers with the given name, trimmed, in order; the bytes past
	 * the head's blank line (a body, a pipelined request) are not read.
	 */
	public static List<String> values(byte[] head, String name) {
		int end = end(head);
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

	/** The first value of the header with the given name; null when absent or blank. */
	public static @Nullable String first(byte[] head, String name) {
		List<String> values = values(head, name);
		if (values.isEmpty()) {
			return null;
		}
		String value = values.get(0);
		return value.isBlank() ? null : value;
	}

	/**
	 * The request line ({@code method SP request-target SP HTTP-version}) of a request
	 * head; null when the first line is not a well-formed request line.
	 */
	public static @Nullable RequestLine requestLine(byte[] head) {
		String text = new String(head, StandardCharsets.ISO_8859_1);
		int eol = text.indexOf("\r\n");
		String line = eol < 0 ? text : text.substring(0, eol);
		String[] parts = line.split(" ");
		if (parts.length != 3 || !parts[2].startsWith("HTTP/")) {
			return null;
		}
		return new RequestLine(parts[0], parts[1], parts[2].substring("HTTP/".length()));
	}

	/**
	 * The status code of a response head; {@code -1} when its first line tells none.
	 */
	public static int status(byte[] head) {
		String text = new String(head, StandardCharsets.ISO_8859_1);
		int eol = text.indexOf("\r\n");
		String line = eol < 0 ? text : text.substring(0, eol);
		String[] parts = line.trim().split(" ");
		if (parts.length < 2) {
			return -1;
		}
		try {
			return Integer.parseInt(parts[1].trim());
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}

	/** Whether the request head asks for an upgrade or is a CONNECT. */
	public static boolean isUpgrade(byte[] head) {
		RequestLine line = requestLine(head);
		if (line != null && "CONNECT".equals(line.method())) {
			return true;
		}
		String connection = first(head, "connection");
		return connection != null && connection.toLowerCase(Locale.ROOT).contains("upgrade")
				|| first(head, "upgrade") != null;
	}

}
