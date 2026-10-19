package am.ik.sluice.server.proxy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rewrites the HTTP/1.1 {@code Host} header line of a request head so the request reaches
 * the upstream with the target's authority, and removes the header lines of a route's
 * credentials. The head is read as ISO-8859-1, so every other byte -- obs-text in a field
 * value included -- is reproduced as is; a head without a Host header is returned
 * verbatim. On relayed plaintext connections the rerouting relay rewrites every head; the
 * HTTP/2 demultiplexing relay re-encodes every request HEADERS block likewise.
 */
final class ConnectionHeadRewriter {

	ConnectionHeadRewriter() {
	}

	/**
	 * Returns the head bytes with the {@code Host} header rewritten to the given
	 * {@code authority}; the original bytes when there is nothing to rewrite.
	 */
	static byte[] rewriteHttp1(byte[] head, String authority) {
		if (authority == null || authority.isBlank()) {
			return head;
		}
		int headerEnd = ConnectionHeadParser.headerEnd(head);
		if (headerEnd < 0) {
			headerEnd = head.length;
		}
		String[] lines = new String(head, 0, headerEnd, StandardCharsets.ISO_8859_1).split("\r\n", -1);
		boolean replaced = false;
		for (int i = 1; i < lines.length - 1; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if ("host".equalsIgnoreCase(lines[i].substring(0, colon).trim())) {
				lines[i] = "Host: " + authority;
				replaced = true;
			}
		}
		if (!replaced) {
			return head;
		}
		byte[] rewrittenHeader = String.join("\r\n", lines).getBytes(StandardCharsets.ISO_8859_1);
		byte[] rewritten = new byte[rewrittenHeader.length + head.length - headerEnd];
		System.arraycopy(rewrittenHeader, 0, rewritten, 0, rewrittenHeader.length);
		System.arraycopy(head, headerEnd, rewritten, rewrittenHeader.length, head.length - headerEnd);
		return rewritten;
	}

	/**
	 * Returns the head bytes without the header lines whose names are in {@code names}
	 * (lowercase); the original bytes when there is none.
	 */
	static byte[] withoutHeaders(byte[] head, Set<String> names) {
		if (names.isEmpty()) {
			return head;
		}
		int headerEnd = ConnectionHeadParser.headerEnd(head);
		if (headerEnd < 0) {
			headerEnd = head.length;
		}
		String[] lines = new String(head, 0, headerEnd, StandardCharsets.ISO_8859_1).split("\r\n", -1);
		List<String> kept = new ArrayList<>(lines.length);
		kept.add(lines[0]);
		for (int i = 1; i < lines.length; i++) {
			int colon = lines[i].indexOf(':');
			if (i < lines.length - 1 && colon > 0
					&& names.contains(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT))) {
				continue;
			}
			kept.add(lines[i]);
		}
		if (kept.size() == lines.length) {
			return head;
		}
		byte[] strippedHeader = String.join("\r\n", kept).getBytes(StandardCharsets.ISO_8859_1);
		byte[] stripped = new byte[strippedHeader.length + head.length - headerEnd];
		System.arraycopy(strippedHeader, 0, stripped, 0, strippedHeader.length);
		System.arraycopy(head, headerEnd, stripped, strippedHeader.length, head.length - headerEnd);
		return stripped;
	}

}
