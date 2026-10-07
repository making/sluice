package am.ik.sluice.server.proxy;

import java.nio.charset.StandardCharsets;

/**
 * Rewrites the HTTP/1.1 {@code Host} header line of a request head so the request reaches
 * the upstream with the target's authority. Rewriting is best-effort; any head the
 * rewriter cannot reproduce (non ASCII bytes, missing Host header) is returned verbatim.
 * The h2 equivalent lives in {@link Http2Rewriter}, which re-encodes every HEADERS block
 * of the connection with a single stateful HPACK encoder pair.
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
		byte[] header = java.util.Arrays.copyOfRange(head, 0, headerEnd);
		if (!isAscii(header)) {
			return head;
		}
		String[] lines = new String(header, StandardCharsets.US_ASCII).split("\r\n", -1);
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
		byte[] rewrittenHeader = String.join("\r\n", lines).getBytes(StandardCharsets.US_ASCII);
		byte[] rewritten = new byte[rewrittenHeader.length + head.length - headerEnd];
		System.arraycopy(rewrittenHeader, 0, rewritten, 0, rewrittenHeader.length);
		System.arraycopy(head, headerEnd, rewritten, rewrittenHeader.length, head.length - headerEnd);
		return rewritten;
	}

	static boolean isAscii(byte[] bytes) {
		for (byte b : bytes) {
			if (b < 0) {
				return false;
			}
		}
		return true;
	}

}
