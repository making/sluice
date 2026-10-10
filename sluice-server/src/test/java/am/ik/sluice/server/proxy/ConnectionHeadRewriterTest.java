package am.ik.sluice.server.proxy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConnectionHeadRewriter}: HTTP/1.1 Host line replacement,
 * verifiable by re-parsing with the production {@link ConnectionHeadParser}. The h2
 * equivalents are covered end to end by the rerouting and demultiplexing E2E tests.
 */
class ConnectionHeadRewriterTest {

	private static final String AUTHORITY = "127.0.0.1:3128";

	@Test
	void rewritesHostHeaderLineOfHttp1Head() {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Any: 1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		byte[] rewritten = ConnectionHeadRewriter.rewriteHttp1(head, AUTHORITY);
		assertThat(new String(rewritten, StandardCharsets.US_ASCII))
			.isEqualTo("GET / HTTP/1.1\r\nHost: " + AUTHORITY + "\r\nX-Any: 1\r\n\r\n");
		assertThat(ConnectionHeadParser.http1HostOf(rewritten)).isEqualTo(AUTHORITY);
	}

	@Test
	void http1HeadWithoutHostIsReturnedVerbatim() {
		byte[] head = "GET / HTTP/1.1\r\nX-Any: 1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(ConnectionHeadRewriter.rewriteHttp1(head, AUTHORITY)).isEqualTo(head);
	}

	@Test
	void blankAuthorityReturnsHeadVerbatim() {
		byte[] head = "GET / HTTP/1.1\r\nHost: demo.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
		assertThat(ConnectionHeadRewriter.rewriteHttp1(head, " ")).isEqualTo(head);
	}

}
