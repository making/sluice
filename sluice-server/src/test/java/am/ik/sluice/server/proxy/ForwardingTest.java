package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Forwarding}, applied to HTTP/1.1 heads as the rerouting relay
 * does ({@link ConnectionHeadRewriter#withHeaders}); the h2 demultiplexing relay applies
 * the same headers to the HEADERS block.
 */
class ForwardingTest {

	private static final String SPOOFED = "X-Forwarded-Proto: http\r\nX-Forwarded-Host: evil.example\r\n"
			+ "X-Forwarded-Port: 1\r\nX-Forwarded-Prefix: /evil\r\nForwarded: proto=http;host=evil.example\r\n";

	@Test
	void anUntrustedPeersClaimsAreReplacedByWhatTheDataPlaneSaw() {
		Forwarding tls = new Forwarding(true, address("203.0.113.9"), false);
		assertThat(forward(tls,
				"GET / HTTP/1.1\r\nHost: demo.local:8443\r\n" + SPOOFED + "X-Forwarded-For: 198.51.100.7\r\n\r\nbody"))
			.isEqualTo("GET / HTTP/1.1\r\nHost: demo.local:8443\r\nx-forwarded-proto: https\r\n"
					+ "x-forwarded-host: demo.local:8443\r\nx-forwarded-port: 8443\r\n"
					+ "x-forwarded-for: 198.51.100.7, 203.0.113.9\r\n\r\nbody");
		Forwarding plaintext = new Forwarding(false, address("::1"), false);
		assertThat(forward(plaintext, "GET / HTTP/1.1\r\nHost: [::1]\r\n\r\n"))
			.isEqualTo("GET / HTTP/1.1\r\nHost: [::1]\r\nx-forwarded-proto: http\r\nx-forwarded-host: [::1]\r\n"
					+ "x-forwarded-port: 80\r\nx-forwarded-for: 0:0:0:0:0:0:0:1\r\n\r\n");
		// no Host, no peer: the protocol and its default port only
		assertThat(forward(new Forwarding(true, null, false), "GET / HTTP/1.0\r\n" + SPOOFED + "\r\n"))
			.isEqualTo("GET / HTTP/1.0\r\nx-forwarded-proto: https\r\nx-forwarded-port: 443\r\n\r\n");
		assertThat(origin(tls, "GET / HTTP/1.1\r\nHost: demo.local:8443\r\n" + SPOOFED + "\r\n"))
			.as("the origin the sign-in returns to")
			.isEqualTo("https://demo.local:8443");
		assertThat(origin(plaintext, "GET / HTTP/1.1\r\nHost: [::1]\r\n\r\n")).isEqualTo("http://[::1]");
		assertThat(origin(tls, "GET / HTTP/1.1\r\nHost: evil.example/x\r\n\r\n")).as("no well-formed host").isNull();
	}

	@Test
	void aTrustedProxysClaimsAreKeptAndTheMissingOnesAdded() {
		Forwarding trusted = new Forwarding(false, address("10.0.0.5"), true);
		// the proxy terminated TLS: the port follows the protocol it stated
		assertThat(forward(trusted,
				"GET / HTTP/1.1\r\nHost: demo.local\r\nX-Forwarded-Proto: https\r\n"
						+ "X-Forwarded-For: 198.51.100.7\r\nForwarded: for=198.51.100.7;proto=https\r\n\r\n"))
			.isEqualTo("GET / HTTP/1.1\r\nHost: demo.local\r\nX-Forwarded-Proto: https\r\n"
					+ "Forwarded: for=198.51.100.7;proto=https\r\nx-forwarded-host: demo.local\r\n"
					+ "x-forwarded-port: 443\r\nx-forwarded-for: 198.51.100.7, 10.0.0.5\r\n\r\n");
		assertThat(forward(trusted, "GET / HTTP/1.1\r\nHost: demo.local\r\n" + SPOOFED + "\r\n"))
			.isEqualTo("GET / HTTP/1.1\r\nHost: demo.local\r\n" + SPOOFED + "x-forwarded-for: 10.0.0.5\r\n\r\n");
		assertThat(origin(trusted, "GET / HTTP/1.1\r\nHost: demo.local\r\nX-Forwarded-Proto: https\r\n\r\n"))
			.as("the origin the sign-in returns to")
			.isEqualTo("https://demo.local");
		assertThat(origin(trusted,
				"GET / HTTP/1.1\r\nHost: internal:8080\r\nX-Forwarded-Proto: https\r\n"
						+ "X-Forwarded-Host: demo.local\r\nX-Forwarded-Port: 8443\r\n\r\n"))
			.isEqualTo("https://demo.local:8443");
		assertThat(origin(trusted, "GET / HTTP/1.1\r\nHost: demo.local\r\n" + SPOOFED + "\r\n"))
			.isEqualTo("http://evil.example:1");
	}

	private static String forward(Forwarding forwarding, String head) {
		byte[] bytes = head.getBytes(StandardCharsets.ISO_8859_1);
		Forwarding.Headers headers = forwarding.headers(ConnectionHeadParser.http1Headers(bytes),
				ConnectionHeadParser.http1HostOf(bytes));
		return new String(ConnectionHeadRewriter.withHeaders(bytes, headers.removed(), headers.added()),
				StandardCharsets.ISO_8859_1);
	}

	private static @Nullable String origin(Forwarding forwarding, String head) {
		byte[] bytes = head.getBytes(StandardCharsets.ISO_8859_1);
		return forwarding.origin(ConnectionHeadParser.http1Headers(bytes), ConnectionHeadParser.http1HostOf(bytes))
			.uri();
	}

	private static @Nullable InetAddress address(String literal) {
		try {
			return InetAddress.getByName(literal);
		}
		catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
	}

}
