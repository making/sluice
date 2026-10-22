package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;

import com.samskivert.mustache.Mustache;
import org.junit.jupiter.api.Test;

import am.ik.sluice.server.auth.Verdict;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorResponseTest {

	private final ErrorResponse errorResponse = new ErrorResponse(
			Mustache.compiler().withLoader(name -> new StringReader("")));

	@Test
	void theConnectionLimitIsAPrefaceAndAGoAwayProcessingNoStream() {
		byte[] debug = "connection limit".getBytes(StandardCharsets.US_ASCII);
		String expected =
				// SETTINGS: empty, no flags, stream 0
				"000000" + "04" + "00" + "00000000"
				// GOAWAY: 8 + 16 bytes, stream 0; last stream id 0, NO_ERROR, the
				// debug data
						+ "000018" + "07" + "00" + "00000000" + "00000000" + "00000000"
						+ HexFormat.of().formatHex(debug);
		assertThat(HexFormat.of().formatHex(this.errorResponse.connectionLimit())).isEqualTo(expected);
	}

	@Test
	void theBusyAnswerIsA503WithRetryAfterInWholeSeconds() {
		ConnectionHeadParser.Head head = head("GET / HTTP/1.1\r\nHost: a.local\r\nAccept: text/html\r\n\r\n");
		assertThat(new String(this.errorResponse.busy(head, Duration.ofMillis(7900)), StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=utf-8\r\n"
					+ "Content-Length: 0\r\nCache-Control: no-store\r\nRetry-After: 7\r\nConnection: close\r\n\r\n");
	}

	/**
	 * A request refused for its credentials gets a 401 with every challenge, a busy check
	 * a 503, and the browser sign-in's answers are a bodiless 302 with its cookies and a
	 * 403 telling why.
	 */
	@Test
	void anUnauthorizedRequestGetsA401WithEveryChallengeAndABusyCheckA503() {
		ConnectionHeadParser.Head head = head("GET / HTTP/1.1\r\nHost: a.local\r\nAccept: text/html\r\n\r\n");
		List<String> challenges = List.of("Basic realm=\"a\", charset=\"UTF-8\"", "Bearer realm=\"a\"");
		assertThat(new String(this.errorResponse.refusal(head, new Verdict.Unauthorized("missing", challenges)),
				StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 401 Unauthorized\r\nContent-Type: text/html; charset=utf-8\r\n"
					+ "Content-Length: 0\r\nCache-Control: no-store\r\nWWW-Authenticate: Basic realm=\"a\", "
					+ "charset=\"UTF-8\"\r\nWWW-Authenticate: Bearer realm=\"a\"\r\nConnection: close\r\n\r\n");
		assertThat(new String(this.errorResponse.refusal(head, new Verdict.Busy(Duration.ofSeconds(1))),
				StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html; charset=utf-8\r\n"
					+ "Content-Length: 0\r\nCache-Control: no-store\r\nRetry-After: 1\r\nConnection: close\r\n\r\n");

		ErrorResponse.StreamPage page = this.errorResponse.refusalPage("a.local",
				new ConnectionHeadParser.Head.Request("GET", "/", "2"),
				new Verdict.Unauthorized("invalid", challenges));
		assertThat(page.headers().status()).hasToString("401");
		assertThat(page.headers().getAll("www-authenticate")).map(CharSequence::toString)
			.containsExactlyElementsOf(challenges);

		Verdict.Redirect redirect = new Verdict.Redirect("https://idp.example/authorize?state=s",
				List.of("__Host-a=1; Path=/", "__Host-b=; Max-Age=0"), Verdict.Redirect.SIGN_IN);
		assertThat(new String(this.errorResponse.refusal(head, redirect), StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 302 Found\r\nContent-Length: 0\r\nCache-Control: no-store\r\n"
					+ "Location: https://idp.example/authorize?state=s\r\nSet-Cookie: __Host-a=1; Path=/\r\n"
					+ "Set-Cookie: __Host-b=; Max-Age=0\r\nConnection: close\r\n\r\n");
		ErrorResponse.StreamPage redirected = this.errorResponse.refusalPage("a.local",
				new ConnectionHeadParser.Head.Request("GET", "/", "2", false), redirect);
		assertThat(redirected.headers().status()).hasToString("302");
		assertThat(redirected.headers().get("location")).hasToString(redirect.location());
		assertThat(redirected.headers().getAll("set-cookie")).map(CharSequence::toString)
			.containsExactlyElementsOf(redirect.cookies());
		assertThat(redirected.headers().contains("content-type")).isFalse();
		assertThat(redirected.body()).isEmpty();
		assertThat(new String(this.errorResponse.refusal(head("GET / HTTP/1.1\r\nHost: a.local\r\n\r\n"),
				new Verdict.Forbidden(Verdict.Forbidden.DENIED)), StandardCharsets.US_ASCII))
			.startsWith("HTTP/1.1 403 Forbidden\r\nContent-Type: application/problem+json\r\n")
			.endsWith("\"status\":403,\"detail\":\"The signed-in user is not allowed to access this host.\"}");
	}

	@Test
	void theDefaultErrorIsAProblemDetailsDocumentWithTheHeadersOfThePage() {
		// no Accept header: the default error is problem details, not the page
		ConnectionHeadParser.Head head = head("GET / HTTP/1.1\r\nHost: a.local\r\n\r\n");
		String json = "{\"type\":\"about:blank\",\"title\":\"Unauthorized\",\"status\":401,"
				+ "\"detail\":\"Credentials are required.\"}";
		assertThat(new String(this.errorResponse.unauthorized(head, List.of("Bearer realm=\"a\"")),
				StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/problem+json\r\n" + "Content-Length: "
					+ json.length() + "\r\nCache-Control: no-store\r\n"
					+ "WWW-Authenticate: Bearer realm=\"a\"\r\nConnection: close\r\n\r\n" + json);
		assertThat(new String(this.errorResponse.busy(head, Duration.ofSeconds(7)), StandardCharsets.US_ASCII))
			.contains("Content-Type: application/problem+json\r\n", "Retry-After: 7\r\n",
					"\"status\":503,\"detail\":\"The server is at its connection limit.\"}");
		// a browser listing text/html as well gets the page
		assertThat(new String(
				this.errorResponse.noRoute(
						head("GET / HTTP/1.1\r\nHost: a.local\r\n" + "Accept: text/html, application/json\r\n\r\n")),
				StandardCharsets.US_ASCII))
			.contains("Content-Type: text/html; charset=utf-8");

		ErrorResponse.StreamPage page = this.errorResponse.refusalPage("a.local",
				new ConnectionHeadParser.Head.Request("POST", "/", "2", true), new Verdict.Busy(Duration.ofSeconds(3)));
		assertThat(page.headers().get("content-type")).hasToString("application/problem+json");
		assertThat(page.headers().get("retry-after")).hasToString("3");
		assertThat(new String(page.body(), StandardCharsets.UTF_8))
			.isEqualTo("{\"type\":\"about:blank\",\"title\":\"Service Unavailable\",\"status\":503,"
					+ "\"detail\":\"The server is checking as many credentials as it may at once.\"}");
		assertThat(page.headers().get("content-length")).hasToString(String.valueOf(page.body().length));
		assertThat(new String(this.errorResponse.refusal(head, new Verdict.Unavailable(Duration.ofSeconds(10))),
				StandardCharsets.US_ASCII))
			.contains("HTTP/1.1 503 Service Unavailable\r\n", "Retry-After: 10\r\n",
					"\"detail\":\"The keys of the identity provider are not loaded yet.\"}");
		ErrorResponse.StreamPage unavailable = this.errorResponse.refusalPage("a.local",
				new ConnectionHeadParser.Head.Request("GET", "/", "2", true),
				new Verdict.Unavailable(Duration.ofSeconds(10)));
		assertThat(unavailable.headers().status()).hasToString("503");
		assertThat(unavailable.headers().get("retry-after")).hasToString("10");
	}

	/**
	 * The busy page tells which of its causes refused the request, the sign-in's denied
	 * page why it forbade it.
	 */
	@Test
	void theBusyPageTellsItsCause() {
		ErrorResponse pages = new ErrorResponse(Mustache.compiler().emptyStringIsFalse(true).withLoader(name -> {
			java.io.InputStream template = getClass().getResourceAsStream("/templates/" + name + ".mustache");
			return new java.io.InputStreamReader(java.util.Objects.requireNonNull(template), StandardCharsets.UTF_8);
		}));
		ConnectionHeadParser.Head head = head("GET / HTTP/1.1\r\nHost: a.local\r\nAccept: text/html\r\n\r\n");
		String limit = new String(pages.busy(head, Duration.ofSeconds(1)), StandardCharsets.UTF_8);
		String verifying = new String(pages.refusal(head, new Verdict.Busy(Duration.ofSeconds(1))),
				StandardCharsets.UTF_8);
		String keys = new String(pages.refusal(head, new Verdict.Unavailable(Duration.ofSeconds(1))),
				StandardCharsets.UTF_8);
		assertThat(limit).contains("at its connection limit", "every connection is taken")
			.doesNotContain("credential", "keys");
		assertThat(verifying).contains("checking as many credentials", "credential checks are all taken")
			.doesNotContain("connection limit", "keys");
		assertThat(keys).contains("has not loaded the keys", "provider keys not loaded")
			.doesNotContain("connection limit", "credential checks");
		String denied = new String(pages.refusal(head, new Verdict.Forbidden(Verdict.Forbidden.DENIED)),
				StandardCharsets.UTF_8);
		String insecure = new String(pages.refusal(head, new Verdict.Forbidden(Verdict.Forbidden.INSECURE)),
				StandardCharsets.UTF_8);
		String failed = new String(pages.refusal(head, new Verdict.Forbidden(Verdict.Forbidden.FAILED)),
				StandardCharsets.UTF_8);
		assertThat(denied).contains("Not allowed for this user", "href=\"/.sluice/oauth2/sign-out\"")
			.doesNotContain("requires https", "Sign-in failed");
		assertThat(insecure).contains("Sign-in requires https").doesNotContain("Not allowed", "Sign-in failed");
		assertThat(failed).contains("Sign-in failed", "href=\"/\"").doesNotContain("Not allowed", "requires https");
	}

	@Test
	void aHeadThatIsNoHttpRequestGetsNoBusyAnswerAndABareNoRoute() {
		ConnectionHeadParser.Head head = head("*1\r\n$4\r\nPING\r\n");
		assertThat(this.errorResponse.busy(head, Duration.ofSeconds(7))).isEmpty();
		assertThat(new String(this.errorResponse.noRoute(head), StandardCharsets.US_ASCII))
			.isEqualTo("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nCache-Control: no-store\r\n"
					+ "Connection: close\r\n\r\n");
	}

	private static ConnectionHeadParser.Head head(String bytes) {
		return new ConnectionHeadParser(64 * 1024)
			.parse(new ByteArrayInputStream(bytes.getBytes(StandardCharsets.US_ASCII)))
			.orElseThrow();
	}

}
