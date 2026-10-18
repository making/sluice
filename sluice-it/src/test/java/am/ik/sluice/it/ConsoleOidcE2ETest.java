package am.ik.sluice.it;

import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import am.ik.sluice.server.SluiceServerApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console's {@code oidc} sign-in delegates to OpenID providers
 * ({@link MockOidcServer}): login lists them by name, a login grants access, sign-out
 * ends the provider session too where the provider has an end session endpoint.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConsoleOidcE2ETest {

	private static final MockOidcServer oidcServer = new MockOidcServer("sluice-console");

	/** A provider without RP-initiated logout. */
	private static final MockOidcServer localLogoutServer = new MockOidcServer("sluice-console", false);

	@LocalServerPort
	int port;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.grpc.server.port", () -> 0);
		registry.add("sluice.data-port", () -> 0);
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.console.auth.type", () -> "oidc");
		registry.add("spring.security.oauth2.client.registration.sluice-console.client-id", () -> "sluice-console");
		registry.add("spring.security.oauth2.client.registration.sluice-console.client-secret", () -> "it-secret");
		registry.add("spring.security.oauth2.client.registration.sluice-console.scope", () -> "openid,profile,email");
		registry.add("spring.security.oauth2.client.provider.sluice-console.issuer-uri", oidcServer::issuer);
		// a second provider whose id and name sort in opposite orders, without an
		// end session endpoint
		registry.add("spring.security.oauth2.client.registration.aaa.client-id", () -> "sluice-console");
		registry.add("spring.security.oauth2.client.registration.aaa.client-secret", () -> "it-secret");
		registry.add("spring.security.oauth2.client.registration.aaa.client-name", () -> "Zeta IdP");
		registry.add("spring.security.oauth2.client.registration.aaa.scope", () -> "openid,profile,email");
		registry.add("spring.security.oauth2.client.provider.aaa.issuer-uri", localLogoutServer::issuer);
	}

	@AfterAll
	static void stopOidc() {
		oidcServer.close();
		localLogoutServer.close();
	}

	private String base() {
		return "http://127.0.0.1:" + this.port;
	}

	/**
	 * A browser-like client: a cookie jar, and redirects followed across the console and
	 * the mock IdP (the IdP consent is instant). HTTP/1.1 like a browser on plaintext
	 * HTTP: the JDK client's default h2c upgrade is a path no browser takes (on CI the
	 * upgraded connection to the console once ended in an EOF).
	 */
	private static HttpClient browser() {
		return HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.cookieHandler(new CookieManager())
			.followRedirects(HttpClient.Redirect.ALWAYS)
			.build();
	}

	private HttpResponse<String> send(HttpClient client, HttpRequest.Builder request) throws Exception {
		return client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
	}

	private HttpResponse<String> signIn(HttpClient client, String registrationId) throws Exception {
		HttpResponse<String> console = send(client,
				HttpRequest.newBuilder(URI.create(base() + "/oauth2/authorization/" + registrationId)));
		assertThat(console.uri().getPath()).isEqualTo("/console");
		return console;
	}

	private HttpResponse<String> signOut(HttpClient client, HttpResponse<String> console) throws Exception {
		return send(client,
				HttpRequest.newBuilder(URI.create(base() + "/logout"))
					.header("Content-Type", "application/x-www-form-urlencoded")
					.POST(HttpRequest.BodyPublishers
						.ofString("_csrf=" + ConsoleAuthE2ETest.urlEncode(ConsoleAuthE2ETest.csrfOf(console)))));
	}

	@Test
	void signOutEndsTheProviderSessionToo() throws Exception {
		try (HttpClient client = browser()) {
			HttpResponse<String> console = signIn(client, "sluice-console");
			HttpResponse<String> loginPage = signOut(client, console);
			assertThat(loginPage.uri()).hasToString(base() + "/login?logout");
			assertThat(oidcServer.lastLogout()).containsEntry("post_logout_redirect_uri", base() + "/login?logout")
				.containsKey("id_token_hint");
			assertThat(send(client, HttpRequest.newBuilder(URI.create(base() + "/console"))).uri().getPath())
				.isEqualTo("/login");
		}
	}

	@Test
	void signOutStaysLocalWhenTheProviderHasNoEndSessionEndpoint() throws Exception {
		try (HttpClient client = browser()) {
			HttpResponse<String> console = signIn(client, "aaa");
			HttpResponse<String> loginPage = signOut(client, console);
			assertThat(loginPage.uri()).hasToString(base() + "/login?logout");
			assertThat(localLogoutServer.lastLogout()).isEmpty();
			assertThat(send(client, HttpRequest.newBuilder(URI.create(base() + "/console"))).uri().getPath())
				.isEqualTo("/login");
		}
	}

	@Test
	void loginPageListsProvidersByName() throws Exception {
		try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
			HttpResponse<String> response = client.send(
					HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + this.port + "/login")).build(),
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			assertThat(Pattern.compile("Sign in with ([^<]+)<")
				.matcher(response.body())
				.results()
				.map(result -> result.group(1))
				.toList()).containsExactly("sluice-console", "Zeta IdP");
		}
	}

	@Test
	void oidcLoginGrantsAccess() throws Exception {
		// the mock IdP consent is instant, so following redirects with a cookie jar
		// walks the whole authorization code flow
		CookieManager cookies = new CookieManager();
		String base = "http://127.0.0.1:" + this.port;
		try (HttpClient client = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.cookieHandler(cookies)
			.followRedirects(HttpClient.Redirect.ALWAYS)
			.build()) {
			HttpResponse<String> loginPage = client.send(HttpRequest.newBuilder(URI.create(base + "/console")).build(),
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			assertThat(loginPage.uri().getPath()).isEqualTo("/login");
			assertThat(loginPage.body()).contains("href=\"/oauth2/authorization/sluice-console\"");

			HttpResponse<String> console = client.send(
					HttpRequest.newBuilder(URI.create(base + "/oauth2/authorization/sluice-console")).build(),
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			assertThat(console.statusCode()).isEqualTo(200);
			assertThat(console.uri().getPath()).isEqualTo("/console");
			// the id token's sub is opaque; the console names the user by a readable
			// claim
			assertThat(console.body()).contains("<span data-testid=\"signed-in-user\">console-user@example.com</span>");
		}
	}

}
