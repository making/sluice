package am.ik.sluice.it;

import java.net.CookieManager;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Pattern;

import am.ik.sluice.server.SluiceServerApplication;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the console authentication: the console requires a signed-in user (form login
 * by default), the actuator endpoints and static assets stay open, and htmx polls get an
 * {@code HX-Redirect} instead of a login document; {@code oidc} sign-in is
 * {@link ConsoleOidcE2ETest}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConsoleAuthE2ETest {

	private static final String USER = "auth-e2e";

	private static final String PASSWORD = "auth-pass";

	@LocalServerPort
	int port;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.grpc.server.port", () -> 0);
		registry.add("sluice.data-port", () -> 0);
		registry.add("sluice.token", () -> "it-token");
		registry.add("spring.security.user.name", () -> USER);
		registry.add("spring.security.user.password", () -> "{noop}" + PASSWORD);
	}

	private HttpClient client;

	@BeforeAll
	void setUp() {
		this.client = newClient();
	}

	/**
	 * A client with its own cookie jar, so tests start anonymous; HTTP/1.1 like a browser
	 * on plaintext HTTP (no h2c upgrade).
	 */
	private static HttpClient newClient() {
		return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).cookieHandler(new CookieManager()).build();
	}

	private String base() {
		return "http://127.0.0.1:" + this.port;
	}

	private HttpResponse<String> get(String path) {
		return get(path, true);
	}

	private HttpResponse<String> anonymousGet(String path, String... headers) throws Exception {
		HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base() + path)).timeout(Duration.ofSeconds(10));
		for (int i = 0; i < headers.length; i += 2) {
			builder.header(headers[i], headers[i + 1]);
		}
		return newClient().send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
	}

	private HttpResponse<String> get(String path, boolean trace) {
		try {
			HttpRequest request = HttpRequest.newBuilder(URI.create(base() + path))
				.timeout(Duration.ofSeconds(10))
				.build();
			HttpResponse<String> response = this.client.send(request,
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			if (trace) {
				System.out.println("TRACE " + path + " -> " + response.statusCode() + " loc="
						+ response.headers().firstValue("Location").orElse("-"));
			}
			return response;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private HttpResponse<String> postForm(String path, String body) {
		try {
			HttpRequest request = HttpRequest.newBuilder(URI.create(base() + path))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.timeout(Duration.ofSeconds(10))
				.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
				.build();
			return this.client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	static String csrfOf(HttpResponse<String> loginPage) {
		return Pattern.compile("name=\"_csrf\" value=\"([^\"]+)\"")
			.matcher(loginPage.body())
			.results()
			.findFirst()
			.orElseThrow(() -> new AssertionError("no csrf parameter in the login page"))
			.group(1);
	}

	static String urlEncode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	@Test
	void consoleRedirectsAnonymousUsersToLogin() throws Exception {
		HttpResponse<String> response = anonymousGet("/console");
		assertThat(response.statusCode()).isEqualTo(302);
		assertThat(response.headers().firstValue("Location"))
			.hasValueSatisfying(loc -> assertThat(loc).endsWith("/login"));
	}

	@Test
	void loginPageRendersTheForm() {
		HttpResponse<String> response = get("/login");
		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(response.body()).contains("Sign in");
	}

	@Test
	void loginPageExplainsRejectedCredentialsInTheConsoleLanguage() throws Exception {
		try (HttpClient client = newClient()) {
			HttpResponse<String> loginPage = client.send(HttpRequest.newBuilder(URI.create(base() + "/login")).build(),
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			HttpResponse<String> login = client.send(HttpRequest.newBuilder(URI.create(base() + "/login"))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.header("Accept-Language", "ja")
				.POST(HttpRequest.BodyPublishers.ofString(
						"username=%s&password=wrong&_csrf=%s".formatted(urlEncode(USER), urlEncode(csrfOf(loginPage)))))
				.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			assertThat(login.headers().firstValue("Location"))
				.hasValueSatisfying(loc -> assertThat(loc).endsWith("/login?error"));
			HttpResponse<String> error = client.send(
					HttpRequest.newBuilder(URI.create(base() + "/login?error")).header("Accept-Language", "ja").build(),
					HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			assertThat(error.body()).contains("Incorrect username or password.");
		}
	}

	@Test
	void actuatorStaysOpenWithoutLogin() {
		assertThat(get("/actuator/health").statusCode()).isEqualTo(200);
		assertThat(get("/actuator/prometheus").statusCode()).isEqualTo(200);
	}

	@Test
	void staticAssetsStayOpenWithoutLogin() {
		assertThat(get("/console/css/console.css").statusCode()).isEqualTo(200);
	}

	@Test
	void htmxPollGetsHxRedirectInsteadOfLoginDocument() throws Exception {
		HttpResponse<String> response = anonymousGet("/console/live", "HX-Request", "true");
		assertThat(response.statusCode()).isEqualTo(401);
		assertThat(response.headers().firstValue("HX-Redirect")).contains("/login");
	}

	@Test
	void formLoginGrantsAccessAndLogoutRevokesIt() throws Exception {
		String csrf = csrfOf(get("/login"));
		HttpResponse<String> login = postForm("/login",
				"username=%s&password=%s&_csrf=%s".formatted(urlEncode(USER), urlEncode(PASSWORD), urlEncode(csrf)));
		assertThat(login.statusCode()).isEqualTo(302);
		assertThat(login.headers().firstValue("Location"))
			.hasValueSatisfying(loc -> assertThat(loc).endsWith("/console"));

		HttpResponse<String> console = get("/console");
		assertThat(console.statusCode()).isEqualTo(200);
		assertThat(console.body()).contains("Sluice console");

		HttpResponse<String> logout = postForm("/logout", "_csrf=" + urlEncode(csrfOf(get("/login"))));
		assertThat(logout.statusCode()).isEqualTo(302);
		HttpResponse<String> after = anonymousGet("/console");
		assertThat(after.statusCode()).isEqualTo(302);
	}

}
