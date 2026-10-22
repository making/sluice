package am.ik.sluice.server.auth;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.HeaderEchoUpstream;
import am.ik.sluice.it.MockOidcServer;
import am.ik.sluice.it.TestKeystore;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The browser sign-in of a route end to end ({@code oidc.client-id}): two nodes sharing
 * the session secret terminate TLS, each with a tunnel client serving the route, and
 * {@link MockOidcServer} is the provider. A browser's navigation is redirected to the
 * provider, the callback sets the session cookie, and the requests with it reach the
 * upstream -- over HTTP/1.1 and h2, on either node -- without the sign-in's cookie, the
 * other cookies kept and the signed-in user's email told. An altered or expired session
 * and a callback of another state are refused.
 */
@TestInstance(Lifecycle.PER_CLASS)
class RouteSignInE2ETest {

	private static final String TOKEN = "it-token";

	private static final String SECRET = "e2e-session-secret-of-32-characters";

	private static final String CLIENT_ID = "sluice-app";

	private static final String HOST = "localhost";

	private static final String EMAIL = "console-user@example.com";

	private final ListAppender<ILoggingEvent> accessEvents = new ListAppender<>();

	private final List<ConfigurableApplicationContext> servers = new ArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private final List<Integer> dataPorts = new ArrayList<>();

	private Path keystore;

	private MockOidcServer provider;

	private HeaderEchoUpstream upstream;

	@BeforeAll
	void start() throws Exception {
		this.keystore = TestKeystore.create();
		this.provider = new MockOidcServer(CLIENT_ID);
		this.upstream = new HeaderEchoUpstream("cookie", "x-forwarded-email");
		for (int node = 0; node < 2; node++) {
			int grpcPort = TestPorts.freePort();
			int dataPort = TestPorts.freePort();
			String auth = "--sluice.route.auth.[" + HOST + "].oidc.";
			this.servers.add(new SpringApplicationBuilder(SluiceServerApplication.class).run(
					"--server.port=" + TestPorts.freePort(), "--spring.grpc.server.port=" + grpcPort,
					"--sluice.data-port=" + dataPort, "--sluice.token=" + TOKEN,
					"--sluice.access-log.rate-limit.enabled=false",
					"--spring.ssl.bundle.jks.data-plane.keystore.location=file:" + this.keystore.toAbsolutePath(),
					"--spring.ssl.bundle.jks.data-plane.keystore.password=" + TestKeystore.PASSWORD,
					"--sluice.data-tls-bundle=data-plane", "--sluice.route.session-secret=" + SECRET,
					auth + "issuer-uri=" + this.provider.issuer(), auth + "client-id=" + CLIENT_ID,
					auth + "client-secret=client-secret", auth + "headers.X-Forwarded-Email=email"));
			this.dataPorts.add(dataPort);
			TunnelClient client = TunnelClient.builder()
				.properties(SluiceClientProperties.builder()
					.serverUrl("grpc://127.0.0.1:" + grpcPort)
					.upstream(Upstream.builder().host(HOST).target("http://127.0.0.1:" + this.upstream.port()).build())
					.token(TOKEN)
					.build())
				.taskExecutor(task -> Thread.ofVirtual().name("e2e-sign-in-worker").start(task))
				.meterRegistry(new SimpleMeterRegistry())
				.build();
			client.start();
			this.clients.add(client);
			// the advertise acknowledgement turns isConnected() true once the routes are
			// registered
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(client::isConnected);
		}
		// after the servers started: their logging setup drops the appenders before
		this.accessEvents.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(this.accessEvents);
	}

	@AfterAll
	void stop() throws Exception {
		this.clients.forEach(TunnelClient::stop);
		this.servers.forEach(ConfigurableApplicationContext::close);
		this.upstream.close();
		this.provider.close();
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(this.accessEvents);
	}

	/**
	 * Signs in on the first node and passes on both: the upstream gets the client's other
	 * cookies and the email, never the session cookie; the access log tells the callback
	 * without its code.
	 */
	@ParameterizedTest
	@EnumSource(names = { "HTTP_1_1", "HTTP_2" })
	void aBrowserSignsInAndPassesWithItsSessionOnEitherNode(HttpClient.Version version) throws Exception {
		HttpClient http = this.http(version);
		String session = this.signIn(http, "/page?x=1");

		for (int node = 0; node < 2; node++) {
			HttpResponse<String> passed = send(http, this.url(node, "/page?x=1"), "accept", "text/html", "cookie",
					"theme=dark; " + session + "; lang=ja");
			assertThat(passed.statusCode()).as("node %d", node).isEqualTo(200);
			assertThat(passed.version()).isEqualTo(version);
			assertThat(passed.body())
				.isEqualTo(HeaderEchoUpstream.GREETING + " cookie=theme=dark; lang=ja x-forwarded-email=" + EMAIL);
		}
		assertThat(List.copyOf(this.accessEvents.list).stream().map(ILoggingEvent::getFormattedMessage))
			.anySatisfy(line -> assertThat(line).contains("path=/.sluice/oauth2/callback http=")
				.contains("status=302 reason=auth-signed-in"))
			.noneSatisfy(line -> assertThat(line).contains("code="));
	}

	/**
	 * An altered session cookie and one past its max age are no session -- a navigation
	 * is redirected to sign in anew, any other request answered 401 --, and a callback of
	 * another state is refused 403.
	 */
	@Test
	void anAlteredOrExpiredSessionAndACallbackOfAnotherStateAreRefused() throws Exception {
		HttpClient http = this.http(HttpClient.Version.HTTP_1_1);
		String session = this.signIn(http, "/");
		int last = session.length() - 2;
		String altered = session.substring(0, last) + (session.charAt(last) == 'A' ? 'B' : 'A')
				+ session.charAt(last + 1);
		String expired = OidcSignIn.SESSION_COOKIE + "="
				+ new SessionCodec(SECRET.getBytes(StandardCharsets.UTF_8), HOST, new SecureRandom()).seal("session",
						Map.of("claims", Map.of("sub", "console-user", "email", EMAIL), "exp",
								Instant.now().getEpochSecond() - 1));

		for (String cookie : List.of(altered, expired)) {
			HttpResponse<String> navigation = send(http, this.url(0, "/"), "accept", "text/html", "cookie", cookie);
			assertThat(navigation.statusCode()).isEqualTo(302);
			assertThat(navigation.headers().firstValue("location")).hasValueSatisfying(
					location -> assertThat(location).startsWith(this.provider.issuer() + "/oauth2/authorize?"));
			assertThat(send(http, this.url(0, "/"), "cookie", cookie).statusCode()).isEqualTo(401);
		}

		HttpResponse<String> redirect = send(http, this.url(0, "/"), "accept", "text/html");
		String state = parameter(redirect.headers().firstValue("location").orElseThrow(), "state");
		HttpResponse<String> callback = send(http,
				this.url(0, "/.sluice/oauth2/callback?code=mock-code&state=" + state.substring(0, 12) + "x".repeat(31)),
				"accept", "text/html", "cookie", cookieOf(redirect));
		assertThat(callback.statusCode()).isEqualTo(403);
		assertThat(callback.body()).contains("Sign-in failed");
	}

	/**
	 * Follows a sign-in from a navigation of the first node through the provider and the
	 * callback, which returns to the page with the session cookie.
	 * @return the session cookie, {@code name=value}
	 */
	private String signIn(HttpClient http, String target) throws Exception {
		HttpResponse<String> navigation = send(http, this.url(0, target), "accept", "text/html");
		assertThat(navigation.statusCode()).isEqualTo(302);
		String authorize = navigation.headers().firstValue("location").orElseThrow();
		assertThat(authorize).startsWith(this.provider.issuer() + "/oauth2/authorize?");
		HttpResponse<String> consent = send(http, URI.create(authorize));
		String callback = consent.headers().firstValue("location").orElseThrow();
		assertThat(callback)
			.startsWith("https://" + HOST + ":" + this.dataPorts.getFirst() + "/.sluice/oauth2/callback?");
		HttpResponse<String> signedIn = send(http, URI.create(callback), "accept", "text/html", "cookie",
				cookieOf(navigation));
		assertThat(signedIn.statusCode()).isEqualTo(302);
		assertThat(signedIn.headers().firstValue("location"))
			.hasValue("https://" + HOST + ":" + this.dataPorts.getFirst() + target);
		String session = signedIn.headers()
			.allValues("set-cookie")
			.stream()
			.filter(cookie -> cookie.startsWith(OidcSignIn.SESSION_COOKIE + "="))
			.findFirst()
			.orElseThrow();
		assertThat(session).endsWith("; Max-Age=28800; Path=/; Secure; HttpOnly; SameSite=Lax");
		return session.substring(0, session.indexOf(';'));
	}

	private HttpClient http(HttpClient.Version version) throws Exception {
		return HttpClient.newBuilder()
			.version(version)
			.sslContext(TestKeystore.clientContext(this.keystore))
			.followRedirects(HttpClient.Redirect.NEVER)
			.connectTimeout(Duration.ofSeconds(10))
			.build();
	}

	private URI url(int node, String target) {
		return URI.create("https://" + HOST + ":" + this.dataPorts.get(node) + target);
	}

	private static HttpResponse<String> send(HttpClient http, URI uri, String... headers) throws Exception {
		HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET();
		for (int i = 0; i < headers.length; i += 2) {
			request.header(headers[i], headers[i + 1]);
		}
		return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
	}

	/** The {@code name=value} of the response's first {@code Set-Cookie}. */
	private static String cookieOf(HttpResponse<String> response) {
		String cookie = response.headers().firstValue("set-cookie").orElseThrow();
		return cookie.substring(0, cookie.indexOf(';'));
	}

	private static String parameter(String location, String name) {
		for (String pair : URI.create(location).getRawQuery().split("&")) {
			if (pair.startsWith(name + "=")) {
				return pair.substring(name.length() + 1);
			}
		}
		throw new IllegalArgumentException(location + " names no " + name);
	}

}
