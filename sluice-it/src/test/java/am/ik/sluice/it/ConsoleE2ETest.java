package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.assertions.LocatorAssertions;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.assertions.PlaywrightAssertions;
import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the management console in a real browser against a running server with one
 * tunnel client advertising an http and a tcp upstream.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConsoleE2ETest {

	private static final String CLIENT_ID = "console-client";

	private static final String CONSOLE_USER = "console-e2e";

	private static final String CONSOLE_PASSWORD = "console-pass";

	/** A second client on the same host; the larger id stands by. */
	private static final String STANDBY_ID = "console-z";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-console").start(task);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static @Nullable HttpServer httpUpstream;

	private static @Nullable ServerSocket tcpUpstream;

	private @Nullable TunnelClient client;

	private @Nullable TunnelClient standby;

	private @Nullable Playwright playwright;

	private @Nullable Browser browser;

	private @Nullable BrowserContext context;

	private @Nullable Page page;

	@LocalServerPort
	int port;

	@Autowired
	SessionRegistry sessions;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.node.id", () -> "console-node");
		registry.add("server.port", () -> 0);
		registry.add("spring.security.user.name", () -> CONSOLE_USER);
		registry.add("spring.security.user.password", () -> "{noop}" + CONSOLE_PASSWORD);
	}

	@BeforeAll
	void setUp() throws Exception {
		HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		http.createContext("/", exchange -> {
			byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		http.start();
		httpUpstream = http;
		ServerSocket tcp = new ServerSocket();
		tcp.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		tcpUpstream = tcp;
		UPSTREAM_EXECUTOR.execute(() -> holdOpen(tcp));
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.clientId(CLIENT_ID)
				.upstream(Upstream.builder()
					.host("demo.local")
					.target("http://127.0.0.1:" + http.getAddress().getPort())
					.build())
				.upstream(Upstream.builder()
					.host("db.local")
					.target("tcp://127.0.0.1:" + tcp.getLocalPort())
					.listenPort(tcpRoutePort)
					.build())
				.token("it-token")
				.build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		TunnelClient standby = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.clientId(STANDBY_ID)
				.upstream(Upstream.builder()
					.host("demo.local")
					.target("http://127.0.0.1:" + http.getAddress().getPort())
					.build())
				.token("it-token")
				.build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		standby.start();
		this.standby = standby;
		for (String clientId : List.of(CLIENT_ID, STANDBY_ID)) {
			Awaitility.await()
				.atMost(Duration.ofSeconds(10))
				.until(() -> this.sessions.find(clientId).map(s -> !s.upstreams().isEmpty()).orElse(false));
		}
		this.playwright = Playwright.create();
		this.browser = this.playwright.chromium().launch();
	}

	/** Accepts connections and keeps each open until the peer closes it. */
	private static void holdOpen(ServerSocket server) {
		while (!server.isClosed()) {
			try {
				Socket socket = server.accept();
				UPSTREAM_EXECUTOR.execute(() -> {
					try (socket; InputStream in = socket.getInputStream()) {
						in.transferTo(OutputStream.nullOutputStream());
					}
					catch (Exception e) {
						// peer gone
					}
				});
			}
			catch (Exception e) {
				return;
			}
		}
	}

	@AfterAll
	void tearDown() throws Exception {
		if (this.browser != null) {
			this.browser.close();
		}
		if (this.playwright != null) {
			this.playwright.close();
		}
		if (this.client != null) {
			this.client.stop();
		}
		if (this.standby != null) {
			this.standby.stop();
		}
		if (httpUpstream != null) {
			httpUpstream.stop(0);
		}
		if (tcpUpstream != null) {
			tcpUpstream.close();
		}
		UPSTREAM_EXECUTOR.shutdownNow();
	}

	@BeforeEach
	void openPage() {
		this.context = Objects.requireNonNull(this.browser).newContext();
		this.page = this.context.newPage();
		this.signIn(this.page);
	}

	/** Form-login through the console login page so the context carries the session. */
	private void signIn(Page page) {
		String base = "http://127.0.0.1:" + this.port;
		page.navigate(base + "/console");
		page.locator("#username").fill(CONSOLE_USER);
		page.locator("#password").fill(CONSOLE_PASSWORD);
		page.locator(".login__submit").click();
		page.waitForURL(base + "/console");
	}

	@AfterEach
	void closePage() {
		if (this.context != null) {
			this.context.close();
		}
	}

	private Page page() {
		return Objects.requireNonNull(this.page);
	}

	@Test
	void rootRedirectsToTheConsoleShowingNodeState() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/");
		assertThat(page.url()).endsWith("/console");
		assertThat(page.title()).isEqualTo("Sluice console");
		assertThat(page.locator(".titleblock__node").innerText()).isEqualToNormalizingWhitespace("Node console-node");
		assertThat(page.getByTestId("state").innerText()).isEqualTo("Accepting traffic");
	}

	@Test
	void signOutEndsTheSession() {
		Page page = page();
		String base = "http://127.0.0.1:" + this.port;
		page.navigate(base + "/console");
		assertThat(page.getByTestId("signed-in-user").innerText()).isEqualTo(CONSOLE_USER);
		page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Sign out")).click();
		page.waitForURL(base + "/login?logout");
		assertThat(page.locator(".login__note").innerText()).isEqualTo("You have signed out.");
		page.navigate(base + "/console");
		assertThat(page.url()).isEqualTo(base + "/login");
	}

	@Test
	void clientAndRoutesAreListed() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		int httpPort = Objects.requireNonNull(httpUpstream).getAddress().getPort();
		int tcpPort = Objects.requireNonNull(tcpUpstream).getLocalPort();
		assertThat(page.locator("[data-client='" + CLIENT_ID + "'] table.upstreams tbody").innerText())
			.isEqualToNormalizingWhitespace("""
					HTTP demo.local http://127.0.0.1:%d Serving Keeps Host
					TCP :%d db.local tcp://127.0.0.1:%d Serving
					""".formatted(httpPort, tcpRoutePort, tcpPort));
		assertThat(page.locator("[data-client='" + STANDBY_ID + "'] table.upstreams tbody").innerText())
			.isEqualToNormalizingWhitespace("""
					HTTP demo.local http://127.0.0.1:%d Standby, console-client serves Keeps Host
					""".formatted(httpPort));
		assertThat(page.locator("tbody[data-route='demo.local']").innerText()).isEqualToNormalizingWhitespace("""
				demo.local Smallest client id wins
				console-client 127.0.0.1:%d Serving
				console-z 127.0.0.1:%d Standby
				""".formatted(httpPort, httpPort));
		assertThat(page.locator("tbody[data-port='" + tcpRoutePort + "']").innerText())
			.isEqualToNormalizingWhitespace("""
					:%d listening, owned by console-client
					console-client 127.0.0.1:%d Serving
					""".formatted(tcpRoutePort, tcpPort));
	}

	@Test
	void drawingTellsWhichRoutesEachClientServes() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		assertThat(page.locator("[id='outlet-" + CLIENT_ID + "'] text").allTextContents())
			.containsExactly("console-client0 open", "Serves demo.local, :" + tcpRoutePort);
		assertThat(page.locator("[id='outlet-" + STANDBY_ID + "'] text").allTextContents())
			.containsExactly("console-z0 open", "Standby for demo.local");
	}

	@Test
	void routeLookupExplainsWhichClientServesAHost() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		int httpPort = Objects.requireNonNull(httpUpstream).getAddress().getPort();
		Locator result = page.getByTestId("lookup-result");

		page.locator("#lookup-host").fill("demo.local:8000");
		PlaywrightAssertions.assertThat(result).containsText("Matched without the port");
		assertThat(result.innerText()).isEqualToNormalizingWhitespace("""
				demo.local:8000 matches demo.local. Matched without the port.
				Smallest client id wins.
				console-client 127.0.0.1:%d Serving
				console-z 127.0.0.1:%d Standby
				""".formatted(httpPort, httpPort));

		page.locator("#lookup-host").fill("nothing.example");
		PlaywrightAssertions.assertThat(result).containsText("No route");
		assertThat(result.innerText()).isEqualToNormalizingWhitespace(
				"No route matches nothing.example. The data plane answers 503 Service Unavailable.");
	}

	@Test
	void healthPageReportsTheOverallStatusAndEveryIndicator() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console/health");
		assertThat(page.title()).isEqualTo("Health | Sluice console");
		assertThat(page.getByTestId("health-overall").innerText()).isEqualTo("UP");
		Locator tunnelRow = page.getByTestId("health-table")
			.locator("tbody tr")
			.filter(new Locator.FilterOptions().setHasText("tunnel"));
		assertThat(tunnelRow.innerText()).contains("UP")
			.contains("clients=" + this.sessions.count())
			.contains("draining=false");
	}

	@Test
	void infoPageListsOneSectionPerInfoEntry() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console/info");
		assertThat(page.title()).isEqualTo("Info | Sluice console");
		assertThat(page.locator(".report").innerText()).contains("java").contains("os");
	}

	@Test
	void metricSearchNarrowsTheListAndSelectionShowsTheSeries() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console/metrics");
		Locator list = page.getByTestId("metric-list");
		// the groups start collapsed; the names are in the dom regardless
		assertThat(list.textContent()).contains("jvm.memory.used").contains("jvm");

		page.locator("#metric-filter").fill("memory");
		Locator diskRow = page.locator("#metric-list a").filter(new Locator.FilterOptions().setHasText("disk"));
		PlaywrightAssertions.assertThat(diskRow)
			.hasCount(0, new LocatorAssertions.HasCountOptions().setTimeout(10_000));
		assertThat(list.textContent()).contains("jvm.memory.used");

		page.locator("#metric-list a")
			.filter(new Locator.FilterOptions().setHasText("jvm.memory.used"))
			.first()
			.click();
		page.waitForURL(Pattern.compile("name=jvm\\.memory\\.used"));
		Locator detail = page.getByTestId("metric-detail");
		PlaywrightAssertions.assertThat(detail)
			.containsText("area=heap", new LocatorAssertions.ContainsTextOptions().setTimeout(10_000));
	}

	@Test
	void liveRegionsFollowOpenConnections() throws Exception {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		Locator open = page.getByTestId("open-connections");
		LocatorAssertions.HasTextOptions wait = new LocatorAssertions.HasTextOptions().setTimeout(10_000);
		PlaywrightAssertions.assertThat(open).hasText("0", wait);
		try (Socket held = new Socket("127.0.0.1", tcpRoutePort)) {
			held.getOutputStream().write("ping".getBytes(StandardCharsets.US_ASCII));
			held.getOutputStream().flush();
			// the poll swaps the new count in without reloading the page
			PlaywrightAssertions.assertThat(open).hasText("1", wait);
			PlaywrightAssertions.assertThat(page.locator("#intake\\:" + tcpRoutePort))
				.hasClass(Pattern.compile("is-active"), new LocatorAssertions.HasClassOptions().setTimeout(10_000));
		}
		PlaywrightAssertions.assertThat(open).hasText("0", wait);
	}

}
