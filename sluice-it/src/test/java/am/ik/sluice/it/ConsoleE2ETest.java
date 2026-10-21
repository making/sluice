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
import am.ik.sluice.server.route.GateState;
import am.ik.sluice.server.route.Router;
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
 * tunnel client advertising an http and a tcp upstream, feeding its access log to a
 * sluice-flume started after the server.
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

	private static int flumePort;

	private static @Nullable Flume flume;

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

	@Autowired
	Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		flumePort = TestPorts.freePort();
		registry.add("sluice.live-log.url", () -> Flume.url(flumePort));
		registry.add("sluice.live-log.token", () -> Flume.TOKEN);
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.node.id", () -> "console-node");
		registry.add("server.port", () -> 0);
		registry.add("spring.security.user.name", () -> CONSOLE_USER);
		registry.add("spring.security.user.password", () -> "{noop}" + CONSOLE_PASSWORD);
		registry.add("sluice.route.auth.[h1only.local].bearer[0]", () -> "{noop}console-token");
		registry.add("sluice.route.access-log.[h1only.local].enabled", () -> "false");
		registry.add("sluice.route.access-log.[tcp:" + tcpRoutePort + "].types", () -> "close");
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
					.rewriteHost(false)
					.build())
				.upstream(Upstream.builder()
					.host("db.local")
					.target("tcp://127.0.0.1:" + tcp.getLocalPort())
					.listenPort(tcpRoutePort)
					.build())
				.upstream(Upstream.builder()
					.host("h1only.local")
					.target("http://127.0.0.1:" + http.getAddress().getPort())
					.rewriteHost(false)
					.forceHttp1(true)
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
					.rewriteHost(false)
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
		// the server is up without its flume: the feed redials until it comes
		flume = Flume.start(flumePort);
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
		if (flume != null) {
			flume.close();
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
					HTTP h1only.local http://127.0.0.1:%d Serving Keeps Host, HTTP/1.1 only
					""".formatted(httpPort, tcpRoutePort, tcpPort, httpPort));
		assertThat(page.locator("[data-client='" + STANDBY_ID + "'] table.upstreams tbody").innerText())
			.isEqualToNormalizingWhitespace("""
					HTTP demo.local http://127.0.0.1:%d Standby, console-client serves Keeps Host
					""".formatted(httpPort));
		assertThat(page.locator("tbody[data-route='demo.local']").innerText()).isEqualToNormalizingWhitespace("""
				demo.local Smallest client id wins Open Shut
				console-client 127.0.0.1:%d Serving
				console-z 127.0.0.1:%d Standby
				""".formatted(httpPort, httpPort));
		assertThat(page.locator("tbody[data-route='h1only.local'] [data-testid=route-auth]").innerText())
			.isEqualTo("Auth: bearer");
		assertThat(page.locator("tbody[data-route='h1only.local'] [data-testid=route-access-log]").innerText())
			.isEqualTo("Log: off");
		assertThat(page.locator("tbody[data-route='demo.local'] [data-testid=route-access-log]").count())
			.as("no tag for a route on the server-wide settings")
			.isZero();
		assertThat(page.locator("tbody[data-port='" + tcpRoutePort + "']").innerText())
			.isEqualToNormalizingWhitespace("""
					:%d listening, owned by console-client Open Log: close Shut
					console-client 127.0.0.1:%d Serving
					""".formatted(tcpRoutePort, tcpPort));
	}

	/**
	 * The gate toggle shuts a route from the browser and reopens it; the state survives
	 * the polled refresh of the routes region, and the flow drawing raises a headgate on
	 * the data port's intake meanwhile.
	 */
	@Test
	void gateToggleShutsAndReopensARoute() {
		try {
			Page page = page();
			page.navigate("http://127.0.0.1:" + this.port + "/console");
			Locator route = page.locator("tbody[data-route='demo.local']");
			Locator gate = route.getByTestId("route-gate");
			Locator headgate = page.locator("#intake\\:" + dataPort + " [data-testid=headgate]");
			assertThat(gate.innerText()).isEqualTo("Open");
			assertThat(headgate.count()).isZero();
			route.getByRole(AriaRole.BUTTON).click();
			PlaywrightAssertions.assertThat(gate).hasText("Shut", new LocatorAssertions.HasTextOptions());
			assertThat(route.getAttribute("data-gate")).isEqualTo("Shut");
			// the other http routes keep serving: the headgate on the data port's intake
			// stands ajar
			int httpRoutes = page.locator("tbody[data-route]").count();
			assertThat(httpRoutes).isGreaterThan(1);
			PlaywrightAssertions.assertThat(headgate).hasAttribute("data-closed", "false");
			PlaywrightAssertions.assertThat(page.locator("#intake\\:" + dataPort + " .detail--shut"))
				.hasText("1 of " + httpRoutes + " routes shut", new LocatorAssertions.HasTextOptions());
			// the polled regions keep the state
			page.waitForTimeout(2500);
			PlaywrightAssertions.assertThat(page.locator("tbody[data-route='demo.local'] [data-testid=route-gate]"))
				.hasText("Shut", new LocatorAssertions.HasTextOptions());
			page.locator("tbody[data-route='demo.local']").getByRole(AriaRole.BUTTON).click();
			PlaywrightAssertions.assertThat(page.locator("tbody[data-route='demo.local'] [data-testid=route-gate]"))
				.hasText("Open", new LocatorAssertions.HasTextOptions());
			PlaywrightAssertions.assertThat(headgate).hasCount(0);
		}
		finally {
			// a failure above must not leave the route shut for the other tests
			this.router.gate().set("demo.local", GateState.OPEN);
		}
	}

	/**
	 * A client built without build metadata (as these, outside a Spring Boot application)
	 * still names its implementation; the rest reads as unknown.
	 */
	@Test
	void clientWithoutBuildMetadataShowsItsImplementation() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		Locator build = page.locator("[data-client='" + CLIENT_ID + "']").getByTestId("build");
		assertThat(build.innerText())
			.isEqualToNormalizingWhitespace("Build sluice-client version unknown commit unknown");
		assertThat(build.locator(".mono").getAttribute("title")).isEqualTo("Build time unknown");
	}

	@Test
	void drawingTellsWhichRoutesEachClientServes() {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		assertThat(page.locator("[id='outlet-" + CLIENT_ID + "'] text").allTextContents())
			.containsExactly("console-client0 open", "Serves demo.local, h1only.local, :" + tcpRoutePort);
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

	/**
	 * The access log page shows the flume's build and the node's lines as they are
	 * logged; a kind filter narrows them (in the page's address too), and a paused view
	 * picks up the lines logged meanwhile once resumed, each once.
	 */
	@Test
	void accessLogShowsTheLinesAsTheyAreLogged() throws Exception {
		Page page = page();
		page.navigate("http://127.0.0.1:" + this.port + "/console/access-log");
		LocatorAssertions.HasTextOptions redial = new LocatorAssertions.HasTextOptions().setTimeout(40_000);
		LocatorAssertions.HasCountOptions wait = new LocatorAssertions.HasCountOptions().setTimeout(10_000);
		PlaywrightAssertions.assertThat(page.getByTestId("livelog-state")).hasText("Live", redial);
		PlaywrightAssertions.assertThat(page.getByTestId("livelog-build")).containsText("sluice-flume 0.0.1-SNAPSHOT");
		PlaywrightAssertions.assertThat(page.locator("#livelog-nodes [data-node='console-node']"))
			.hasText("console-node", redial);
		Locator rows = page.locator("#livelog-rows tr");

		dataPlaneGet("/first");
		Locator first = rows.filter(new Locator.FilterOptions().setHasText("path=/first"));
		PlaywrightAssertions.assertThat(first).hasCount(1, wait);
		assertThat(first.locator("td").allInnerTexts().subList(1, 4)).containsExactly("console-node", "request",
				"demo.local");
		PlaywrightAssertions.assertThat(page.locator("#livelog-rows tr[data-kind='close']").first()).isAttached();

		page.locator("input[name=kind][value=request]").check();
		page.waitForURL(Pattern.compile("kind=request"));
		// the flume's backfill, of requests only
		PlaywrightAssertions.assertThat(first).hasCount(1, wait);
		dataPlaneGet("/second");
		Locator second = rows.filter(new Locator.FilterOptions().setHasText("path=/second"));
		PlaywrightAssertions.assertThat(second).hasCount(1, wait);
		assertThat(page.locator("#livelog-rows tr[data-kind]:not([data-kind='request'])").count()).isZero();

		page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Pause")).click();
		PlaywrightAssertions.assertThat(page.getByTestId("livelog-state")).hasText("Paused");
		dataPlaneGet("/paused");
		Locator paused = rows.filter(new Locator.FilterOptions().setHasText("path=/paused"));
		assertThat(paused.count()).isZero();
		page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Resume")).click();
		PlaywrightAssertions.assertThat(paused).hasCount(1, wait);
		PlaywrightAssertions.assertThat(page.getByTestId("livelog-state")).hasText("Live");
		assertThat(second.count()).isEqualTo(1);
	}

	/** A request to demo.local through the data plane, read to its end. */
	private static void dataPlaneGet(String path) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET " + path + " HTTP/1.1\r\nHost: demo.local\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			assertThat(new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII)).contains(" 200 ");
		}
	}

}
