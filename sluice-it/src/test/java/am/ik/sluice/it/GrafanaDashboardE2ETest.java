package am.ik.sluice.it;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.server.SluiceServerApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Grafana dashboard ({@code dashboards/sluice.json}) against a real scrape: every
 * metric it queries is exposed by the server or the client once traffic went through, so
 * a renamed meter fails the build instead of leaving a panel without data; every query
 * selects the dashboard's label variables on the {@code $datasource} variable. The
 * security auto-configuration is excluded on the client, as in
 * {@link ClientActuatorE2ETest}.
 */
@TestInstance(Lifecycle.PER_CLASS)
class GrafanaDashboardE2ETest {

	private static final Path DASHBOARD = Path.of("..", "dashboards", "sluice.json");

	private static final String LABELS = "{app=~\"$app\", cluster=~\"$cluster\", instance=~\"$instance\", "
			+ "namespace=~\"$namespace\", pod=~\"$pod\"";

	/** A metric selector in a query: the name directly followed by its label matchers. */
	private static final Pattern SELECTOR = Pattern.compile("([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{)");

	/** The metric a variable query takes its label values from. */
	private static final Pattern LABEL_VALUES = Pattern.compile("label_values\\(([a-zA-Z_:][a-zA-Z0-9_:]*)");

	private static final String SECURITY_EXCLUDES = String.join(",",
			"org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterAutoConfiguration");

	/** HTTP/1.1, as Prometheus scrapes: no h2c upgrade to the actuator. */
	private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

	private final ExecutorService upstreamExecutor = Executors.newVirtualThreadPerTaskExecutor();

	private @Nullable ServerSocket h2Upstream;

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable ConfigurableApplicationContext clientContext;

	@AfterEach
	void stop() throws IOException {
		if (this.clientContext != null) {
			this.clientContext.close();
			this.clientContext = null;
		}
		if (this.serverContext != null) {
			this.serverContext.close();
			this.serverContext = null;
		}
		if (this.h2Upstream != null) {
			this.h2Upstream.close();
			this.h2Upstream = null;
		}
	}

	@Test
	void everyMetricTheDashboardQueriesIsScraped() throws Exception {
		Set<String> queried = queriedMetrics(JsonMapper.shared().readTree(DASHBOARD.toFile()));
		// the lazily registered per-route meters are among them: the traffic below must
		// register them
		assertThat(queried).contains("sluice_tunnel_bytes_total", "sluice_h2demux_queued_bytes",
				"jvm_gc_pause_seconds_sum");
		int serverPort = TestPorts.freePort();
		int clientPort = TestPorts.freePort();
		int grpcPort = TestPorts.freePort();
		int dataPort = TestPorts.freePort();
		int upstreamPort = startH2Upstream();
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run(
				"--server.port=" + serverPort, "--spring.grpc.server.port=" + grpcPort,
				"--sluice.data-port=" + dataPort, "--sluice.token=it-token",
				"--spring.grpc.server.shutdown.grace-period=2s");
		this.clientContext = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--server.port=" + clientPort, "--sluice.server-url=grpc://127.0.0.1:" + grpcPort,
				"--sluice.client.upstream[0].host=dashboard.local",
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + upstreamPort, "--sluice.token=it-token",
				"--spring.autoconfigure.exclude=" + SECURITY_EXCLUDES);
		String serverMetrics = "http://127.0.0.1:" + serverPort + "/actuator/prometheus";
		String clientMetrics = "http://127.0.0.1:" + clientPort + "/actuator/prometheus";
		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.ignoreExceptions()
			.until(() -> scrape("http://127.0.0.1:" + clientPort + "/actuator/health").contains("\"UP\""));

		// an h2 request: registers the route's tunnel bytes counter and queued bytes
		// gauge
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=dashboard.local", ":method=GET", ":path=/");
			assertThat(TestH2.bodyOf(TestH2.readResponseFrames(socket.getInputStream()))).isEqualTo("h2-ok");
		}
		// the GC pause timer is registered on the first GC notification
		System.gc();
		Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
			Set<String> scraped = sampleNames(scrape(serverMetrics));
			scraped.addAll(sampleNames(scrape(clientMetrics)));
			Set<String> missing = new TreeSet<>(queried);
			missing.removeAll(scraped);
			assertThat(missing).isEmpty();
		});
		assertThat(value(scrape(serverMetrics), "sluice_tunnel_sessions")).isEqualTo(1.0);
		assertThat(value(scrape(clientMetrics), "sluice_tunnel_sessions")).isEqualTo(1.0);

		// the client gone: the server's session gauge drops to 0
		this.clientContext.close();
		this.clientContext = null;
		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.until(() -> value(scrape(serverMetrics), "sluice_tunnel_sessions") == 0.0);
	}

	@Test
	void everyQuerySelectsTheDashboardVariables() throws Exception {
		JsonNode dashboard = JsonMapper.shared().readTree(DASHBOARD.toFile());
		List<String> violations = new ArrayList<>();
		int selectors = 0;
		for (JsonNode panel : dashboard.get("panels")) {
			if ("row".equals(panel.get("type").asString())) {
				continue;
			}
			String title = panel.get("title").asString();
			if (!"$datasource".equals(panel.path("datasource").path("uid").asString())) {
				violations.add(title + ": panel datasource");
			}
			for (JsonNode target : panel.get("targets")) {
				if (!"$datasource".equals(target.path("datasource").path("uid").asString())) {
					violations.add(title + ": target datasource");
				}
				String expr = target.get("expr").asString();
				Matcher matcher = SELECTOR.matcher(expr);
				while (matcher.find()) {
					selectors++;
					if (!expr.startsWith(LABELS, matcher.start(2))) {
						violations.add(title + ": " + matcher.group(1));
					}
				}
			}
		}
		assertThat(selectors).isGreaterThan(30);
		assertThat(violations).isEmpty();
	}

	private int startH2Upstream() throws IOException {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		this.h2Upstream = server;
		this.upstreamExecutor.execute(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.upstreamExecutor.execute(() -> answer(socket));
				}
				catch (IOException ex) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	private static void answer(Socket socket) {
		try (socket) {
			TestH2.readClientHead(socket.getInputStream());
			socket.getOutputStream().write(TestH2.cannedResponse(1, "h2-ok"));
			socket.getOutputStream().flush();
			// hold the connection until the relay hangs up
			socket.getInputStream().read(new byte[1]);
		}
		catch (IOException ex) {
			// connection ended
		}
	}

	private String scrape(String url) throws IOException, InterruptedException {
		HttpResponse<String> response = this.http.send(HttpRequest.newBuilder(URI.create(url)).build(),
				HttpResponse.BodyHandlers.ofString());
		assertThat(response.statusCode()).isEqualTo(200);
		return response.body();
	}

	/** The metric names the panel and variable queries reference. */
	private static Set<String> queriedMetrics(JsonNode dashboard) {
		Set<String> names = new TreeSet<>();
		for (JsonNode panel : dashboard.get("panels")) {
			for (JsonNode target : panel.path("targets")) {
				Matcher matcher = SELECTOR.matcher(target.get("expr").asString());
				while (matcher.find()) {
					names.add(matcher.group(1));
				}
			}
		}
		for (JsonNode variable : dashboard.get("templating").get("list")) {
			Matcher matcher = LABEL_VALUES.matcher(variable.path("definition").asString());
			if (matcher.find()) {
				names.add(matcher.group(1));
			}
		}
		return names;
	}

	/** The names of the samples in a Prometheus text exposition. */
	private static Set<String> sampleNames(String exposition) {
		Set<String> names = new TreeSet<>();
		for (String line : exposition.split("\n")) {
			if (!line.isEmpty() && !line.startsWith("#")) {
				names.add(line.split("[{ ]", 2)[0]);
			}
		}
		return names;
	}

	/** The sum of the samples of a metric, -1 when it has none. */
	private static double value(String exposition, String name) {
		double sum = 0;
		boolean found = false;
		for (String line : exposition.split("\n")) {
			if (line.startsWith(name + "{") || line.startsWith(name + " ")) {
				sum += Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
				found = true;
			}
		}
		return found ? sum : -1;
	}

}
