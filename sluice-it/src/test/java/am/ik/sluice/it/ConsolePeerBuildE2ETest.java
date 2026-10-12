package am.ik.sluice.it;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.PeerBuild;
import am.ik.sluice.v1.proto.TunnelGrpc;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The build each peer announces reaches the server's console: a real client application
 * reports the build packaged with it, a wasmlet-shaped peer its own, a peer that predates
 * build reporting shows as unknown, and a peer without a connection window is marked
 * legacy and counted.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class ConsolePeerBuildE2ETest {

	private static final String CLIENT_ID = "build-client";

	private static final String WASMLET_ID = "build-wasmlet";

	private static final String LEGACY_ID = "build-legacy";

	private static final String HOSTILE_ID = "build-hostile";

	private static final String CONSOLE_USER = "build-e2e";

	private static final String CONSOLE_PASSWORD = "build-pass";

	private static final String WASMLET_COMMIT = "0123456789abcdef0123456789abcdef01234567";

	/**
	 * Security lands on the shared classpath through the server; the client ships none.
	 */
	private static final String SECURITY_EXCLUDES = String.join(",",
			"org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterAutoConfiguration");

	private static int grpcPort;

	private @Nullable ConfigurableApplicationContext clientContext;

	private final List<ManagedChannel> channels = new ArrayList<>();

	private @Nullable Playwright playwright;

	private @Nullable Browser browser;

	private @Nullable BrowserContext context;

	private @Nullable Page page;

	@LocalServerPort
	int port;

	@Autowired
	SessionRegistry sessions;

	@Autowired
	MeterRegistry meterRegistry;

	@Autowired
	ConfigurableApplicationContext serverContext;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
		registry.add("spring.security.user.name", () -> CONSOLE_USER);
		registry.add("spring.security.user.password", () -> "{noop}" + CONSOLE_PASSWORD);
	}

	@BeforeAll
	void setUp() throws Exception {
		this.clientContext = new SpringApplicationBuilder(SluiceClientApplication.class).run("--server.port=0",
				"--sluice.server-url=grpc://127.0.0.1:" + grpcPort, "--sluice.client.id=" + CLIENT_ID,
				"--sluice.client.upstream[0].host=client.local",
				"--sluice.client.upstream[0].target=http://127.0.0.1:1", "--sluice.token=it-token",
				"--spring.autoconfigure.exclude=" + SECURITY_EXCLUDES);
		TunnelClient tunnelClient = this.clientContext.getBean(TunnelClient.class);
		// connected = the ack, and the server build line, arrived
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(tunnelClient::isConnected);
		advertise(WASMLET_ID,
				Advertise.newBuilder()
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
					.setBuild(PeerBuild.newBuilder()
						.setImplementation("sluice-wasmlet")
						.setVersion("0.0.1-SNAPSHOT")
						.setCommit(WASMLET_COMMIT)
						.setBuildTime("2026-10-12T00:00:00Z")));
		// a client from before build reporting and per-connection flow control
		advertise(LEGACY_ID, Advertise.newBuilder());
		advertise(HOSTILE_ID,
				Advertise.newBuilder()
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
					.setBuild(PeerBuild.newBuilder()
						.setImplementation("<img src=x onerror=\"window.pwned=1\">")
						.setVersion("9".repeat(100))
						.setCommit("abc\ndef")
						.setBuildTime("<b>now</b>")));
		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.until(() -> List.of(CLIENT_ID, WASMLET_ID, LEGACY_ID, HOSTILE_ID)
				.stream()
				.allMatch(id -> this.sessions.find(id).map(TunnelSession::advertised).orElse(false)));
		this.playwright = Playwright.create();
		this.browser = this.playwright.chromium().launch();
	}

	/** Opens a raw tunnel stream as the given client and waits for the ack. */
	private void advertise(String clientId, Advertise.Builder advertise) throws InterruptedException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.channels.add(channel);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), clientId);
		CountDownLatch acked = new CountDownLatch(1);
		StreamObserver<Frame> requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					if (frame.hasAdvertiseAck()) {
						acked.countDown();
					}
				}

				@Override
				public void onError(Throwable t) {
				}

				@Override
				public void onCompleted() {
				}

			});
		requests.onNext(Frame.newBuilder()
			.setAdvertise(advertise.addUpstreams(
					Upstream.newBuilder().setHost(clientId + ".local").setTargetUrl("http://127.0.0.1:1").build()))
			.build());
		assertThat(acked.await(10, TimeUnit.SECONDS)).as("advertise of %s acknowledged", clientId).isTrue();
	}

	@AfterAll
	void tearDown() {
		if (this.browser != null) {
			this.browser.close();
		}
		if (this.playwright != null) {
			this.playwright.close();
		}
		if (this.clientContext != null) {
			this.clientContext.close();
		}
		this.channels.forEach(ManagedChannel::shutdownNow);
	}

	@BeforeEach
	void openPage() {
		this.context = Objects.requireNonNull(this.browser).newContext();
		this.page = this.context.newPage();
		String base = "http://127.0.0.1:" + this.port;
		this.page.navigate(base + "/console");
		this.page.locator("#username").fill(CONSOLE_USER);
		this.page.locator("#password").fill(CONSOLE_PASSWORD);
		this.page.locator(".login__submit").click();
		this.page.waitForURL(base + "/console");
	}

	@AfterEach
	void closePage() {
		if (this.context != null) {
			this.context.close();
		}
	}

	private Locator row(String clientId) {
		return Objects.requireNonNull(this.page).locator("[data-client='" + clientId + "']");
	}

	@Test
	void clientRowShowsTheBuildPackagedWithTheClient() {
		ConfigurableApplicationContext client = Objects.requireNonNull(this.clientContext);
		BuildProperties build = client.getBeanProvider(BuildProperties.class).getIfAvailable();
		GitProperties git = client.getBeanProvider(GitProperties.class).getIfAvailable();
		// precondition: the client has build metadata to report, else "unknown" would
		// match
		assertThat(build).as("client BuildProperties").isNotNull();
		assertThat(git).as("client GitProperties").isNotNull();
		String version = Objects.requireNonNull(build.getVersion());
		String commit = Objects.requireNonNull(git.getCommitId());
		String shortCommit = Objects.requireNonNull(git.getShortCommitId());
		assertThat(version).isNotBlank();
		assertThat(shortCommit).isNotBlank();
		boolean dirty = Boolean.parseBoolean(git.get("dirty"));

		Locator buildLine = row(CLIENT_ID).getByTestId("build");
		assertThat(buildLine.innerText()).isEqualToNormalizingWhitespace(
				"Build sluice-client " + version + " " + shortCommit + (dirty ? " dirty" : ""));
		assertThat(buildLine.locator(".mono").getAttribute("title")).isEqualTo("Commit " + commit + ", built "
				+ Objects.requireNonNull(build.getTime()).truncatedTo(ChronoUnit.SECONDS));
		assertThat(row(CLIENT_ID).getByTestId("legacy").count()).isZero();
	}

	@Test
	void wasmletRowShowsItsBuild() {
		Locator buildLine = row(WASMLET_ID).getByTestId("build");
		assertThat(buildLine.innerText()).isEqualToNormalizingWhitespace("Build sluice-wasmlet 0.0.1-SNAPSHOT 0123456");
		assertThat(buildLine.locator(".mono").getAttribute("title"))
			.isEqualTo("Commit " + WASMLET_COMMIT + ", built 2026-10-12T00:00:00Z");
		assertThat(row(WASMLET_ID).getByTestId("legacy").count()).isZero();
	}

	@Test
	void peerWithoutBuildAndWindowIsUnknownAndLegacy() {
		assertThat(row(LEGACY_ID).getByTestId("build").innerText())
			.isEqualToNormalizingWhitespace("Build Unknown (older build)");
		assertThat(row(LEGACY_ID).getByTestId("legacy").innerText()).isEqualTo("Legacy flow control");
	}

	@Test
	void gaugesCountLegacyAndUnknownBuildSessions() {
		// precondition: the legacy peer without a build is live; the others announce both
		assertThat(this.sessions.find(LEGACY_ID).map(TunnelSession::legacy)).hasValue(true);
		assertThat(List.of(CLIENT_ID, WASMLET_ID, HOSTILE_ID)).allSatisfy(id -> assertThat(this.sessions.find(id))
			.hasValueSatisfying(session -> assertThat(session.legacy()).isFalse()));
		assertThat(this.meterRegistry.get("sluice.tunnel.sessions.legacy").gauge().value()).isEqualTo(1.0);
		assertThat(this.meterRegistry.get("sluice.tunnel.sessions.build.unknown").gauge().value()).isEqualTo(1.0);
	}

	@Test
	void reconnectedPeerShowsItsNewBuild() throws Exception {
		String clientId = "build-upgraded";
		advertise(clientId,
				Advertise.newBuilder()
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
					.setBuild(PeerBuild.newBuilder().setImplementation("sluice-client").setVersion("1.0.0")));
		// the upgraded process comes back under the same id; the server replaces the
		// session
		advertise(clientId,
				Advertise.newBuilder()
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW)
					.setBuild(PeerBuild.newBuilder().setImplementation("sluice-client").setVersion("1.1.0")));
		Page page = Objects.requireNonNull(this.page);
		page.navigate("http://127.0.0.1:" + this.port + "/console");
		assertThat(row(clientId).getByTestId("build").innerText())
			.isEqualToNormalizingWhitespace("Build sluice-client 1.1.0 commit unknown");
	}

	@Test
	void peerSuppliedStringsAreBoundedAndEscaped() {
		Page page = Objects.requireNonNull(this.page);
		Locator buildLine = row(HOSTILE_ID).getByTestId("build");
		assertThat(buildLine.innerText()).isEqualToNormalizingWhitespace(
				"Build <img src=x onerror=\"window.pwned=1\"> " + "9".repeat(64) + "... abc?def");
		assertThat(buildLine.locator(".mono").getAttribute("title")).isEqualTo("Commit abc?def, built <b>now</b>");
		assertThat(row(HOSTILE_ID).locator("img, b").count()).isZero();
		assertThat(page.evaluate("() => window.pwned === undefined")).isEqualTo(true);
	}

	@Test
	void serverLogsEachClientsBuildOnceAndBounded(CapturedOutput output) {
		assertThat(
				output.getAll().lines().filter(line -> line.contains(" runs ")).map(ConsolePeerBuildE2ETest::message))
			.containsOnlyOnce(
					"client " + WASMLET_ID
							+ " runs sluice-wasmlet 0.0.1-SNAPSHOT (0123456, built 2026-10-12T00:00:00Z)",
					"client " + LEGACY_ID + " runs " + PeerBuilds.UNKNOWN
							+ "; it announced no connection window (legacy flow control)",
					"client " + HOSTILE_ID + " runs <img src=x onerror=\"window.pwned=1\"> " + "9".repeat(64)
							+ "... (abc?def, built <b>now</b>)");
	}

	/** The text after the logger name of a log line. */
	private static String message(String line) {
		int separator = line.indexOf(" : ");
		return separator < 0 ? line : line.substring(separator + 3);
	}

	@Test
	void clientLogsTheServerBuildFromTheAck(CapturedOutput output) {
		BuildProperties build = this.serverContext.getBeanProvider(BuildProperties.class).getIfAvailable();
		GitProperties git = this.serverContext.getBeanProvider(GitProperties.class).getIfAvailable();
		assertThat(build).as("server BuildProperties").isNotNull();
		assertThat(git).as("server GitProperties").isNotNull();
		String expected = "[grpc://127.0.0.1:" + grpcPort + "] server build: sluice-server " + build.getVersion() + " ("
				+ git.getShortCommitId() + (Boolean.parseBoolean(git.get("dirty")) ? ", dirty" : "") + ", built "
				+ Objects.requireNonNull(build.getTime()).truncatedTo(ChronoUnit.SECONDS) + ")";
		assertThat(output.getAll().lines().filter(line -> line.contains("] server build: ")))
			.anySatisfy(line -> assertThat(line).endsWith(expected));
	}

}
