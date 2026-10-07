package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.Response;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A request for a host no tunnel client serves gets the no-route error page from the data
 * plane, in the protocol the request arrived in.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class NoRouteE2ETest {

	private static final int GOAWAY = 0x7;

	private static final ExecutorService WRITER = Executors.newVirtualThreadPerTaskExecutor();

	private static int dataPort;

	private @Nullable Playwright playwright;

	private @Nullable Browser browser;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		dataPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(freePort()));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
	}

	@BeforeAll
	void setUp() {
		this.playwright = Playwright.create();
		this.browser = this.playwright.chromium().launch();
	}

	@AfterAll
	void tearDown() {
		if (this.browser != null) {
			this.browser.close();
		}
		if (this.playwright != null) {
			this.playwright.close();
		}
		WRITER.shutdownNow();
	}

	@Test
	void browserShowsTheNoRoutePage() {
		try (Page page = Objects.requireNonNull(this.browser).newPage()) {
			Response response = Objects.requireNonNull(page.navigate("http://127.0.0.1:" + dataPort + "/orders?id=1"));

			assertThat(response.status()).isEqualTo(503);
			assertThat(response.headerValue("content-type")).isEqualTo("text/html; charset=utf-8");
			assertThat(page.title()).isEqualTo("No route - sluice");
			assertThat(page.locator("main").innerText()).isEqualToNormalizingWhitespace("""
					request
					gate shut
					no outlet
					no tunnel client serves this host
					503 SERVICE UNAVAILABLE
					No route to this host
					No tunnel client serves 127.0.0.1:%d right now.
					The client may be stopped or reconnecting, or the host is not registered with this server.
					Status
					503
					Host
					127.0.0.1:%d
					Request
					GET /orders?id=1
					""".formatted(dataPort, dataPort));
		}
	}

	@Test
	void requestValuesAreEscaped() throws Exception {
		String response = exchange("GET /<b>path</b> HTTP/1.1\r\nHost: <i>host</i>\r\nConnection: close\r\n\r\n");
		try (Page page = Objects.requireNonNull(this.browser).newPage()) {
			page.setContent(response.substring(response.indexOf("\r\n\r\n") + 4));

			assertThat(page.locator("main b, main i").count()).isZero();
			assertThat(page.getByTestId("host").innerText()).isEqualTo("<i>host</i>");
			assertThat(page.getByTestId("request").innerText()).isEqualTo("GET /<b>path</b>");
		}
	}

	@Test
	void requestWithoutHostSaysSo() throws Exception {
		String response = exchange("GET / HTTP/1.0\r\n\r\n");
		try (Page page = Objects.requireNonNull(this.browser).newPage()) {
			page.setContent(response.substring(response.indexOf("\r\n\r\n") + 4));

			assertThat(page.locator(".lede").innerText())
				.isEqualTo("The request carried no Host header, so no route could be chosen.");
			assertThat(page.getByTestId("host").innerText()).isEqualTo("(none)");
		}
	}

	@Test
	void headRequestCarriesTheHeadOnly() throws Exception {
		String response = exchange("HEAD / HTTP/1.1\r\nHost: nothing.local\r\nConnection: close\r\n\r\n");

		assertThat(response.replaceFirst("Content-Length: \\d+", "Content-Length: N"))
			.isEqualToNormalizingWhitespace("""
					HTTP/1.1 503 Service Unavailable
					Content-Type: text/html; charset=utf-8
					Content-Length: N
					Cache-Control: no-store
					Connection: close
					""");
	}

	@Test
	void unreadRequestBodyDoesNotResetTheResponse() throws Exception {
		byte[] body = new byte[512 * 1024];
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(("POST /upload HTTP/1.1\r\nHost: nothing.local\r\nContent-Length: " + body.length + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			Future<?> upload = WRITER.submit(() -> {
				out.write(body);
				out.flush();
				return null;
			});
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			upload.get();

			assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 503 Service Unavailable");
			assertThat(response).endsWith("</html>\n");
		}
	}

	@Test
	void h2cPriorKnowledgeGetsThePageOverHttp2() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":method=GET", ":scheme=http", ":path=/",
					":authority=nothing.local");
			InputStream in = socket.getInputStream();
			List<TestH2.Frame> frames = TestH2.readResponseFrames(in);
			TestH2.Frame goaway = Objects.requireNonNull(TestH2.readFrame(in));

			TestH2.Frame headersFrame = frames.getFirst();
			assertThat(headersFrame.type()).isEqualTo(TestH2.HEADERS);
			assertThat(headersFrame.streamId()).isEqualTo(1);
			Http2Headers headers = new DefaultHttp2HeadersDecoder(true).decodeHeaders(1,
					Unpooled.wrappedBuffer(headersFrame.payload()));
			assertThat(headers.status()).hasToString("503");
			assertThat(headers.get("content-type")).hasToString("text/html; charset=utf-8");
			String body = TestH2.bodyOf(frames);
			assertThat(headers.get("content-length"))
				.hasToString(String.valueOf(body.getBytes(StandardCharsets.UTF_8).length));
			assertThat(body).startsWith("<!DOCTYPE html>").endsWith("</html>\n");
			assertThat(goaway.type()).isEqualTo(GOAWAY);
			assertThat(TestH2.readFrame(in)).isNull();
		}
	}

	private static String exchange(String request) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

}
