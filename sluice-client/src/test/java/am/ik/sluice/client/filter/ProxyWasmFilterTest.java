package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.client.filter.UpstreamFilter.BodyHandler;
import am.ik.sluice.client.filter.UpstreamFilter.BodyObserver;
import am.ik.sluice.client.filter.UpstreamFilter.Exchange;
import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.util.unit.DataSize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Plugins of the proxy-wasm SDK, the SDK's own examples among them, interpreted by Endive
 * behind the pipe: the same on a JVM and, with {@code -PnativeTest}, on a native image.
 */
@SuppressWarnings("NullAway")
class ProxyWasmFilterTest {

	private static final String TEST_PLUGIN = "src/test/resources/wasm/test_plugin.wasm";

	private static final String HTTP_BODY = "src/test/resources/wasm/http_body.wasm";

	private static final String HTTP_HEADERS = "src/test/resources/wasm/http_headers.wasm";

	private static final String GC_PLUGIN = "src/test/resources/wasm/gc_plugin.wasm";

	private static final String LISP_PLUGIN = "src/test/resources/wasm/lisp_plugin.wasm";

	private static final String SIZED = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde";

	private static final String CHUNKED = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nabcde\r\n0\r\n\r\n";

	private static final byte[] NO_CONTENT = "HTTP/1.1 204 No Content\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

	private static FilterConfig.Builder limits(String plugin) {
		return FilterConfig.builder()
			.plugin(plugin)
			.vms(1)
			.maxMemory(DataSize.ofMegabytes(16))
			.callTimeout(Duration.ofSeconds(30))
			.maxBody(DataSize.ofMegabytes(1))
			.maxMeters(16);
	}

	private UpstreamFilter filter(FilterConfig definition, FilterConfig use) {
		return filter(this.registry, definition, use);
	}

	private static UpstreamFilter filter(MeterRegistry registry, FilterConfig definition, FilterConfig use) {
		return Filters.builder()
			.definitions(Map.of("plugin", definition))
			.meters(registry)
			.budget(TunnelBudget.unlimited())
			.build()
			.forRoute("route.test", List.of(use));
	}

	private UpstreamFilter filter(String plugin) {
		return filter(limits(plugin).build(), FilterConfig.builder().name("plugin").build());
	}

	/** The head's lines but for the framing, then the payload of the body. */
	private static String decoded(String response) {
		int end = response.indexOf("\r\n\r\n");
		StringBuilder decoded = new StringBuilder();
		List<String> lines = new ArrayList<>(List.of(response.substring(0, end).split("\r\n")));
		boolean chunked = lines.removeIf(line -> line.equalsIgnoreCase("transfer-encoding: chunked"));
		lines.removeIf(line -> line.toLowerCase().startsWith("content-length:"));
		decoded.append(String.join("\n", lines.stream().map(String::toLowerCase).sorted().toList())).append("\n\n");
		String body = response.substring(end + 4);
		if (!chunked) {
			return decoded.append(body).toString();
		}
		byte[] bytes = body.getBytes(StandardCharsets.ISO_8859_1);
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		int at = 0;
		while (true) {
			int eol = at;
			while (bytes[eol] != '\r') {
				eol++;
			}
			int size = Integer.parseInt(new String(bytes, at, eol - at, StandardCharsets.US_ASCII), 16);
			if (size == 0) {
				break;
			}
			payload.write(bytes, eol + 2, size);
			at = eol + 2 + size + 2;
		}
		return decoded.append(payload.toString(StandardCharsets.ISO_8859_1)).toString();
	}

	@Test
	void theSdksOwnExamplesRun() throws Exception {
		UpstreamFilter body = filter(HTTP_BODY);

		// it pauses until the end, and has removed content-length
		assertThat(decoded(
				FilterTests.relayed(body, "GET", "/", "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\na secret", 3)))
			.isEqualTo("http/1.1 200 ok\n\nOriginal message body (8 bytes) redacted.\n");
		assertThat(decoded(FilterTests.relayed(body, "GET", "/",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na s\r\n5\r\necret\r\n0\r\n\r\n", 4)))
			.isEqualTo("http/1.1 200 ok\n\nOriginal message body (8 bytes) redacted.\n");
		assertThat(decoded(FilterTests.relayed(body, "GET", "/", SIZED, 2))).isEqualTo("http/1.1 200 ok\n\nabcde");

		UpstreamFilter headers = filter(HTTP_HEADERS);
		String response = "HTTP/1.1 200 OK\r\nX-Name: Value\r\nContent-Length: 5\r\n\r\nplain";
		assertThat(FilterTests.relayed(headers, "GET", "/", response, 1024)).isEqualTo(response);
		// the SDK fails on what is not UTF-8: it reads as replacement characters, and the
		// line stays as it came
		String notUtf8 = "HTTP/1.1 200 OK\r\nX-Name: ÿ\r\nContent-Length: 5\r\n\r\nplain";
		assertThat(FilterTests
			.exchange(headers, FilterTests.head("GET", "/"), notUtf8.getBytes(StandardCharsets.ISO_8859_1), 1024)
			.relayed()).isEqualTo(notUtf8);
		assertThat(FilterTests.relayed(headers, "GET", "/hello", response, 1024))
			.isEqualTo("HTTP/1.1 200 OK\r\nhello: World\r\npowered-by: proxy-wasm\r\nContent-Length: 14\r\n"
					+ "Connection: close\r\n\r\nHello, World!\n");
		assertThat(FilterTests.counters(this.registry)).isEmpty();
	}

	@Test
	void aMisbehavingPluginLeavesTheResponseAlone() throws Exception {
		for (String target : List.of("/spin-request", "/trap-request", "/host", "/pairs-malformed", "/spin-head",
				"/trap-head", "/hog-head", "/local-late", "/framing", "/smuggle", "/spin-body", "/trap-body",
				"/hog-body", "/replace-sized")) {
			for (String response : List.of(SIZED, CHUNKED)) {
				if (target.equals("/replace-sized") && response == CHUNKED) {
					continue;
				}
				SimpleMeterRegistry registry = new SimpleMeterRegistry();
				UpstreamFilter filter = filter(registry,
						limits(TEST_PLUGIN).callTimeout(Duration.ofMillis(500)).build(),
						FilterConfig.builder().name("plugin").build());

				String relayed = FilterTests.relayed(filter, "GET", target, response, 64 * 1024);

				assertThat(decoded(relayed)).as(target).isEqualTo(decoded(response));
				String phase = target.endsWith("-body") || target.equals("/replace-sized") ? "body" : "head";
				assertThat(FilterTests.counters(registry)).as(target).isEqualTo(Map.of("failures plugin " + phase, 1L));
				// the VM is started anew for the next stream
				assertThat(FilterTests.relayed(filter, "GET", "/", SIZED, 1024)).isEqualTo(SIZED);
			}
		}
	}

	/**
	 * A command, not a reactor, of objects on the heap: it refuses a second start. What
	 * it drops comes back, here ten times its limit.
	 */
	@Test
	void aPluginOfObjectsRuns() throws Exception {
		UpstreamFilter filter = filter(GC_PLUGIN);
		String response = "HTTP/1.1 204 No Content\r\n\r\n";

		assertThat(FilterTests.relayed(filter, "GET", "/edit", response, 1024))
			.isEqualTo("HTTP/1.1 204 No Content\r\nx-plugin: built on the heap\r\nx-count: 1\r\n\r\n");
		for (int i = 0; i < 20; i++) {
			assertThat(FilterTests.relayed(filter, "GET", "/garbage", response, 1024)).isEqualTo(response);
		}
		assertThat(FilterTests.relayed(filter, "GET", "/edit", response, 1024))
			.isEqualTo("HTTP/1.1 204 No Content\r\nx-plugin: built on the heap\r\nx-count: 2\r\n\r\n");
		assertThat(FilterTests.counters(this.registry)).isEmpty();
	}

	/**
	 * Its limit of 16MB bounds what it holds on the heap with its memory. The last path
	 * of each takes it past that; each array here is 8MiB, and the one of {@code /huge}
	 * 128MiB, which the JVM would give it.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "/array /array", "/structs /structs /structs", "/raised /raised /raised", "/table /table",
			"/array /memory /memory", "/huge" })
	void aPluginHoldingPastItsMemoryFails(String paths) throws Exception {
		UpstreamFilter filter = filter(GC_PLUGIN);
		String response = "HTTP/1.1 204 No Content\r\n\r\n";

		for (String path : paths.split(" ")) {
			assertThat(FilterTests.counters(this.registry)).as(path).isEmpty();
			assertThat(FilterTests.relayed(filter, "GET", path, response, 1024)).as(path).isEqualTo(response);
		}

		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures plugin head", 1L));
		// the VM started anew holds nothing of it
		assertThat(FilterTests.relayed(filter, "GET", "/edit", response, 1024))
			.isEqualTo("HTTP/1.1 204 No Content\r\nx-plugin: built on the heap\r\nx-count: 1\r\n\r\n");
		assertThat(FilterTests.relayed(filter, "GET", paths.split(" ")[0], response, 1024)).isEqualTo(response);
	}

	/**
	 * What a real compiler makes of it. rontolisp's start allocates and drops an array of
	 * 16Mi elements, 128MiB here, which is what its limit is for: what it holds later
	 * goes past that as well, each array 8MiB, and the VM started anew does not.
	 */
	@Test
	void aPluginInLispRunsWithinItsMemory() throws Exception {
		UpstreamFilter filter = filter(limits(LISP_PLUGIN).maxMemory(DataSize.ofMegabytes(129)).build(),
				FilterConfig.builder().name("plugin").build());
		String response = "HTTP/1.1 204 No Content\r\n\r\n";
		String stamped = "HTTP/1.1 204 No Content\r\nx-filtered: yes\r\n\r\n";

		assertThat(FilterTests.relayed(filter, "GET", "/", response, 1024)).isEqualTo(stamped);
		assertThat(FilterTests.relayed(filter, "GET", "/hello", response, 1024))
			.isEqualTo("HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\nHello from Lisp\n");
		assertThat(FilterTests.counters(this.registry)).isEmpty();

		for (int i = 0; i < 16; i++) {
			assertThat(FilterTests.relayed(filter, "GET", "/hold", response, 1024)).isEqualTo(stamped);
		}
		assertThat(FilterTests.counters(this.registry)).isEmpty();
		assertThat(FilterTests.relayed(filter, "GET", "/hold", response, 1024)).isEqualTo(response);
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures plugin head", 1L));

		assertThat(FilterTests.relayed(filter, "GET", "/", response, 1024)).isEqualTo(stamped);
	}

	/** Without that thread nothing stops a plugin, so not even reporting may end it. */
	@Test
	void theWatchdogOutlivesRunningOutOfMemory() throws Exception {
		Logger logger = (Logger) LoggerFactory.getLogger(ProxyWasmFilter.class);
		CountDownLatch reported = new CountDownLatch(1);
		AppenderBase<ILoggingEvent> noMemory = new AppenderBase<>() {
			@Override
			protected void append(ILoggingEvent event) {
				reported.countDown();
				throw new OutOfMemoryError("reporting");
			}
		};
		noMemory.start();
		logger.addAppender(noMemory);
		try {
			Thread running = new Thread(() -> {
				ProxyWasmFilter.Watchdog.Call call = new ProxyWasmFilter.Watchdog.Call(Duration.ZERO);
				try {
					reported.await();
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				call.end();
			}) {
				@Override
				public void interrupt() {
					throw new OutOfMemoryError("a round");
				}
			};
			running.start();
			assertThat(reported.await(30, TimeUnit.SECONDS)).isTrue();
			running.join();
		}
		finally {
			logger.detachAppender(noMemory);
		}

		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		UpstreamFilter filter = filter(registry, limits(TEST_PLUGIN).callTimeout(Duration.ofMillis(200)).build(),
				FilterConfig.builder().name("plugin").build());
		assertTimeoutPreemptively(Duration.ofSeconds(30),
				() -> FilterTests.relayed(filter, "GET", "/spin-head", SIZED, 1024));
		assertThat(FilterTests.counters(registry)).isEqualTo(Map.of("failures plugin head", 1L));
	}

	@Test
	void aPluginFailingClosedFailsWhatItCouldNotSee() throws Exception {
		UpstreamFilter filter = filter(limits(TEST_PLUGIN).failClosed(true).build(),
				FilterConfig.builder().name("plugin").build());

		for (String target : List.of("/trap-request", "/trap-head", "/trap-body", "/replace-sized")) {
			assertThatThrownBy(() -> FilterTests.relayed(filter, "GET", target, SIZED, 1024)).as(target)
				.isInstanceOf(IOException.class);
		}
	}

	/**
	 * A stream on a VM another stream makes fail goes on, unseen from there, and its
	 * calls harm neither the VM started anew nor the streams on it.
	 */
	@Test
	void aVmFailingEndsTheObservationOfItsOtherStreamsOnly() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		UpstreamFilter.Request request = UpstreamFilter.Request.builder().method("GET").target("/observed").build();

		Exchange open = filter.onRequest(request, new NoHeaders());
		BodyHandler handler = open.onResponse(200, new NoHeaders("content-length: 2"), false);
		assertThat(handler).isInstanceOf(BodyObserver.class);
		BodyObserver observer = (BodyObserver) handler;
		observer.onData(new byte[] { 'a' }, 0, 1);

		assertThat(FilterTests.relayed(filter, "GET", "/trap-body", SIZED, 1024)).isEqualTo(SIZED);
		UpstreamFilter.Request edit = UpstreamFilter.Request.builder().method("GET").target("/edit").build();
		Exchange next = filter.onRequest(edit, new NoHeaders());
		observer.onData(new byte[] { 'b' }, 0, 1);
		observer.onEnd(true);

		NoHeaders headers = new NoHeaders();
		next.onResponse(204, headers, true);
		assertThat(headers.replaced).containsExactly("x-plugin: edited");
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures plugin body", 2L));
	}

	/** A body's call does not reach a stream that ended, as a response ending first. */
	@Test
	void aRequestBodyGoingOnPastItsResponseEndsQuietly() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		String post = "POST /request-read HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\n";
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		upstream.answer("HTTP/1.1 204 No Content\r\n\r\n");
		FilterTests.onAVirtualThread(() -> {
			try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, filter)) {
				pipe.sink().write((post + "a").getBytes(StandardCharsets.US_ASCII));
				assertThat(new String(pipe.source().readNBytes(27), StandardCharsets.US_ASCII))
					.isEqualTo("HTTP/1.1 204 No Content\r\n\r\n");
				pipe.sink().write('b');
			}
			return null;
		});

		assertThat(upstream.received()).isEqualTo(post + "ab");
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("body.bytes plugin request", 1L));
	}

	/** What a plugin changed of a request does not make its response's failure a cut. */
	@Test
	void aChangeIsOfTheDirectionItWasMadeIn() throws Exception {
		String response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nabcde\r\n0\r\n\r\n";
		FilterTests.Exchanged exchanged = FilterTests.exchange(filter(TEST_PLUGIN),
				"POST /request-then-trap HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\nab",
				response.getBytes(StandardCharsets.US_ASCII), 1024);

		assertThat(exchanged.upstream())
			.isEqualTo("POST /request-then-trap HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"
					+ "2\r\nAB\r\n0\r\n\r\n");
		assertThat(decoded(exchanged.relayed())).isEqualTo("http/1.1 200 ok\n\nabcde");
	}

	/** A plugin's output and logs reach the log, whatever it writes them with. */
	@Test
	void aPluginWritesToItsStdout() throws Exception {
		assertThat(FilterTests.relayed(filter(TEST_PLUGIN), "GET", "/stdout", SIZED, 1024)).isEqualTo(SIZED);
		assertThat(FilterTests.counters(this.registry)).isEmpty();
	}

	/** What a client or a plugin puts in a head costs the host a bounded amount. */
	@Test
	void aHeadOfTooManyFieldsIsNotOffered() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		StringBuilder many = new StringBuilder("GET /request-edit HTTP/1.1\r\nHost: a\r\n");
		for (int i = 0; i < 300; i++) {
			many.append("a:\r\n");
		}
		String head = many.append("\r\n").toString();

		assertThat(FilterTests.exchange(filter, head, NO_CONTENT, 1024).upstream()).isEqualTo(head);
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures plugin head", 1L));

		// the time of a call is not what stops a plugin adding fields for good
		assertTimeoutPreemptively(Duration.ofSeconds(20),
				() -> assertThat(FilterTests.relayed(filter, "GET", "/hog-fields", SIZED, 1024)).isEqualTo(SIZED));
	}

	@Test
	void aPluginReplacesAResponseBodyAndAFailureCutsIt() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		String chunked = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";

		for (String response : List.of(chunked, SIZED)) {
			assertThat(decoded(FilterTests.relayed(filter, "GET", "/replace", response, 1024)))
				.isEqualTo("http/1.1 200 ok\n\nABCDE<end>");
		}
		assertThatThrownBy(() -> FilterTests.relayed(filter, "GET", "/replace-trap", chunked, 1024))
			.isInstanceOf(IOException.class);
		// an HTTP/1.0 client takes no chunks: the connection's end ends the body
		assertThat(FilterTests
			.exchange(filter, "GET /replace HTTP/1.0\r\nHost: a\r\n\r\n", SIZED.getBytes(StandardCharsets.US_ASCII),
					1024)
			.relayed()).isEqualTo("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nABCDE<end>");
	}

	/** Past its limit a body held goes on when no one changed it, unless it may not. */
	@Test
	void aBodyHeldPastItsLimitOrTheBudgetIsLetGo() throws Exception {
		String response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n8\r\n12345678\r\n8\r\n12345678\r\n0\r\n\r\n";
		FilterConfig.Builder small = limits(TEST_PLUGIN).maxBody(DataSize.ofBytes(10));
		FilterConfig use = FilterConfig.builder().name("plugin").build();
		String expected = "http/1.1 200 ok\n\n1234567812345678";

		assertThat(decoded(FilterTests.relayed(filter(small.build(), use), "GET", "/hold", response, 1024)))
			.isEqualTo(expected);
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("failures plugin body", 1L));
		assertThatThrownBy(
				() -> FilterTests.relayed(filter(small.failClosed(true).build(), use), "GET", "/hold", response, 1024))
			.isInstanceOf(IOException.class);
		String budgeted = FilterTests
			.exchange(filter(TEST_PLUGIN), new TunnelBudget(0), FilterTests.head("GET", "/hold"),
					response.getBytes(StandardCharsets.US_ASCII), 1024)
			.relayed();
		assertThat(decoded(budgeted)).isEqualTo(expected);
	}

	@Test
	void aPluginSeesARequestBodyAndReplacesIt() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		String post = "POST %s HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nabcde";

		// read: as it came
		String read = post.formatted("/request-read");
		assertThat(FilterTests.exchange(filter, read, NO_CONTENT, 2).upstream()).isEqualTo(read);
		assertThat(FilterTests.counters(this.registry)).isEqualTo(Map.of("body.bytes plugin request", 5L));
		// held to its end: a length, no framing changed
		assertThat(FilterTests.exchange(filter, post.formatted("/request-replace"), NO_CONTENT, 2).upstream())
			.isEqualTo("POST /request-replace HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nABCDE");
		// replaced as it streams: chunked from its first piece
		assertThat(FilterTests.exchange(filter, post.formatted("/request-stream"), NO_CONTENT, 1024).upstream())
			.isEqualTo("POST /request-stream HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"
					+ "5\r\nABCDE\r\n0\r\n\r\n");
		// a chunked request's trailers follow what was replaced
		assertThat(FilterTests
			.exchange(filter,
					"POST /request-stream HTTP/1.1\r\nHost: a\r\n"
							+ "Transfer-Encoding: chunked\r\n\r\n2\r\nab\r\n0\r\nX-T: 1\r\n\r\n",
					NO_CONTENT, 1024)
			.upstream())
			.isEqualTo("POST /request-stream HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"
					+ "2\r\nAB\r\n0\r\nX-T: 1\r\n\r\n");
		// a plugin failing on a body that keeps its length: it goes on
		String trapped = post.formatted("/request-trap");
		assertThat(FilterTests.exchange(filter, trapped, NO_CONTENT, 1024).upstream()).isEqualTo(trapped);
	}

	@Test
	void aPluginEditsARequestHead() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);

		assertThat(
				FilterTests.exchange(filter, "GET /request-edit HTTP/1.1\r\nHost: a\r\nX-Up: 1\r\n\r\n", NO_CONTENT, 64)
					.upstream())
			.isEqualTo("GET /request-edit HTTP/1.1\r\nHost: a\r\nX-Up: 1\r\nx-plugin: request\r\n\r\n");
		assertThat(
				FilterTests.exchange(filter, "GET /request-end HTTP/1.1\r\nHost: a\r\n\r\n", NO_CONTENT, 64).upstream())
			.isEqualTo("GET /request-end HTTP/1.1\r\nHost: a\r\nx-end: yes\r\n\r\n");
		assertThat(FilterTests
			.exchange(filter, "POST /request-end HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nz", NO_CONTENT, 64)
			.upstream()).isEqualTo("POST /request-end HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nx-end: no\r\n\r\nz");
		// the whole map set back: what it may not change stays, as it came
		assertThat(FilterTests
			.exchange(filter, "POST /request-pairs HTTP/1.1\r\nHost: a\r\nX-Gone: 1\r\nContent-Length: 1\r\n\r\nz",
					NO_CONTENT, 64)
			.upstream())
			.isEqualTo("POST /request-pairs HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nx-pairs: set\r\n\r\nz");
	}

	@Test
	void aPluginAnswersInTheUpstreamsPlace() throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);
		String denied = "HTTP/1.1 403 Forbidden\r\nx-plugin: local\r\nContent-Length: 7\r\nConnection: close\r\n\r\ndenied\n";

		FilterTests.Exchanged exchanged = FilterTests.exchange(filter, FilterTests.head("GET", "/local"), new byte[0],
				64);
		assertThat(exchanged.upstream()).isEmpty();
		assertThat(exchanged.relayed()).isEqualTo(denied);
		// after the responses before it, and what follows it goes nowhere
		exchanged = FilterTests.exchange(filter,
				FilterTests.head("GET", "/") + FilterTests.head("GET", "/local") + FilterTests.head("GET", "/next"),
				SIZED.getBytes(StandardCharsets.US_ASCII), 1024);
		assertThat(exchanged.upstream()).isEqualTo(FilterTests.head("GET", "/"));
		assertThat(exchanged.relayed()).isEqualTo(SIZED + denied);
		// HEAD: no body
		assertThat(FilterTests.exchange(filter, FilterTests.head("HEAD", "/local"), new byte[0], 64).relayed())
			.isEqualTo(denied.replace("denied\n", ""));
		// having read the whole body: nothing of the request reached the upstream
		exchanged = FilterTests.exchange(filter,
				"POST /local-body HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n0\r\n\r\n",
				new byte[0], 5);
		assertThat(exchanged.relayed()).startsWith("HTTP/1.1 451 Unavailable For Legal Reasons\r\n")
			.endsWith("read it\n");
	}

	@Test
	void aPluginReadsItsSettingsAndReachesNothingOutside() throws Exception {
		FilterConfig definition = limits(TEST_PLUGIN).configuration("defined").build();
		String withBody = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n";

		assertThat(FilterTests.relayed(filter(definition, FilterConfig.builder().name("plugin").build()), "GET",
				"/properties", withBody + "\r\n", 64))
			.isEqualTo(withBody + "x-route: route.test\r\nx-code: 200\r\nx-configuration: defined\r\n\r\n");
		assertThat(FilterTests.relayed(
				filter(definition, FilterConfig.builder().name("plugin").configuration("of the route").build()), "GET",
				"/properties", withBody + "\r\n", 64))
			.isEqualTo(withBody + "x-route: route.test\r\nx-code: 200\r\nx-configuration: of the route\r\n\r\n");
		assertThat(FilterTests.relayed(filter(TEST_PLUGIN), "GET", "/outgoing", withBody + "\r\n", 64))
			.isEqualTo(withBody + "x-outgoing: refused\r\n\r\n");
		// refused at the start, not on the first request
		assertThatThrownBy(() -> filter(limits(TEST_PLUGIN).configuration("refuse").build(),
				FilterConfig.builder().name("plugin").build()))
			.hasMessage("the plugin refused its start");
	}

	@Test
	void pathsKeepAPluginFromAllElse() throws Exception {
		UpstreamFilter filter = filter(limits(TEST_PLUGIN).paths(List.of("/edit", "/prefix/*")).build(),
				FilterConfig.builder().name("plugin").build());
		String response = "HTTP/1.1 204 No Content\r\n\r\n";

		assertThat(FilterTests.relayed(filter, "GET", "/edit", response, 64))
			.isEqualTo("HTTP/1.1 204 No Content\r\nx-plugin: edited\r\n\r\n");
		// it would trap: it is not even called
		assertThat(FilterTests.relayed(filter, "GET", "/trap-request", response, 64)).isEqualTo(response);
		assertThat(FilterTests.relayed(filter, "GET", "/prefix/x", response, 64)).isEqualTo(response);
		assertThat(FilterTests.counters(this.registry)).isEmpty();
	}

	@Test
	void whatAPluginSendsIsBoundedAndWhatIsNoPluginRefused(@TempDir Path directory) throws Exception {
		UpstreamFilter filter = filter(TEST_PLUGIN);

		FilterTests.relayed(filter, "GET", "/meters", "HTTP/1.1 204 No Content\r\n\r\n", 1024);
		assertThat(this.registry.find("sluice.client.filter.plugin.meters").counters()).hasSize(16);
		assertThat(this.registry.get("sluice.client.filter.plugin.meters.dropped").counter().count())
			.isEqualTo(100 - 16);

		assertThat(FilterTests.relayed(filter, "GET", "/edit", "HTTP/1.1 204 No Content\r\nX-Up: 1\r\n\r\n", 1024))
			.isEqualTo("HTTP/1.1 204 No Content\r\nX-Up: 1\r\nx-plugin: edited\r\n\r\n");

		// a module with a memory and nothing else
		Path module = directory.resolve("module.wasm");
		Files.write(module, new byte[] { 0, 'a', 's', 'm', 1, 0, 0, 0, 5, 3, 1, 0, 1 });
		assertThatThrownBy(() -> filter(module.toString())).isInstanceOf(IllegalArgumentException.class)
			.hasMessage("plugin plugin is no proxy-wasm 0.2.1 plugin: no proxy_abi_version_0_2_1");

		// a table of 10 million entries and a memory
		Files.write(module, new byte[] { 0, 'a', 's', 'm', 1, 0, 0, 0, 4, 7, 1, 0x70, 0, (byte) 0x80, (byte) 0xad,
				(byte) 0xe2, 4, 5, 3, 1, 0, 1 });
		assertThatThrownBy(() -> filter(module.toString())).isInstanceOf(IllegalArgumentException.class)
			.hasMessage("plugin plugin needs 120065536 bytes of memory and tables, past its limit of 16777216");
	}

	private static final class NoHeaders implements UpstreamFilter.Headers {

		private final List<String> lines;

		private List<String> replaced = List.of();

		NoHeaders(String... lines) {
			this.lines = List.of(lines);
		}

		@Override
		public List<String> values(String name) {
			return this.lines.stream()
				.filter(line -> line.startsWith(name + ":"))
				.map(line -> line.substring(name.length() + 1).trim())
				.toList();
		}

		@Override
		public void set(String name, String value) {
		}

		@Override
		public void remove(String name) {
		}

		@Override
		public List<String> lines() {
			return this.lines;
		}

		@Override
		public void replaceLines(List<String> lines) {
			this.replaced = lines;
		}

	}

}
