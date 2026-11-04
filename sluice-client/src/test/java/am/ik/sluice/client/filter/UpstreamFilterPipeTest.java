package am.ik.sluice.client.filter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.client.filter.UpstreamFilter.BodyHandler;
import am.ik.sluice.client.filter.UpstreamFilter.BodyObserver;
import am.ik.sluice.client.filter.UpstreamFilter.BodyRewriter;
import am.ik.sluice.client.filter.UpstreamFilter.Exchange;
import am.ik.sluice.client.filter.UpstreamFilter.Headers;
import am.ik.sluice.client.filter.UpstreamFilter.LocalResponse;
import am.ik.sluice.tunnel.TunnelBudget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * What the pipe holds to whatever a filter does, and whatever the two ends of a
 * connection send: what goes out stays framed, a filter is told how its part ended, and
 * neither side waits for good.
 */
@SuppressWarnings("NullAway")
class UpstreamFilterPipeTest {

	private static final byte[] NO_CONTENT = "HTTP/1.1 204 No Content\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	private static final Duration TIMEOUT = Duration.ofSeconds(20);

	/** Takes a head's framing out, and then leaves the body alone. */
	private static final UpstreamFilter FRAMING_LOST = (request, requestHeaders) -> {
		dropLength(requestHeaders);
		return (status, headers, bodiless) -> {
			dropLength(headers);
			return null;
		};
	};

	private static void dropLength(Headers headers) {
		headers.replaceLines(headers.lines().stream().filter(line -> !line.startsWith("Content-Length")).toList());
	}

	/** A body with its length gone would read as the next request or response. */
	@Test
	void whatIsNotRewrittenKeepsItsFraming() throws Exception {
		String smuggling = "GET /internal HTTP/1.1\r\nHost: a\r\n\r\n";
		String request = "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: " + smuggling.length() + "\r\n\r\n" + smuggling;
		String response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde";

		FilterTests.Exchanged exchanged = FilterTests.exchange(FRAMING_LOST, request,
				response.getBytes(StandardCharsets.US_ASCII), 1024);

		assertThat(exchanged.upstream()).isEqualTo(request);
		assertThat(exchanged.relayed()).isEqualTo(response);
	}

	/** Pipelined past what may wait, with the responses ended: nothing is awaited. */
	@Test
	void aRequestWaitingForRoomEndsWithTheResponses() throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		String large = "GET / HTTP/1.1\r\nHost: a\r\nX-Padding: " + "x".repeat(60 * 1024) + "\r\n\r\n";
		UpstreamFilter filter = (request, headers) -> null;
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, filter)) {
			CompletableFuture<Void> writing = CompletableFuture.runAsync(() -> {
				try {
					pipe.sink().write((large + large + large).getBytes(StandardCharsets.US_ASCII));
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			});
			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received().equals(large));
			upstream.end();

			assertThat(pipe.source().read()).isEqualTo(-1);
			assertTimeoutPreemptively(TIMEOUT, () -> writing.join());
			// what still comes goes nowhere: the relay's next read ends as it should
			pipe.sink().write(large.getBytes(StandardCharsets.US_ASCII));
		}
		assertThat(upstream.received()).isEqualTo(large);
	}

	/** The upstream gets all it needs to answer what a next request waits for. */
	@Test
	void whatCameBeforeAWaitingRequestIsFlushed() throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		String first = "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nX-Padding: " + "x".repeat(32 * 1024)
				+ "\r\n\r\n";
		String next = "GET / HTTP/1.1\r\nHost: a\r\nX-Padding: " + "x".repeat(33 * 1024) + "\r\n\r\n";
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, (request, headers) -> null)) {
			pipe.sink().write(first.getBytes(StandardCharsets.US_ASCII));
			CompletableFuture<Void> writing = CompletableFuture.runAsync(() -> {
				try {
					pipe.sink().write(("z" + next).getBytes(StandardCharsets.US_ASCII));
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			});

			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received().equals(first + "z"));
			upstream.answer("HTTP/1.1 204 No Content\r\n\r\n");
			assertThat(pipe.source().readNBytes(NO_CONTENT.length)).isEqualTo(NO_CONTENT);
			assertTimeoutPreemptively(TIMEOUT, () -> writing.join());
		}
		assertThat(upstream.received()).isEqualTo(first + "z" + next);
	}

	/** One request at a time reaches a filter past what may wait, none held for it. */
	@Test
	void aRequestReachesTheFilterOnlyOnceThereIsRoomForIt() throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		AtomicInteger offered = new AtomicInteger();
		UpstreamFilter counting = (request, headers) -> {
			offered.incrementAndGet();
			return null;
		};
		String request = "GET / HTTP/1.1\r\nHost: a\r\n\r\n";
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, counting)) {
			CompletableFuture<Void> writing = CompletableFuture.runAsync(() -> {
				try {
					pipe.sink().write(request.repeat(6).getBytes(StandardCharsets.US_ASCII));
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			});

			Awaitility.await().atMost(TIMEOUT).until(() -> upstream.received().equals(request.repeat(4)));
			assertThat(offered).hasValue(4);
			upstream.answer("HTTP/1.1 204 No Content\r\n\r\n".repeat(2));
			assertTimeoutPreemptively(TIMEOUT, () -> {
				assertThat(pipe.source().readNBytes(NO_CONTENT.length * 2)).hasSize(NO_CONTENT.length * 2);
				writing.join();
			});
		}
		assertThat(upstream.received()).isEqualTo(request.repeat(6));
		assertThat(offered).hasValue(6);
	}

	/** What a rewriter holds is given back however its end goes. */
	@Test
	void aRewriterIsClosedWhenItsEndFails() {
		AtomicInteger closed = new AtomicInteger();
		BodyRewriter failing = new BodyRewriter() {

			@Override
			public byte[] onData(byte[] bytes, int offset, int length) {
				return new byte[0];
			}

			@Override
			public byte[] onEnd() {
				throw new IllegalStateException("its end failed");
			}

			@Override
			public void onClose() {
				closed.incrementAndGet();
			}

		};
		UpstreamFilter filter = (request, headers) -> new Exchange() {

			@Override
			public BodyHandler onRequestBody() {
				return failing;
			}

			@Override
			public BodyHandler onResponse(int status, Headers responseHeaders, boolean bodiless) {
				return failing;
			}

		};

		assertThatThrownBy(() -> FilterTests.exchange(filter,
				"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nz", NO_CONTENT, 1024))
			.isInstanceOf(IOException.class);
		assertThat(closed).hasValue(1);
		assertThatThrownBy(() -> FilterTests.exchange(filter, FilterTests.head("GET", "/"),
				"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz".getBytes(StandardCharsets.US_ASCII), 1024))
			.isInstanceOf(IOException.class);
		assertThat(closed).hasValue(2);
	}

	/** The relay took the 200 as declined: requests go on being read and filtered. */
	@Test
	void anUpgradeDeclinedStaysDeclinedWhateverFollows() throws Exception {
		FilterTests.BlockingUpstream upstream = new FilterTests.BlockingUpstream();
		String upgrade = "GET /ws HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n";
		String declined = "HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n";
		UpstreamFilter marking = (request, headers) -> {
			headers.set("x-filtered", "yes");
			return null;
		};
		try (UpstreamFilterPipe pipe = FilterTests.pipe(upstream, marking)) {
			pipe.sink().write(upgrade.getBytes(StandardCharsets.US_ASCII));
			upstream.answer(declined);
			assertThat(pipe.source().readNBytes(declined.length())).hasSize(declined.length());

			pipe.sink().write("GET /next HTTP/1.1\r\nHost: a\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
		}

		assertThat(upstream.received()).endsWith("GET /next HTTP/1.1\r\nHost: a\r\nx-filtered: yes\r\n\r\n");
	}

	/** h2 past a 101 would pass unseen: to the upstream it is an ordinary request. */
	@Test
	void anUpgradeToH2cDoesNotReachTheUpstream() throws Exception {
		String request = "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade, HTTP2-Settings, keep-alive\r\n"
				+ "Upgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA\r\nX-Kept: 1\r\n\r\n";
		String response = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz";

		FilterTests.Exchanged exchanged = FilterTests.exchange((r, headers) -> null, request,
				response.getBytes(StandardCharsets.US_ASCII), 1024);

		assertThat(exchanged.upstream())
			.isEqualTo("GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive\r\nX-Kept: 1\r\n\r\n");
		assertThat(exchanged.relayed()).isEqualTo(response);
	}

	/** Asks to answer once the request's body is read, the upstream answering too. */
	private static UpstreamFilter answeringAfterTheBody(LocalResponse local) {
		return (request, headers) -> request.bodyFollows() ? new Exchange() {

			private volatile boolean read;

			@Override
			public BodyHandler onRequestBody() {
				return new BodyObserver() {

					@Override
					public void onData(byte[] bytes, int offset, int length) {
						read = true;
					}

					@Override
					public void onEnd(boolean complete) {
					}

				};
			}

			@Override
			public @Nullable LocalResponse localResponse() {
				return this.read ? local : null;
			}

			@Override
			public @Nullable BodyHandler onResponse(int status, Headers responseHeaders, boolean bodiless) {
				return null;
			}

		} : null;
	}

	@Test
	void anAnswerInTheUpstreamsPlaceIsNotLostToTheUpstreamsAnswer() throws Exception {
		String first = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz";
		String second = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret";

		FilterTests.Exchanged exchanged = FilterTests.exchange(
				answeringAfterTheBody(new LocalResponse(403, List.of(), "no\n".getBytes(StandardCharsets.US_ASCII))),
				FilterTests.head("GET", "/") + "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nz",
				(first + second).getBytes(StandardCharsets.US_ASCII), 64 * 1024);

		assertThat(exchanged.relayed())
			.isEqualTo(first + "HTTP/1.1 403 Forbidden\r\nContent-Length: 3\r\nConnection: close\r\n\r\nno\n");
	}

	@Test
	void anAnswerWithoutABodyHasNoneToFrame() throws Exception {
		UpstreamFilter answering = (request, headers) -> new Exchange() {

			@Override
			public LocalResponse localResponse() {
				return new LocalResponse(204, List.of(), "dropped".getBytes(StandardCharsets.US_ASCII));
			}

			@Override
			public @Nullable BodyHandler onResponse(int status, Headers responseHeaders, boolean bodiless) {
				return null;
			}

		};

		assertThat(FilterTests.exchange(answering, FilterTests.head("GET", "/"), new byte[0], 64).relayed())
			.isEqualTo("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
	}

	private static BodyRewriter upperCase() {
		return new BodyRewriter() {

			@Override
			public byte[] onData(byte[] bytes, int offset, int length) {
				return new String(bytes, offset, length, StandardCharsets.ISO_8859_1).toUpperCase()
					.getBytes(StandardCharsets.ISO_8859_1);
			}

			@Override
			public byte[] onEnd() {
				return new byte[0];
			}

		};
	}

	private static final UpstreamFilter UPPER_CASE = (request, requestHeaders) -> new Exchange() {

		@Override
		public BodyHandler onRequestBody() {
			return upperCase();
		}

		@Override
		public BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			return upperCase();
		}

	};

	@Test
	void anHttp10ExchangeTakesNoChunks() throws Exception {
		// its client: no trailers, nor chunks, in what ends with the connection
		assertThat(FilterTests
			.exchange(UPPER_CASE, "GET / HTTP/1.0\r\nHost: a\r\n\r\n",
					"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nz\r\n0\r\nX-U: 2\r\n\r\n"
						.getBytes(StandardCharsets.US_ASCII),
					1024)
			.relayed()).isEqualTo("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nZ");
		// its upstream: a request rewritten in pieces could not be framed
		assertThatThrownBy(() -> FilterTests.exchange(UPPER_CASE,
				"POST / HTTP/1.0\r\nHost: a\r\nContent-Length: 1\r\n\r\nz", NO_CONTENT, 1024))
			.isInstanceOf(IOException.class);
	}

	/** A coding that is not the framing stays what the body is coded with. */
	@Test
	void aRewriteKeepsTheCodingsOfTheBody() throws Exception {
		String response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n1\r\nz\r\n0\r\n\r\n";

		assertThat(FilterTests
			.exchange(UPPER_CASE, FilterTests.head("GET", "/"), response.getBytes(StandardCharsets.US_ASCII), 1024)
			.relayed()).isEqualTo("HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n1\r\nZ\r\n0\r\n\r\n");
	}

	/** What a filter writes past what the output holds has to fit the budget. */
	@Test
	void aFiltersOutputChargesTheBudgetAsItIsWritten() {
		byte[] large = new byte[1024 * 1024];
		UpstreamFilter growing = (request, requestHeaders) -> (status, headers, bodiless) -> new BodyRewriter() {

			@Override
			public byte[] onData(byte[] bytes, int offset, int length) {
				return large;
			}

			@Override
			public byte[] onEnd() {
				return new byte[0];
			}

		};

		assertThatThrownBy(() -> FilterTests.exchange(growing, new TunnelBudget(1024), FilterTests.head("GET", "/"),
				"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz".getBytes(StandardCharsets.US_ASCII), 1024))
			.isInstanceOf(IOException.class);
	}

	/** A head grown past what the relay follows would part it from the pipe. */
	@Test
	void aHeadCannotGrowPastWhatTheRelayFollows() {
		String padding = "x".repeat(40 * 1024);
		UpstreamFilter growing = (request, requestHeaders) -> (status, headers, bodiless) -> {
			headers.set("x-a", padding);
			headers.set("x-b", padding);
			return null;
		};

		assertThatThrownBy(() -> FilterTests.exchange(growing, FilterTests.head("GET", "/"), NO_CONTENT, 1024))
			.isInstanceOf(IOException.class);
	}

	private static Filters filters(SimpleMeterRegistry meters, UpstreamFilterFactory... factories) {
		return Filters.builder().meters(meters).budget(TunnelBudget.unlimited()).factories(List.of(factories)).build();
	}

	private static UpstreamFilterFactory factory(String name, UpstreamFilter filter) {
		return new UpstreamFilterFactory() {

			@Override
			public String name() {
				return name;
			}

			@Override
			public UpstreamFilter create(Context context) {
				return filter;
			}

		};
	}

	private static FilterConfig use(String name) {
		return FilterConfig.builder().name(name).build();
	}

	/**
	 * A request passes the filters in order, its response in reverse, each body as left.
	 */
	@Test
	void filtersPassInOrderEachBodyThroughWhatThePreviousLeft() throws Exception {
		List<String> seen = new ArrayList<>();
		UpstreamFilter observing = (request, requestHeaders) -> {
			seen.add("request observer");
			return (status, headers, bodiless) -> {
				seen.add("response observer");
				return new BodyObserver() {

					@Override
					public void onData(byte[] bytes, int offset, int length) {
						seen.add(new String(bytes, offset, length, StandardCharsets.ISO_8859_1));
					}

					@Override
					public void onEnd(boolean complete) {
						seen.add("end " + complete);
					}

				};
			};
		};
		UpstreamFilter rewriting = (request, requestHeaders) -> {
			seen.add("request rewriter");
			return (status, headers, bodiless) -> {
				seen.add("response rewriter");
				return upperCase();
			};
		};
		SimpleMeterRegistry meters = new SimpleMeterRegistry();
		UpstreamFilter chain = filters(meters, factory("observing", observing), factory("rewriting", rewriting))
			.forRoute("route.test", List.of(use("observing"), use("rewriting")));

		String relayed = FilterTests
			.exchange(chain, FilterTests.head("GET", "/"),
					"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz".getBytes(StandardCharsets.US_ASCII), 1024)
			.relayed();

		// the observer is first on the request, so last on the response: it sees the
		// rewrite
		assertThat(seen).containsExactly("request observer", "request rewriter", "response rewriter",
				"response observer", "Z", "end true");
		assertThat(relayed).isEqualTo("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nZ\r\n0\r\n\r\n");
	}

	/**
	 * A filter failing closed: those that took part are told, and nothing is left held.
	 */
	@Test
	void aFilterFailingClosedLeavesTheOthersToldOfIt() {
		List<String> told = new ArrayList<>();
		UpstreamFilter telling = (request, requestHeaders) -> new Exchange() {

			@Override
			public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
				return new BodyObserver() {

					@Override
					public void onData(byte[] bytes, int offset, int length) {
					}

					@Override
					public void onEnd(boolean complete) {
						told.add("observer ended " + complete);
					}

				};
			}

			@Override
			public void abandon() {
				told.add("abandoned");
			}

		};
		UpstreamFilter failing = (request, requestHeaders) -> (status, headers, bodiless) -> {
			throw new IllegalStateException("it failed");
		};
		SimpleMeterRegistry meters = new SimpleMeterRegistry();
		// the response reaches the last filter first: the one before it is not offered it
		UpstreamFilter lastFails = filters(meters, factory("telling", telling), factory("failing", failing)).forRoute(
				"route.test", List.of(use("telling"), FilterConfig.builder().name("failing").failClosed(true).build()));
		assertThatThrownBy(() -> FilterTests.exchange(lastFails, FilterTests.head("GET", "/"), NO_CONTENT, 1024))
			.isInstanceOf(IOException.class);
		assertThat(told).containsExactly("abandoned");

		told.clear();
		UpstreamFilter firstFails = filters(meters, factory("telling", telling), factory("failing", failing)).forRoute(
				"route.test", List.of(FilterConfig.builder().name("failing").failClosed(true).build(), use("telling")));
		assertThatThrownBy(() -> FilterTests.exchange(firstFails, FilterTests.head("GET", "/"),
				"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nz".getBytes(StandardCharsets.US_ASCII), 1024))
			.isInstanceOf(IOException.class);
		assertThat(told).containsExactly("observer ended false");
	}

}
