package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.config.SluiceServerProperties.AccessLog.Type;
import am.ik.sluice.server.route.Router;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.LoggerFactory;

import am.ik.sluice.server.auth.Verdict;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The access log: the line kinds emitted follow the server-wide settings or, for a line
 * of a route, the route's; at most {@code maxRate} lines per kind within a period are
 * emitted, and the suppressed count is reported by a {@code type=ratelimit} line when the
 * period rolls over. A request refused for its route's credentials is a request line
 * naming the status and the reason.
 */
class AccessLoggerTest {

	private final AtomicLong now = new AtomicLong(1_000_000_000L);

	private ListAppender<ILoggingEvent> events;

	@BeforeEach
	void attachAppender() {
		this.events = new ListAppender<>();
		this.events.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(this.events);
	}

	@AfterEach
	void detachAppender() {
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(this.events);
	}

	private List<String> messages() {
		return this.events.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	/**
	 * A route's settings replace the server-wide ones for the lines of that route: a
	 * request line follows its request's route, a close / reject line its connection's
	 * route, an accept line the server-wide settings on the data port and its route's on
	 * a tcp route port.
	 */
	@Test
	void routeSettingsReplaceTheServerWideOnesForTheLinesOfTheRoute() throws Exception {
		AccessLogger accessLogger = new AccessLogger(SluiceServerProperties.builder()
			.accessLog(SluiceServerProperties.AccessLog.builder()
				.types(Set.of(Type.ACCEPT, Type.REQUEST))
				.rateLimit(SluiceServerProperties.AccessLog.RateLimit.builder().enabled(false).build())
				.build())
			.route(SluiceServerProperties.Route.builder()
				.accessLog(Map.of("quiet.local", new SluiceServerProperties.Route.AccessLog(false, null),
						" close.local ", new SluiceServerProperties.Route.AccessLog(null, Set.of(Type.CLOSE)),
						"tcp:9000", new SluiceServerProperties.Route.AccessLog(null, Set.of(Type.CONNECTION)),
						"tcp:9002", new SluiceServerProperties.Route.AccessLog(null, Set.of(Type.REJECT)), "*",
						new SluiceServerProperties.Route.AccessLog(true, Set.of())))
				.build())
			.build(), this.now::get);
		ConnectionHeadParser.Head.Request request = new ConnectionHeadParser.Head.Request("GET", "/", "1.1", false,
				null);
		try (ServerSocket serverSocket = loopbackServer();
				Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
				Socket accepted = serverSocket.accept()) {
			// head route quiet.local, its keep-alive successors rerouted
			AccessLogger.Connection quiet = accessLogger.accepted("data", socket);
			quiet.accept();
			quiet.route(route("quiet.local", 0));
			quiet.request(request);
			quiet.request(route("close.local", 0), request);
			quiet.request(route("other.local", 0), request);
			quiet.request(null, request);
			quiet.request(route("", 0), request);
			quiet.close();
			AccessLogger.Connection close = accessLogger.accepted("data", socket);
			close.accept();
			close.route(route("close.local", 0));
			close.request(request);
			close.refused(route("close.local", 0), request, 401, "auth-missing");
			close.close();
			// rejected once routed (an h2 relay that did not fit): neither its reject
			// line
			// nor a close line after it
			AccessLogger.Connection closeRejected = accessLogger.accepted("data", socket);
			closeRejected.route(route("close.local", 0));
			closeRejected.reject("h2-limit");
			closeRejected.close();
			AccessLogger.Connection unrouted = accessLogger.accepted("data", socket);
			unrouted.reject("head-timeout");
			AccessLogger.Connection tcp = accessLogger.accepted("tcp:9000", socket).routeKey("tcp:9000");
			tcp.accept();
			tcp.route(route("db.local", 9000));
			tcp.close();
			AccessLogger.Connection tcpRejected = accessLogger.accepted("tcp:9000", socket).routeKey("tcp:9000");
			tcpRejected.reject("limit");
			AccessLogger.Connection otherPort = accessLogger.accepted("tcp:9001", socket).routeKey("tcp:9001");
			otherPort.accept();
			otherPort.close();
			AccessLogger.Connection rejectOnly = accessLogger.accepted("tcp:9002", socket).routeKey("tcp:9002");
			rejectOnly.accept();
			rejectOnly.reject("address-limit");
		}
		assertThat(this.messages().stream().map(AccessLoggerTest::shape)).containsExactly(
				"type=conn id=1 event=accept listener=data", "type=req id=1 route=other.local", "type=req id=1 route=-",
				"type=conn id=2 event=accept listener=data",
				"type=conn id=2 event=close listener=data route=close.local",
				"type=conn id=5 event=accept listener=tcp:9000",
				"type=conn id=5 event=close listener=tcp:9000 route=db.local",
				"type=conn id=6 event=reject listener=tcp:9000", "type=conn id=7 event=accept listener=tcp:9001",
				"type=conn id=8 event=reject listener=tcp:9002");
		assertThat(accessLogger.describeRoute("close.local")).isEqualTo("close");
		assertThat(accessLogger.describeRoute("tcp:9000")).isEqualTo("accept, close, reject");
		assertThat(accessLogger.describeRoute("tcp:9002")).isEqualTo("reject");
		assertThat(accessLogger.describeRoute("quiet.local")).isEqualTo("off");
		assertThat(accessLogger.describeRoute("")).as("the catch-all").isEqualTo("off");
		assertThat(accessLogger.describeRoute("other.local")).isEmpty();
	}

	/**
	 * With the access log off server-wide, a route turning it on still gets its lines:
	 * the connections are tracked, their accept lines left out.
	 */
	@Test
	void aRouteEmitsItsLinesWithTheAccessLogOffServerWide() throws Exception {
		AccessLogger accessLogger = new AccessLogger(SluiceServerProperties.builder()
			.accessLog(SluiceServerProperties.AccessLog.builder().enabled(false).build())
			.route(SluiceServerProperties.Route.builder()
				.accessLog(Map.of("loud.local", new SluiceServerProperties.Route.AccessLog(true, null)))
				.build())
			.build(), this.now::get);
		ConnectionHeadParser.Head.Request request = new ConnectionHeadParser.Head.Request("GET", "/", "1.1", false,
				null);
		try (ServerSocket serverSocket = loopbackServer();
				Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
				Socket accepted = serverSocket.accept()) {
			AccessLogger.Connection other = accessLogger.accepted("data", socket);
			other.accept();
			other.route(route("other.local", 0));
			other.request(request);
			other.request(route("loud.local", 0), request);
			other.close();
			AccessLogger.Connection loud = accessLogger.accepted("data", socket);
			loud.accept();
			loud.route(route("loud.local", 0));
			loud.close();
			assertThat(new AccessLogger(SluiceServerProperties.builder()
				.accessLog(SluiceServerProperties.AccessLog.builder().enabled(false).build())
				.build()).accepted("data", socket))
				.as("nothing logged anywhere: no connection tracked")
				.isSameAs(new AccessLogger(SluiceServerProperties.builder()
					.accessLog(SluiceServerProperties.AccessLog.builder().types(Set.of()).build())
					.build()).accepted("data", socket))
				.isNotSameAs(other);
		}
		assertThat(this.messages().stream().map(AccessLoggerTest::shape)).containsExactly(
				"type=req id=1 route=loud.local", "type=conn id=2 event=close listener=data route=loud.local");
	}

	/** The fields of a line naming its kind, connection, listener and route. */
	private static String shape(String line) {
		return Arrays.stream(line.split(" "))
			.filter(field -> field.matches("(type|id|event|listener|route)=.*"))
			.collect(Collectors.joining(" "));
	}

	private static Router.Route route(String domain, int listenPort) {
		return Router.Route.builder()
			.clientId("client")
			.domain(domain)
			.address("127.0.0.1:1")
			.listenPort(listenPort)
			.build();
	}

	@Test
	void limitsLinesPerKindAndReportsSuppressedCountOnRollover() {
		AccessLogger.RateLimiter limiter = AccessLogger.RateLimiter.builder()
			.kind("conn-accept")
			.maxRate(2)
			.period(Duration.ofMillis(100))
			.clock(this.now::get)
			.build();
		assertThat(limiter.tryAcquire()).isTrue();
		assertThat(limiter.tryAcquire()).isTrue();
		// over the rate limit within the same period
		assertThat(limiter.tryAcquire()).isFalse();
		assertThat(limiter.tryAcquire()).isFalse();
		assertThat(this.messages()).isEmpty();
		// the period rolls over: one summary line, then the window resets
		this.now.addAndGet(Duration.ofMillis(150).toNanos());
		assertThat(limiter.tryAcquire()).isTrue();
		List<String> messages = this.messages();
		assertThat(messages).hasSize(1);
		assertThat(messages.get(0)).isEqualTo("type=ratelimit kind=conn-accept suppressed=2 periodMs=100");
		assertThat(limiter.tryAcquire()).isTrue();
		assertThat(limiter.tryAcquire()).isFalse();
		assertThat(this.messages()).hasSize(1);
	}

	@Test
	void rateLimitsAccessLoggerLines() throws Exception {
		SluiceServerProperties properties = SluiceServerProperties.builder()
			.accessLog(SluiceServerProperties.AccessLog.builder()
				.rateLimit(SluiceServerProperties.AccessLog.RateLimit.builder()
					.maxRate(1)
					.period(Duration.ofMillis(50))
					.build())
				.build())
			.build();
		AccessLogger accessLogger = new AccessLogger(properties, this.now::get);
		try (ServerSocket serverSocket = loopbackServer();
				Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
				Socket accepted = serverSocket.accept()) {
			AccessLogger.Connection connection = accessLogger.accepted("test", socket);
			connection.accept();
			connection.accept();
			connection.close();
			this.now.addAndGet(Duration.ofMillis(100).toNanos());
			connection = accessLogger.accepted("test", socket);
			connection.accept();
			List<String> messages = this.messages();
			// first accept passes, second is suppressed, rollover reports it
			assertThat(messages).contains("type=ratelimit kind=conn-accept suppressed=1 periodMs=50");
			assertThat(countOf(messages, "event=accept")).isEqualTo(2);
		}
	}

	@Test
	void credentialRefusalsAreRequestLinesWithTheirStatusAndReason() throws Exception {
		SluiceServerProperties properties = SluiceServerProperties.builder()
			.accessLog(SluiceServerProperties.AccessLog.builder()
				.rateLimit(SluiceServerProperties.AccessLog.RateLimit.builder()
					.maxRate(3)
					.period(Duration.ofMillis(50))
					.build())
				.build())
			.build();
		AccessLogger accessLogger = new AccessLogger(properties, this.now::get);
		try (ServerSocket serverSocket = loopbackServer();
				Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
				Socket accepted = serverSocket.accept()) {
			AccessLogger.Connection connection = accessLogger.accepted("test", socket);
			ConnectionHeadParser.Head.Request request = new ConnectionHeadParser.Head.Request("GET", "/x", "1.1", false,
					"agent/1 \"q\" \\ \n\u007f\u009b");
			Router.Route route = route("a.local", 0);
			connection.refused(route, request, new Verdict.Unauthorized("missing", List.of()));
			connection.refused(route, request, new Verdict.Busy(Duration.ofSeconds(1)));
			connection.refused(route, request, new Verdict.Unavailable(Duration.ofSeconds(10)));
			connection.refused(route, request, new Verdict.Unauthorized("invalid", List.of()));
			assertThat(this.messages()).as("the fourth past the request lines' rate")
				.containsExactly("type=req id=1 route=a.local method=GET path=/x http=1.1 remote=" + remoteOf(socket)
						+ " status=401 reason=auth-missing userAgent=\"agent/1 \\\"q\\\" \\\\ \\u000a\\u007f\\u009b\"",
						"type=req id=1 route=a.local method=GET path=/x http=1.1 remote=" + remoteOf(socket)
								+ " status=503 reason=auth-busy userAgent=\"agent/1 \\\"q\\\" \\\\ \\u000a\\u007f\\u009b\"",
						"type=req id=1 route=a.local method=GET path=/x http=1.1 remote=" + remoteOf(socket)
								+ " status=503 reason=auth-unavailable userAgent=\"agent/1 \\\"q\\\" \\\\ \\u000a\\u007f\\u009b\"");
		}
	}

	private static String remoteOf(Socket socket) {
		java.net.InetSocketAddress remote = (java.net.InetSocketAddress) socket.getRemoteSocketAddress();
		return remote.getHostString() + ":" + remote.getPort();
	}

	@Test
	void rateLimitsRejectLinesAndOmitsTheCloseLineOfARejectedConnection() throws Exception {
		SluiceServerProperties properties = SluiceServerProperties.builder()
			.accessLog(SluiceServerProperties.AccessLog.builder()
				.rateLimit(SluiceServerProperties.AccessLog.RateLimit.builder()
					.maxRate(1)
					.period(Duration.ofMillis(50))
					.build())
				.build())
			.build();
		AccessLogger accessLogger = new AccessLogger(properties, this.now::get);
		try (ServerSocket serverSocket = loopbackServer();
				Socket socket = new Socket("127.0.0.1", serverSocket.getLocalPort());
				Socket accepted = serverSocket.accept()) {
			AccessLogger.Connection first = accessLogger.accepted("data", socket);
			first.reject("limit");
			first.close();
			accessLogger.accepted("data", socket).reject("limit");
			accessLogger.accepted("data", socket).reject("limit");
			this.now.addAndGet(Duration.ofMillis(100).toNanos());
			accessLogger.accepted("data", socket).reject("head-timeout");
			List<String> messages = this.messages()
				.stream()
				.map(m -> m.replaceAll("durationMs=\\d+", "durationMs=N"))
				.toList();
			String remote = "127.0.0.1:" + serverSocket.getLocalPort();
			assertThat(String.join("\n", messages)).isEqualToNormalizingWhitespace("""
					type=conn id=1 event=reject listener=data remote=%s reason=limit durationMs=N
					type=ratelimit kind=conn-reject suppressed=2 periodMs=50
					type=conn id=4 event=reject listener=data remote=%s reason=head-timeout durationMs=N
					""".formatted(remote, remote));
		}
	}

	@Test
	void noRateLimitWhenDisabled() {
		AccessLogger.RateLimiter limiter = AccessLogger.RateLimiter.builder()
			.kind("request")
			.maxRate(Integer.MAX_VALUE)
			.period(Duration.ofSeconds(10))
			.clock(this.now::get)
			.build();
		for (int i = 0; i < 100; i++) {
			assertThat(limiter.tryAcquire()).isTrue();
		}
		assertThat(this.messages()).isEmpty();
	}

	private static long countOf(List<String> messages, String fragment) {
		return messages.stream().filter(message -> message.contains(fragment)).count();
	}

	/**
	 * Listens on the loopback address the tests dial, so a listener of another process on
	 * the same port number cannot take the connection, and bounds {@code accept} so a
	 * stray connection fails the test instead of hanging it.
	 */
	private static ServerSocket loopbackServer() throws IOException {
		ServerSocket server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
		server.setSoTimeout(5_000);
		return server;
	}

}
