package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.server.config.SluiceServerProperties;
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
 * Rate limiting of the access log: at most {@code maxRate} lines per kind within a period
 * are emitted, and the suppressed count is reported by a {@code type=ratelimit} line when
 * the period rolls over. A request refused for its route's credentials is a request line
 * naming the status and the reason.
 */
class AccessLoggerRateLimitTest {

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
			connection.refused("a.local", request, new Verdict.Unauthorized("missing", List.of()));
			connection.refused("a.local", request, new Verdict.Busy(Duration.ofSeconds(1)));
			connection.refused("a.local", request, new Verdict.Unavailable(Duration.ofSeconds(10)));
			connection.refused("a.local", request, new Verdict.Unauthorized("invalid", List.of()));
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
