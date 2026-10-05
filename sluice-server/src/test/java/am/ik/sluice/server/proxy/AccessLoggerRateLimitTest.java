package am.ik.sluice.server.proxy;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rate limiting of the access log: at most {@code maxRate} lines per kind within a period
 * are emitted, and the suppressed count is reported by a {@code type=ratelimit} line when
 * the period rolls over.
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
		try (ServerSocket serverSocket = new ServerSocket(0);
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

}
