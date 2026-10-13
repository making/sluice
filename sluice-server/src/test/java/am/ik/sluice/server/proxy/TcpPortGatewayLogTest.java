package am.ik.sluice.server.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.springframework.core.task.TaskExecutor;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.LogThrottle;
import am.ik.sluice.server.tunnel.SessionRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tcp route port lines under churn: a token holder advertising listen ports and
 * ending its streams in a loop logs only the permits of a period at info / warn, the rest
 * at debug; the next line logged reports how many were demoted.
 */
class TcpPortGatewayLogTest {

	private static final Duration PERIOD = Duration.ofSeconds(10);

	private static final int PERMITS = 2;

	private final AtomicLong now = new AtomicLong(42_000_000_000L);

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private final TaskExecutor directExecutor = Runnable::run;

	private final SessionRegistry sessions = new SessionRegistry(SluiceServerProperties.builder().build());

	private final TcpPortGateway gateway = new TcpPortGateway(new Router(), this.sessions,
			SluiceServerProperties.builder().dataPort(0).tcpPortRange("19100-19110").build(),
			new AccessControl(SluiceServerProperties.builder().build()), this.directExecutor, this.meters,
			new AccessLogger(SluiceServerProperties.builder().build()),
			new ConnectionLimits(SluiceServerProperties.builder().build(), this.meters),
			new LogThrottle(PERIOD, PERMITS, this.now::get));

	private final ListAppender<ILoggingEvent> events = new ListAppender<>();

	private final Logger logger = (Logger) LoggerFactory.getLogger(TcpPortGateway.class);

	private @Nullable Level level;

	@BeforeEach
	void attachAppender() {
		this.level = this.logger.getLevel();
		this.logger.setLevel(Level.DEBUG);
		this.events.start();
		this.logger.addAppender(this.events);
	}

	@AfterEach
	void detachAppender() {
		this.logger.detachAppender(this.events);
		this.logger.setLevel(this.level);
		this.gateway.close();
	}

	private List<String> messages(Level atLeast) {
		return this.events.list.stream()
			.filter(event -> event.getLevel().isGreaterOrEqual(atLeast))
			.map(ILoggingEvent::getFormattedMessage)
			.toList();
	}

	private List<String> debugMessages() {
		return this.events.list.stream()
			.filter(event -> event.getLevel() == Level.DEBUG)
			.map(ILoggingEvent::getFormattedMessage)
			.toList();
	}

	@Test
	void churningBindsAndUnbindsLogThePermitsOfAPeriodAtInfo() {
		for (int i = 0; i < 6; i++) {
			assertThat(this.gateway.reconcile("c" + i, Set.of(19100))).isEmpty();
			assertThat(this.gateway.reconcile("c" + i, Set.of())).isEmpty();
		}

		assertThat(messages(Level.INFO)).as("only the first lines of the period")
			.containsExactly("bound tcp route port 19100 for client c0", "unbound tcp route port 19100 (client c0)");
		assertThat(debugMessages()).as("the others demoted, not dropped")
			.hasSize(10)
			.contains("bound tcp route port 19100 for client c5", "unbound tcp route port 19100 (client c5)");
	}

	@Test
	void rejectionsAreBoundedAndTheFirstStaysAtWarn() {
		for (int i = 0; i < 5; i++) {
			assertThat(this.gateway.reconcile("c", Set.of(19200 + i))).containsExactly(19200 + i);
		}

		assertThat(messages(Level.WARN)).hasSize(PERMITS)
			.allMatch(message -> message.contains("is outside tcp-port-range"));
		assertThat(debugMessages()).hasSize(3);
	}

	@Test
	void theNextLineLoggedReportsHowManyWereDemoted() {
		for (int i = 0; i < 5; i++) {
			this.gateway.reconcile("c" + i, Set.of(19100));
			this.gateway.reconcile("c" + i, Set.of());
		}
		this.events.list.clear();
		this.now.addAndGet(PERIOD.toNanos());

		this.gateway.reconcile("d", Set.of(19101));

		assertThat(messages(Level.INFO)).containsExactly(
				"bound tcp route port 19101 for client d (8 more tcp route port lines logged at debug since the last report)");
	}

}
