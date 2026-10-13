package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Upstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-stream log lines under churn: the streams of a period beyond the stream log's
 * permits log their lines at debug, the first ones of the period at info (warn), each
 * stream all or nothing; the next stream logged reports how many were demoted. An
 * advertise repeating the stream's routes logs at debug; one changing them takes a permit
 * of its own.
 */
@Timeout(30)
class TunnelSessionStreamLogTest {

	private static final Duration PERIOD = Duration.ofSeconds(10);

	private static final int PERMITS = 2;

	private final AtomicLong now = new AtomicLong(42_000_000_000L);

	private final LogThrottle streamLog = new LogThrottle(PERIOD, PERMITS, this.now::get);

	private final SessionRegistry registry = new SessionRegistry(SluiceServerProperties.builder().build());

	private final Router router = new Router();

	private final ListAppender<ILoggingEvent> events = new ListAppender<>();

	private final Logger logger = (Logger) LoggerFactory.getLogger(TunnelSession.class);

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
	}

	private TunnelSession session(String clientId, RecordingStream stream) {
		return TunnelSession.builder()
			.clientId(clientId)
			.sourceAddress("10.0.0.1")
			.router(this.router)
			.sender(new SessionSender(stream))
			.registry(this.registry)
			.tcpRoutes((id, listenPorts) -> Set.of())
			.streamLog(this.streamLog)
			.build();
	}

	private static Frame advertise(int window, String... hosts) {
		Advertise.Builder advertise = Advertise.newBuilder().setConnectionWindow(window);
		for (String host : hosts) {
			advertise.addUpstreams(Upstream.newBuilder().setHost(host).setTargetUrl("http://127.0.0.1:9"));
		}
		return Frame.newBuilder().setAdvertise(advertise).build();
	}

	/** A stream that opens, advertises one upstream, and ends. */
	private void churn(String clientId) throws InterruptedException {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = session(clientId, stream);
		assertThat(session.start()).as("precondition: admitted").isEmpty();
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW, clientId + ".example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		// waits for the advertise, whose lines follow the ack
		session.close();
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

	private static List<String> streamLines(String clientId) {
		return List.of("tunnel stream established for client " + clientId,
				"client " + clientId + " runs " + PeerBuilds.UNKNOWN,
				"client " + clientId + " advertised 1 upstream(s), 0 listen port(s) rejected",
				"client " + clientId + " upstream: host=[" + clientId
						+ ".example.com] target=http://127.0.0.1:9 rewrite-host=false tls-passthrough=false "
						+ "listen-port=0 verbatim-idle-timeout=server default",
				"closing session for client " + clientId);
	}

	@Test
	void churningStreamsLogTheFirstOfAPeriodAndDemoteTheRest() throws Exception {
		for (int i = 0; i < 6; i++) {
			churn("s" + i);
		}

		assertThat(messages(Level.INFO)).as("the first %d streams of the period, each whole", PERMITS)
			.containsExactlyElementsOf(concat(streamLines("s0"), streamLines("s1")));
		assertThat(debugMessages()).as("the others demoted, not dropped")
			.containsAll(streamLines("s2"))
			.containsAll(streamLines("s5"));

		this.now.addAndGet(PERIOD.toNanos());
		this.events.list.clear();
		churn("s6");

		List<String> expected = new ArrayList<>(streamLines("s6"));
		expected.set(0, "tunnel stream established for client s6 "
				+ "(4 more tunnel streams or route changes logged at debug since the last report)");
		assertThat(messages(Level.INFO)).as("a new period: logged again, reporting the demoted")
			.containsExactlyElementsOf(expected);
	}

	@Test
	void aRepeatedAdvertiseLogsAtDebugAndAChangedOneTakesAPermit() throws Exception {
		RecordingStream stream = new RecordingStream();
		TunnelSession session = session("r", stream);
		assertThat(session.start()).isEmpty();
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW, "a.example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 1);
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW, "a.example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 2);
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW, "b.example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 3);
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW, "c.example.com"));
		stream.await(Frame.BodyCase.ADVERTISE_ACK, 4);
		session.close();

		assertThat(messages(Level.INFO).stream().filter(line -> line.startsWith("client r advertised")))
			.as("the first advertise and the first change; the repeat and the change past the permits at debug")
			.hasSize(2);
		assertThat(messages(Level.INFO).stream().filter(line -> line.startsWith("client r upstream: ")))
			.containsExactly(
					"client r upstream: host=[a.example.com] target=http://127.0.0.1:9 rewrite-host=false "
							+ "tls-passthrough=false listen-port=0 verbatim-idle-timeout=server default",
					"client r upstream: host=[b.example.com] target=http://127.0.0.1:9 rewrite-host=false "
							+ "tls-passthrough=false listen-port=0 verbatim-idle-timeout=server default");
		assertThat(debugMessages().stream().filter(line -> line.startsWith("client r advertised"))).hasSize(2);
	}

	@Test
	void theWarningsOfADemotedStreamAreDemotedToo() throws Exception {
		churn("s0");
		churn("s1");
		this.events.list.clear();
		RecordingStream stream = new RecordingStream();
		TunnelSession windowless = session("w", stream);
		assertThat(windowless.start()).isEmpty();

		windowless.handle(advertise(0));
		stream.awaitFailure();

		assertThat(messages(Level.INFO)).isEmpty();
		assertThat(debugMessages()).anySatisfy(line -> assertThat(line).isEqualTo("client w runs " + PeerBuilds.UNKNOWN
				+ "; rejecting its tunnel stream: " + TunnelSession.WINDOW_REQUIRED));
	}

	private static List<String> concat(List<String> first, List<String> second) {
		return Stream.concat(first.stream(), second.stream()).toList();
	}

}
