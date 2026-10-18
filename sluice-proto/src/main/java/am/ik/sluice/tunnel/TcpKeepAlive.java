package am.ik.sluice.tunnel;

import java.io.IOException;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import jdk.net.ExtendedSocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP keepalive on a relayed socket: once the connection carried nothing for the idle
 * time, the kernel probes the peer every {@link #PROBE_INTERVAL}, and after
 * {@link #PROBE_COUNT} probes unanswered, or one answered with a reset, fails the
 * socket's reads and writes -- which ends the relay blocked on them.
 * <p>
 * A relay waits on its peer for as long as the connection lives, half-closed included,
 * and nothing else tells it of a peer gone without its FIN or reset arriving: a host or
 * pod removed, a NAT or conntrack entry dropped while the connection idled. The kernel
 * reclaims a half-closed socket only once the application closed it, so without the
 * probes such a socket stays open, and holds its slot, for good.
 */
public final class TcpKeepAlive {

	private static final Logger log = LoggerFactory.getLogger(TcpKeepAlive.class);

	/** How often an idle connection's peer is probed. */
	public static final Duration PROBE_INTERVAL = Duration.ofSeconds(10);

	/** The probes left unanswered before the connection fails. */
	public static final int PROBE_COUNT = 3;

	/** The idle time the options take at most, Linux's bound. */
	static final int MAX_IDLE_SECONDS = 32767;

	private static final AtomicBoolean unsupportedLogged = new AtomicBoolean();

	private TcpKeepAlive() {
	}

	/**
	 * Probes the peer of {@code socket} once it idled for {@code idle}, rounded up to a
	 * second; zero leaves keepalive off. Where the platform cannot set the idle time, the
	 * probes start after the system's default idle time instead, logged once.
	 */
	public static void enable(Socket socket, Duration idle) throws IOException {
		if (idle.isZero() || idle.isNegative()) {
			return;
		}
		socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
		if (!socket.supportedOptions().contains(ExtendedSocketOptions.TCP_KEEPIDLE)) {
			if (unsupportedLogged.compareAndSet(false, true)) {
				log.warn("TCP keepalive idle time is not supported here: probes start after the system default");
			}
			return;
		}
		socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, seconds(idle));
		socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, (int) PROBE_INTERVAL.toSeconds());
		socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, PROBE_COUNT);
	}

	private static int seconds(Duration idle) {
		long seconds = idle.toSeconds() + (idle.toNanosPart() == 0 ? 0 : 1);
		return Math.clamp(seconds, 1, MAX_IDLE_SECONDS);
	}

}
