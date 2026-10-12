package am.ik.sluice.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP ports for servers a test configures before they start.
 * <p>
 * A port probed with {@code new ServerSocket(0)} and closed goes back to the OS, which
 * may hand it to the next probe or to a server binding port 0. These ports come from
 * below the ephemeral ranges of Linux, macOS and Windows, so the OS never assigns them on
 * its own, and each is handed out at most once per JVM. The test JVMs of one build run at
 * once, so each takes its own slice of the range, told by the system properties
 * {@code sluice.test.fork} (1-based) and {@code sluice.test.forks}; a port one probed
 * free is never probed by another before its server binds it. The random start keeps
 * concurrent builds on one host apart.
 */
public final class TestPorts {

	private static final int FIRST = 20000;

	private static final int END = 32768;

	private static final Slice SLICE = Slice.of(Integer.getInteger("sluice.test.fork", 1),
			Integer.getInteger("sluice.test.forks", 1));

	private static final int START = ThreadLocalRandom.current().nextInt(SLICE.size());

	private static final AtomicInteger cursor = new AtomicInteger();

	private TestPorts() {
	}

	public static int freePort() {
		int n;
		while ((n = cursor.getAndIncrement()) < SLICE.size()) {
			int port = SLICE.first() + (START + n) % SLICE.size();
			if (isFree(port)) {
				return port;
			}
		}
		throw new IllegalStateException(
				"No free port left in " + SLICE.first() + "-" + (SLICE.first() + SLICE.size() - 1));
	}

	private static boolean isFree(int port) {
		try (ServerSocket socket = new ServerSocket(port)) {
			return true;
		}
		catch (IOException e) {
			return false;
		}
	}

	/** The ports of one test JVM: {@code size} of them from {@code first} on. */
	record Slice(int first, int size) {

		/** The slice of the {@code fork}th (1-based) of {@code forks} JVMs. */
		static Slice of(int fork, int forks) {
			if (forks < 1 || fork < 1 || fork > forks) {
				throw new IllegalArgumentException("fork " + fork + " of " + forks);
			}
			int size = (END - FIRST) / forks;
			return new Slice(FIRST + (fork - 1) * size, size);
		}

	}

}
