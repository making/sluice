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
 * its own, and each is handed out at most once per JVM. The random start keeps concurrent
 * builds on one host apart.
 */
final class TestPorts {

	private static final int FIRST = 20000;

	private static final int SIZE = 32768 - FIRST;

	private static final int START = ThreadLocalRandom.current().nextInt(SIZE);

	private static final AtomicInteger cursor = new AtomicInteger();

	private TestPorts() {
	}

	static int freePort() {
		int n;
		while ((n = cursor.getAndIncrement()) < SIZE) {
			int port = FIRST + (START + n) % SIZE;
			if (isFree(port)) {
				return port;
			}
		}
		throw new IllegalStateException("No free port left in " + FIRST + "-" + (FIRST + SIZE - 1));
	}

	private static boolean isFree(int port) {
		try (ServerSocket socket = new ServerSocket(port)) {
			return true;
		}
		catch (IOException e) {
			return false;
		}
	}

}
