package am.ik.sluice.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP ports for servers a test configures before they start.
 * <p>
 * A port probed with {@code new ServerSocket(0)} and closed goes back to the OS, which
 * may hand it to the next probe or to a server binding port 0. These ports come from
 * below the ephemeral ranges of Linux, macOS and Windows, so the OS never assigns them on
 * its own. Each is claimed for the JVM's lifetime with a lock on its byte of a file
 * shared by every JVM of the user on the host -- the test JVMs of one build run at once,
 * and so may other builds -- so no other JVM hands it out, though its server binds it
 * only later. The random start keeps the JVMs from contending for the same ports.
 */
public final class TestPorts {

	private static final int FIRST = 20000;

	private static final int SIZE = 32768 - FIRST;

	private static final Ports PORTS = new Ports(
			Path.of(System.getProperty("java.io.tmpdir"), "sluice-test-ports.lock"),
			ThreadLocalRandom.current().nextInt(SIZE));

	private TestPorts() {
	}

	public static int freePort() {
		return PORTS.next();
	}

	/**
	 * The ports claimed through one lock file, walked from {@code start} on; the lock
	 * channel stays open for the JVM's lifetime, as closing it would release every claim.
	 */
	static final class Ports {

		private final FileChannel claims;

		/** The locks taken: the JVM's table of them refers to them weakly. */
		private final List<FileLock> locks = new CopyOnWriteArrayList<>();

		private final int start;

		private final AtomicInteger cursor = new AtomicInteger();

		Ports(Path lockFile, int start) {
			try {
				this.claims = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			this.start = start;
		}

		int next() {
			int n;
			while ((n = this.cursor.getAndIncrement()) < SIZE) {
				int port = FIRST + (this.start + n) % SIZE;
				if (claim(port) && isFree(port)) {
					return port;
				}
			}
			throw new IllegalStateException("No free port left in " + FIRST + "-" + (FIRST + SIZE - 1));
		}

		/** Locks the port's byte for good; false when another holds it. */
		private boolean claim(int port) {
			try {
				FileLock lock = this.claims.tryLock(port, 1, false);
				if (lock == null) {
					return false;
				}
				this.locks.add(lock);
				return true;
			}
			catch (OverlappingFileLockException e) {
				// held by this JVM through another channel
				return false;
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
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

}
