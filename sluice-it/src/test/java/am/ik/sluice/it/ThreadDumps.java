package am.ik.sluice.it;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * Thread dumps that include virtual threads, unlike {@link Thread#getAllStackTraces()}:
 * tests pin where a relay is parked with them.
 */
final class ThreadDumps {

	private ThreadDumps() {
	}

	/** Whether any thread of this JVM currently has the method on its stack. */
	static boolean anyThreadIn(String className, String methodName) throws IOException {
		Path dump = Files.createTempDirectory("sluice-thread-dump").resolve("threads.txt");
		try {
			// dumpThreads refuses an existing path, hence the fresh directory per call
			ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
				.dumpThreads(dump.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
			return Files.readString(dump).contains(className + "." + methodName + "(");
		}
		finally {
			Files.deleteIfExists(dump);
			Files.deleteIfExists(dump.getParent());
		}
	}

	/** Whether a writer is parked waiting for per-connection send credit. */
	static boolean anyThreadWaitingForCredit() throws IOException {
		return anyThreadIn("am.ik.sluice.tunnel.VirtualConnection", "awaitCredit");
	}

}
