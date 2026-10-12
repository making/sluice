package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class TestPortsTest {

	private static final int START = 1000;

	@Test
	void portsAreNeverHandedOutTwice() {
		List<Integer> ports = IntStream.range(0, 200).map(i -> TestPorts.freePort()).boxed().toList();

		assertThat(ports).doesNotHaveDuplicates();
	}

	@Test
	void portsStayBelowTheEphemeralRanges() {
		List<Integer> ports = IntStream.range(0, 100).map(i -> TestPorts.freePort()).boxed().toList();

		assertThat(ports).allSatisfy(port -> assertThat(port).isBetween(1024, 32767));
	}

	/**
	 * Another JVM walking the same ports from the same start gets none of those this one
	 * claimed through the shared lock file; with a lock file of its own it would have got
	 * the same first port.
	 */
	@Test
	void portsClaimedByAnotherJvmAreSkipped(@TempDir Path dir) throws Exception {
		Path lockFile = dir.resolve("ports.lock");
		Process holder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
				System.getProperty("java.class.path"), Holder.class.getName(), lockFile.toString())
			.redirectError(ProcessBuilder.Redirect.INHERIT)
			.start();
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(holder.getInputStream(), StandardCharsets.US_ASCII));
			List<Integer> held = new ArrayList<>();
			for (int i = 0; i < Holder.COUNT; i++) {
				held.add(Integer.parseInt(reader.readLine()));
			}

			List<Integer> ours = take(new TestPorts.Ports(lockFile, START), Holder.COUNT);

			assertThat(ours).doesNotContainAnyElementsOf(held);
			assertThat(take(new TestPorts.Ports(dir.resolve("other.lock"), START), 1))
				.as("precondition: the same walk without the shared claims")
				.containsExactly(held.getFirst());
		}
		finally {
			holder.getOutputStream().close();
			if (!holder.waitFor(10, TimeUnit.SECONDS)) {
				holder.destroyForcibly();
			}
		}
	}

	private static List<Integer> take(TestPorts.Ports ports, int count) {
		return IntStream.range(0, count).map(i -> ports.next()).boxed().toList();
	}

	/**
	 * Claims {@link #COUNT} ports from {@link #START} through the lock file given, prints
	 * them, and holds them until its stdin closes.
	 */
	public static final class Holder {

		static final int COUNT = 5;

		public static void main(String[] args) throws Exception {
			TestPorts.Ports ports = new TestPorts.Ports(Path.of(args[0]), START);
			for (int i = 0; i < COUNT; i++) {
				System.out.println(ports.next());
			}
			System.out.flush();
			System.in.read();
		}

	}

}
