package am.ik.sluice.it;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestPortsTest {

	@Test
	void portsAreNeverHandedOutTwice() {
		List<Integer> ports = IntStream.range(0, 1000).map(i -> TestPorts.freePort()).boxed().toList();

		assertThat(ports).doesNotHaveDuplicates();
	}

	@Test
	void portsStayBelowTheEphemeralRanges() {
		List<Integer> ports = IntStream.range(0, 100).map(i -> TestPorts.freePort()).boxed().toList();

		assertThat(ports).allSatisfy(port -> assertThat(port).isBetween(1024, 32767));
	}

}
