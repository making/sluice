package am.ik.sluice.it;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TestPortsTest {

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

	@Test
	void theForksOfABuildTakeAdjacentSlicesThatDoNotOverlap() {
		List<TestPorts.Slice> slices = IntStream.rangeClosed(1, 4)
			.mapToObj(fork -> TestPorts.Slice.of(fork, 4))
			.toList();

		assertThat(slices).containsExactly(new TestPorts.Slice(20000, 3192), new TestPorts.Slice(23192, 3192),
				new TestPorts.Slice(26384, 3192), new TestPorts.Slice(29576, 3192));
	}

	@Test
	void aSingleJvmTakesTheWholeRange() {
		assertThat(TestPorts.Slice.of(1, 1)).isEqualTo(new TestPorts.Slice(20000, 12768));
	}

	@Test
	void aForkOutsideTheCountIsRejected() {
		assertThatIllegalArgumentException().isThrownBy(() -> TestPorts.Slice.of(5, 4));
		assertThatIllegalArgumentException().isThrownBy(() -> TestPorts.Slice.of(0, 4));
	}

}
