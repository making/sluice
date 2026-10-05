package am.ik.sluice.server.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TcpPortRangeTest {

	@Test
	void blankAllowsEveryPort() {
		TcpPortRange range = TcpPortRange.parse("  ");
		assertThat(range.contains(1)).isTrue();
		assertThat(range.contains(65535)).isTrue();
	}

	@Test
	void nullAllowsEveryPort() {
		assertThat(TcpPortRange.parse(null).contains(8000)).isTrue();
	}

	@Test
	void singlePort() {
		TcpPortRange range = TcpPortRange.parse("8080");
		assertThat(range.contains(8080)).isTrue();
		assertThat(range.contains(8081)).isFalse();
	}

	@Test
	void rangeIsInclusive() {
		TcpPortRange range = TcpPortRange.parse("9000-9010");
		assertThat(range.contains(9000)).isTrue();
		assertThat(range.contains(9010)).isTrue();
		assertThat(range.contains(8999)).isFalse();
		assertThat(range.contains(9011)).isFalse();
	}

	@Test
	void mixedEntries() {
		TcpPortRange range = TcpPortRange.parse("9000-9010, 8080,7000-7002");
		assertThat(range.contains(8080)).isTrue();
		assertThat(range.contains(7001)).isTrue();
		assertThat(range.contains(9005)).isTrue();
		assertThat(range.contains(9011)).isFalse();
		assertThat(range.contains(7003)).isFalse();
	}

	@Test
	void malformedEntriesAreIgnored() {
		TcpPortRange range = TcpPortRange.parse("abc,9010-,8080");
		assertThat(range.contains(8080)).isTrue();
		assertThat(range.contains(1)).isFalse();
	}

	@Test
	void unparseableValueFallsBackToAnyPort() {
		TcpPortRange range = TcpPortRange.parse("abc,def");
		assertThat(range.contains(1)).isTrue();
	}

	@Test
	void reversedRangeMatchesNothingButParses() {
		TcpPortRange range = TcpPortRange.parse("9010-9000");
		assertThat(range.contains(9005)).isFalse();
	}

}
