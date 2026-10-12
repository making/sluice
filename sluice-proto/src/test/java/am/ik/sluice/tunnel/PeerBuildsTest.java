package am.ik.sluice.tunnel;

import org.junit.jupiter.api.Test;

import am.ik.sluice.v1.proto.PeerBuild;

import static org.assertj.core.api.Assertions.assertThat;

class PeerBuildsTest {

	/** RIGHT-TO-LEFT OVERRIDE, built at runtime: the literal is a compile error. */
	private static final String RTL_OVERRIDE = new String(Character.toChars(0x202E));

	@Test
	void sanitizeReplacesControlAndFormatCharacters() {
		PeerBuild build = PeerBuild.newBuilder()
			.setImplementation("sluice-client\nforged log line")
			.setVersion("1.0" + RTL_OVERRIDE + "gpj.exe")
			.setCommit("abc\u0000def")
			.setDirty(true)
			.setBuildTime("2026-10-12T01:04:33Z\u2028x")
			.build();
		assertThat(PeerBuilds.sanitize(build)).isEqualTo(PeerBuild.newBuilder()
			.setImplementation("sluice-client?forged log line")
			.setVersion("1.0?gpj.exe")
			.setCommit("abc?def")
			.setDirty(true)
			.setBuildTime("2026-10-12T01:04:33Z?x")
			.build());
	}

	@Test
	void sanitizeCutsLongValuesAtTheLimit() {
		String limit = "v".repeat(PeerBuilds.MAX_FIELD_LENGTH);
		PeerBuild build = PeerBuild.newBuilder().setVersion(limit).setCommit(limit + "x").build();
		PeerBuild sanitized = PeerBuilds.sanitize(build);
		assertThat(sanitized.getVersion()).isEqualTo(limit);
		assertThat(sanitized.getCommit()).isEqualTo(limit + "...");
	}

	@Test
	void sanitizeCountsCodePointsNotChars() {
		String emoji = "\uD83D\uDE00";
		String value = emoji.repeat(PeerBuilds.MAX_FIELD_LENGTH + 1);
		assertThat(PeerBuilds.sanitize(PeerBuild.newBuilder().setVersion(value).build()).getVersion())
			.isEqualTo(emoji.repeat(PeerBuilds.MAX_FIELD_LENGTH) + "...");
	}

	@Test
	void shortCommitAbbreviatesOnlyHexIds() {
		assertThat(PeerBuilds.shortCommit("73124728ab1e627fd8e02011f057709c0dc1fe59")).isEqualTo("7312472");
		assertThat(PeerBuilds.shortCommit("unknown-build")).isEqualTo("unknown-build");
		assertThat(PeerBuilds.shortCommit("abc")).isEqualTo("abc");
	}

	@Test
	void describeNamesEveryKnownPart() {
		PeerBuild build = PeerBuild.newBuilder()
			.setImplementation("sluice-server")
			.setVersion("0.0.1-SNAPSHOT")
			.setCommit("73124728ab1e627fd8e02011f057709c0dc1fe59")
			.setDirty(true)
			.setBuildTime("2026-10-12T01:04:33Z")
			.build();
		assertThat(PeerBuilds.describe(build))
			.isEqualTo("sluice-server 0.0.1-SNAPSHOT (7312472, dirty, built 2026-10-12T01:04:33Z)");
	}

	@Test
	void describeReadsEmptyFieldsAsUnknown() {
		assertThat(PeerBuilds.describe(PeerBuild.getDefaultInstance()))
			.isEqualTo("unknown implementation version unknown (commit unknown)");
	}

}
