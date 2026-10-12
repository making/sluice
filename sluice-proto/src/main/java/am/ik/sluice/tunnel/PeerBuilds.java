package am.ik.sluice.tunnel;

import java.util.ArrayList;
import java.util.List;

import am.ik.sluice.v1.proto.PeerBuild;

/**
 * Helpers for the {@link PeerBuild} a peer announces: bounding what another peer sent
 * before it is logged or shown, and a one-line description for the logs.
 */
public final class PeerBuilds {

	/** Longest value kept from each string field of a received build, in code points. */
	public static final int MAX_FIELD_LENGTH = 64;

	/** Length of a shortened commit id (git's default abbreviation). */
	public static final int SHORT_COMMIT_LENGTH = 7;

	/** How a peer that sent no build (it predates the field) is described. */
	public static final String UNKNOWN = "unknown (older build)";

	private PeerBuilds() {
	}

	/**
	 * A copy of a build received from another peer whose strings are safe to log and to
	 * show: see {@link #bound(String)}.
	 */
	public static PeerBuild sanitize(PeerBuild build) {
		return PeerBuild.newBuilder()
			.setImplementation(bound(build.getImplementation()))
			.setVersion(bound(build.getVersion()))
			.setCommit(bound(build.getCommit()))
			.setDirty(build.getDirty())
			.setBuildTime(bound(build.getBuildTime()))
			.build();
	}

	/**
	 * Replaces control, format (e.g. bidi overrides), line / paragraph separator,
	 * unassigned and lone surrogate characters with {@code ?} and keeps at most
	 * {@link #MAX_FIELD_LENGTH} code points; a cut value ends with {@code ...}.
	 */
	static String bound(String value) {
		StringBuilder bounded = new StringBuilder(Math.min(value.length(), MAX_FIELD_LENGTH + 3));
		int kept = 0;
		for (int i = 0; i < value.length();) {
			if (kept == MAX_FIELD_LENGTH) {
				bounded.append("...");
				break;
			}
			int codePoint = value.codePointAt(i);
			bounded.appendCodePoint(printable(codePoint) ? codePoint : '?');
			kept++;
			i += Character.charCount(codePoint);
		}
		return bounded.toString();
	}

	private static boolean printable(int codePoint) {
		if (Character.isISOControl(codePoint)) {
			return false;
		}
		return switch (Character.getType(codePoint)) {
			case Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR, Character.UNASSIGNED,
					Character.SURROGATE, Character.PRIVATE_USE ->
				false;
			default -> true;
		};
	}

	/**
	 * The first {@link #SHORT_COMMIT_LENGTH} characters of a hexadecimal commit id; any
	 * other value as is.
	 */
	public static String shortCommit(String commit) {
		boolean hex = commit.chars().allMatch(c -> Character.digit(c, 16) >= 0);
		return hex && commit.length() > SHORT_COMMIT_LENGTH ? commit.substring(0, SHORT_COMMIT_LENGTH) : commit;
	}

	/**
	 * One line naming the build, e.g.
	 * {@code sluice-server 0.0.1-SNAPSHOT (7312472, built 2026-10-12T01:04:33Z)}; an
	 * empty field reads as unknown. Sanitize a build received from a peer first.
	 */
	public static String describe(PeerBuild build) {
		String implementation = build.getImplementation().isEmpty() ? "unknown implementation"
				: build.getImplementation();
		String version = build.getVersion().isEmpty() ? "version unknown" : build.getVersion();
		List<String> details = new ArrayList<>();
		details.add(build.getCommit().isEmpty() ? "commit unknown" : shortCommit(build.getCommit()));
		if (build.getDirty()) {
			details.add("dirty");
		}
		if (!build.getBuildTime().isEmpty()) {
			details.add("built " + build.getBuildTime());
		}
		return implementation + " " + version + " (" + String.join(", ", details) + ")";
	}

}
