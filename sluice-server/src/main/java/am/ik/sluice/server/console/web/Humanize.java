package am.ik.sluice.server.console.web;

import java.time.Duration;
import java.util.Locale;

/**
 * Compact, human readable renderings of byte counts and elapsed time.
 */
final class Humanize {

	private static final String[] UNITS = { "KiB", "MiB", "GiB", "TiB", "PiB" };

	private Humanize() {
	}

	/**
	 * Binary units with one decimal below 100 ({@code 0 B}, {@code 512 B},
	 * {@code 1.5 KiB}, {@code 120 MiB}).
	 */
	static String bytes(long bytes) {
		if (bytes < 1024) {
			return bytes + " B";
		}
		double value = bytes;
		int unit = -1;
		while (value >= 1024 && unit < UNITS.length - 1) {
			value /= 1024;
			unit++;
		}
		return (value < 100 ? String.format(Locale.ROOT, "%.1f", value) : String.format(Locale.ROOT, "%.0f", value))
				+ " " + UNITS[unit];
	}

	/**
	 * The two most significant units ({@code 45s}, {@code 12m 03s}, {@code 3h 12m},
	 * {@code 2d 04h}).
	 */
	static String duration(Duration duration) {
		long seconds = Math.max(0, duration.toSeconds());
		long days = seconds / 86_400;
		long hours = seconds % 86_400 / 3_600;
		long minutes = seconds % 3_600 / 60;
		long secs = seconds % 60;
		if (days > 0) {
			return String.format(Locale.ROOT, "%dd %02dh", days, hours);
		}
		if (hours > 0) {
			return String.format(Locale.ROOT, "%dh %02dm", hours, minutes);
		}
		if (minutes > 0) {
			return String.format(Locale.ROOT, "%dm %02ds", minutes, secs);
		}
		return secs + "s";
	}

}
