package am.ik.sluice.server.config;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * The listen ports a client may claim for tcp routes, parsed from
 * {@code sluice.tcp-port-range} (a comma separated list of single ports or
 * {@code min-max} inclusive ranges, e.g. {@code 9000-9010,8080}). An empty configuration
 * allows every port.
 */
public record TcpPortRange(List<Bound> bounds) {

	public record Bound(int min, int max) {

		boolean contains(int port) {
			return port >= this.min && port <= this.max;
		}

	}

	private static final TcpPortRange ANY = new TcpPortRange(List.of());

	public static TcpPortRange any() {
		return ANY;
	}

	/**
	 * Parses the property value; a blank value allows every port. An unparseable entry is
	 * ignored.
	 */
	public static TcpPortRange parse(@Nullable String value) {
		if (value == null || value.isBlank()) {
			return ANY;
		}
		List<Bound> bounds = new ArrayList<>();
		for (String part : value.split(",")) {
			String trimmed = part.trim();
			if (trimmed.isEmpty()) {
				continue;
			}
			int separator = trimmed.indexOf('-');
			try {
				if (separator < 0) {
					int port = Integer.parseInt(trimmed);
					bounds.add(new Bound(port, port));
				}
				else {
					bounds.add(new Bound(Integer.parseInt(trimmed.substring(0, separator).trim()),
							Integer.parseInt(trimmed.substring(separator + 1).trim())));
				}
			}
			catch (NumberFormatException e) {
				// ignore the malformed entry
			}
		}
		return bounds.isEmpty() ? ANY : new TcpPortRange(List.copyOf(bounds));
	}

	/**
	 * Whether the port may be claimed as a tcp route.
	 */
	public boolean contains(int port) {
		if (this.bounds.isEmpty()) {
			return true;
		}
		for (Bound bound : this.bounds) {
			if (bound.contains(port)) {
				return true;
			}
		}
		return false;
	}

}
