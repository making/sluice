package am.ik.sluice.server.livelog;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.jspecify.annotations.Nullable;

/**
 * One access log line as structured data: what the {@code sluice.access} logger emits
 * ({@link #line()}) and the live access log shows.
 *
 * @param kind {@code accept}, {@code close}, {@code reject}, {@code request} or
 * {@code ratelimit}
 * @param routeKey the route key whose settings the line follows; empty before routing
 * @param fields the key=value pairs of the line, in order
 */
public record AccessEvent(String kind, String routeKey, List<Field> fields) {

	public AccessEvent {
		fields = List.copyOf(fields);
	}

	public static Builder builder(String kind, String routeKey) {
		return new Builder(kind, routeKey);
	}

	/** The logfmt line: {@code name=value} pairs separated by one space. */
	public String line() {
		StringJoiner line = new StringJoiner(" ");
		for (Field field : this.fields) {
			line.add(field.name() + "=" + (field.text() ? quoted(field.value()) : field.value()));
		}
		return line.toString();
	}

	/**
	 * A free-text value as a logfmt value: {@code -} when absent, else in double quotes
	 * with quotes and backslashes backslash-escaped and control characters as Java
	 * unicode escapes, so it stays one field on one line.
	 */
	static String quoted(@Nullable String value) {
		if (value == null) {
			return "-";
		}
		StringBuilder quoted = new StringBuilder(value.length() + 2).append('"');
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c == '"' || c == '\\') {
				quoted.append('\\').append(c);
			}
			else if (Character.isISOControl(c)) {
				quoted.append(String.format("\\u%04x", (int) c));
			}
			else {
				quoted.append(c);
			}
		}
		return quoted.append('"').toString();
	}

	/**
	 * One pair of the line.
	 *
	 * @param name the key
	 * @param value the value; for free text, {@code null} when absent
	 * @param text whether the value is free text, quoted in the line
	 */
	public record Field(String name, @Nullable String value, boolean text) {

	}

	public static final class Builder {

		private final String kind;

		private final String routeKey;

		private final List<Field> fields = new ArrayList<>();

		private Builder(String kind, String routeKey) {
			this.kind = Objects.requireNonNull(kind, "kind is required");
			this.routeKey = Objects.requireNonNull(routeKey, "routeKey is required");
		}

		/** A value written as is ({@link String#valueOf(Object)}). */
		public Builder field(String name, Object value) {
			this.fields.add(new Field(name, String.valueOf(value), false));
			return this;
		}

		/** A free-text value, quoted in the line; {@code -} when {@code null}. */
		public Builder text(String name, @Nullable String value) {
			this.fields.add(new Field(name, value, true));
			return this;
		}

		public AccessEvent build() {
			return new AccessEvent(this.kind, this.routeKey, this.fields);
		}

	}

}
