package am.ik.sluice.client.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * The settings of a filter. Under {@code sluice.client.filters.<name>} they define it:
 * its plugin, when it is one, and its defaults for every route using it. Under
 * {@code sluice.client.upstream[n].filters[m]} they are a route's use of it: its name,
 * and what that use sets over the defaults.
 *
 * @param name the filter a route uses: a built-in, or a filter defined by a plugin or a
 * factory; route's use only
 * @param plugin where a proxy-wasm plugin is read from: a Spring resource location
 * ({@code file:}, {@code classpath:}, {@code https:}, ...), or a file path without a
 * scheme; definition only
 * @param sha256 the SHA-256 of the plugin, in hex: the plugin is refused when it does not
 * match, and when it comes from elsewhere than a file or the class path without one;
 * definition only
 * @param paths the request paths the filter sees: exact, or a prefix ending in {@code *};
 * empty = every path. Compared as sent, so a limit of what a filter costs, not a policy
 * @param failClosed whether a request or response fails with the filter failing on it;
 * otherwise it goes on as if the filter were not there
 * @param configuration the plugin's configuration, as it reads it on its start
 * @param args the settings of a built-in or a factory's filter, merged key by key
 * @param vms instances of the plugin; a stream stays on one, one call at a time
 * @param maxMemory the memory one instance of the plugin may hold
 * @param callTimeout how long one call into the plugin may run
 * @param maxBody the bytes of a body the filter may hold, or write at once
 * @param maxMeters the counters the plugin may define; past it they are dropped and
 * counted
 */
public record FilterConfig(@DefaultValue("") String name, @DefaultValue("") String plugin,
		@DefaultValue("") String sha256, @DefaultValue List<String> paths, @Nullable Boolean failClosed,
		@Nullable String configuration, @DefaultValue Map<String, String> args, @Nullable Integer vms,
		@Nullable DataSize maxMemory, @Nullable Duration callTimeout, @Nullable DataSize maxBody,
		@Nullable Integer maxMeters) {

	private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

	public FilterConfig {
		name = name == null ? "" : name.trim();
		plugin = plugin == null ? "" : plugin.trim();
		sha256 = sha256 == null ? "" : sha256.trim().toLowerCase(Locale.ROOT);
		if (!sha256.isEmpty() && !SHA256.matcher(sha256).matches()) {
			throw new IllegalArgumentException("sha256 must be 64 hex digits: " + sha256);
		}
		paths = paths == null ? List.of() : List.copyOf(paths);
		args = args == null ? Map.of() : Map.copyOf(args);
		if (vms != null && vms < 1) {
			throw new IllegalArgumentException("vms must be positive: " + vms);
		}
		if (maxMeters != null && maxMeters < 0) {
			throw new IllegalArgumentException("max-meters must not be negative: " + maxMeters);
		}
	}

	/** These settings with what {@code use} sets over them. */
	public FilterConfig overriddenBy(FilterConfig use) {
		Map<String, String> merged = new LinkedHashMap<>(this.args);
		merged.putAll(use.args());
		return new FilterConfig(use.name().isEmpty() ? this.name : use.name(), this.plugin, this.sha256,
				use.paths().isEmpty() ? this.paths : use.paths(), first(use.failClosed(), this.failClosed),
				first(use.configuration(), this.configuration), merged, first(use.vms(), this.vms),
				first(use.maxMemory(), this.maxMemory), first(use.callTimeout(), this.callTimeout),
				first(use.maxBody(), this.maxBody), first(use.maxMeters(), this.maxMeters));
	}

	private static <T> @Nullable T first(@Nullable T over, @Nullable T under) {
		return over != null ? over : under;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String name = "";

		private String plugin = "";

		private String sha256 = "";

		private List<String> paths = List.of();

		private @Nullable Boolean failClosed;

		private @Nullable String configuration;

		private Map<String, String> args = Map.of();

		private @Nullable Integer vms;

		private @Nullable DataSize maxMemory;

		private @Nullable Duration callTimeout;

		private @Nullable DataSize maxBody;

		private @Nullable Integer maxMeters;

		private Builder() {
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder plugin(String plugin) {
			this.plugin = plugin;
			return this;
		}

		public Builder sha256(String sha256) {
			this.sha256 = sha256;
			return this;
		}

		public Builder paths(List<String> paths) {
			this.paths = paths;
			return this;
		}

		public Builder failClosed(boolean failClosed) {
			this.failClosed = failClosed;
			return this;
		}

		public Builder configuration(String configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder args(Map<String, String> args) {
			this.args = args;
			return this;
		}

		public Builder vms(int vms) {
			this.vms = vms;
			return this;
		}

		public Builder maxMemory(DataSize maxMemory) {
			this.maxMemory = maxMemory;
			return this;
		}

		public Builder callTimeout(Duration callTimeout) {
			this.callTimeout = callTimeout;
			return this;
		}

		public Builder maxBody(DataSize maxBody) {
			this.maxBody = maxBody;
			return this;
		}

		public Builder maxMeters(int maxMeters) {
			this.maxMeters = maxMeters;
			return this;
		}

		public FilterConfig build() {
			return new FilterConfig(this.name, this.plugin, this.sha256, this.paths, this.failClosed,
					this.configuration, this.args, this.vms, this.maxMemory, this.callTimeout, this.maxBody,
					this.maxMeters);
		}

	}

}
