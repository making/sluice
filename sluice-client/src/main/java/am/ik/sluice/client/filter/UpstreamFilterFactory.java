package am.ik.sluice.client.filter;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Makes the filters known by a name, one for each use of it on a route. The built-ins are
 * factories; a custom build adds its own as beans, and a route uses them by name like any
 * other ({@code sluice.client.upstream[n].filters[m].name}).
 */
public interface UpstreamFilterFactory {

	/** The name routes use the filter by. */
	String name();

	/**
	 * A filter for one use on a route.
	 * @throws IllegalArgumentException when the settings are invalid: the client does not
	 * start
	 */
	UpstreamFilter create(Context context);

	/**
	 * What a filter is made for.
	 *
	 * @param route the route's key
	 * @param settings the settings of this use, over the defaults of the filter's name
	 * @param meters where the filter counts
	 * @param budget what bytes it holds past a connection's allowance charge
	 */
	record Context(String route, FilterConfig settings, MeterRegistry meters, TunnelBudget budget) {

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String route;

			private @Nullable FilterConfig settings;

			private @Nullable MeterRegistry meters;

			private @Nullable TunnelBudget budget;

			private Builder() {
			}

			public Builder route(String route) {
				this.route = route;
				return this;
			}

			public Builder settings(FilterConfig settings) {
				this.settings = settings;
				return this;
			}

			public Builder meters(MeterRegistry meters) {
				this.meters = meters;
				return this;
			}

			public Builder budget(TunnelBudget budget) {
				this.budget = budget;
				return this;
			}

			public Context build() {
				return new Context(Objects.requireNonNull(this.route, "route is required"),
						Objects.requireNonNull(this.settings, "settings is required"),
						Objects.requireNonNull(this.meters, "meters is required"),
						Objects.requireNonNull(this.budget, "budget is required"));
			}

		}

	}

}
