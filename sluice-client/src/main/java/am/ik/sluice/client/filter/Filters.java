package am.ik.sluice.client.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

/**
 * Makes the filters of the routes from their names: a plugin defined under
 * {@code sluice.client.filters.<name>}, a built-in, or a factory of a custom build, each
 * use on a route with its settings over the defaults of its name. Nothing unknown or
 * invalid survives the client's start.
 */
public final class Filters {

	private final Map<String, FilterConfig> definitions;

	private final Map<String, UpstreamFilterFactory> factories = new LinkedHashMap<>();

	private final Map<String, PluginModule> modules = new HashMap<>();

	private final MeterRegistry meters;

	private final TunnelBudget budget;

	private final ResourceLoader resourceLoader;

	private long reservedBytes;

	/** The most plugins on one route. */
	private int mostPlugins;

	private Filters(Builder builder) {
		this.definitions = Map.copyOf(builder.definitions);
		this.meters = Objects.requireNonNull(builder.meters, "meters is required");
		this.budget = Objects.requireNonNull(builder.budget, "budget is required");
		this.resourceLoader = builder.resourceLoader;
		register(LlmUsageFilter.factory());
		builder.factories.forEach(this::register);
		this.definitions.forEach((name, definition) -> {
			if (!definition.plugin().isEmpty() && this.factories.containsKey(name)) {
				throw new IllegalArgumentException("filter " + name + " is defined by a plugin and by a factory");
			}
		});
	}

	private void register(UpstreamFilterFactory factory) {
		if (this.factories.putIfAbsent(factory.name(), factory) != null) {
			throw new IllegalArgumentException("two factories make filter " + factory.name());
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private Map<String, FilterConfig> definitions = Map.of();

		private List<UpstreamFilterFactory> factories = List.of();

		private @Nullable MeterRegistry meters;

		private @Nullable TunnelBudget budget;

		private ResourceLoader resourceLoader = new DefaultResourceLoader();

		private Builder() {
		}

		/** The filters defined by name ({@code sluice.client.filters.<name>}). */
		public Builder definitions(Map<String, FilterConfig> definitions) {
			this.definitions = definitions;
			return this;
		}

		/** The factories of a custom build, next to the built-ins. */
		public Builder factories(List<UpstreamFilterFactory> factories) {
			this.factories = factories;
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

		/** Resolves where plugins are read from; defaults to the JDK's URL handlers. */
		public Builder resourceLoader(ResourceLoader resourceLoader) {
			this.resourceLoader = resourceLoader;
			return this;
		}

		public Filters build() {
			return new Filters(this);
		}

	}

	/**
	 * The filter of a route using these filters, in request order.
	 * @throws IllegalArgumentException when one of them is unknown or invalid
	 */
	public UpstreamFilter forRoute(String route, List<FilterConfig> uses) {
		List<FilterChain.Link> links = new ArrayList<>(uses.size());
		int plugins = 0;
		for (FilterConfig use : uses) {
			String name = use.name();
			FilterConfig definition = this.definitions.get(name);
			FilterConfig settings = definition == null ? use : definition.overriddenBy(use);
			FilterFailures failures = new FilterFailures(this.meters, route, name);
			plugins += settings.plugin().isEmpty() ? 0 : 1;
			links.add(new FilterChain.Link(create(route, name, settings, failures), settings.paths(),
					Boolean.TRUE.equals(settings.failClosed()), failures));
		}
		this.mostPlugins = Math.max(this.mostPlugins, plugins);
		return new FilterChain(links);
	}

	private UpstreamFilter create(String route, String name, FilterConfig settings, FilterFailures failures) {
		UpstreamFilterFactory.Context context = UpstreamFilterFactory.Context.builder()
			.route(route)
			.settings(settings)
			.meters(this.meters)
			.budget(this.budget)
			.build();
		if (!settings.plugin().isEmpty()) {
			PluginModule module = this.modules.computeIfAbsent(name,
					key -> PluginModule.load(name, settings.plugin(), settings.sha256(), this.resourceLoader));
			this.reservedBytes += ProxyWasmFilter.reservedBytes(settings);
			return new ProxyWasmFilter(name, module, context, failures);
		}
		UpstreamFilterFactory factory = this.factories.get(name);
		if (factory == null) {
			throw new IllegalArgumentException("unknown filter " + name + ": no sluice.client.filters." + name
					+ ".plugin, and neither a built-in nor a factory has that name");
		}
		return factory.create(context);
	}

	/**
	 * The most the instances of the plugins made so far hold: native memory, outside the
	 * heap the connection limit derives from.
	 */
	public long reservedBytes() {
		return this.reservedBytes;
	}

	/** What a connection holds at most for the plugins of its route, in the host. */
	public long pluginConnectionBytes() {
		return this.mostPlugins * ProxyWasmFilter.CONNECTION_BYTES;
	}

}
