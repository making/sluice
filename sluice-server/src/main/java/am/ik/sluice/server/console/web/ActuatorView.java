package am.ik.sluice.server.console.web;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.boot.health.actuate.endpoint.CompositeHealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.actuate.endpoint.IndicatedHealthDescriptor;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;

/**
 * Renderings of this node's actuator data -- health, info and metrics -- as display text
 * for the console pages, so the templates stay logic-less.
 */
@Component
class ActuatorView {

	private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

	/** Detail keys of {@link IndicatedHealthDescriptor} whose values are byte counts. */
	private static final Set<String> BYTE_DETAILS = Set.of("total", "free", "threshold");

	private final HealthEndpoint healthEndpoint;

	private final InfoEndpoint infoEndpoint;

	private final MeterRegistry meterRegistry;

	private final JsonMapper jsonMapper;

	ActuatorView(HealthEndpoint healthEndpoint, InfoEndpoint infoEndpoint, MeterRegistry meterRegistry,
			JsonMapper jsonMapper) {
		this.healthEndpoint = healthEndpoint;
		this.infoEndpoint = infoEndpoint;
		this.meterRegistry = meterRegistry;
		this.jsonMapper = jsonMapper;
	}

	HealthReport health() {
		HealthDescriptor descriptor = this.healthEndpoint.health();
		List<HealthRow> rows = new ArrayList<>();
		this.collect("", descriptor, rows);
		String code = descriptor.getStatus().getCode();
		return new HealthReport(code, code.toLowerCase(Locale.ROOT), LocalTime.now().format(CLOCK), List.copyOf(rows));
	}

	/**
	 * Flattens the health tree: every named component becomes a row, composites first
	 * with their children following under a prefixed path.
	 */
	private void collect(String path, HealthDescriptor descriptor, List<HealthRow> rows) {
		if (descriptor instanceof CompositeHealthDescriptor composite) {
			Map<String, HealthDescriptor> components = composite.getComponents();
			if (components == null) {
				return;
			}
			components.forEach((name, child) -> {
				String childPath = path.isEmpty() ? name : path + " / " + name;
				String code = child.getStatus().getCode();
				String details = child instanceof IndicatedHealthDescriptor indicated ? details(indicated.getDetails())
						: "";
				rows.add(new HealthRow(childPath, code, code.toLowerCase(Locale.ROOT), "DOWN".equals(code),
						!"UP".equals(code) && !"DOWN".equals(code), details));
				this.collect(childPath, child, rows);
			});
		}
	}

	private static String details(@Nullable Map<String, Object> details) {
		if (details == null) {
			return "";
		}
		return details.entrySet()
			.stream()
			.map(entry -> entry.getKey() + "=" + detailValue(entry.getKey(), entry.getValue()))
			.collect(Collectors.joining(", "));
	}

	private static String detailValue(String key, Object value) {
		if (value instanceof Number number && BYTE_DETAILS.contains(key)) {
			return Humanize.bytes(number.longValue());
		}
		return String.valueOf(value);
	}

	List<InfoSection> info() {
		Map<String, Object> info = this.infoEndpoint.info();
		if (info == null) {
			return List.of();
		}
		return info.entrySet()
			.stream()
			.sorted(Map.Entry.comparingByKey())
			.map(section -> new InfoSection(section.getKey(), flatten("", section.getValue(), new ArrayList<>())))
			.toList();
	}

	/**
	 * Flattens the entry into dot pathed keys. Values that are neither maps nor scalars
	 * -- the info contributors hand over beans such as {@code JavaInfo} -- are normalized
	 * the same way the endpoint serializes them.
	 */
	private List<InfoEntry> flatten(String prefix, Object value, List<InfoEntry> entries) {
		Object normalized = value instanceof Map<?, ?> ? value : this.jsonMapper.convertValue(value, Object.class);
		if (normalized instanceof Map<?, ?> nested) {
			nested.forEach((name, child) -> flatten(prefix.isEmpty() ? String.valueOf(name) : prefix + "." + name,
					child, entries));
		}
		else {
			entries.add(new InfoEntry(prefix, String.valueOf(normalized)));
		}
		return entries;
	}

	/**
	 * The number of distinct metrics registered with this node.
	 */
	int metricCount() {
		return (int) this.meterRegistry.getMeters().stream().map(meter -> meter.getId().getName()).distinct().count();
	}

	/**
	 * The metric names matching the filter, grouped by their first dot separated segment
	 * -- the library or subsystem they come from. A group is expanded when a filter is
	 * set (its matches are the point) or when it holds the selection.
	 */
	List<MetricGroup> metricGroups(@Nullable String filter, @Nullable String selected) {
		String query = filter == null ? "" : filter.strip().toLowerCase(Locale.ROOT);
		String selection = selected == null ? "" : selected;
		Map<String, List<MetricLink>> grouped = new LinkedHashMap<>();
		this.meterRegistry.getMeters()
			.stream()
			.map(meter -> meter.getId().getName())
			.distinct()
			.filter(name -> query.isEmpty() || name.toLowerCase(Locale.ROOT).contains(query))
			.sorted()
			.forEach(name -> grouped.computeIfAbsent(prefixOf(name), key -> new ArrayList<>())
				.add(new MetricLink(name, name.equals(selection))));
		return grouped.entrySet()
			.stream()
			.map(entry -> new MetricGroup(entry.getKey(), entry.getValue().size(),
					!query.isEmpty() || entry.getValue().stream().anyMatch(MetricLink::selected),
					List.copyOf(entry.getValue())))
			.toList();
	}

	private static String prefixOf(String name) {
		int dot = name.indexOf('.');
		return dot < 0 ? name : name.substring(0, dot);
	}

	@Nullable MetricDetail metric(String name) {
		Collection<Meter> meters = this.meterRegistry.find(name).meters();
		if (meters.isEmpty()) {
			return null;
		}
		Meter.Id id = meters.iterator().next().getId();
		List<MetricSeries> series = meters.stream()
			.map(meter -> new MetricSeries(tags(meter), measures(meter, id.getBaseUnit())))
			.sorted(Comparator.comparing(row -> String.join(",", row.tagNames())))
			.toList();
		return new MetricDetail(id.getName(), id.getDescription() == null ? "" : id.getDescription(),
				id.getBaseUnit() == null ? "" : id.getBaseUnit(), series);
	}

	private static List<TagPair> tags(Meter meter) {
		return meter.getId().getTags().stream().map(tag -> new TagPair(tag.getKey(), tag.getValue())).toList();
	}

	private static List<Measure> measures(Meter meter, @Nullable String baseUnit) {
		return StreamSupport.stream(meter.measure().spliterator(), false)
			.map(measurement -> measure(measurement, baseUnit))
			.toList();
	}

	/**
	 * One measurement of a meter; byte valued statistics in unit words, everything else
	 * raw.
	 */
	private static Measure measure(Measurement measurement, @Nullable String baseUnit) {
		double value = measurement.getValue();
		String rendered = "bytes".equals(baseUnit) && measurement.getStatistic() != Statistic.COUNT
				? Humanize.bytes(Math.round(value)) : number(value);
		return new Measure(measurement.getStatistic().name().toLowerCase(Locale.ROOT), rendered);
	}

	/**
	 * Whole numbers without a decimal mark, everything else as rendered by
	 * {@link Double}.
	 */
	private static String number(double value) {
		return value == Math.rint(value) && !Double.isInfinite(value) ? String.valueOf((long) value)
				: String.valueOf(value);
	}

	/**
	 * The health of the node as a whole.
	 *
	 * @param state the overall status, lower cased as a style hook ({@code up},
	 * {@code down}, ...)
	 * @param checkedAt the clock time the snapshot was taken
	 */
	public record HealthReport(String status, String state, String checkedAt, List<HealthRow> rows) {

	}

	/**
	 * One row of the health table; a composite contributes a row without details and its
	 * children follow with the path prefixed.
	 *
	 * @param state the status, lower cased as a style hook
	 * @param fault whether the status is {@code DOWN}
	 * @param degraded whether the status is neither {@code UP} nor {@code DOWN}
	 */
	public record HealthRow(String name, String status, String state, boolean fault, boolean degraded, String details) {

	}

	/**
	 * One top level entry of the info document with its nested keys flattened
	 * ({@code commit.id}).
	 *
	 * @param entries empty when the section holds a single scalar
	 */
	public record InfoSection(String name, List<InfoEntry> entries) {

	}

	/**
	 * One leaf of the info document; the key is the path below the section, empty for a
	 * scalar section.
	 */
	public record InfoEntry(String key, String value) {

	}

	public record MetricLink(String name, boolean selected) {

	}

	/**
	 * One group of the metric chooser: the names sharing a first dot separated segment.
	 *
	 * @param open whether the group starts expanded
	 */
	public record MetricGroup(String name, int count, boolean open, List<MetricLink> entries) {

	}

	/**
	 * A metric of the selected name: its description, unit and one row per tag
	 * combination.
	 */
	public record MetricDetail(String name, String description, String baseUnit, List<MetricSeries> series) {

	}

	/** One tag combination of a metric and its measurements, one row per statistic. */
	public record MetricSeries(List<TagPair> tags, List<Measure> measures) {

		public List<String> tagNames() {
			return this.tags.stream().map(tag -> tag.key() + "=" + tag.value()).toList();
		}

	}

	/** One statistic of a meter and its rendered value. */
	public record Measure(String statistic, String value) {

	}

	/** One tag of a meter, rendered as a chip. */
	public record TagPair(String key, String value) {

	}

}
