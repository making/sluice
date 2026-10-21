package am.ik.sluice.server.console.web;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.jspecify.annotations.Nullable;

import org.springframework.stereotype.Component;

import am.ik.sluice.v1.proto.AccessEvent;
import am.ik.sluice.v1.proto.Cursor;
import am.ik.sluice.v1.proto.WatchGap;
import am.ik.sluice.v1.proto.WatchRecord;

/**
 * Renders what the flume streams into the HTML fragments the live access log page
 * inserts: one table row per line or gap, and the strip of nodes.
 */
@Component
class LiveLogView {

	/** The fields the other columns show: left out of the line column. */
	private static final Set<String> SHOWN_APART = Set.of("type", "event", "route");

	private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
		.withZone(ZoneId.systemDefault());

	private final Template row;

	private final Template gap;

	private final Template nodes;

	LiveLogView(Mustache.Compiler compiler) {
		this.row = compiler.loadTemplate("console/access-log-row");
		this.gap = compiler.loadTemplate("console/access-log-gap");
		this.nodes = compiler.loadTemplate("console/access-log-nodes");
	}

	/** The SSE event id of a position: {@code <epoch>-<seq>}. */
	static String idOf(long epoch, long seq) {
		return epoch + "-" + seq;
	}

	/** The position an SSE event id names; {@code null} for anything else. */
	static @Nullable Cursor cursorOf(@Nullable String id) {
		if (id == null) {
			return null;
		}
		int dash = id.indexOf('-');
		try {
			return dash < 0 ? null
					: Cursor.newBuilder()
						.setEpoch(Long.parseUnsignedLong(id.substring(0, dash).strip()))
						.setSeq(Long.parseUnsignedLong(id.substring(dash + 1).strip()))
						.build();
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

	String row(WatchRecord record) {
		AccessEvent event = record.getEvent();
		Instant time = Instant.ofEpochMilli(event.getTimeUnixMillis());
		String route = event.getRouteKey().isEmpty() ? "-" : event.getRouteKey();
		StringJoiner line = new StringJoiner(" ");
		for (AccessEvent.Field field : event.getFieldsList()) {
			if (field.getName().equals("route")) {
				route = field.getValue();
			}
			if (!SHOWN_APART.contains(field.getName())) {
				line.add(field.getName() + "=" + field.getValue());
			}
		}
		return this.row.execute(new Row(record.getSeq(), TIME.format(time), time.toString(), record.getNodeId(),
				event.getKind(), route, line.toString()));
	}

	String gap(WatchGap gap) {
		String text = switch (gap.getCause()) {
			case NODE_DROPPED -> "%s dropped %d %s: its queue to the flume was full".formatted(gap.getNodeId(),
					gap.getDropped(), gap.getDropped() == 1 ? "line" : "lines");
			default -> "Up to %d %s missed: this view fell behind the flume".formatted(gap.getDropped(),
					gap.getDropped() == 1 ? "line" : "lines");
		};
		return this.gap.execute(new Gap(gap.getDropped(), text));
	}

	/** A gap of unknown size: the flume restarted since the position resumed from. */
	String restarted() {
		return this.gap.execute(new Gap(0, "The flume restarted: lines logged meanwhile may be missing"));
	}

	String nodes(Map<String, Boolean> connected) {
		List<NodeChip> chips = connected.entrySet()
			.stream()
			.map(entry -> new NodeChip(entry.getKey(), entry.getValue()))
			.toList();
		return this.nodes.execute(Map.of("nodes", chips));
	}

	/**
	 * One line of a node.
	 *
	 * @param seq the flume's position of the line
	 * @param time when the node logged it, to the millisecond in this node's zone
	 * @param instant the same as an ISO instant
	 * @param node the node that logged it
	 * @param kind accept, close, reject, request or ratelimit
	 * @param route the route as the line names it
	 * @param line the rest of the line's key=value pairs
	 */
	record Row(long seq, String time, String instant, String node, String kind, String route, String line) {

	}

	/**
	 * Lines that did not reach the view.
	 *
	 * @param dropped how many, {@code 0} when unknown
	 * @param text what happened
	 */
	record Gap(long dropped, String text) {

	}

	/**
	 * A node and whether its feed to the flume is open.
	 *
	 * @param nodeId the node
	 * @param connected whether it feeds
	 */
	record NodeChip(String nodeId, boolean connected) {

	}

	/**
	 * A choice of a filter.
	 *
	 * @param value the query parameter's value
	 * @param label what the choice reads
	 * @param selected whether it is chosen
	 */
	record Choice(String value, String label, boolean selected) {

	}

}
