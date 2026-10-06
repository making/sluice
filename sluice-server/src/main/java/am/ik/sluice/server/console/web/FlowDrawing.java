package am.ik.sluice.server.console.web;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Plan view of the traffic through this node, drawn as a sluice: intake channels from the
 * public listeners converge into a lock, pass the node's mitre gates and fan out to one
 * outlet per connected client. The gates swing open with the number of open connections
 * and stay shut while the node is not accepting traffic.
 * <p>
 * All geometry is in the units of a {@value #WIDTH}-wide SVG view box.
 *
 * @param state readiness key ({@code accepting}, {@code warming}, {@code draining})
 */
public record FlowDrawing(List<Intake> intakes, List<Outlet> outlets, int openConnections, String controlPort,
		String state) {

	/** View box width. */
	public static final int WIDTH = 1200;

	static final int ROW = 64;

	static final int MIN_HEIGHT = 400;

	static final int INTAKE_START = 300;

	static final int CONDUIT_IN = 470;

	static final int GATE = 600;

	static final int CONDUIT_OUT = 730;

	static final int OUTLET_END = 900;

	static final int OUTLET_LABEL = 916;

	/** Width between the banks of an intake or outlet channel. */
	static final int CHANNEL = 14;

	/** Half width of the conduit through the lock. */
	static final int CONDUIT = 26;

	/** Depth of the concrete lock walls each side of the conduit. */
	static final int WALL = 64;

	/** Gate swing when fully open, in degrees. */
	static final double MAX_SWING = 66;

	public FlowDrawing {
		intakes = List.copyOf(intakes);
		outlets = List.copyOf(outlets);
	}

	public int width() {
		return WIDTH;
	}

	public int height() {
		return height(this.intakes.size(), this.outlets.size());
	}

	static int height(int intakes, int outlets) {
		int rows = Math.max(Math.max(intakes, outlets), 2);
		return Math.max(MIN_HEIGHT, rows * ROW + 2 * (CONDUIT + WALL) + 40);
	}

	/** Centre line of the conduit. */
	public int cy() {
		return this.height() / 2;
	}

	public boolean flowing() {
		return this.openConnections > 0;
	}

	public String clients() {
		int count = this.outlets.size();
		return count + (count == 1 ? " client" : " clients");
	}

	/**
	 * Gate swing in degrees: shut unless accepting traffic, ajar when idle, then opening
	 * logarithmically with the open connections.
	 */
	public String swing() {
		double swing;
		if (!"accepting".equals(this.state)) {
			swing = 0;
		}
		else if (this.openConnections == 0) {
			swing = 12;
		}
		else {
			swing = Math.min(MAX_SWING, 28 + 12 * (Math.log(1 + this.openConnections) / Math.log(2)));
		}
		return String.format(Locale.ROOT, "%.1f", swing);
	}

	/**
	 * Annotation of the gate state: shut while not accepting traffic, ajar when idle,
	 * open while connections flow. The swing angle itself is only a visual cue.
	 */
	public String gateLabel() {
		if (!"accepting".equals(this.state)) {
			return "Gates shut";
		}
		return this.openConnections == 0 ? "Gates ajar" : "Gates open";
	}

	/** Leader line from the lower hinge to the gate annotation. */
	public String gateLeader() {
		int y = this.wallBottom() + WALL / 2;
		return "M 596 " + (this.hingeBottom() + 6) + " L 556 " + y + " L 430 " + y;
	}

	public int gateLabelY() {
		return this.wallBottom() + WALL / 2 - 6;
	}

	public int hingeTop() {
		return this.cy() - CONDUIT;
	}

	public int hingeBottom() {
		return this.cy() + CONDUIT;
	}

	/** Upper lock wall (y of its top edge). */
	public int wallTop() {
		return this.cy() - CONDUIT - WALL;
	}

	public int wallBottom() {
		return this.cy() + CONDUIT;
	}

	/**
	 * Streamlines through the conduit: one per load step, spread across its width.
	 */
	public List<String> conduitStreams() {
		int lines = Math.max(1, load(this.openConnections) + 1);
		List<String> streams = new ArrayList<>();
		for (int i = 0; i < lines; i++) {
			int y = this.cy() + (int) Math.round((i - (lines - 1) / 2.0) * 10);
			streams.add("M " + CONDUIT_IN + " " + y + " L " + CONDUIT_OUT + " " + y);
		}
		return streams;
	}

	public String conduitPath() {
		return "M " + CONDUIT_IN + " " + this.cy() + " L " + CONDUIT_OUT + " " + this.cy();
	}

	/** Water speed through the conduit. */
	public String duration() {
		return FlowDrawing.duration(load(this.openConnections));
	}

	public int strokeWidth() {
		return FlowDrawing.strokeWidth(load(this.openConnections));
	}

	/** Baseline of the open connection figure above the lock. */
	public int figureY() {
		return this.wallTop() - 46;
	}

	public int captionY() {
		return this.wallTop() - 22;
	}

	public int footY() {
		return this.cy() + CONDUIT + WALL + 34;
	}

	/**
	 * Lays out intakes and outlets on evenly spaced lanes around the conduit.
	 */
	public static Builder builder() {
		return new Builder();
	}

	static Lane lane(int index, int count, int cy) {
		double offset = (index - (count - 1) / 2.0) * ROW;
		return new Lane((int) Math.round(cy + offset), cy);
	}

	/** Stroke weight bucket: 0 idle, 1 to 4 by open connections. */
	static int load(int open) {
		if (open <= 0) {
			return 0;
		}
		if (open < 3) {
			return 1;
		}
		if (open < 10) {
			return 2;
		}
		return open < 50 ? 3 : 4;
	}

	static String duration(int load) {
		return switch (load) {
			case 0 -> "0s";
			case 1 -> "2.4s";
			case 2 -> "1.6s";
			case 3 -> "1.1s";
			default -> "0.75s";
		};
	}

	/** Water width inside a channel ({@value #CHANNEL} wide between its banks). */
	static int strokeWidth(int load) {
		return 4 + 2 * load;
	}

	static String truncate(String value, int max) {
		return value.length() <= max ? value : value.substring(0, max - 1) + "…";
	}

	/**
	 * One horizontal lane of the drawing.
	 *
	 * @param y lane centre
	 * @param cy conduit centre the lane's channel joins
	 */
	public record Lane(int y, int cy) {

		/** Text baseline of the lane's first label line. */
		public int labelY() {
			return this.y - 4;
		}

		/** Text baseline of the lane's second label line. */
		public int detailY() {
			return this.y + 15;
		}

	}

	/**
	 * A public listener feeding the lock.
	 *
	 * @param open connections currently relayed from the listener
	 */
	public record Intake(String port, String protocol, String detail, int open, Lane lane) {

		public boolean active() {
			return this.open > 0;
		}

		public String path() {
			int y = this.lane.y();
			int cy = this.lane.cy();
			return "M " + INTAKE_START + " " + y + " C 400 " + y + " 380 " + cy + " " + CONDUIT_IN + " " + cy;
		}

		public String duration() {
			return FlowDrawing.duration(load(this.open));
		}

		public int strokeWidth() {
			return FlowDrawing.strokeWidth(load(this.open));
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String port;

			private @Nullable String protocol;

			private String detail = "";

			private int open;

			private Builder() {
			}

			public Builder port(String port) {
				this.port = port;
				return this;
			}

			public Builder protocol(String protocol) {
				this.protocol = protocol;
				return this;
			}

			public Builder detail(String detail) {
				this.detail = detail;
				return this;
			}

			public Builder open(int open) {
				this.open = open;
				return this;
			}

			Intake build(Lane lane) {
				return new Intake(Objects.requireNonNull(this.port, "port is required"),
						Objects.requireNonNull(this.protocol, "protocol is required"), this.detail, this.open, lane);
			}

		}

	}

	/**
	 * The tunnel of one connected client leaving the lock.
	 *
	 * @param serving the routes this client serves, as display text (empty when none)
	 * @param standby the routes this client stands by for, as display text
	 */
	public record Outlet(String clientId, int open, String serving, String standby, Lane lane) {

		public boolean active() {
			return this.open > 0;
		}

		/** Client id shortened to fit the drawing; the full id goes in a tooltip. */
		public String label() {
			return truncate(this.clientId, 26);
		}

		public String servingLabel() {
			return truncate(this.serving, 40);
		}

		public String standbyLabel() {
			return truncate(this.standby, 40);
		}

		/** Baseline of the line below the serving routes. */
		public int standbyY() {
			return this.lane.detailY() + 16;
		}

		public String path() {
			int y = this.lane.y();
			int cy = this.lane.cy();
			return "M " + CONDUIT_OUT + " " + cy + " C 820 " + cy + " 800 " + y + " " + OUTLET_END + " " + y;
		}

		public int labelX() {
			return OUTLET_LABEL;
		}

		public String duration() {
			return FlowDrawing.duration(load(this.open));
		}

		public int strokeWidth() {
			return FlowDrawing.strokeWidth(load(this.open));
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String clientId;

			private int open;

			private String serving = "";

			private String standby = "";

			private Builder() {
			}

			public Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			public Builder open(int open) {
				this.open = open;
				return this;
			}

			public Builder serving(String serving) {
				this.serving = serving;
				return this;
			}

			public Builder standby(String standby) {
				this.standby = standby;
				return this;
			}

			Outlet build(Lane lane) {
				return new Outlet(Objects.requireNonNull(this.clientId, "clientId is required"), this.open,
						this.serving, this.standby, lane);
			}

		}

	}

	public static final class Builder {

		private final List<Intake.Builder> intakes = new ArrayList<>();

		private final List<Outlet.Builder> outlets = new ArrayList<>();

		private int openConnections;

		private String controlPort = "";

		private String state = "accepting";

		private Builder() {
		}

		public Builder intake(Intake.Builder intake) {
			this.intakes.add(intake);
			return this;
		}

		/**
		 * Adds the outlet of a connected client; outlets are drawn in the order added.
		 */
		public Builder outlet(Outlet.Builder outlet) {
			this.outlets.add(outlet);
			return this;
		}

		public Builder openConnections(int openConnections) {
			this.openConnections = openConnections;
			return this;
		}

		public Builder controlPort(String controlPort) {
			this.controlPort = controlPort;
			return this;
		}

		public Builder state(String state) {
			this.state = state;
			return this;
		}

		public FlowDrawing build() {
			// the height, and so the conduit centre, depends only on the lane counts
			int cy = height(this.intakes.size(), this.outlets.size()) / 2;
			List<Intake> intakes = new ArrayList<>();
			for (int i = 0; i < this.intakes.size(); i++) {
				intakes.add(this.intakes.get(i).build(lane(i, this.intakes.size(), cy)));
			}
			List<Outlet> outlets = new ArrayList<>();
			for (int i = 0; i < this.outlets.size(); i++) {
				outlets.add(this.outlets.get(i).build(lane(i, this.outlets.size(), cy)));
			}
			return new FlowDrawing(intakes, outlets, this.openConnections, this.controlPort, this.state);
		}

	}

}
