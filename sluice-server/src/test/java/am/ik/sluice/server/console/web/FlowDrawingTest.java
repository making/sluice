package am.ik.sluice.server.console.web;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FlowDrawingTest {

	private static FlowDrawing.Intake.Builder intake(String port, int open) {
		return FlowDrawing.Intake.builder().port(port).protocol("TCP").open(open);
	}

	@Test
	void lanesAreSpreadEvenlyAroundTheConduit() {
		FlowDrawing drawing = FlowDrawing.builder()
			.intake(intake(":8000", 0))
			.intake(intake(":9100", 0))
			.intake(intake(":9101", 0))
			.outlet(FlowDrawing.Outlet.builder().clientId("alpha"))
			.build();
		int cy = drawing.cy();
		assertThat(drawing.height()).isEqualTo(3 * FlowDrawing.ROW + 2 * (FlowDrawing.CONDUIT + FlowDrawing.WALL) + 40);
		assertThat(FlowDrawing.builder().outlet(FlowDrawing.Outlet.builder().clientId("alpha")).build().height())
			.isEqualTo(FlowDrawing.MIN_HEIGHT);
		assertThat(drawing.intakes()).extracting(intake -> intake.lane().y())
			.containsExactly(cy - FlowDrawing.ROW, cy, cy + FlowDrawing.ROW);
		assertThat(drawing.outlets()).extracting(outlet -> outlet.lane().y()).containsExactly(cy);
		assertThat(drawing.intakes().get(0).path()).isEqualTo("M 300 " + (cy - FlowDrawing.ROW) + " H 330 C 412 "
				+ (cy - FlowDrawing.ROW) + " 396 " + cy + " 470 " + cy);
	}

	@Test
	void manyLanesGrowTheDrawing() {
		FlowDrawing.Builder builder = FlowDrawing.builder();
		for (int i = 0; i < 8; i++) {
			builder.outlet(FlowDrawing.Outlet.builder().clientId("client-" + i));
		}
		FlowDrawing drawing = builder.build();
		assertThat(drawing.height()).isEqualTo(8 * FlowDrawing.ROW + 2 * (FlowDrawing.CONDUIT + FlowDrawing.WALL) + 40);
		assertThat(drawing.outlets().get(0).lane().y()).isGreaterThan(0);
		assertThat(drawing.outlets().get(7).lane().y()).isLessThan(drawing.height());
	}

	@Test
	void gatesFollowReadinessAndLoad() {
		assertThat(FlowDrawing.builder().state("draining").openConnections(40).build().gateLabel())
			.isEqualTo("Gates shut");
		assertThat(FlowDrawing.builder().state("warming").build().swing()).isEqualTo("0.0");
		assertThat(FlowDrawing.builder().state("accepting").build().gateLabel()).isEqualTo("Gates ajar");
		assertThat(FlowDrawing.builder().state("accepting").openConnections(1).build().gateLabel())
			.isEqualTo("Gates open");
		assertThat(FlowDrawing.builder().state("accepting").openConnections(10_000).build().swing()).isEqualTo("66.0");
	}

	@Test
	void outletLabelsAreShortenedForTheDrawing() {
		FlowDrawing drawing = FlowDrawing.builder()
			.outlet(FlowDrawing.Outlet.builder().clientId("33d95341-a80d-495c-9302-1d95ddb098f0").open(3))
			.build();
		FlowDrawing.Outlet outlet = drawing.outlets().get(0);
		assertThat(outlet.label()).isEqualTo("33d95341-a80d-495c-9302-1…");
		assertThat(outlet.active()).isTrue();
		assertThat(outlet.duration()).isEqualTo("1.6s");
	}

	@Test
	void conduitCarriesOneStreamlinePerLoadStep() {
		assertThat(FlowDrawing.builder().build().conduitStreams()).hasSize(1);
		assertThat(FlowDrawing.builder().openConnections(60).build().conduitStreams()).hasSize(5);
	}

	@Test
	void shutRoutesRaiseAHeadgateOnTheIntakeLeadIn() {
		FlowDrawing drawing = FlowDrawing.builder()
			.intake(FlowDrawing.Intake.builder().port(":8000").protocol("HTTP").routes(3).shutRoutes(1))
			.intake(FlowDrawing.Intake.builder().port(":8443").protocol("HTTP").routes(2).shutRoutes(2))
			.intake(intake(":9100", 0).routes(1).shutRoutes(1))
			.intake(intake(":9101", 0).routes(1))
			.build();
		FlowDrawing.Intake partial = drawing.intakes().get(0);
		FlowDrawing.Intake closed = drawing.intakes().get(1);
		FlowDrawing.Intake tcp = drawing.intakes().get(2);
		assertThat(partial.shutLabel()).isEqualTo("1 of 3 routes shut");
		assertThat(closed.shutLabel()).isEqualTo("All 2 routes shut");
		assertThat(tcp.shutLabel()).isEqualTo("Route shut");
		assertThat(drawing.intakes().get(3).gated()).isFalse();
		// the outer lanes bend furthest; their gates still stand on the straight lead-in,
		// piers on the banks
		for (FlowDrawing.Intake intake : List.of(partial, tcp)) {
			int y = intake.lane().y();
			assertThat(y).isNotEqualTo(drawing.cy());
			assertThat(intake.path()).startsWith("M 300 " + y + " H 330 ");
			assertThat(intake.pierX()).isGreaterThan(300);
			assertThat(intake.pierX() + intake.pierWidth()).isLessThan(330);
			assertThat(intake.pierTop() + intake.pierHeight()).isEqualTo(y - FlowDrawing.CHANNEL / 2);
			assertThat(intake.pierBottom()).isEqualTo(y + FlowDrawing.CHANNEL / 2);
		}
		// a closed leaf spans the channel; an ajar one swings downstream within the
		// lead-in
		assertThat(closed.leaf()).isEqualTo("M 315 " + (closed.lane().y() - 7) + " V " + (closed.lane().y() + 7));
		assertThat(partial.leaf())
			.isEqualTo("M 315 " + (partial.lane().y() - 7) + " L 324 " + (partial.lane().y() + 4));
	}

}
