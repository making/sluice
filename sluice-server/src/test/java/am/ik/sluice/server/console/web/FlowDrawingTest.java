package am.ik.sluice.server.console.web;

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
		assertThat(drawing.intakes().get(0).path()).isEqualTo(
				"M 300 " + (cy - FlowDrawing.ROW) + " C 400 " + (cy - FlowDrawing.ROW) + " 380 " + cy + " 470 " + cy);
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

}
