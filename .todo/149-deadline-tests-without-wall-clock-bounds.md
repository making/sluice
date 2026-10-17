Difficulty: Medium

# Deadline tests: assert the deadline, not the wall clock

Left over from the test consolidation (2026-10-17): several tests tell two deadlines
apart, or "at once" from "at the deadline", by a tight wall-clock bound, which fails on a
loaded machine. Replace each with a deterministic check -- a clock / scheduler seam the
component already has or gains (see `WaitClock`), or a counter of which deadline fired --
and keep wall-clock bounds only as generous hang guards.

- `ConnectionLimitsTest`: `isLessThan(Duration.ofMillis(290))` (head timeout from the
  first entry, not the second) and the stall-sample windows
  (`STALL_TIMEOUT + STALL_TIMEOUT / 4 + 100ms`).
- `TunnelSessionReceivePathTest.closeReturnsWithinItsBoundWhileAnAdvertiseIsStuck`:
  `isBetween(300L, 1000L)`.
- `TunnelSessionAdvertiseDeadlineTest`: `isBetween(TIMEOUT, TIMEOUT.plusMillis(700))`.
- `MalformedRequestE2ETest`: half-closed after the 400 `isLessThan(500ms)`.
- `H2DemuxUpstreamFlowControlE2ETest`: probe at `800ms..3500ms`, "before any probe"
  `< 800ms`.
- `ConnectionLimitOverflowE2ETest` (`AT_ONCE = 1s` against a 2s overflow timeout),
  `HalfCloseProbeE2ETest` (`< 3 intervals`), `KeepAliveIdleTimeoutE2ETest`
  (`< IDLE_TIMEOUT - 500ms`).

Each item: name the behaviour the bound stands for, assert it without the wall clock,
and show by mutation that the new assertion fails without it.
