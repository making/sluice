Difficulty: Medium

# netty h2 demux, step 10: remove the legacy relay

## Plan change (2026-10-13, the user)

Production is a test deployment; breaking changes are fine, and no code is written just to
keep two engines side by side. New order: 102 -> 107 (remove the legacy relay now, netty
only) -> 103, 104, 105 (in parallel, each re-enables its disabled tests on netty) -> 106.

This step now comes right after 102, before 103-105:
- Remove the legacy relay, the engine setting, the engine parameterization and the
  legacy-only tests that pin legacy-specific behaviour (end-to-end windows, settings
  replay, write seams, the GOAWAY grace...). Every h2 connection runs on netty.
- A test of behaviour the netty engine does not have yet (stall / idle / keep-alive
  timeouts: 103; leg limits, slots, retirement, the remaining metrics: 104; the remaining
  per-hop rewrites: 105) is `@Disabled` with the missing behaviour as its reason (no todo
  numbers in source), and listed by class and method in the todo that brings it back.
- README: the h2 demux is netty's; drop the engine setting; state the behaviour changes
  only as far as tests show them (105 completes the README).
- Production runs netty after this step without the timeouts and leg limits until 103 /
  104 land; the user accepted that.

Originally the last step of the netty h2 demux migration (context and user decisions: 098);
now right after 102 (see above).

## Scope

- Remove `Http2DemuxRelay`'s hand-written framing and state machine, its constants and test
  seams (`WriteSeam` and the like), the engine setting and the engine parameterization of
  the tests.
- README and `application.properties` drop the engine setting.
- Close any h2 todo the migration has subsumed in the meantime.

## Notes from 099 (engine switch and routing, 2026-10-13)

- The engine parameterization to remove: `H2EngineTest`, `H2Engines`, `LegacyOnly` (sluice-it,
  `am.ik.sluice.server.proxy`), the `@Parameter H2DemuxEngine engine` field of every
  `@H2EngineTest` class (and the engine-specific assertion in
  `H2DemuxConnectE2ETest#anUndecodableLaterHeadEndsTheConnectionWithACompressionError`),
  `H2DemuxEngineE2ETest`, the `DataProxyServer#h2DemuxEngine` seam, and the `engine` tag of
  `sluice_h2demux_connections_total` (or the meter).

## Notes from 101 (concurrency waiting, 2026-10-13)

- Drop "netty engine only" / "the legacy engine has no such deadline" for
  `sluice.limits.h2-stream-wait-timeout` (README table and "Connection limits",
  `application.properties`, the `Limits` javadoc) and `queuedBytesGauged()` in
  `H2DemuxUpstreamStreamLimitE2ETest`.
