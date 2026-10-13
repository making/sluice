Difficulty: Medium

# netty h2 demux, step 10: remove the legacy relay

Last step of the netty h2 demux migration (context and user decisions: 098). Depends on
106 and its soak.

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
