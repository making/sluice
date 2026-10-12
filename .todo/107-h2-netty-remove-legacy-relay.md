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
