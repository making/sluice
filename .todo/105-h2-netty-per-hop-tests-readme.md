Difficulty: Medium

# netty h2 demux, step 8: the remaining per-hop test rewrites and README

Step 8 of the netty h2 demux migration (context and user decisions: 098). Depends on 104.

## Scope

- Rewrite every test still excluded for the netty engine against netty's semantics (list
  in `.todo/artefacts/096/FINDINGS.md`: settings replay and end-to-end windows, raw stubs without SETTINGS
  first, exact connection grants, error codes of 088 / 089, PING ordering, GOAWAY grace,
  `WriteSeam` races replaced by tests of the lock boundary). No netty exclusion remains.
- README: each intentional behaviour change (per-hop SETTINGS and windows, padding not
  relayed, error codes, no GOAWAY grace), stated only as far as tests show it.

## Notes from 099 (engine switch and routing, 2026-10-13)

- Excluded for netty naming this step: `H2DemuxFramingE2ETest`,
  `H2DemuxClientFrameValidationE2ETest`, `H2DemuxResetRaceE2ETest` (classes) and
  `H2DemuxFrameLimitsE2ETest#clientHeaderBlockAboveTheListLimitEndsTheConnection`. Not
  parameterized: `HttpPostE2ETest` (`h2PriorKnowledgePostCarriesTheDataFrames`).
- Behaviour changes for README, each pinned by a test:
  - a header block that does not decode: netty's GOAWAY `COMPRESSION_ERROR` names stream
    2^31-1 as the last processed (legacy: the last routed),
    `H2DemuxConnectE2ETest#anUndecodableLaterHeadEndsTheConnectionWithACompressionError`;
  - a header list past 64KiB is answered `431` and the connection goes on; netty ends the
    connection (`ENHANCE_YOUR_CALM`) only past 80KiB of block (legacy: a connection error
    past 64KiB), `NettyH2DemuxRelayE2ETest#aHeaderListPastTheLimitIsAnswered431`.
  - Read from netty's `PrefaceDecoder`, untested: a client whose preface is not followed
    by SETTINGS is a connection error (legacy took a HEADERS right after the magic).
- A parameterized class runs both engines on one context and one router: a test that
  registers a route under a fixed host per invocation collides with the other run's client
  (`H2DemuxFrameLimitsE2ETest` needed per-engine route names, `route(...)`).

## Notes from 100 (upstream error scope, 2026-10-13)

- netty's frame reader ends a header block past 80KiB with `PROTOCOL_ERROR`
  (`Http2CodecUtil#headerListSizeExceeded`), shown for an upstream by
  `H2DemuxFrameLimitsE2ETest#upstreamHeaderBlockAboveTheListLimitEndsItsLeg`; the 099 note
  above says `ENHANCE_YOUR_CALM` for the client, check it when rewriting
  `clientHeaderBlockAboveTheListLimitEndsTheConnection`.
- README: this step added a netty paragraph to "h2 upstream errors" (the `502` page, netty's
  frame validation and GOAWAY last stream id 2^31-1, the close right after its GOAWAY, the
  relay's own GOAWAY after an upstream's); fold it into this step's pass.
- Tests kept legacy only in `H2DemuxUpstreamErrorE2ETest` because they encode legacy
  behaviour, each with a netty counterpart in `NettyH2DemuxRelayE2ETest`:
  `#aConnectionErrorStillDeliversTheResponsesTheUpstreamCompleted` (end-to-end windows),
  `#upstreamIgnoringTheGoAwayIsClosedAfterTheGrace` (the legacy close grace),
  `#clientGoneWhileTheLegFailsReleasesEverything` (`WriteSeam`). Remove them with the
  legacy relay.

## Notes from 101 (concurrency waiting, 2026-10-13)

- netty's client endpoint rejects a server whose first frame is not a SETTINGS without the
  ack flag. `WindowedH2Upstream` now holds its ack of the relay's SETTINGS until it wrote
  its own SETTINGS (its preface); a test peer writing the ack first (a raw stub) breaks the
  netty leg at once (`PROTOCOL_ERROR`; seen, not pinned by a test: the route's next stream
  then opened a new leg).
- README: the netty waiting paragraph says no more than the tests show (refusal at the wait
  deadline, the buffer left on a client reset or the leg's end); fold it into this pass.
