Difficulty: Medium

# netty h2 demux, step 8: the remaining per-hop test rewrites and README

## Plan change (2026-10-13, the user)

Production is a test deployment; breaking changes are fine, and no code is written just to
keep two engines side by side. New order: 102 -> 107 (remove the legacy relay now, netty
only) -> 103, 104, 105 (in parallel, each re-enables its disabled tests on netty) -> 106.

Depends on 107 (re-enable or delete the tests 107 disabled for per-hop rewrites; complete the README). Can run in parallel with 103 and 104.

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

## Notes from 102 (flow control and budget, 2026-10-13)

- `@LegacyOnly` with per-hop reasons in the flow-control and budget classes (each a
  rewrite or a keep-legacy-until-107 decision for this step):
  - netty's codec answers frames on a stream it forgot or closed with a reset of its
    own (088 decision): `H2DemuxClientWindowE2ETest#zeroStreamWindowIncrementResetsOnlyThatStream`,
    `#zeroIncrementOnAnUntrackedStreamIsIgnored`, `#zeroIncrementOnAStreamClosedBothWaysIsIgnored`,
    `H2DemuxBudgetE2ETest#aStreamWhoseResetWaitsForTheUpstreamIsResetOnceAndRelaysNothing`
    (also: netty relays no unknown frame).
  - legacy mechanisms with netty counterparts in `NettyH2DemuxBudgetE2ETest`:
    `H2DemuxBudgetE2ETest#upstreamFramesOnAStreamTheRelayResetNeverReachTheClient`
    (end-to-end windows, PING fence), `#clientGoneAsTheRelayWritesItsResetReleasesWhatTheRelayHeld`
    (`WriteSeam`), `H2DemuxClientWindowE2ETest#trailersPastTheAllowancePauseTheLegUntilTheClientGrants`
    (per-leg trailer allowance), `H2DemuxLegWriterE2ETest#idleLegWriterSleeps` (leg writer
    thread), `#clientWindowUpdatesCoalesceWhileQueued` (no relayed window updates).
  - behaviour change: `H2DemuxLegWriterE2ETest#upstreamPingFloodEndsItsLeg` -- netty
    pauses reading such an upstream (the hop's read pause) instead of ending its leg.
- Behaviour changes for README, each pinned by a `@NettyOnly` test (the user's call: a
  rewritten test is netty-only rather than branching by engine):
  - netty grants a connection window back once half of it is spent, exactly what was
    spent (`H2DemuxFlowControlE2ETest#paddedDataIsReleasedInFullAndRelayedUnpadded`,
    `#upstreamConnectionWindowIsReleasedForDataOnly`,
    `H2ReroutingE2ETest#grantsConnectionWindowWhileRelayingRequestData`);
  - padding is not relayed (`H2DemuxClientWindowE2ETest#paddedFramesSplitAcrossTheClientWindowKeepTheAccounting`);
  - an upstream ignoring the relay's windows passes the stream window first: the stream
    is reset `FLOW_CONTROL_ERROR` towards the upstream, no GOAWAY
    (`H2DemuxClientWindowE2ETest#upstreamIgnoringTheRelayWindowEndsItsLeg`);
  - the relay holds a response up to one stream window per hop, granting half of it back
    once taken (`H2DemuxClientWindowE2ETest#awaitWaiting`, `HELD_RESPONSE`).
- Test peers must send SETTINGS first (netty's client endpoint): the offence handlers
  of `H2DemuxUpstreamFlowControlE2ETest` now call `settings(null)` first; a request with
  a body must not set END_STREAM on its head (`H2ReroutingE2ETest` had, the legacy relay
  ignored the DATA after it).
