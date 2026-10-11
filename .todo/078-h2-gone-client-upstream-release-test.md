Difficulty: Low

# h2 demux: the gone-client test checks the upstream's half-close, not its release

Found while working on 064. `H2DemuxUpstreamFlowControlE2ETest#clientGoneWhileAResponseIsOwedEndsTheRelay`
asserts that the upstream read EOF. The demux half-closes the leg (CLOSE) before its PING
probe notices the client gone and ends the relay (ERROR), so that EOF is the half-close,
not the release. Until 064 the Java client ignored an ERROR that followed a CLOSE and
kept the upstream socket open; `StreamRelay` now tears down on it (unit test
`StreamRelayTest#remoteErrorAfterItsCloseTearsTheRelayDown`, E2E for h1 in
`HalfCloseProbeE2ETest`), but no h2 test shows it.

## Test (watch it fail first)

In that test, also await the client's `sluice.connections.active` gauge reaching 0 after
the client closed; with `StreamRelay`'s `onRemoteFailure` registration removed it stays 1.
