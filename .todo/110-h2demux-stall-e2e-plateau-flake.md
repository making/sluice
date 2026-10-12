Difficulty: Medium

# H2DemuxStallE2ETest#stalledLegKeepsTheOtherLegsFlowing: awaitOutboundPlateau flake

CI run 38279790937 (head df46a3a, a test-only change unrelated to this test) failed once:
`H2DemuxStallE2ETest.stalledLegKeepsTheOtherLegsFlowing:160->awaitOutboundPlateau:274`
ConditionTimeout (30 s). A rerun of the failed job passed.

Find why the outbound byte count does not reach a plateau within 30 s on a slow runner
(timeline of the counter first), then make the precondition deterministic.
