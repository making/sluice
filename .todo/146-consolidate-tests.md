Difficulty: Medium

# Reduce the test count: merge and parameterize redundant tests

Requested 2026-10-17: the test suite keeps growing (maintenance cost, CI test job ~6 min).
Cover behaviors by varying the conditions of existing tests rather than adding new ones,
and bring the total down.

## Work

- Inventory the tests per area (`sluice-it` E2E classes first: each class starts a full
  server + client stack, so classes cost the most), listing what each one proves.
- Merge tests that prove the same behavior or differ only in a parameter (JUnit
  `@ParameterizedTest`, one stack shared by several cases where isolation allows), and delete
  ones fully covered by others.
- Keep every behavior covered: for each removed test, name the test that still fails
  without that behavior. Do not weaken a test's preconditions pinning.
- Report the count of test classes / methods and the CI test job time before and after.

## Also (folded in from a cancelled follow-up of the h2 credit work)

A mutation passing 0 instead of the connection's charged credit where `NettyHttp2DemuxRelay`
calls `admitLocked` survives the tests (`NettyHttp2DemuxRelayShareTest` pins only the pure
`share` / `atShare` / `pastBlockShare`). While merging the `H2DemuxConnectionShareE2ETest` /
`H2DemuxBudgetE2ETest` cases, vary the conditions of one (smaller socket buffers, stalled
upstream) so a connection's credit decides the admission and the mutation fails it.

## Also: remove benchmark-type assertions

Tests must not assert throughput, latency, elapsed time or relative speed (they depend on
the machine). Find such assertions (round trips counted against wall-clock delays as in
`TunnelTransportWindowE2ETest` / `H2DemuxWindowRoundTripE2ETest`, MiB/s or "within N ms"
checks, throttled-upstream rate expectations) and replace each with a deterministic
counter / protocol-state assertion (window granted, bytes in flight, grants per window), or
delete it and keep the evidence in the `.todo/artefacts/106` harness.
