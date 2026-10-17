Difficulty: Low

# h2 demux: passing 0 as the credit at the admitLocked call site survives the tests

Left from 142 (2026-10-17). `NettyHttp2DemuxRelayShareTest` pins `share` / `atShare` /
`pastBlockShare`, but a mutation that passes 0 instead of the connection's charged credit
where `NettyHttp2DemuxRelay` calls `admitLocked` is not detected: no test admits a block on a
connection whose charged credit alone pushes it past `BLOCK_SHARE`. An E2E attempt failed to
build the exact byte counts (socket buffers absorb >2MB; `MAX_CONCURRENT_STREAMS` = 100 hits
first).

## Work

- Pin the call site by changing the conditions of an existing test (e.g. one of the
  `H2DemuxConnectionShareE2ETest` / `H2DemuxBudgetE2ETest` cases, with smaller socket buffers or
  a stalled upstream) so that a connection's credit makes the admission decision. It must
  fail with the credit argument replaced by 0. Add a new test only if no existing one can
  reach it; if the cost outweighs the gap, close this item as cancelled with the reason.
