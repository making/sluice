Difficulty: Low

# h2 demux: passing 0 as the credit at the admitLocked call site survives the tests

Left from 142 (2026-10-17). `NettyHttp2DemuxRelayShareTest` pins `share` / `atShare` /
`pastBlockShare`, but a mutation that passes 0 instead of the connection's charged credit
where `NettyHttp2DemuxRelay` calls `admitLocked` is not detected: no test admits a block on a
connection whose charged credit alone pushes it past `BLOCK_SHARE`. An E2E attempt failed to
build the exact byte counts (socket buffers absorb >2MB; `MAX_CONCURRENT_STREAMS` = 100 hits
first).

## Work

- Pin the call site: a test, at whatever level reaches it (a relay-level unit test with an
  embedded channel, or an E2E with smaller socket buffers / a stalled upstream), where a
  connection's credit makes the admission decision. It must fail with the credit argument
  replaced by 0.
