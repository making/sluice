Difficulty: Medium

# Stall timeout: write-side progress pinned only coarsely

Found while working on 071. The stall timeout sees a client's reads only as the relay's
writes to it complete, which needs the client's TCP stack to reopen its window. On macOS
loopback (16KiB MSS, autotuned receive buffers) a client reading slowly with a full receive
buffer reopens it only when the sender's persist probes find it: the writes resume 4 to 5s
apart at 64KiB read per second, and the gaps grow past 15s at 16KiB per second (measured with
a plain socket pair, and in `StallTimeoutSlowReaderE2ETest`). So:

- The 071 item "a client reading one 16KiB read per second is not cut" is pinned for h2 only
  (`H2DemuxStallTimeoutE2ETest#aClientGrantingSlowlyButSteadilyIsNotCut`, flow-control
  grants); for h1 the test reads 256KiB per second under a 10s stall timeout.
- No test pins the write pieces (`ConnectionLimits.KeepAliveDeadline#WRITE_PIECE`): a reader
  taking 16KiB to 64KiB per stall timeout is cut without them, which needs window reopenings
  finer than 64KiB to show.

Linux loopback is no better (measured later with the same plain socket pair, writer in 16KiB
pieces): a blocked write resumes only once the reader drained about 900KB, 12 to 15s apart at
64KiB read per second, 3 to 4s apart at 256KiB per second. `StallTimeoutSlowReaderE2ETest`
now reads 256KiB per second for that reason. So neither macOS nor Linux loopback shows window
reopenings finer than 64KiB.

## Direction

A relay-level test with a sink accepting bytes at a set rate (no kernel socket buffers in
between): one 16KiB read per second under a 2s stall timeout is not cut, and fails with the
pieces removed (one write per 64KiB read). Possibly a small `SO_SNDBUF` on the relay side
plus a small `SO_RCVBUF` on the client narrows the loopback gaps enough; untested.

Done together with 108 (which replaces `StallTimeoutSlowReaderE2ETest` by this relay-level test).
