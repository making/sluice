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
  grants); for h1 the test reads 64KiB per second under a 10s stall timeout.
- No test pins the write pieces (`ConnectionLimits.KeepAliveDeadline#WRITE_PIECE`): a reader
  taking 16KiB to 64KiB per stall timeout is cut without them, which needs window reopenings
  finer than 64KiB to show.

## Direction

Run the h1 slow-reader case where the receiver announces reopened windows promptly (a Linux
TCP stack: a Testcontainers client, or CI on Linux): one 16KiB read per second under a 2s
stall timeout is not cut, and fails with the pieces removed (one write per 64KiB read).
Alternatively a relay-level test with a sink accepting bytes at a set rate.
