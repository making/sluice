Difficulty: Medium

# h2 demux: a stream waiting for an upstream's concurrency limit waits with no deadline

Left open by 083. A stream beyond its upstream's SETTINGS_MAX_CONCURRENT_STREAMS waits in
the relay until a stream of its leg closes on the upstream (`Http2DemuxRelay.Leg#waitingLocked`).
As long as the upstream keeps its streams open, or announces a limit of 0, the waiting
streams and their queued bytes stay: nothing bounds the wait. Same class as an upstream
that never answers an open stream (no bound either).

Since 083 the bytes are bounded while the client's connection window is withheld: the
waiting streams then hold at most the connection's allowance, the most recent of them
refused (`Http2DemuxRelay#refuseWaiting`, `H2DemuxWaitingStreamBudgetE2ETest`). While the
budget has room they keep their share of it for as long as they wait. The stall timeout
(071) counts a waiting stream only while its client holds window to send its request
(`requestDueLocked`); a waiting stream whose client spent its window is not counted.

## Direction

User decision: the policy and the default.

- A wait deadline per waiting stream: past it the stream is refused (`REFUSED_STREAM`;
  its upstream never saw it, RFC 9113 8.7), its queued bytes released.
- Or one upstream response timeout covering an open stream without a response and a
  waiting stream alike (an error response or `RST_STREAM` for open streams,
  `REFUSED_STREAM` for waiting ones); keep it consistent with 081's error scope.

## Tests (`sluice-it`; watch each fail first)

- Upstream limit 1 holding stream 1 open, stream 3 waiting with DATA queued: past the
  deadline stream 3 is refused, the route's queued bytes and tracked streams drop, nothing
  of stream 3 reached the upstream, and stream 1 still completes.
- A waiting stream that opens before the deadline is not refused.
