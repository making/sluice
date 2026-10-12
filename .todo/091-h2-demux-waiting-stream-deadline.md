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

## Decision (2026-10-12)

- A wait deadline per waiting stream, not a shared upstream response timeout. Waiting for a
  slot is a relay-internal queue the upstream never saw, so `REFUSED_STREAM` is always safe
  for the client to retry; a response timeout on open streams would cut legitimate long
  polls and streams, which the stall timeout deliberately never counts. No response
  timeout for open streams is added.
- Setting under `sluice.limits.*` (e.g. `h2-stream-wait-timeout`), default 30s, `0`
  disables. Count refusals as `sluice.h2demux.streams.refused{reason=wait-timeout}`.
- An upstream announcing a limit of 0 is covered by the same deadline.
- Do it after 081 so the error scope matches.

Since 081: a waiting stream is also refused (`REFUSED_STREAM`) when its leg ends --
upstream closed, failed or made a connection error (`Http2DemuxRelay.Leg#fail`) -- and when
its upstream sends a GOAWAY (`Http2DemuxRelay#onUpstreamGoAway`; a GOAWAY'd leg opens no
waiting stream any more, `Leg#mayOpenLocked`). The deadline refusal follows the same
pattern: marked reset (`resetLocked`), its entries dropped (`Leg#dropEntriesLocked`) and
the RST_STREAM written as one step under the sink lock (`WriteSeam.Site.RESET`).
