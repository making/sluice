Difficulty: High

# h2 demux leg queue: cap vs compliant clients, header-block atomicity (045 review)

Parent: `.todo/041-tunnel-head-of-line-blocking.md`. Follow-up of 045 (b6d51c2). Do it
before or with 046. No tunnel protocol change.

## Findings

R1. **The 4MiB leg cap tears down clients that honor every window** (confirmed).
045 assumes the stream windows bound a leg's queue and calls an overflow a client
flow-control violation. Only each stream is bounded: the relay advertises no
`SETTINGS_MAX_CONCURRENT_STREAMS` (unlimited), grants the connection window on enqueue,
and passes the upstream's stream WINDOW_UPDATEs through verbatim (any size). A leg's
queue is bounded by the sum of its streams' windows, which has no limit.
Probe: 116 streams on one route, each sending exactly its 65535-byte initial window,
connection window honored, upstream not reading: the queue reaches 4182343 bytes and
`abort()` kills the connection, other routes included. Before 045 the same client was
only stalled. A tunnel slower than the client's upload with many concurrent uploads
(gRPC client streaming, parallel uploads) does the same without a stalled upstream.

R2. **Header blocks are not contiguous on the leg** (confirmed; pre-dates 045, kept by
it). RFC 9113 6.10: nothing may come between HEADERS and its CONTINUATIONs.
`Leg#relayFrames` enqueues each frame separately while `pumpInbound` enqueues connection
WINDOW_UPDATE / SETTINGS ACK / PING ACK on the same queue. Probe: a ~40KiB request head
(HEADERS + 2 CONTINUATION after re-encoding) on a leg streaming a response; a
`WINDOW_UPDATE` on stream 0 lands inside a header block after about 70 requests, both on
b6d51c2 and on b6d51c2~. A real upstream answers with a PROTOCOL_ERROR and the leg dies.
The same holds towards the client (by reading): `Leg#inboundHead` writes the re-encoded
frames one `writeToClient` at a time, so `grantClient`, PING / SETTINGS ACKs and other
legs' frames can land between them.

R3. **Per-connection meter tag** (by reading). The `sluice.h2demux.queued.bytes` gauge
carries `connection=<id>`: one time series per leg ever opened, unbounded cardinality
in Prometheus. Every other meter is tagged by route only.

R4. **Idle polling** (by reading). `pumpLegOutbound` polls every 100ms for its exit
condition, so every idle leg (long-lived h2 / gRPC connections) wakes 10 times a second
for its whole life.

R5. **Control frames wait behind the data** (by reading). SETTINGS ACK, PING ACK and
the connection WINDOW_UPDATE towards the upstream are queued behind up to 4MiB of request
DATA. Before 045 they waited at most for one frame write. An upstream's ping or settings
timeout can expire while the queue drains over a slow tunnel.

R6. Cleanups: the comment in the `Leg` constructor was wrapped by the formatter mid
sentence; `queuedBytes` says "guarded by this" but is decremented outside the lock;
unused `pusher` in `H2DemuxStallE2ETest#stalledLegKeepsTheOtherLegsFlowing`.

Probes: `.todo/artefacts/051-h2-demux-leg-queue-follow-up/`
(`LegCapCompliantClientProbeTest`: R1; `HeaderBlockInterleaveProbeTest`: R2). Copy them
into `sluice-it/src/test/java/am/ik/sluice/it/`.

## Design

- **R1, recommended: the relay owns the stream windows towards the client.** Advertise
  `SETTINGS_MAX_CONCURRENT_STREAMS` (e.g. 128) on the client face. Per stream, hold back
  the upstream's stream WINDOW_UPDATE increments while the stream's queued bytes exceed
  a per-stream limit L, and release the accumulated increment when the leg writer drains
  the stream below L. A slow stream then stalls only itself (finer than 045's per-leg
  isolation), and a compliant client's queue per connection is at most
  `max-concurrent-streams x (65535 + L)`. Derive the hard cap from that, so an overflow
  really is a violation (answer it with GOAWAY FLOW_CONTROL_ERROR, then `abort()`).
  Drop the held increment when the stream ends or is reset; keep the sum <= 2^31-1.
  Rejected: deferring the connection-level grant (one stalled leg drains the shared
  window and the other legs stall again, which undoes 045); a larger fixed cap (the sum
  of windows has no bound).
- **R2**: enqueue a whole header block as one queue entry (or under the leg lock), and
  write a whole response header block under one `sinkLock` hold.
- **R3**: one gauge per route, summing the queued bytes of its live legs.
- **R4**: block on `take()`; wake the writer with a terminal entry on `close()` and when
  its exit condition becomes true.
- **R5**: a priority lane for relay-generated connection-level frames, which the writer
  takes first, never between the frames of a header block (R2) and never before the
  preface.
- Update 048's worst-case memory formula with the new bound (it uses "h2 leg cap 4MiB per
  leg").

## Tests (`sluice-it`; watch each fail on the current code first)

- R1: the compliant-client probe (more streams than the old cap allows, each within its
  window, connection window honored) is not torn down, and its queued bytes stay below
  the derived bound. Pin that the client never exceeded a window (count it in the test
  client). A stream over its window on the same connection still gets the connection
  closed (keep `legQueueOverflowClosesTheConnection`).
- R1 isolation: a stalled stream and a flowing stream on the same leg; the flowing one
  completes.
- R2: the interleave probe in both directions (large request head to the upstream, and a
  large response head to the client while another leg streams), with an upstream / client
  that checks contiguity. 1500 iterations with no violation.
- R4: a package-private counter of writer wakeups; an idle leg for 1s wakes at most once
  or twice.
- R5: an upstream PING sent while the leg queue holds > 1MiB is acked before that DATA is
  written.
- `H2DemuxStallE2ETest`: assert the leg queue gauge is > 0 when the second request is
  sent (pins that the stall reached the queue, not just the socket buffers).
- `H2DemuxStallE2ETest`, `H2ReroutingE2ETest` and `H2cDataPlaneE2ETest` stay green.
