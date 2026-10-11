Difficulty: Medium

# h2 demux: one stalled leg must not stall the client connection (041 child 3/6)

Parent: `.todo/041-tunnel-head-of-line-blocking.md` (Finding F7). Depends on 043 and 044:
they make a leg's writes block per connection, which exposes this. It must land before
046 (or with it), because 046 makes leg writes wait for credit. No protocol change.

## Problem

`Http2DemuxRelay#pumpOutbound` is one parse loop for the client's h2 connection. It
writes each frame inline to its leg (`Leg#relayFrame` -> `VirtualConnection` sink ->
`SessionSender`). Only after that write does it grant the client's connection-level
window (`grantClient`). A blocked write on leg A stalls the parse loop: frames for every
other leg stop, and the client's connection window is no longer refilled, so the whole
client connection stalls.

## Design

- Each `Leg` gets an outbound queue and its own virtual thread (`sluice-h2demux-leg-out-*`)
  that performs the `connection.sink()` writes. The `upstreamWriteLock` users
  (`relayFrame`, `writeToLeg`, the preface in the `Leg` constructor) all go through that
  queue, so the frame order on a leg is the enqueue order.
- `pumpOutbound` enqueues and calls `grantClient` right away. The bytes are now buffered
  by the relay, which is when an h2 receiver legitimately releases connection window.
- Bound: per-stream buffering is limited by the upstream's stream windows, which the
  client honors because stream WINDOW_UPDATEs pass through verbatim. Add a hard cap per
  leg (e.g. 4MiB of queued bytes) against clients that ignore stream windows. Overflow is
  a client flow-control violation: `abort()` the relay.
- Failures: a write failure on the leg thread is treated as one in `pumpOutbound` today
  (`abort()`). Leg-thread exit takes part in `pending` / `settle()` like the inbound
  pumps. `halfClose` and `close` are queued after the pending frames (half-close) or
  bypass the queue (close).
- `ReroutingRelay` needs no change: h1 writes one leg at a time and a stall there is per
  connection by design.

## Tests (`sluice-it`; watch each fail first)

- Two routes on one h2 prior-knowledge client connection (`TestH2`), each served by a
  different `TunnelClient`, so F1 (still present until 046) does not couple them through
  one tunnel stream:
  - route A's upstream accepts and never reads; the test sends a large POST on route A
    until it plateaus (pin: bytes accepted > 044's per-connection cap + socket buffers);
  - a GET on route B on the same connection completes within 5s.
- The per-leg cap: a raw h2 client that ignores stream windows and pushes past the cap
  gets the connection closed. Assert the close, and that the server's memory did not grow
  past the cap (count the queued bytes via a package-private accessor or a metric).
- The existing `H2ReroutingE2ETest` and `H2cDataPlaneE2ETest` stay green.
