Difficulty: High

# h2 demux: bound the queued bytes across connections

Found while working on 048. The connection limits (`sluice.limits.max-connections`)
bound the data plane's buffering only for connections that hold one virtual connection:
the derived default fits 2.125MiB per connection (window + send queue cap + relay
buffers) in half the max heap. A plaintext or TLS-terminated h2 connection counts as one
slot but can hold far more:

- its demux queue: up to `Http2DemuxRelay.MAX_QUEUED_BYTES` (about 25MiB: 100 streams x
  (128KiB payload budget + two 64KiB header blocks + framing));
- one leg (virtual connection, 2.125MiB) per route it reaches.

With the derived limit the h2 worst case is about 12x what the heap budget assumes, so
an anonymous peer opening h2 connections whose upstreams do not read can still exhaust
memory. README "Connection limits" states the gap. Since 053 the relay grants the
client each stream's budget whatever the upstream grants, and holds the data until the
upstream's window admits it: any upstream that stops granting (not only one granting
ahead of reading) fills its streams' budgets.

## Direction

A budget shared by every demux relay of the node (for example a fraction of the heap
budget), charged as bytes are queued for a leg and released as the leg writer drains
them. While the budget is exhausted the relay withholds stream window grants (the client
stops sending), instead of ending connections. Keep the 051 rules (header blocks atomic,
priority lane, per-stream budget) and the per-connection backstop. Also decide whether
legs count against the connection limit.

## Tests (`sluice-it`; watch each fail first)

- With a small budget, two h2 connections whose upstream does not read stop at the
  budget in total (assert on `sluice_h2demux_queued_bytes`), and both complete once the
  upstream reads.
- A third connection on another route keeps flowing while the budget has room.
