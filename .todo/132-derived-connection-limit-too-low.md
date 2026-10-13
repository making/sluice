Difficulty: High

# The derived data-plane connection limit is 34 in production since the 1MiB windows

Found verifying production after 127 / 130 (2026-10-14).

## Problem

127 raised the h2 demux windows to 1MiB, so the per-slot worst case
(`ConnectionLimits.connectionBytes`: `ConnectionLimit.CONNECTION_BYTES` +
`NettyHttp2DemuxRelay.UNBUDGETED_BYTES` + `RESPONSE_QUEUE_BYTES`) grew from 3340KiB to
5261KiB and the derived limit from 54 to 34 for production's 409MiB heap. The limit counts
every data-plane connection, h1 and TLS passthrough included. Production log:

    data plane connection limit 34 (derived: (half of max heap 409MiB - h2 queue budget) /
    5261KiB per connection), per address 34, ...

Observed: 40 concurrent `curl https://httpbin2.apps.sluice.ik.am/get` (40 TCP connections)
-> 34 x 200, 6 x `SSL_ERROR_SYSCALL` (access log `type=conn event=reject reason=limit`).
Before 127 all 40 passed. A held connection is 163-228KiB of live heap (106), so the
derived bound reserves 20-30x what connections use; the reservation is static per slot
whether the connection is h1 or h2, idle or busy.

## Directions

- Stop reserving the h2 worst case statically per slot: charge what a connection actually
  holds past a small fixed allowance to the shared budget (or a second budget), so the slot
  cost is the small part every connection needs (h1 / passthrough buffers, the allowance)
  and the large windows are covered by the budget and the per-connection share (130).
  Re-derive what is still unbounded by the budget (withheld arrivals, hop queues) and
  whether it can be charged or bounded otherwise.
- Or a separate, larger limit for h1 / passthrough connections and an h2 slot limit for the
  costly part (an h2 connection takes an extra slot only while it holds past its allowance).
- Keep the memory bound provable (the derivation stays in `ConnectionLimits`, tested), and
  say in README what the limit becomes for 409MiB.
- A rejected connection is closed during the TLS handshake (`SSL_ERROR_SYSCALL` for the
  client); consider whether an HTTP 503 after the handshake is better for TLS-terminated
  routes (separate decision; note it if out of scope).

## Tests

- `ConnectionLimitsTest` for the new derivation (409MiB -> the new number), and a stack test
  showing the memory bound still holds with many slow-reading h2 connections at the new
  limit (heap or queued-bytes gauges stay under the derived bound).
