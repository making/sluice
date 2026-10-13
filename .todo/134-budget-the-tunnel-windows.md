Difficulty: High

# Budget the tunnel connection windows instead of reserving them per connection

Split from 132 (2026-10-14).

## Problem

Since 132 every connection reserves its virtual connection's worst case at accept,
2213KiB (`ConnectionLimit.CONNECTION_BYTES`: the 1MiB receive window plus slack, the 1MiB
send queue cap, two 64KiB relay buffers). That is now what limits production: 178.9MiB of
connection memory on the 409MiB heap holds 82 connections, while a held connection is
143-228KiB of live heap (106). Every connection reserves the full window whether it moves
data or idles.

## Direction

- The server announces each virtual connection's receive window in `Connect.connection_window`
  (sluice-client and sluice-wasmlet honor it already). Announce a small initial window and
  grant more (WindowUpdate past what was consumed) only against a node-wide budget, charged
  while granted or held, released as the relay drains it: what the remote may send is
  memory, so the credit itself is what has to be charged, idle connections included.
- Charge the send queue past a small allowance to the same budget (the pump waits for room).
- Then `connectionBytes()` becomes the allowance, and the derived limit grows by the ratio
  (hundreds at 409MiB); the budget's size decides how many connections get the full 1MiB
  window at once (throughput at a 20ms tunnel RTT: 1MiB/RTT).
- Measure with the 106 harness: throughput at 20ms RTT with many connections, heap.

## Tests

- `ConnectionLimitsTest` for the new derivation; a stack test with many slow-reading h1
  connections keeping the receive buffers within the budget (gauges); the window grants
  bounded by the budget; throughput of one connection unchanged when the budget has room.
