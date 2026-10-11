Difficulty: Medium

# wasmlet: bound the guest instances in flight

Found while working on 048. `--max-connections` caps the wasmlet's connections, which
bounds the buffered request bytes (N x 1MiB windows). Each request in flight also holds
a fresh guest instance whose linear memory may grow to the route's `memory-mib` (default
256MiB), and one h2 connection carries many requests at once. Nothing bounds the
instances in flight, so the guest memory is unbounded in the number of concurrent
requests (README "--max-connections" says so).

## Direction

A per-process (or per-route) cap on instances in flight: a request beyond it waits for a
slot within its budget, or is answered 503. Relate it to hyper's h2
`max_concurrent_streams` so one connection cannot take every slot. Read the pool
(`Pool`, `pool=N`) first: pooled units already bound their own count.

## Tests (`cargo test`; watch each fail first)

- With a cap of 1, a second concurrent request waits (or gets 503) while the first runs,
  and completes after it.
- The cap is released when a request ends by its budget, by the client leaving (the
  server's ERROR; its CLOSE is a half-close), and by a guest trap.
