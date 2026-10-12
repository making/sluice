Difficulty: Medium

# sluice-wasmlet: make the instance pool observable

The pool counters (`units` / `idle` / `spawned` / `served` on `Pool` in
`wasm_host.rs`) are read only in tests. In production an exhausted pool
falls back to the per-request path silently, and recycling shows up only
as per-event stderr lines, so `pool=N` cannot be sized from evidence (how
often requests went cold, how often units were recycled and why).

Options:

- a periodic per-route summary log line (served / cold fallbacks /
  recycled by reason), only while it changes
- report the counters to the server over the tunnel and show them on the
  console's wasm route (proto change)
- a local metrics endpoint (wasmlet has no HTTP listener today)

The instance limit (`--max-instances`, `Instances` in `wasm_host.rs`) is
silent too: requests that waited for a slot and the 503s of those that found
none within their budget are neither logged nor counted, so the limit cannot
be sized from evidence either. The tests read only `Handler`'s gauge (live /
peak / created stores) and `Instances::held`.

The tunnel stall watchdog (074, `--tunnel-stall-timeout`) only logs each node stream
it gives up: the wasmlet neither exports the time a stream has stalled nor counts the
streams given up, which the Java client and server do (`sluice.tunnel.stall` /
`sluice.tunnel.stall.failures`). Cover them with the rest.

Decide with the user before implementing. Acceptance: the cold-fallback
and recycle counts per route, and the instance-limit waits and 503s, are
visible without a debugger; a test asserts them for exhaustion, for a
recycle and for a 503; the stall time and the streams given up are visible too, and a
test asserts them for a stalled stream.
