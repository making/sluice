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

Decide with the user before implementing. Acceptance: the cold-fallback
and recycle counts per route are visible without a debugger; a test
asserts them for exhaustion and for a recycle.
