Difficulty: Medium

# sluice-wasmlet: free a store whose client left mid-body while the guest stalls

After the response head, a departed client is noticed only when the relay's
next send fails (`Outcome::Uncertain`). A guest stalled mid-body (awaiting a
host future, producing no frame) keeps its pooled unit -- or its cold store
-- until `budget-ms` (default 10s).

The body drop cannot signal it: hyper drops the body as soon as a
`content-length` is satisfied while the guest still owes its trailers, so
the client guard is disarmed at the head (`Awaited::response` in
`wasm_host.rs`).

Options:

- tell an early drop from a finished body: wrap the relayed body and track
  end of stream (`is_end_stream`, or bytes vs `content-length`)
- signal from the connection: `Ctrl::Abort` / tunnel close cancels the
  in-flight requests of that connection (h1: the one request; h2: per
  stream reset)

Acceptance:

- a hello route that stalls after its first chunk; the client aborts
  mid-body -> the unit is replaced well before the budget (pool and cold)
- `pooled_units_serve_repeated_requests_without_reinstantiating` keeps
  `spawned == 1` (a satisfied `content-length` is not a departure)
