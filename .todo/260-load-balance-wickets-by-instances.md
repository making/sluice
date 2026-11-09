Difficulty: High

# sluice server / sluice-wicket-tunnel: spread a host over the wickets serving it, by their instances

A host several clients serve goes to the client of the smallest id (`sluice.http-load-balance`, default
`smallest-client-id`): with 252's spread one wicket serves, the others stand by. `round-robin` (per node) and
`random` spread it, but server-wide (every route, the Java clients' too) and evenly: `-i 3` over two wickets
(2 + 1) gives the wicket of one instance half the requests, and a wicket whose share is full (a wasm route's
`instances` x `concurrency` busy: a 503 after the budget) still gets its turn.

## To weigh

- A weight per advertised upstream (`Upstream.weight`, 0: none): a wicket sends its app's instances; the server
  picks by weight (smooth weighted round robin) among the candidates carrying one, by its strategy among the
  rest. An app's numbers name no target (a PUT changing them advertises nothing today): a weight change is
  advertised on its own
- The fewest requests in flight per candidate (the server knows its virtual connections per client), alone or
  weighted
- A strategy per upstream rather than server-wide
- Session affinity (the gorouter's `JSESSIONID` / `__VCAP_ID__`): in scope or not; within a wicket a pool
  spreads the connections without it today
- Each node picks on its own (round robin is per node already)

## Acceptance

`RouterTest`: weighted picks, the unweighted unchanged; the wicket's advertisement test: the weight follows a
PUT changing the app's `instances`. The root README (Cluster's routing, `sluice.http-load-balance`), the
cf-api's Compose sample of 252.
