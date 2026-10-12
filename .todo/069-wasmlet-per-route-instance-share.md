Difficulty: Medium

# wasmlet: a per-route share of the instance slots

Found while working on 059. `--max-instances` is one pool of slots for every route
(`Instances` in `wasm_host.rs`); a connection may hold half of them. Busy routes (many
connections, or slow guests) can hold every slot, so the other routes' requests wait and
end in 503 although their own load is light (README "Known limitations").

## Direction

A route option, e.g. `instances=N` (a cap on the route's slots) and / or a reservation
taken at startup like `pool=N` is. Decide with the user which one, and how it relates to
`pool` (pooled units already hold slots of their route).

## Tests (`cargo test`; watch each fail first)

- Two routes on `--max-instances 2`, one route capped at 1: with that route's slot held,
  a request on the other route is served at once.
- The route's cap and the pool: a route with `pool=1` and a cap of 1 never creates a
  per-request store.

## Decision (2026-10-12)

(a) A per-route cap `instances=N` (a ceiling on the route's slots, counting its pooled
units). No separate reservation: `pool=N` already reserves slots for a route. A cap below
the route's `pool` is a startup error. Unset = no per-route cap (today's behavior).
