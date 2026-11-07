Difficulty: High

# sluice-wicket-runtime-wasi: a store serving several requests at once

Start from consideration: decide whether the wicket gives up "a store, a request", and in what shape, then implement
what is decided (or record why it stays as it is).

## Now

A store takes one request at a time: a pooled unit's job loop (`wasm_host.rs`, `Pool`) and a per-request instance
alike. The limit is the wicket's, not wasm's: `wasi:http` p3's handler is an async export, which the component model
lets an instance run concurrently unless the guest sets backpressure, and `Store::run_concurrent` drives several tasks
in one store. What builds on one request per store:

- the budget: one epoch deadline per store, re-armed per request; an overrun stops the store
- a trap, a failed conversion, an interrupted relay, a departed client retire the store
- `memory-mib` caps a store's memory; growing past half of it recycles a unit
- a guest's stdout / stderr lines name the request they were written during and its `instance` index (live access
  log, `cf logs`)

And a store is driven by one task: its requests interleave on one core.

## Wanted

A route option `concurrency=N` (default 1, today's): a store takes up to N requests at once; one more starts another
store, within the route's `instances` (then counting stores, not requests). sluice-wicket-cf-api maps
`wicket.sluice.ik.am/concurrency` to it, so a CF instance is one store, as on CF and Cloud Run, and `memory` becomes
what an instance holds, not each request's.

## To weigh

- A guest reentrant-unsafe across concurrent calls (module state shared mid-request): opt-in per route, default 1.
- The budget per request in a shared store: a host timer cancelling the request's task (its future dropped?) vs the
  epoch deadline, which stops the whole store. What wasmtime 50 lets a host cancel inside `run_concurrent`.
- A trap ends every request of the store: which status the others get, and whether that is acceptable for N > 1.
- Output attribution: a line written while several requests run names which one? (task-local context in the host's
  stdout impl, or the store's index alone).
- Throughput: one core per store; measure against today's store per request (`.kb/wicket-wasi-findings.md`, the
  workers table) before choosing a default.
- Todo 244 (`keep-ms`, a pool growing and shrinking): with this, the units it grows and ends are stores of N requests;
  its slots / idle / reload logic carries over.
- Docs: the wasi README (options, "Instance pool", live access log), the cf-api README "Instances" (the 16 requests
  table, `memory`), `.kb`.
