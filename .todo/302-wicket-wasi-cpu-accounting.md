Difficulty: High

# sluice-wicket-runtime-wasi: CPU time per request and a share between routes

A request is bounded by wall clock only (`budget-ms`, epochs of 10 ms). No CPU time is counted (no fuel,
no thread CPU clock), nothing shares the workers between routes, and the process sets no affinity nor
worker count (only `TOKIO_WORKER_THREADS`). The README lists fuel under what is not there. 042 is the
worker a spinning guest holds; this is the accounting and the share once it yields.

To decide: measure (291) before limiting; CPU time per request from the epoch callback's ticks while wasm
runs or `CLOCK_THREAD_CPUTIME_ID` around polls; a share per route (weights, or a cap of workers a route
holds at once) against fuel's cost (measure; record in `.kb/wicket-wasi-findings.md`).

Acceptance: a route spinning on every core leaves another route's latency within a stated bound; a
request's CPU time in the access log; tests beside 042's.
