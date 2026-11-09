Difficulty: Medium

# sluice-wicket-runtime-wasi / sluice-wicket-cf-api: give an instance's first request the start's timeout

A component's initialization (rontolisp's top level, say: Shiori creates its schema and sample data
there) runs in the first request an instance serves, under that request's `budget-ms` (10s by default).
Overrun, the store is stopped, its work is rolled back, and the next request starts over: on an empty
database Shiori built by rontolisp `6686836` (15.6s of first start) never starts (`504` after 10s, every
time; `.kb/shiori-on-wasi-wicket.md`), unless `wicket.sluice.ik.am/budget-ms` raises every request's budget.
Since rontolisp `442dca5f2` its first start takes 0.32s; reproduce it with a top level running past 10s.

On CF the start has its own bound, the health check's timeout (`cf push -t`, a manifest's `timeout`,
60s by default, 180s at most). The Firecracker wicket maps it to `boot-ms`; on the wasi wicket the
health check is "accepted and shown, no effect".

Give a new instance's first request (a warm unit's, a spare's, a per-request instance's) a budget of
its own, a route's option, and have the cf-api map the health check's timeout onto it, so `cf push -t`
bounds the start and `budget-ms` the requests after it. Decide what an instance whose first request
overruns it shows (see todo 249's listing). Pin it in the wicket's budget tests and the cf-api's mapping
test; update both READMEs and the .kb file's first start section (`timeout:` in place of the annotation).
