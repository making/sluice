Difficulty: High

# sluice-wicket-runtime-firecracker / sluice-wicket-tunnel / sluice-wicket-cf-api: an instance's readiness decides where its route's connections go

On CF an instance's readiness check decides its routing: its route is advertised once the check passes after
its start and removed while it fails, the instance not restarted (a liveness check, `health-check-type`,
restarts it). The manifest alone sets it -- `readiness-health-check-type` (`process` by default, `port`, `http`),
`readiness-health-check-http-endpoint` (`/`), `readiness-health-check-invocation-timeout` (1s),
`readiness-health-check-interval` (30s by the health checks page) -- the CLI having no command for it (8.19:
`get-readiness-health-check` alone). A rolling deployment takes old instances down once the new ones are
"healthy and routable".

## Now

- The cf-api drops a manifest's `readiness-health-check-*` keys as unknown ones; the process shows
  `readiness_health_check: process` whatever was asked, and a stats entry's `routable` is its being `RUNNING`.
  Of the liveness keys only `timeout` acts (a Firecracker wicket's `boot-ms`)
- A Firecracker wicket counts a VM up once its guest accepts on the process's port, and advertises a route
  whatever its VMs: with none up and starting again (a process that keeps exiting, a snapshot gone, a boot
  failing) its connections fail, a booted route's at once (`guest unreachable: no VM up: the route is
  restarting`), a pool's after its `wait-ms`
- Several wickets serving a host (252), the server sends each its share whether or not its instances can serve;
  a wicket restarted takes requests once it advertised, at once under `smallest-client-id` when its id is the
  smaller: an app accepting before it serves (warming a cache, waiting for its database) fails them

## To do

- A Firecracker route's readiness options (type, endpoint, invocation timeout, interval): each VM it keeps
  probed from its start, `port` a connection to the process's port, `http` a `GET` of the endpoint answered
  200, as CF's. Decide where a probe runs: the wicket through the guest's relay, or `/init` in the VM reporting
  over its wire (`guest/src/wire.rs`, a copy of `src/wire.rs`), as Diego runs its check in the container
- A VM not ready takes no new connection (a pool picks among the ready ones). At `warm=0` (no VM kept ahead
  of the connections) the readiness check is ignored or refused, either one (the user's call; refusing, by
  the cf-api at the manifest and at the annotation's edit, is `manifest.rs`'s rule: what does not apply is
  refused, not ignored). A VM made for a connection already waiting (a pool's past `warm`, `per-connection`)
  takes it unprobed: decide whether it is probed after
- A route with no VM ready (none alive and down, or every one failing its check) leaves the advertisement on
  every node stream (the table re-advertised without its hosts) until one is ready; a route starting with the
  wicket is advertised once one is. The local listener: ready VMs only, else as now
- The server: a host no client advertises any more falls through as a stopped app's does (a pattern, the
  catch-all, else 404): check it, say so
- The listing: whether each instance alive is ready, which the cf-api shows as `routable`
- The cf-api: the `readiness-health-check-*` keys (of the app and of its `web` process, as the liveness keys)
  kept, shown (`cf get-readiness-health-check`, the process's `readiness_health_check`) and put as the route's
  options. A wasm wicket: `process` alone (an instance there is a request's or a pooled unit's: `port` means
  nothing, `http` would be a request of its own to a warm unit): the others refused naming why, or decide
  otherwise
- Liveness on the same probes (`health-check-type: http`, a VM failing it restarted as in A VM going down):
  decide whether here
- Used by 256 (a share's next step once its new instances are ready) and 261 (a wicket ready once its routes
  have a ready instance). Todo 154 is the same lever by hand (a gate per client)
- READMEs: the cf-api's Mapping (the health check rows), the Firecracker wicket's route options and A VM going
  down, the tunnel's advertisement

## Tests

`tests/vm.rs` (where KVM is): a VM whose `http` endpoint fails takes no connection, and with none ready the
route leaves the advertisement the tunnel test server (`session::testing`) sees, advertised again once it
answers; a process that exits likewise. The cf-api's manifest tests: the keys kept and shown, a wasm wicket
refusing all but `process`. `the_cf_cli_deploys_to_a_firecracker_wicket`: `cf push` of a manifest with a
readiness check, the process stats' `routable` false until the endpoint answers.
