Difficulty: Medium

# sluice-wicket-cf-api: an instance stops without a request lost, and says when it is ready

Needs 278, 280, 285. Part of 275-284. 285 is the stop of one instance (the requests under way
finished, `/healthz`); this is what several add: steps 2 and 4 below, and who takes an
instance out of the routing.

## What holds it to one instance

- `lib.rs` `serve`: `select!` of `api::run` and `shutdown`: a SIGTERM drops the listener and
  every request under way (an upload, a start waiting for a health check, a held log read) and
  aborts the controller's watch mid-put. Fine while a stop is an outage anyway; an update of
  one instance of several must lose nothing
- there is no readiness: whoever routes to it (the wicket's `/init` takes the bound listener
  for ready; a sluice server relays to every wicket advertising the host) sends requests before
  it has read the store, and after it began to stop. Nothing unauthenticated answers "ready"
  but `/` and `/v3/info`
- the builds, operations and the lease it holds end with it, taken over only after their
  leases expire (278, 280)

## Design (to confirm)

On SIGTERM (and stdin's end), in order, within a bound (`--stop-grace`, below the wicket's
stop timeout for a VM):

1. not ready (`GET /healthz` 503; where the wicket routes by the listener, stop accepting: the
   listener closed, the connections kept)
2. the lease released (278): the next leader takes over before the requests end, an edit under
   way here is forwarded to it (279)
3. the requests under way finished; idle keep-alive connections closed after their answer
   (`connection: close`); the long ones (a log read held, a job poll) answered at once
4. the work it owns handed over (280: leases released, a build on a wicket left running for the
   next owner), what cannot be handed over finished within the bound

- `GET /healthz`: 200 once the store answers and, the leader, once it listed the wickets; no
  token. How a Firecracker wicket and the sluice server take an instance out before it stops is
  284's: check whether a wicket's stop of a VM (SIGTERM, then its drain) already stops routing
  to it first
- `sluice-wicket-server`'s accept loop gets the drain if the wickets' listeners want it too

## Tests

`lib::tests`: a request under way at the stop is answered, a new connection refused, the lease
released before the listener is gone; a stop past the bound ends. The CLI test of 284: `cf push`
across a stop of the instance it talks to.
