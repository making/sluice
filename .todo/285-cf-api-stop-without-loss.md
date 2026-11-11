Difficulty: Medium

# sluice-wicket-cf-api: a stop ends no request under way, and `/healthz` says when it is ready

Of one instance, for its upgrades today; 282 builds the rest for several on it. Needs nothing.

`lib.rs` `serve` is a `select!` of `api::run` and `shutdown`: a SIGTERM (an upgrade: the wicket
stops the old VM, README "Operating it") drops every request under way -- an upload, a start
waiting for its health check, an edit between its put to a wicket and its commit -- and aborts
the controller's watch mid-put. The handlers run in tasks of their own (`serve_connection`), so
some go on until the runtime is dropped, their answer lost. Nothing unauthenticated says
"ready" but `/` and `/v3/info`, which answer before the wickets are listed.

## Design (to confirm)

On SIGTERM (and stdin's end), within `--stop-grace` (a default below the wicket's stop of a
VM: check what `/init` and the wicket give a process after SIGTERM, and a disk's 20s):

1. the listener closed: no new connection
2. the requests under way finished and answered, with `connection: close`; idle connections
   closed; the held ones (a log read, `STAGING_WAIT`) answered at once
3. the controller's puts under way finished, no new one begun; the state written
4. past the grace, as today

- a build under way: left to 286 (taken up by the next start); without it, failed as today
- `GET /healthz`, no token: 200 once the state is read and the wickets listed once (or found
  not answering), 503 before and from the stop on
- the drain of the accept loop in `sluice-wicket-server` if the wickets' listeners take it too;
  else here

## Tests

`lib::tests`, extending the listener tests there: a request under way at the stop is answered
whole, a new connection refused, a stop past the grace ends; `/healthz` 503 then 200. Measure
an upgrade's gap again (`.kb/wicket-cf-api-findings.md`: 0.7-0.8s of no API) with a `cf push`
under way.
