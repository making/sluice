Difficulty: High

# sluice-wicket-cf-api: an edit on any instance

Needs 275, 278. Part of 275-284.

## What holds it to one instance

- an edit is tried against the wickets before its commit (`api.rs`: `Tried`, `try_shares`; "an
  edit a wicket refuses is answered 422 and changes nothing"): routes, `cf scale -i`, a start,
  a restart. The handler puts the shares itself, holding `Controller::hold` (an app, a route)
  and `hold_share` (an app on a wicket): locks of the process
- so an edit on an instance that is not the leader (278) writes to the wickets beside it, with
  no epoch of its own, and two edits of one app on two instances interleave their puts and
  commits
- a start waits for the app's health check timeout in the request; `cf push`'s polling
  (`/v3/jobs`, the process stats) may reach any instance

## Design (to confirm)

Two ways; decide with a measurement of the first:

1. the leader applies: an edit's handler asks the leader to try and commit it (an internal
   endpoint between instances, the lease naming the leader's address; authenticated by a key of
   the store) and relays the answer. One writer to the wickets, the locks stay in a process,
   the 422 as today. Costs a hop, an address every instance reaches the others by, and a retry
   when the leader changes mid-edit (the edit is idempotent or its trial undone: `Tried::put`
   goes back on a refusal already)
2. desired state only: the handler commits what is wanted; the leader converges and the outcome
   shows in the stats and the log, as a start's failure does today. No hop, but routes and
   `instances` no longer refuse at once (the README's and `api.rs`'s contract, and what the CLI
   prints on `cf map-route` / `cf scale`)

Recommended: 1, as it keeps what the CLI shows; 2 only for what already applies later.

- every wicket write outside the controller goes the same way: `staging.rs` (`PUT /api/images`,
  `/api/layers`, `POST /api/builds` are not app writes: they stay on the instance staging, see
  280), `services.rs` / `security_groups.rs` where they put
- a request an instance cannot forward (no leader within a bound) is answered 503 with
  `Retry-After`; the CLI retries a 503? Check v8.19.0, else hold the request for a takeover

## One instance (275's rule)

The leader is the instance itself: an edit is tried and committed in the process as today, no
hop, no address of its own to configure.

## Tests

The API tests with two `Api`s on one store: an edit through the follower refused by the mock
wicket is a 422 and changes nothing; two edits of one app through both, serialized; the leader
stopped mid-edit, the edit answered once by the next.
