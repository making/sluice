Difficulty: Medium

# sluice-wicket-tunnel / both wickets: a wicket's readiness not held by one app's

After 259 and 261. A wicket's `GET /ready` answers 503 while any route of its table has no instance ready
(`Routes::readiness`: `no instance ready for <host>`). A route's readiness is its app's: a Firecracker route
whose every VM fails its `readiness=http` check (a user's app answering 500 on its endpoint, waiting for a
database that is down) keeps the whole wicket not ready, however many other routes it serves.

What waits on that probe stalls with it, and only a user can fix the app:
- `docker compose up -d --force-recreate --wait wicket-a` (the cf-api README's update of several wickets) times
  out, and the next wicket is never updated
- a StatefulSet's rolling update (`OrderedReady`) waits on the pod forever. Its Service and the headless DNS
  (`publishNotReadyAddresses: false`) drop the pod as well, so the cf-api's `--wicket` URL of that pod stops
  resolving, and the cf-api can no longer fix the app on that wicket

On CF a cell's readiness does not depend on the apps it runs: a crashing app is the app's state, not the
cell's.

## To do

- Decide what of the routes the wicket's readiness waits for. Candidates:
  - each route once (a VM ready since the wicket's start), or until a bound (`--ready-grace`) after which a
    route still without one is logged and left out
  - only the routes that were ready before the wicket stopped (their state in the console directory)
  - none: the routes started and every node acknowledged, a route's own readiness left to its advertisement
    (259 already withdraws a route with no ready instance)
- `/ready`'s body still names the routes without a ready instance (an operator's view), whatever decides
  the status
- READMEs: both wickets' Readiness probe, the Firecracker wicket's Readiness, the cf-api's update of several
  wickets

## Tests

sluice-wicket-tunnel's readiness test (`ready.rs`): a route with no instance ready past what the decision
allows leaves the wicket ready, and the body names it. The Firecracker VM test's readiness section to match.
