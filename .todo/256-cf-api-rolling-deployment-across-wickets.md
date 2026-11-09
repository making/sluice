Difficulty: High

# sluice-wicket-cf-api: rolling deployments across wickets

After 252. `POST /v3/deployments` takes the new snapshot and puts it at once: the wicket swaps the app in place
(the new target starts before the old drains), and the deployment is `FINALIZED` / `DEPLOYED` at once
(`create_deployment`, `views::deployment`); every `actions/...` (cancel, continue) is refused.

Over several wickets "at once" starts the new droplet everywhere together: no share is checked before the
next, and none keeps serving the old droplet while the others take the new one.

## To do

- A rolling deployment goes a wicket's share at a time, or `options.max_in_flight` instances (`cf push
  --strategy rolling --max-in-flight N`): the next once the put returned (a Firecracker wicket's when its VMs
  started, a wasm wicket's when its route loaded) and, with a readiness check, once the new instances are
  ready (259: CF takes old instances down once the new are "healthy and routable"). `ACTIVE` / `DEPLOYING` meanwhile, `FINALIZED` / `DEPLOYED`
  at the end, which `cf push --strategy rolling` waits for
- A share refused: the deployment stops, the shares not reached on the old snapshot; decide what it shows
  (CC: `ACTIVE`, the new instances crashing, until canceled)
- `cf cancel-deployment`: the shares reached back on the previous droplet, `CANCELED`. A new deployment
  supersedes one under way (`SUPERSEDED`)
- `--strategy canary` (`--instance-steps`, `PAUSED` until `cf continue-deployment`), which several wickets make
  possible: decide whether here or later
- The state holds the progress and which snapshot each wicket's share runs; a restart of the cf-api takes it
  up. `cf restart --strategy rolling` is the same with the current droplet; `cf push` without a strategy stays
  a stop and a start, as on CF
- README Mapping (rolling deployment), Behavior

## Tests

The CLI test's two wickets: `cf push --strategy rolling` of a new droplet puts one wicket, then the other, both
listeners answering throughout; a droplet that does not load stops it with the second wicket on the old one;
`cf cancel-deployment` puts the first back.
