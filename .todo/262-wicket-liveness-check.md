Difficulty: Medium

# sluice-wicket-runtime-firecracker / sluice-wicket-cf-api: a liveness check on the readiness probes

After 259 (its probes). On CF the liveness check (`health-check-type`, `-http-endpoint`, `-invocation-timeout`,
`-interval`; `cf set-health-check`) restarts an instance failing it, and is its startup check as well: an
`http` instance is started once its endpoint answered 200 within `timeout`, a `port` one once its port
accepts, a `process` one at once.

## Now

- A Firecracker wicket starts a VM once its guest accepts on the route's port (a `port` startup check
  alone), and restarts one only for what ends or refuses (README, A VM going down): a process that lives,
  keeps its port and answers nothing, or answers its health endpoint 500, serves on. A readiness check
  (`readiness=port|http`) takes such a VM out of the rotation, never restarts it
- The cf-api keeps the liveness keys and shows them; of them only `timeout` acts (`boot-ms`). The
  manifest's `health-check-interval` is dropped as an unknown key

## To do

- A route's liveness options (`liveness=port|http`, its endpoint, timeout, interval): `http` the start's
  check too (a booted VM up, a template snapshotted, once the endpoint answered 200 within `boot-ms`), then
  probed as the readiness check is (`readiness::probe`, a VM's probe task), a VM failing it gone down as in
  A VM going down (`VmPool::went_down`, the restart's backoff). Decide how many failures in a row restart one
  (Diego: one), and whether a clone per connection is probed at all
- `process` (no port asked): what a wicket relaying to a port makes of it (refused, or `port`)
- The cf-api: `health-check-type` / `-http-endpoint` / `-invocation-timeout` / `-interval` put as the route's
  liveness options, `cf set-health-check` (a restart takes it, as on CF); a wasm wicket's: decide (an
  instance there is a request's or a pooled unit's)
- READMEs: the cf-api's Mapping to VMs (the liveness rows), the wicket's route options and A VM going down

## Tests

`tests/vm.rs`: a VM whose `http` liveness endpoint fails restarted, its route served by the others meanwhile;
a start whose endpoint never answers failing at `boot-ms`. The cf-api's manifest tests, and `cf
set-health-check` in `the_cf_cli_deploys_to_a_firecracker_wicket`.
