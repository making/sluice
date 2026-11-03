Difficulty: Medium

# sluice-wicket-cf-api: `cf app` shows the instances alive, not the limit

`cf app` says `N/N`, `RUNNING` with no VM alive: `stats` (`views.rs`) gives one entry per `instances`, all in the
state of the app's outcome, whatever the wicket runs. With `cf scale` a limit (todo 215) and VMs or instances
coming and going below it, that says nothing, and the wicket knows the count (`GET /api/routes`, `vms.alive`).

- an entry per instance alive as `RUNNING`, its `index` the one `cf logs` names, so the two lists agree; what
  the others are: CF has `DOWN` and `STARTING`, and a limit not reached is neither failed nor starting
- **a scale to zero is not a failure**: `cf push` and `cf restart` wait for a `RUNNING` instance, so `warm: 0`
  with none alive must still finish them, and show as healthy in `cf app`. Read what the CF CLI v8 waits for
  (its source, and `tests/cli.rs` runs the OSS CLI) before choosing the states
- a rolling deployment's wait (`--strategy rolling`) the same
- the wasi wicket: what its listing gives of the instances alive (a pool's warm ones, a request's own), and
  what a component gone by the time it is read shows
- the wicket's listing is read per `cf app` or kept from the controller's poll: not a request per instance,
  and an unreachable wicket stays `STARTING` as now

The test: the scale steps of `tests/cli.rs` on both wickets: `cf app` before and after a request reaches an
app kept at none, seen failing first. The README's "instance state" row and "How long a VM lives" in step.
