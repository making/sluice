Difficulty: High

# sluice-wicket-runtime-firecracker: a scale keeps the VMs that are running

On CF, `cf scale -i 3` to 4 starts one instance and touches none of the three. Here, read from the code and not
yet run: `RouteSpec::same_vms` (`route.rs`) compares the whole spec, `vms` and `warm` among it, so a changed count
is a target restarted (README, reloads: "a cloned route's template booted and snapshotted again"): a new
template, a new snapshot, every VM replaced, for a change of a number. `target()` carries the same options to the
server, so the advertised target changes as well.

- first see it on a real wicket: a pool, `cf scale -i` up and down, the VMs' ids and `cf logs` across it
- a change of `vms`, `warm`, `conns`, `keep-ms`, `uses`, `lifetime-ms`, and what else only says how many or how
  long, is applied to the running target: same snapshot, same VMs, new ones cloned for an increase, and for a
  decrease the VMs past the limit going as `keep-ms` has them go, open connections finishing
- a VM's own spec (`mem`, `vcpus`, the rootfs, `env`, `egress`) still restarts the target
- the advertised target stays or changes in step with this: a server's dial must keep reaching the route
- `shared` to `pool` (`-i 1` to 2 with no annotation) has no snapshot to keep, so it replaces the VM: if
  that is what is left of a scale disturbing a running app, weigh a `pool` of one against the snapshot's
  memory, or a shared VM staying as instance 0. The user wants the ideal spec and does not hold back a change
  for what exists today
- the same question on the wasi wicket (`sandbox.pool`, `instances` changing gives a new target; a request
  open stays on it, and both caps count until it ended, `README`): a scale should not rebuild the pool

The test: `tests/vm.rs`'s pool steps (a VM's id and process survive a change of the count), and the scale steps of
`sluice-wicket-cf-api/tests/cli.rs`, seen failing first. Do this after todo 215.
