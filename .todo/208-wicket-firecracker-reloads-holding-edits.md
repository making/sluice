Difficulty: Medium

# sluice-wicket-runtime-firecracker: what still holds the edits for a boot

An edit's VMs start in a task (`routes::Ahead`): no other edit waits for them. Two reloads are still
made within the reloader's loop, a never-listening process holding every edit `boot-ms` long:

- a VM on a disk replaced (`Way::Swap`): the old VM stopped, then the new one started. In a task the
  route is `stopped()` meanwhile, which `tend_restarts` would start again on the same disk: it must
  know of the swap, and the way back on a failure (`Reloader::back`) has to come along
- a reload of the files or on SIGHUP: `Reloader::run` calls `reload` in place

Also: a pool's fill failing for room (`Warm::fill`, its failure a string) reads as its VMs not
starting (`Refused::DidNotStart`), so a controller does not try it again as the routes change. `fits`
takes most of these cases before any start.

Test: `never_listening_app` in `tests/vm.rs`, on a disk app and through a config file.
