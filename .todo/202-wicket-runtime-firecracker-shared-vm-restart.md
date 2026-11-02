Difficulty: Medium

# sluice-wicket-runtime-firecracker: a shared VM whose process died starts again

A shared VM's process ending leaves the VM up and its route serving nothing until a reload replaces
it: `guest: <program> exited` on the wicket's stderr is all that says so, and a PUT of the same app
answers `no change` (the VM is up), so it takes another env. For the cf-api in a VM that is the API
down, the apps serving on (its README, "a crash of the cf-api").

A pool's VMs are replaced when they fail, so `mode=pool&vms=1` is the way out today, which a disk
rules out: a disk needs `mode=shared` (one writer; a clone's snapshot holds its guest's page cache of
the disk). The wicket's README lists it under Not yet.

To settle:

- what tells the wicket: `/init`'s line on the log stream, or a failed dial, and what a process that
  stops on purpose is
- the replacement as a reload does it (`apply` in `routes.rs`): with a disk the old VM stopped first,
  its `flock` gone, the new one on the budget the old one held
- a process failing at once: a backoff rather than a loop of boots, and what the console shows
- what a route's connections see meanwhile

The test: `tests/vm.rs`'s disk steps, the sample guest ended on request, its data still there.
