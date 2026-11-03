Difficulty: Medium

# sluice-wicket-runtime-firecracker: what the VM tests leave out of the stops, the take overs and the jails

Reported as not covered when todos 210, 211 and 212 were finished. Extend the existing steps of `tests/vm.rs`; each
seen failing with what it guards undone.

- **a wedged guest taken over** (210): a disk's VM dropped while `/init` does not answer goes from the stop to
  its kill at 20s, with the `flock` released no later: only the normal stop is tested. The next start's wait
  (`Disk::ready`) must still be within `START_WAIT` and `FREE_WAIT` when it takes the whole 20s
- **a reload or a restart taken over** (210): the path dropping `Vm` is shared with the edit's, but only the edit's
  runs on a real VM. Add what a real one needs that the shared path does not already show, and say if nothing
- **an edit taking over a reload, jailed** (211): the step sends SIGHUP to the wicket directly, so it is skipped in
  jail mode. Reach the jailed wicket another way (its pid through `sudo -n`, or a touched file and the poll with
  what `reload_due` needs)
- **a killed pool VM's index, jailed and in an image** (212): `replaced`'s kill is not run in jail mode or in an image

Not by adding jailed variants for their own sake: where a path has nothing of the jail in it, say so here and
drop the step.

## Done

- **a wedged guest taken over**: the "VM never listening" step of `disk_app` runs a second time with an `/init` not
  answering its stop (`STOP_UNANSWERED`, hello mode only: the dial passes, no answer comes). Both modes. The edit's PUT
  takes the stop's whole 20s and is answered 200, which it could not be with the `flock` held past the kill.
  `FREE_WAIT` is shown by that. `START_WAIT` bounds the room, which nothing fills in this test, so it is a compile-time
  assert against `disk::STOP_MOST`, beside one for `FREE_WAIT`.
- **a reload taken over**: already ran on a real VM (`disk_kept`), the `Vm` dropped as for an edit. It lacked what that
  VM wrote: now a file it overwrites, read back after the edit. **A restart taken over**: new in `disk_app`.
  `restarts_end_on` is its own, and nothing ran it on a real VM.
- **an edit taking over a reload, jailed**: SIGHUP to the keeper through `sudo -n`, which also shows it passes it on.
- **a killed pool VM's index**: jailed, `replaced` runs: the keeper reaps a VM it did not kill, and is then asked to
  kill it. The index itself is the wicket's alone. In an image it is dropped: the image is the same binary and the same
  paths, unjailed or jailed, with nothing of an image in them.
- Left unjailed, nothing of the jail in them past the above: the frozen guest and the killed VM of `disk_app`,
  `adjusted_route`'s VM counts.

Seen failing, one at a time: `FREE_WAIT` 15s and a stop given up at 5s (the wedged step), a restart not ended by the
edit, a dropped `Vm` killed without its stop (the restart's read, then the reload's, the assertions before them left
out), jailed: a keeper keeping SIGHUP, an edit not taking over, a pool VM's index not freed.
