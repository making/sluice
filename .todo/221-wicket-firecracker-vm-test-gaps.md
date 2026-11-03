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
