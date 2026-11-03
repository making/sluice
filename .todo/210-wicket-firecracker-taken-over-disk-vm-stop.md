Difficulty: Medium

# sluice-wicket-runtime-firecracker: a disk's VM whose start is taken over may be killed, not stopped

The wicket's README, "a stop", tells `/init` to end the processes, write out what is dirty and unmount
the disk, for a reload's old VM, the wicket's end, a process's end and a process exiting before it
listens. A VM whose start is taken over (`Reloader::taken_over`, `routes::Ahead`) is not on that list.

Seen on a real wicket: a process counting its starts in a file on the disk with no fsync, 8s before it
listens. A PUT, then a touched `--kernel` and SIGHUP (the disk's VM replaced), then a PUT of the same
app during that start: three starts, the counter at 2. What the VM taken over from wrote did not stay
(`.kb/wicket-firecracker-findings.md`, Disks). That it is killed is read from this; the path has not
been followed in the code.

The file system survives it: a crash to it, the journal replayed. A process that takes long to start,
initializing a database say, loses what it had not fsynced when another edit arrives.

- follow what ends such a VM: an aborted task dropping `Vm` (`kill_on_drop`), or a stop
- settle which: a stop as the others have (the edit taking over then waits up to its 20s), or a kill
  the README says so of; a hold of the disk's `flock` must not outlast either
- the same for an edit taking over from an edit on a disk
- the test: `disk_app` in `tests/vm.rs`, where a never-listening start is already taken over: what the
  taken-over VM wrote is there, or the README says it need not be
