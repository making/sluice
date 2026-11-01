Difficulty: High

# sluice-wicket-firecracker: the jailer

Firecracker's production guide runs every VM through its `jailer`: a chroot of its own,
cgroups (cpu, memory, pids), a new PID and mount namespace, a user without privileges, and
Firecracker's seccomp filters (on by default already). The wicket spawns `firecracker`
itself (`src/vm.rs`: the launcher mode, `posix_spawn`, the parent death signal), as a user
in the `kvm` group, without any of it.

- The jailer starts as root and drops to `--uid` / `--gid`: decide the privilege model
  (the wicket as root, a setuid helper, or root in a container: with `.todo` 182) and keep a
  VM from outliving the wicket: the parent death signal is cleared for a fork's child, and
  the jailer forks for `--new-pid-ns` (`--daemonize` detaches): check what else holds
- Each VM's files then live in its chroot (`<chroot-base>/firecracker/<id>/root`): the
  rootfs and kernel hard-linked or bind-mounted in, the snapshot's memory file shared by the
  clones of a route (today one file mapped private by every clone), the vsock and API
  sockets below 108 bytes
- cgroups: the VM's memory limit against `mem` (plus Firecracker's overhead), so the budget
  (`src/capacity.rs`) holds on the host and not by count alone
- Measure what it costs a clone's restore (today about 10ms) and the clone rate
