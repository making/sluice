Difficulty: High

# sluice-wicket-runtime-firecracker: a VM's CPU / memory / pids limits without the jail

Without `--jail-ids` a VM has no cgroup of its own: `cpu=` is a configuration error (`config.rs`),
`memory.max` / `memory.oom.group` / `pids.max` (`jail.rs`, `keeper.rs`) are not written, and only `vcpus`
and the rlimits (`vm.rs`: FSIZE, CORE, NOFILE) bound it. The README lists "CPU limits without the jail"
under Not yet, and that another member of the `kvm` group starts VMs outside the budget.

To decide: a delegated cgroup subtree (systemd `Delegate=yes`, a container's writable cgroup) the
unprivileged wicket writes itself, sharing the layout and the arithmetic with the keeper's; what it does
where no subtree is delegated (refuse `cpu=` as today, say so at startup).

Acceptance: `cpu`, the memory ceiling and `pids.max` hold on a jail-less wicket with a delegated subtree;
the VM test covers both ways with the assertions it has for the jail; README's host setup updated.
