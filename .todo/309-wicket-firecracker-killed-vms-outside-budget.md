Difficulty: Medium

# sluice-wicket-runtime-firecracker: a VM killed is out of the budget before it is gone

A VM gives its slot back as it is killed (`vm.rs`: `Vm`'s drop), not as it is reaped. Its end
takes KVM's teardown, which waits for an RCU barrier (`kvm_mmu_uninit_tdp_mmu`), one VM after
another over the whole host. Under a storm of clones (a VM per connection, warm=64, c=64, 1,880/s
on the 64-core host) the VMs killed and not gone pile up: 2,100 after 10s, 200 more each second,
as many tasks (`pids.current`), their kernel memory (stacks, slab: 111 MiB by then) charged to
the wicket's own cgroup, where the forker forks them; all gone within 2s of the storm's end
(findings' "The memory budget"; `.todo/artefacts/294/storm.sh`). The same before 294.

So `--max-instances` does not bound the `firecracker` processes: a client opening connections for
minutes grows them (pids, kernel memory beside the memory budget's reserve).

To decide: the slot held until the VM is reaped (the clone rate then follows the teardown's, about
1,700/s here: measure), or the dying counted apart, with a bound of their own; whether the teardown
can be made cheaper (`kvm.tdp_mmu`; `kvm.nx_huge_pages=never` spares a recovery thread per VM);
jailed alike (the keeper reaps).

Acceptance: the processes bounded under a storm (the VM test, or `capacity.rs`'s); findings' rates
by mode measured again; README's Limits.
