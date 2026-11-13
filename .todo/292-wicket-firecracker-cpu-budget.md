Difficulty: High

# sluice-wicket-runtime-firecracker: a CPU budget beside the memory one

`Capacity` admits a VM by count and memory only (`capacity.rs`: `Need { memory, route }`). vCPUs
overcommit without bound: `--max-instances` x `vcpus`, never compared with the host's CPUs.

- `cpu=N` (cgroup `cpu.max`) is the only cap, jail only, and opt-in per route.
- VMs without `cpu` share one `uncapped/` group with no cpu controller (`jail.rs`): no `cpu.weight`, no
  cpuset, so one busy VM competes with every other and with the wicket itself as an equal.
- A build VM is `cpu: None` always (`stage.rs`).
- Known and not covered by a cgroup: x86_64's `kvm-pit` thread (README).

What a VM takes is measured now (291): its `firecracker`'s CPU time and its rate over 5s, every
thread, in the listings' `usage` (`usage.rs`), the evidence to size the overcommit by.

To decide: the unit (vCPUs, or `cpu` percent where set), `--max-cpus` with the overcommit ratio the
operator states, what the wicket reports in its listing's budget (`identity`) for the cf-api's placement
(305), and a default share (`cpu.weight` by vCPUs) so the wicket's own threads keep theirs. The .kb file
records that a cpu group per VM cost 10% of the clone rate: measure before giving every VM one.

Acceptance: a VM beyond the CPU budget waits or is refused as one beyond the memory budget is;
`capacity.rs` tests extended (not new ones); the VM test pins the weight; README and .kb updated.
