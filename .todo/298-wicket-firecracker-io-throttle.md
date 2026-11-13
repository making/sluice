Difficulty: Medium

# sluice-wicket-runtime-firecracker: throttle a VM's disk IO and egress

- Disk: `/drives` is put without a `rate_limiter` (`vm.rs`), `cache_type: Writeback`, and the cgroups
  enable `+memory +pids` (`+cpu` when capped) but no `io` (`jail.rs`): a VM writing its disk or scratch
  takes the device and the host's page cache from the others.
- Egress: 256 connections per VM and timeouts (`egress.rs`), no bandwidth nor byte bound.

To decide: Firecracker's drive `rate_limiter` (works without the jail) against cgroup `io.max` (jail or a
delegated subtree, 293; buffered writeback needs the memory controller beside it), route options
(`disk-iops`, `disk-mbps`, `egress-mbps`) and their defaults, a token bucket in the wicket's egress relay.
Measure the limiter's cost on the clone path; record in the .kb file.

Acceptance: the VM test shows a throttled VM held to its rate; `egress.rs` test for the bucket; README.
