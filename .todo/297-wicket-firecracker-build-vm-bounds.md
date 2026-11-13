Difficulty: Medium

# sluice-wicket-runtime-firecracker: bound what builds take

A build (`stage.rs`) may ask for up to 32 vCPUs, 1 TiB of memory and 1 TiB of scratch. Its memory is in
the budget (unbounded where `--max-memory-mib` is unset, 294); its scratch is sparse, not reserved and
not counted (`disk.rs`); its VM has no `cpu` cap even jailed; and builds at once are bounded only by the
VM budget they share with the apps (records: 64), so builds can take every slot serving VMs would get.

To decide: builds at once (`--max-builds`, the rest queued), a share of the budget builds may not exceed,
operator ceilings on a build's request (295), the scratch reserved in the disk budget (296), a `cpu`
default for a build under the jail (292).

Acceptance: `stage.rs` tests extended for the queue and a refused request; README's staging section.
