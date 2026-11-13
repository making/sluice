Difficulty: High

# sluice-wicket-runtime-firecracker: the memory budget counts what the host really spends

`--max-memory-mib` defaults to none (`capacity.rs`: `u64::MAX`) and is never compared with the host's
memory. It counts `memory-mib` per VM and nothing else:

- not Firecracker's overhead (the jail's `memory.max` adds `32 + mem/128 + 16` MiB, `keeper.rs`; the
  budget does not)
- not a snapshot's memory file, shmem on the state dir's tmpfs (the jail charges a template `2 x mem`;
  the README has the operator add it by hand and size the tmpfs)
- clones count in full though they share the snapshot's pages (safe, but it is the other half of the
  same arithmetic)

Host pressure has no answer: no balloon, `memory.high`, `memory.swap.max`, hugepages, nor an
`oom_score_adj` keeping the wicket alive over a VM ("the kernel kills firecracker or the wicket", README).

To decide: a default budget from the host (MemTotal or the wicket's own cgroup limit, less a reserve),
overhead and snapshots inside it, the tmpfs checked against the snapshots it must hold at startup and
reload (`fits`), `oom_score_adj` so a VM goes before the wicket; whether a balloon is worth it (measure
first; record in `.kb/wicket-firecracker-findings.md`).

Acceptance: a configuration the host cannot hold is refused at startup / reload with the arithmetic in the
message; `capacity.rs` and `routes.rs` (`fits`) tests extended; README's sizing section replaced by what
the wicket now does itself.
