Difficulty: High

# wickets: measure what a VM / an instance uses (CPU, memory, disk) and show it

Do this first: the other resource items (292-304) size limits that nothing measures today.

Neither wicket reads actual usage. `GET /api/routes` and `/api/apps` show configured values and counts only.

- Firecracker: no read of `cpu.stat`, `memory.current` / `memory.peak` / `memory.events`, `io.stat` or
  `/proc/<pid>`; the only measured values are a snapshot's memory file blocks (`route.rs`, the route's
  status) and a rootfs's bytes. `GET /api/disks` shows a disk's file size, not what the guest uses of it.
  A VM the kernel's OOM killer took looks like any exit (`memory.events` unread; nothing in the guest
  reports its own OOM kills either).
- wasi: `MemoryCap.used` (`wasm_host.rs`) feeds only the recycle at half the cap; `Gauge` (live / peak /
  created stores) is `#[cfg(test)]`. No CPU time per request or route.
- The cf-api's process stats answer zero usage (`views.rs`), as reported by the wickets' audits; the
  cf-api's side was not audited (305).

To decide: where the numbers go (the apps' listing per instance, so `cf app` shows them; the route status;
the flume; a metrics endpoint: 034 weighs the same for the pool's counters), and what a jail-less
Firecracker wicket reads (`/proc/<pid>/stat`, `smaps_rollup`) where it has no cgroup per VM (293).

Acceptance: per VM / instance CPU time, memory in use and its peak, disk used; an OOM kill named as one.
Tests in each wicket's suite (the VM test for the cgroup's numbers); READMEs updated.
