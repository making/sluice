Difficulty: High

# wickets: a disk budget for the console directory (free space, total, per owner)

Nothing checks free space (`statvfs`) or bounds a console directory's total.

- Firecracker (`rootfs/`, `uploads/`, `builds/`, `builders/`, `disks/`, `caches/`, layers): an image is
  4 GiB at most each (`console.rs`), but their number is not bounded; a persistent disk's `fallocate`
  (`disk.rs`) is the only real reservation; a build's scratch is sparse (297). Caches, buildpack layers
  and builder rootfs are swept by age only (7 days, `stage.rs`); an app's disk outlives the app until an
  explicit DELETE (`deployed.rs`).
- wasi: a files tar is 1 GiB (as much again unpacked, `files.rs`), a component 128 MiB, any number of them
  within the 10 minutes an unnamed upload lives (`sluice-wicket-console/src/store.rs`). keyvalue's sqlite
  bounds a namespace (64 Mi), not their number nor the file.

`GET /api/disks` lists each disk's `allocated_bytes` (its file's blocks: the whole size, a disk being
`fallocate`d) and `used_bytes`, what its guest's file system uses while a VM runs on it; an app's VM
lists the latter as `usage.disk_bytes` (291).

To decide: one budget in `sluice-wicket-console`'s `store` both wickets use (bytes reserved before an
upload / image / disk / scratch is written, released on sweep), a floor of free space refused below, a
share per owner, eviction of caches by size when the budget is short, and the budget in the listing's
identity (the cf-api places by memory only today, per the wickets' side; 305).

Acceptance: an upload / disk / build that does not fit is refused with what is free; store tests extended;
both READMEs.
