Difficulty: High

# sluice-wicket-cf-api running on sluice-wicket-runtime-firecracker

Serve the cf-api as an app of the Firecracker wicket it deploys to, so a host with KVM needs one
container: the wicket.

## State (2026-11-02)

Done and reviewed: what is left is the CI of its push (`Test the jail`, `Test the cf-api on it`). The
numbers are in the wicket's README, Findings; `.todo/artefacts/200-wicket-cf-api-on-firecracker/`
runs them again (`disk-measure.sh`, `spike.sh`, `one-container.sh [jail]`, `jail-test.sh`).

1. **An empty state deletes no app.** `fix` left the state's own guids to delete: `State::removals`
   holds what the state deleted with the wicket not taking it, all else of the owner is logged and
   left. Staged apart from the rest in the index, for a commit of its own, built and tested so. With
   it, found by its test: a wicket restarted back at the revision it had left its apps `STARTING`
   for good.
2. **A VM's persistent disk: a drive, not an API of the wicket's.** Nothing measured stood in the
   way; an API needs one writer as much (a fencing token), and an asynchronous store in the cf-api,
   for the cf-api alone. What the "Decide, measuring" list came to:
   - one writer: nothing in Firecracker refuses a second VM. A `flock` held by `firecracker`
     (the launcher's, or in jail mode the keeper's, on an open file its exec keeps), gone only with
     the process, 43ms after a SIGKILL. A reload starts every other target first, stops the holder,
     starts the new VM; a failed start starts the old VM again and refuses the edit; failing as
     well, the route serves nothing until the next reload (SIGHUP, or the same PUT again)
   - outage of a restart or an upgrade: 0.7-0.8s of the cf-api; a failed start 11.5s
   - shared mode only, refused with the reason in the others
   - jail mode: a third fd, attached writable in the template, owned by the VM's id per boot (the
     wicket's group keeps 0660); `RLIMIT_FSIZE` the larger of the log's and the drive's (a write past
     the limit ends the VM, whatever the file's size)
   - lifecycle: by owner and app name, kept across restarts, images, options; apart from the app
     (`GET` / `DELETE /api/disks`); grows only (`EXT4_IOC_RESIZE_FS` from `/init`), 8 MiB at least
     (below, no journal); the room taken at making (`fallocate`); no fsck (a journal, replayed in
     60ms); a backup is the file, stopped
   - new against the plan: a VM killed is a crash of its file system, and under ext4's default a
     file written and renamed without an fsync reads as NUL bytes: `/init` mounts
     `nodioread_nolock`, a stop notice (SIGTERM to every process, 10s, sync, unmount) precedes a
     reload's kill, the cf-api fsyncs. A guest flush reaches the host's disk (`Writeback`: 3.6ms a
     save on NVMe, 12ms on SATA)
3. **The cf-api's use of it**: `--data-dir` on the disk by its default (the image's `/data`). The
   item's premise was wrong: droplets are not up to 160 MiB, a zip's unpacked without a bound, into
   memory. An upload, a package and a droplet now go through files: 128 MiB of VM took a 150 MiB zip,
   where 256 MiB lost the process to a 120 MiB one. `/v3/info` names the runtime.
4. **A documented setup and a test**: its README's "On a Firecracker wicket", from the wicket's start
   through the CLI's use, whose commands `one-container.sh` takes from the page and runs, unjailed
   and jailed. `tests/cli.rs`'s Firecracker test goes on to run the cf-api in a VM with a disk, the
   flume and a provider through its egress: a restart keeping the apps and the login, a wicket
   killed right after a push, an upgrade.

Found on the way, and fixed with a failing test first: jail mode ran the wicket at `/`, a relative
`--console-dir` (the default) failing; a PUT replacing an app's image answered `no change`; an upload
to a package a newer one replaced panicked under the state's lock, every later request with it; a
vsock dial had no bound, so a wedged guest held the reloader (and now a stop) for good.

## Left for later

`.todo/201-wicket-cf-api-open-mode-behind-a-wicket.md`: on a VM, the loopback-only rule of `--listen`
guards nothing; whether the cf-api refuses the open mode there. The owner's decision: as it is for now.

`.todo/202-wicket-runtime-firecracker-shared-vm-restart.md`: the cf-api's process dying leaves its VM
up and the API down until a PUT starts it again.
