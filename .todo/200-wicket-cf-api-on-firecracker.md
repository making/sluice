Difficulty: High

# sluice-wicket-cf-api running on sluice-wicket-runtime-firecracker

Serve the cf-api as an app of the Firecracker wicket it deploys to, so a host with KVM needs one
container: the wicket. It runs today, but loses its state with its VM, and then removes every app
it manages. Most of this item is giving a VM a disk that survives it.

## Measured (spike on the debug wicket, 2026-11-02)

`.todo/artefacts/200-wicket-cf-api-on-firecracker/spike.sh` runs this again.

The image of `sluice-wicket-cf-api/Dockerfile` in a local registry, `PUT /api/images`, then an app
(owner `ops`, shared mode, `mem` 256, `egress` to the console, the options in
`SLUICE_WICKET_CF_API_*` env):

- works: rootfs made in 6.2s; `cf api`, `cf login`, `cf push --docker-image` through the VM
  (3.7s), the pushed app listed by the wicket under owner `cf`. The API's URLs follow the `Host`
  header; the port is the image's `EXPOSE 9091`; `localhost:<console port>` as egress reaches the
  console. Owner `ops` keeps the cf-api's own app out of its reconciling
- the default `--data-dir` fails: `Read-only file system (os error 30)`, and the PUT answers 422
  `guest not listening on vsock port 8080 within 10s`. `/tmp` (tmpfs) works
- a restart of its VM (another env) loses `state.json`: `fix` finds the wicket's apps of owner `cf`
  unknown and deletes them (`CELL: removed from the wicket: no such app`), and the CLI's token is
  refused (`Invalid Auth Token: bad signature`: the signing key was in the data directory too)

Not tried: the flume and an OpenID Provider through egress (names, TLS), the cf-api's own TLS
behind the tunnel, `cf push -p` of a large zip (a tmpfs is the guest's memory, and droplets are
read whole), a wicket restart, jail mode (no `sudo -n` here).

## To do

1. **An empty state must not delete apps.** A bug on any host, VM or not: a lost or new
   `--data-dir` removes every app of the owner. Refuse to delete what a state that never knew them
   did not put (an id of the state in the owner or tag, say), and say so in the log. Failing test
   first: a cf-api on a fresh data directory, a wicket holding apps of its owner.
2. **A VM's persistent disk**: a route option naming a writable drive kept in the console's
   directory, made by the wicket (`mkfs.ext4`), mounted by `/init` at a path for the image's user
   before the process starts (the kernel's command line says where, as `wire::ENV_PARAMETER`).
   Decide, measuring:
   - one writer: a reload starts new VMs before it drops the old ones, and a removed target serves
     on until acknowledged, so two VMs would mount one drive and corrupt it. It needs a stop before
     the start, and then a failed start can no longer keep the old VMs serving: what does it fall
     back to?
   - shared mode only: a clone's snapshot holds the guest's page cache of the drive, and clones
     cannot share it. Refused with the reason in the other modes
   - jail mode: the keeper opens the rootfs and kernel by fd (`keeper.rs`, `jail.rs`): a third file,
     writable by the VM's id alone; the file size limit and the budget of a route
   - its lifecycle: kept across a restart and a change of image, removed with the app or apart,
     its size, a crash's fsck, a backup
   If this is too much, the alternative to weigh is the cf-api keeping its state through an API of
   the wicket's, on the console's directory: no drive, no change to the guest, the cf-api's alone.
3. **The cf-api's use of it**: `--data-dir` on the disk; what is read whole (droplets up to 160 MiB)
   against a VM's memory; `/v3/info`'s description ("deploys wasm...") names the runtime.
4. **A documented setup and a test**: how the cf-api's app is put (the console token and the egress
   it needs, flume and provider included, that its owner must differ), upgrading it, a wicket
   restart. The cf CLI test of `tests/cli.rs` run against the cf-api in a VM, a restart of that VM
   keeping the apps.
