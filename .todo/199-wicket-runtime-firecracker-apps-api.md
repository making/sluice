Difficulty: High

# sluice-wicket-runtime-firecracker: the console's apps API, a rootfs built on the wicket

The Firecracker wicket's console (README, "Console") deploys an uploaded ext4 image as a
route of its own. What `sluice-wicket-cf-api` deploys through on the wasm wicket is not
there: its apps API (`PUT /api/apps/<name>`, `GET /api/apps`, `PUT /api/components/<sha256>`
by digest, an owner, a tag, an env; the wasm README's "Apps"). A `cf push` built by
buildpacks is a CNB image, not an ext4 file: the cf-api needs the image turned into a rootfs.

- the apps API as the wasm console's: hosts served by one rootfs in one set of options,
  put and removed by its owner, its upload by digest first (`uploads/<sha256>.ext4`,
  `UPLOAD_GRACE`, linked below `rootfs/` when an app names it: `Blobs::materialize` in
  `sluice-wicket-console` does it already), an env passed to the guest (how: the guest's
  `/init` takes its process's environment from where? a file on a second drive, the kernel
  command line is too short), each app's VMs its own (its target names it, as the wasm
  one's `?app=`)
- a rootfs from an OCI / CNB image on the wicket: `scripts/cnb-rootfs.sh` does it with
  docker and sudo; `mkfs.ext4 -d <dir>` builds one without root (e2fsprogs), the layers
  pulled from a registry and unpacked as the wicket's user (ownership: `-d` keeps the
  files' uids only as root; `mke2fs -E root_owner` / a tar-to-ext4 writer of one's own),
  the guest's `/init` added. Decide: on the wicket (`PUT /api/images` naming a registry
  reference, or a docker-archive upload), or in the cf-api before it uploads (the cf-api
  then needs no docker either)
- then `sluice-wicket-cf-api` deploying to this wicket: its integration test against it
  where `/dev/kvm` is usable

Also: the page's flow (the intake, the deploy form, the row editors, the drag and drop) is
alike in both consoles' scripts but copied; `kit.js` shares only the helpers, the requests,
the progress / outcome views and the long poll. Share the flow too, the wicket's script
supplying its fields (the sandbox, the options) and its row cells -- verified in a browser
(the environment of todo 198 had none; a jsdom harness compared the wasm page's rendering
before and after the split: `.todo/artefacts/199-wicket-runtime-firecracker-apps-api/`, its
README says how to run it and what it misses).

Tests: the apps API in the VM test (an app of two hosts on one rootfs uploaded by digest,
its env seen by the guest, kept across a restart), unit tests of its persistence as the
wasm wicket's `deployed.rs` has them.
