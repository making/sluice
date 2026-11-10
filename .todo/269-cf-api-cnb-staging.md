Difficulty: High

# sluice-wicket-cf-api: `cf push` of source staged by Cloud Native Buildpacks into a rootfs

Today a Firecracker app is an image built elsewhere (`--docker-image`) or an `.ext4` in a zip; a
zip of source fails staging (`no .ext4 rootfs`). Here the cf-api stages it as CF does: a builder's
buildpacks build it, the droplet runs on the stack's base, and the wicket makes the ext4.

## Approach: be the platform, without the exporter

Run only `detector` and `builder` (the build has no registry, no daemon, no cache to restore);
replace `analyzer` with an `analyzed.toml` the wicket writes, and the exporter with an export to a
droplet the platform spec's exporter section defines (launch layers, app dir, launcher,
`config/metadata.toml`, the entrypoint and env rules). The exporter's `-layout` is experimental
and needs the run image as an OCI layout inside the VM: not taken.

A droplet is CF's: a tar of what goes over the stack's root file system, the stack being the
builder's run image pinned to its digest. Each wicket composes the rootfs from it as it makes one
of an image, so a droplet is built once and placed on any wicket without building it again.

```text
cf push (zip) -> cf-api -> wicket: POST /api/builds {builder, run image, source, env}
                             builder's rootfs (PUT /api/images' path, kept by digest)
                             a VM of it, booted to completion: /init's stage mode
                               source -> /workspace, detector, builder (as CNB_USER_ID)
                               out: the droplet tar, the phases' output as lines
                  <- droplet tar (kept by the cf-api, as a zip's rootfs is), the log -> STG lines
cf-api -> each wicket: PUT /api/images {run image, layers: [droplet sha256], launch}
```

## To do

- guest (`/init`): a stage mode. Mount a scratch disk (`disk.rs`, made per build) under `/layers`
  and `/workspace` (the paths the app runs at: buildpacks write absolute paths); unpack the source
  from the wicket over vsock; write `/platform/env/*` (the app's env, as CF's staging env) and
  `<layers>/analyzed.toml` (`run-image` reference and `target` os/arch from the run image's
  config); run `/cnb/lifecycle/detector` then `builder` as the builder's `CNB_USER_ID` /
  `CNB_GROUP_ID`, `CNB_PLATFORM_API` one the builder's `/cnb/lifecycle/lifecycle.toml` supports;
  stream their output; send the droplet tar: the launch layers (`<layer>.toml` `launch = true`)
  and their `.toml`, `<layers>/config/metadata.toml`, `/workspace`, `/cnb/lifecycle/launcher`,
  a `/cnb/process/<type>` link per process. Keep `guest/src/wire.rs` a copy of `src/wire.rs`
- wicket: `POST /api/builds`, streaming the output lines and then the droplet, within a build
  timeout and the VM / memory budget (a build VM counts as any other); the builder's rootfs made
  as an image's. `PUT /api/images` takes `layers` (uploaded tars by digest, over the image's) and
  the launch from the droplet: entrypoint `/cnb/process/<default>` else the launcher, env the run
  image's plus `CNB_LAYERS_DIR`, `CNB_APP_DIR`, `PATH=/cnb/process:...`, workdir `/workspace`, the
  run image's user. The droplet tar goes through `rootfs::Tree` as a registry's layer does: the
  host parses guest bytes here, under the same bounds
- cf-api: a `Buildpack::Cnb` (`staging.rs`), detecting last, where `--cnb-builder <image>` is set
  (stack `firecracker`); the zip as a tar to the wicket; the droplet kept with its run image's
  pinned reference, placed on the other wickets as above; `cf push -b <buildpack id>` as the
  build's order (later, if cheap)
- egress: the build downloads dependencies (Paketo: GitHub releases, vendors' JDKs, Maven
  Central, npm). The guest resolves only named rules (`guest/src/egress.rs`): an operator's
  `--cnb-egress` list, or an HTTP proxy in it with `HTTPS_PROXY` set. Measure the hosts of a Maven,
  a Node and a Go build first
- a cache (later): a per-app disk kept between builds as `<cache>`, with `restorer`

## Measure first (`.kb/cnb-java-on-firecracker.md`)

A Spring Boot Maven app on `paketobuildpacks/builder-jammy-base` (or the noble one; arm64 too):
the builder's rootfs size and making time, the build's memory / scratch disk / time, the hosts
it reaches. If a cold build exceeds `CF_STAGING_TIMEOUT` (15 min) or the wicket's budget, rework
the plan around the numbers.

## Tests

- wicket VM test (`tests/vm.rs`, KVM): a tiny builder made in the test (one shell buildpack, the
  lifecycle binary, a busybox-like base as build and run image) served by a registry double; the
  build's droplet composed and served. No network
- cf-api CLI test (KVM): `cf push` of a source dir with that builder, served, `cf logs --recent`
  showing the phases' output; extend the existing Firecracker push rather than add one
- the real Paketo build as a manual check, its numbers in the `.kb`
