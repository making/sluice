# Recording the cf CLI demo

`sluice-cf-demo.mp4` is the demo the [README](../README.md) shows (as `sluice-cf-demo.gif`):
the stock `cf` CLI against the cf-api and a wicket in local mode on a flume, with Dex as the
OpenID Provider -- `cf login --sso`, push, routes, scale, environment variables, a keyvalue
service, logs, delete. `demo.tape` is its script for
[VHS](https://github.com/charmbracelet/vhs), which types it into a real shell and records the
terminal; `record.sh` runs it on a fresh stack. The browser's part of `cf login --sso` (the
sign-in at Dex, the page with the one-time code) is done by `passcode.sh` before the recording,
and the tape types the code at the prompt (`{{PASSCODE}}`, replaced by `record.sh`).

## On a Firecracker wicket

`sluice-cf-firecracker-demo.mp4` (and `.gif`) is the same for a Firecracker wicket: `cf login --sso`
through Dex, a container image and a root filesystem pushed, logs, routes, a pool of VMs,
environment variables, a restart of the wicket, delete. `firecracker.tape` is its script,
`dex-firecracker.yaml` its provider. On an Apple silicon Mac, `record-firecracker.sh` builds the
images from the work tree in the wicket's Lima VM (`scripts/lima.sh`, see
[On a Mac](../../sluice-wicket-runtime-firecracker/README.md#on-a-mac)), runs them with Dex and
records the `cf` CLI on the Mac, on the ports 5556, 8080 and 9090:

```text
sluice-wicket-cf-api/demo/record-firecracker.sh
```

It needs `vhs`, `ffmpeg`, `jq`, `curl`, `nc`, `limactl` and the OSS cf CLI v8 below. The wicket's log
is in `../target/demo-firecracker/wicket.log`.

## Source staged by buildpacks

`sluice-cf-cnb-demo.mp4` (and `.gif`) is `cf push` of source on a Firecracker wicket: a Node.js
and a Spring Boot app (`cnb/`) built by the buildpacks of
`paketobuildpacks/ubuntu-resolute-builder` in a microVM, served, their staging log. `cnb.tape`
is its script. On Linux with a usable `/dev/kvm`, `record-cnb.sh` builds the flume, the wicket
and the cf-api from the work tree, runs them here (`--any-credentials`: no provider), has the
builder's rootfs made by a push before the recording, and records the `cf` CLI, on the ports
8002, 8080, 9090 and 9091:

```text
sluice-wicket-cf-api/demo/record-cnb.sh
```

It needs `vhs` (with `ttyd` and `ffmpeg`; it fetches a Chromium on its first run), `curl`, `nc`, `firecracker`,
a guest kernel (the wicket's [Setup](../../sluice-wicket-runtime-firecracker/README.md#setup)),
the OSS cf CLI v8 below, and the network: the builder's image and what its buildpacks download.
The logs are in `../target/demo-cnb/logs`.

The rest of this page is the wasi wicket's demo.

## Prerequisites

- the toolchains the Rust workspaces need (`record.sh` builds the release binaries and the
  guests), `protoc`
- `vhs` (with `ttyd` and `ffmpeg`), `jq`, `curl`, `nc`: `brew install vhs jq` on macOS
- Docker (OrbStack or Docker Desktop on macOS): Dex runs as a container, its port published on
  the loopback (`ghcr.io/dexidp/dex`, pulled on the first run), its users in `dex.yaml`
- the OSS cf CLI v8: `SLUICE_CF_CLI`, else `cf8` or `cf` on the PATH. The Tanzu `cf` 10.x
  refuses this API

  ```text
  gh release download v8.19.0 -R cloudfoundry/cli -p 'cf8-cli_8.19.0_macosarm.tgz'
  tar xzf cf8-cli_8.19.0_macosarm.tgz && export SLUICE_CF_CLI=$PWD/cf
  ```

- the loopback ports 5556, 8002, 8080, 9090 and 9091 free

## Record

```text
sluice-wicket-cf-api/demo/record.sh
```

It builds, starts the stack on an empty state (`start.sh`), records `demo.tape` (about two
minutes, in real time), writes `sluice-cf-demo.mp4` and the GIF next to it and stops the
stack. The state, the logs and the CLI's login are in `../target/demo` (`CF_HOME` there:
`~/.cf` is left alone).

Before committing the new files:

- `../target/demo/logs/cf-api.log` and `wicket.log` hold no error: every command of the tape
  succeeds
- each scene ends as expected: the tape clears the screen between scenes, so a frame just
  before a clear shows a whole scene

  ```text
  ffmpeg -ss 23.5 -i sluice-cf-demo.mp4 -frames:v 1 scene.png
  ```

- the GIF stays a few MB: it is the copy the README shows, since GitHub plays no
  video committed to a repository

## Changing the demo

- try a command first on a running stack:

  ```text
  sluice-wicket-cf-api/demo/start.sh
  export CF_HOME=$PWD/sluice-wicket-cf-api/target/demo/cfhome
  cf api http://127.0.0.1:9091 && cf login --sso-passcode "$(sluice-wicket-cf-api/demo/passcode.sh)"
  cd sluice-wicket-cf-api/target/demo/apps && cf push hello -p hello/
  sluice-wicket-cf-api/demo/stop.sh --reset    # --reset: no apps, services or login left
  ```

- in the tape, `Wait` holds until the prompt (`>`) is back, then `Sleep` gives the viewer time.
  Each scene starts with `Hide`, `clear`, `Show`, so it begins at the top of the screen
- quote a path in the tape (`Output "out/sluice-cf-demo.mp4"`): VHS refuses a bare absolute one
- the apps' packages are `apps/hello` and `apps/counter` in `../target/demo`, copied by
  `start.sh` from the guests the wicket's examples build
- the code `passcode.sh` prints is good for 5 minutes and once: the login is the tape's first
  scene, and a failed take needs a new code (`record.sh` takes one each run)
