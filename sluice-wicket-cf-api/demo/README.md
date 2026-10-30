# Recording the cf CLI demo

`sluice-cf-demo.mp4` is the demo the [README](../README.md) shows (as `sluice-cf-demo.gif`):
the stock `cf` CLI against the cf-api and a wicket in local mode on a flume, with Dex as the
OpenID Provider -- `cf login --sso`, push, routes, scale, environment variables, a keyvalue
service, logs, delete. `demo.tape` is its script for
[VHS](https://github.com/charmbracelet/vhs), which types it into a real shell and records the
terminal; `record.sh` runs it on a fresh stack. The browser's part of `cf login --sso` (the
sign-in at Dex, the page with the one-time code) is done by `passcode.sh` before the recording,
and the tape types the code at the prompt (`{{PASSCODE}}`, replaced by `record.sh`).

## Prerequisites

- the toolchains the Rust workspaces need (`record.sh` builds the release binaries and the
  guests), `protoc`
- `vhs` (with `ttyd` and `ffmpeg`), `jq`, `curl`, `nc`: `brew install vhs jq` on macOS
- Docker (OrbStack or Docker Desktop on macOS): Dex runs as a container on the host's network
  (`ghcr.io/dexidp/dex`, pulled on the first run), its users in `dex.yaml`
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

- the GIF stays a few MB (2.5MB now): it is the copy the README shows, since GitHub plays no
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
