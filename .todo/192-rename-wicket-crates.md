Difficulty: Medium

# Rename the wicket crates: sluice-wicket-tunnel, sluice-wicket-oidc, sluice-wicket-runtime-{wasi,firecracker}

`sluice-tunnel` and `sluice-oidc` serve only the wicket family (tunnel: wasi, firecracker;
oidc: wasi's console, cf-api), and once they carry the `wicket` prefix the runtimes no longer
stand out. Rename:

| now | new |
|---|---|
| `sluice-tunnel` | `sluice-wicket-tunnel` |
| `sluice-oidc` | `sluice-wicket-oidc` |
| `sluice-wicket-wasi` | `sluice-wicket-runtime-wasi` |
| `sluice-wicket-firecracker` | `sluice-wicket-runtime-firecracker` |

`sluice-wicket-cf-api` is not a runtime (a facade over one wicket) and keeps its name; check
the table still reads well with it and say so in the commit.

One rename per crate, everything that names it in the same commit, all gates green each time:

- directories (`git mv`), package / binary / lib names (`sluice_tunnel` -> `sluice_wicket_tunnel`
  in `use`s), path dependencies, `Cargo.lock`s; the guest crate `sluice-firecracker-guest`
  and wasi's `examples/` workspace follow their runtime
- environment variables: `SLUICE_WICKET_WASI_*` -> `SLUICE_WICKET_RUNTIME_WASI_*`,
  `SLUICE_WICKET_FIRECRACKER_*` -> `SLUICE_WICKET_RUNTIME_FIRECRACKER_*`, and the
  `SLUICE_FIRECRACKER_*` ones (`_CONSOLE`, the tests' `_IMAGE` ...); no aliases for the old
  names (no deployment outside this repository depends on them); default state dir names
  (`sluice-wicket-firecracker-<pid>`) and log / usage text
- Docker: `Dockerfile`, `Dockerfile.dockerignore` (the allow-lists name the paths), image
  names `ghcr.io/making/sluice/sluice-wicket-runtime-{wasi,firecracker}`; the old packages
  on ghcr stay as they are (note them in the README once, or delete them by hand)
- CI: `.github/workflows/rust.yaml` (path filters, job names, the VM test's image tag),
  `deploy-wicket.yaml` -> `deploy-wicket-runtime-wasi.yaml`, `deploy-firecracker.yaml` ->
  `deploy-wicket-runtime-firecracker.yaml`, `deploy-cf-api.yaml` (it builds a wicket), and
  the image yaml written to the `images` branch (rename its files and whatever reads them)
- Java side: `sluice-it` (tests building / running the wicket, e.g.
  `ConsolePeerBuildE2ETest`), comments in `sluice-server` and `sluice-proto` (`build.proto`,
  `tunnel.proto`)
- docs: `CLAUDE.md` Layout, `README.md`, each crate's README, `.kb/` (`live-access-log.md`,
  `tunnel-flow-control.md`), `c4.html`, `sluice-wicket-cf-api/demo/`, the examples' READMEs
- open todos: rename files named `*-wicket-wasi-*` / `*-wicket-firecracker-*` and update the
  crate names and paths in every open todo's body; `.todo/history/` and `.todo/artefacts/`
  stay as written (they record the past)
- what stays: the role word `wicket` in protocol and data (`wicket:<client-id>` flume nodes,
  proto field names), and the git history

Finish with `grep -rE 'sluice-(tunnel|oidc|wicket-wasi|wicket-firecracker)|SLUICE_WICKET_(WASI|FIRECRACKER)_|SLUICE_FIRECRACKER_'`
outside `target/`, `.git/`, `.todo/history/` and `.todo/artefacts/` coming back empty,
`./mvnw verify`, every crate's gates (the VM test with `/dev/kvm`), and the renamed deploy
workflows pushing their images.

Do it while few todos are in flight: every open wicket todo touches these paths.
