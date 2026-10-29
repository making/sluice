Difficulty: High

# sluice-wicket-cf-api: deploy wasm components to a wicket with the cf CLI (MVP)

A new component next to `sluice-wicket-wasi`: a Cloud Foundry API facade -- the subset of
the Cloud Controller v3 API, the UAA token endpoints and log-cache's read API the stock `cf`
CLI uses. It keeps the desired state of its apps and has one wicket serve them through the
app API (todo 172, with the wicket's `--console-token`).

```text
cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> sluice-wicket-wasi <--tunnel-- sluice server
```

```text
cf login -a http://127.0.0.1:9091 -u me -p x     # any credentials (MVP)
cf push hello -p dist/                            # dist/ holds hello.wasm
curl http://hello.localhost:8080/                 # the wicket's --listen
cf map-route hello localhost --hostname hi
cf scale hello -i 2
cf push hello -p dist/ --strategy rolling         # swapped in place
```

Then: services and bindings 174, guest env 175, `cf logs` from the flume 176, OIDC 177, OCI
images 178.

## Spike

`.todo/artefacts/173/`: a Python stub of the API, driven by the OSS cf CLI 8.19.0 through
every command below; `requests.md` lists the requests each made, `README.md` the findings:

- the target is the OSS CLI (`cloudfoundry/cli` v8). The Tanzu distribution (`cf` 10.x,
  the one installed on this host) reads `/v3/info` and refuses any other platform
- one origin serves the three APIs: `links.login` / `links.uaa` may carry a path (`/uaa`),
  `links.log_cache` may not (the CLI reads `/api/v1/read/<app guid>` at the root)
- every push starts with `POST /v3/spaces/:guid/actions/apply_manifest` (a manifest the CLI
  generates: `default-route: true` and the local `path`, which the server ignores; with a
  manifest file, `manifest_diff` before it): app, process, routes, services and metadata are
  applied server side
- `cf push -p hello.wasm` fails in the CLI (`zip: not a valid zip file`): a push names a
  directory or a zip
- an instance CRASHED with `details` is printed as `Error starting instances: '<details>'`:
  the wicket's refusal reaches the user as it is

## Mapping

| CF | wicket |
|---|---|
| app, started, with a droplet | an app (172) named by its guid: its routes' hosts, the droplet's component, the sandbox below |
| app stopped, or without routes | no app in the wicket |
| route `<host>.<domain>` | a host. No path, port, wildcard or `_` (422). A route serves one app: a second destination is refused; zero-downtime updates are `--strategy rolling` |
| shared domain | `--domain` (repeatable; the first takes default and random routes): `localhost` in local mode, else a suffix whose wildcard DNS reaches the sluice server |
| package (bits) | the uploaded zip |
| build | picks the component: the one the web process command names (`-c hello.wasm`, `command:`), else the package's only `.wasm`; it must be a component (`\0asm`, layer 1). Anything else fails the build, naming what it found |
| droplet | that component, by sha256 (`process_types.web`: its path; stack `wasip3`, buildpack `wasm`) |
| web process `instances` | `pool`: warm instances. A request beyond them gets an instance of its own, as on every route: 0 still serves |
| web process `memory` | `memory-mib` |
| annotations `wicket.sluice.ik.am/budget-ms`, `wicket.sluice.ik.am/instances` | `budget-ms`, `instances` (the route's cap) |
| disk, health check, log rate, stack, buildpacks | accepted and shown, no effect |
| instance state | RUNNING once the wicket took the app; CRASHED with the wicket's refusal as `details`; STARTING while the wicket is unreachable |
| rolling deployment | the app PUT with the new droplet: the wicket swaps it in place |
| org / space | one of each (`--org`, `--space`) |

## Behavior

- as on CF, routes and `instances` apply at once; the droplet and the rest of the sandbox
  (memory, annotations; bindings and env later) at start, restart and restage: the running
  app is the snapshot taken then. For `scale -m` the CLI stops and starts the app itself
- an edit the wicket answers at once (map-route, scale -i) is refused with its 409 / 422 and
  changes nothing; a start, restart or deployment is accepted, and its outcome shows in the
  process stats and the log
- the desired state is the cf-api's: at startup and whenever the wicket's revision moves
  (`GET /api/routes?after=`), it lists the wicket's apps of its owner and PUTs / DELETEs the
  difference. The wicket serves on while the cf-api is down
- `cf logs` (log-cache `read`): an in-memory ring per app (source id: the app guid) of the
  cf-api's own lines: staging (STG), lifecycle (API), the wicket's outcome (CELL)
- auth (MVP): `POST /uaa/oauth/token` takes any credentials (the password, refresh_token and
  client_credentials grants) and answers a JWT signed with a key kept in the data directory;
  every request but the root document and the UAA endpoints needs a valid one (log-cache's
  included: the CLI sends it); everyone may do everything.
  Without real auth the listener binds a loopback address only, as the wicket's console
  without a token
- state: in memory, written by rename to one file in `--data-dir` on every change; blobs next
  to it: `packages/<guid>.zip` (the newest per app, and one staging), `droplets/<sha256>.wasm`
  (the current and the previous per app)
- refused with why (422): route paths, ports, wildcards, private and internal domains,
  docker images (178), tasks, ssh, sidecars, process types but `web`, route services, env
  vars (175), services (174)
- errors in CC's shape (`{"errors": [{"code", "title", "detail"}]}`); `/v3/info` names this
  API, never another platform

## Layout

Rust, an own Cargo workspace laid out as `sluice-flume` (`src/lib.rs` wires the modules,
`src/main.rs` reads the command line, `sluice-build-info` for `--version`), the same gates;
flags with `SLUICE_WICKET_CF_API_<NAME>` from the environment, as the wicket's. The wicket
family's stack (hyper, serde, serde-saphyr for the manifests, sha2) and an image beside the
wicket's. It never links wasmtime: whether a component loads is the wicket's call.

## Acceptance

- one integration test runs the OSS CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; CI
  installs 8.19.0) against the cf-api and a wicket in local mode (`--listen`, `--console`, a
  token): login; push `hello` from a directory, and `Host: hello.localhost` answers on the
  listener; map-route, and the second host answers; `scale -i 2`, and the wicket's listing
  shows `pool: 2`; a rolling push of another component, with no failed request during it;
  stop (404), start (served); the cf-api restarted: its state kept, the wicket's generation
  unchanged; push `counter`: the CLI prints the wicket's reason (`needs wasi=keyvalue`);
  `delete -r`: gone from the wicket
- README (what it is for, how to run it, the mapping, the limits); the root README and the
  CLAUDE.md layout; a job in `rust.yaml`; an image as the wicket's
