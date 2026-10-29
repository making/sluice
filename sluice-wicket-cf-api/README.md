# sluice-wicket-cf-api

A Cloud Foundry API facade for one [sluice-wicket-wasi](../sluice-wicket-wasi/README.md): the
subset of the Cloud Controller v3 API, the UAA token endpoints and log-cache's read API the
stock `cf` CLI uses, on one origin. It keeps the desired state of its apps and has the wicket
serve them through the wicket's [app API](../sluice-wicket-wasi/README.md#apps), so a wasm
component is deployed with `cf push`, and routes, scaling, rolling updates, stop and start,
services (the wicket's keyvalue stores and capabilities) and `cf logs` work as the CLI expects.

```text
cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> sluice-wicket-wasi <--tunnel-- sluice server
                                             ^                                  |
                                             +--Watch-- sluice-flume <--Feed----+  (cf logs, optional)
```

The target is the OSS CLI ([cloudfoundry/cli](https://github.com/cloudfoundry/cli) v8, `cf8`);
the Tanzu distribution (`cf` 10.x) reads `/v3/info` and refuses any other platform.

## Run

```text
cargo build --release                                # needs protoc, and git (the build embeds the commit)

# optional, for the apps' output and requests in cf logs: a flume (see sluice-flume)
sluice-flume --token FLUME_SECRET                     # grpc://0.0.0.0:8002

# the wicket, serving on 8080 with its console on 9090 (see its README)
sluice-wicket-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

# the cf-api, on http://127.0.0.1:9091
target/release/sluice-wicket-cf-api --wicket http://127.0.0.1:9090 --wicket-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

cf8 login -a http://127.0.0.1:9091 -u me -p x   # any credentials
cf8 push hello -p dist/                          # dist/ holds hello.wasm (a wasi:http 0.3 component)
curl http://hello.localhost:8080/                # the wicket's --listen; *.localhost is the loopback
cf8 map-route hello localhost --hostname hi
cf8 scale hello -i 2
cf8 push hello -p dist/ --strategy rolling       # swapped in place
cf8 logs hello --recent
cf8 marketplace                                  # the wicket's keyvalue stores and capabilities
cf8 create-service keyvalue local kv             # a namespace of the store local
cf8 bind-service counter kv                      # counter: a wasi:keyvalue component
cf8 restart counter                              # a binding applies at the next start
```

| option | |
|---|---|
| `--listen <host:port>` | bind address (default `127.0.0.1:9091`); a loopback address only, as the API takes any credentials |
| `--wicket <url>` | the wicket's console, `http://host:port` |
| `--wicket-token <token>` / `--wicket-token-file <path>` | its `--console-token` |
| `--domain <name>` | a shared domain; repeatable (or comma-separated): the first takes the default and random routes (default `localhost`). Through a sluice server, a suffix whose wildcard DNS reaches it |
| `--data-dir <path>` | the state and the blobs (default `cf-api`): `state.json` (written by a rename on every change), `jwt.key`, `packages/<guid>.zip` (the newest per app), `droplets/<sha256>.wasm` (the current and the previous per app) |
| `--org <name>` / `--space <name>` | the one organization and space (default `sluice` / `wicket`) |
| `--flume <url>` | the sluice-flume the wicket feeds (`grpc://host:port` / `grpcs://host:port`): the apps' output and requests reach `cf logs` from it |
| `--flume-token <token>` / `--flume-token-file <path>` | its bearer token |
| `--flume-ca-cert <path>` / `--flume-insecure` | a `grpcs://` flume verified against this PEM CA (default the webPKI roots), or not at all |
| `--exit-on-stdin-close` | exit when stdin reaches its end (a parent's child process) |

Each option also reads `SLUICE_WICKET_CF_API_<NAME>` from the environment. The image
`ghcr.io/making/sluice/sluice-wicket-cf-api` (`latest` / `<sha>`, built from
`Dockerfile` at the repository root) keeps `/data` as the working directory; it listens on
the loopback, so it is reached from the wicket's network namespace (`--network host`, a
sidecar, `kubectl port-forward`).

## Mapping

| CF | wicket |
|---|---|
| app, started, with a droplet and a route | an app named by its guid, owner `cf`: its routes' hosts, the droplet's component, the sandbox below |
| app stopped, or without a route | no app in the wicket |
| route `<host>.<domain>` | a host: labels of `a-z`, `0-9` and `-`. No path, port or wildcard (422). A route serves one app: a second destination is refused; zero-downtime updates are `--strategy rolling` |
| shared domain | `--domain` |
| package (bits) | the uploaded zip: `cf push` takes a directory or a zip (`-p hello.wasm` fails in the CLI: `not a valid zip file`) |
| build | picks the component: the one the web process command names (`-c hello.wasm`, `command:`), else the package's only `.wasm`; it must be a component (`\0asm`, layer 1). Anything else fails the build, naming what it found |
| droplet | that component, by sha256 (`process_types.web` its path; stack `wasip3`, buildpack `wasm`) |
| web process `instances` | `pool`: warm instances. A request beyond them gets an instance of its own, as on every route: 0 still serves |
| web process `memory` | `memory-mib` |
| annotations `wicket.sluice.ik.am/budget-ms`, `wicket.sluice.ik.am/instances` | `budget-ms`, `instances` (the route's cap; below `pool` the wicket refuses) |
| disk, health check, log rate, stack, buildpacks | accepted and shown, no effect |
| instance state | `RUNNING` once the wicket took the app; `CRASHED` with the wicket's refusal as `details` (`cf push` prints it: `Error starting instances: '...'`); `STARTING` while the wicket is unreachable |
| rolling deployment | the app put with the new droplet: the wicket swaps it in place; finalized at once |
| org / space | one of each |
| service offering `keyvalue`, plan `<store>` | a keyvalue store of the wicket (`keyvalue.stores`, else `local`) |
| service offering `wasi`, plan `<capability>` | a capability of the sandbox's `wasi`: `cli`, `inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp` |
| keyvalue service instance | the namespace `cf/<instance guid>` of its plan's store |
| binding to a keyvalue instance | the sandbox's `keyvalue` map: its identifier (parameter `identifier`, default `""`) -> `<store>/cf/<instance guid>`, and the capability `keyvalue` |
| binding to a wasi instance | the plan's capability; a network one `cli` too |

## Behavior

- as on CF, routes and `instances` apply at once; the droplet and the rest of the sandbox
  (memory, annotations, the service bindings) at a start, restart, restage or deployment: the
  running app is the snapshot taken then. For `scale -m` the CLI stops and starts the app itself
- an edit the wicket answers at once (map-route, unmap-route, scale -i, a manifest's routes)
  is refused with 422 and the wicket's reason, and changes nothing; a start, restart or
  deployment is accepted, and its outcome shows in the process stats and the log (`cf logs`)
- the desired state is the cf-api's: at startup and whenever the wicket's revision moves
  (`GET /api/apps?after=`), it lists the wicket's apps of its owner and puts / deletes the
  difference (the same app again changes nothing: the wicket's generation stays). The wicket
  serves on while the cf-api is down; with the wicket down, the apps show `STARTING` and are
  put when it is back
- `cf logs` (log-cache `read`, polled by the CLI): an in-memory ring of 1000 lines per app of
  the cf-api's own lines: staging (`STG`), lifecycle (`API`), the wicket's outcome (`CELL`).
  With `--flume`, the app's own too, from the live access log the wicket feeds: its output
  (`APP/PROC/WEB` OUT / ERR), its requests (`RTR`, in the gorouter's access log format) and
  the lines lost on the way (`LGR`). The cf-api watches its apps' hosts and resumes past the
  last line it took (kept in `state.json`); see
  [Live access log](../.kb/live-access-log.md#wickets-and-the-cf-api)
- auth: `POST /uaa/oauth/token` takes any credentials (the password, refresh_token and
  client_credentials grants) and answers a JWT (HS256) signed with the key in `--data-dir`, so
  a login outlives a restart; every request but the root document, `/v3`, `/v3/info` and the
  UAA endpoints needs a valid one (log-cache's included), and everyone may do everything. That is
  why the listener binds a loopback address only
- errors are in CC's shape (`{"errors": [{"code", "title", "detail"}]}`); `/v3/info` names
  this API

## Services

The wicket's keyvalue stores and capabilities are the plans of the service offerings
`keyvalue` and `wasi` of the broker `sluice-wicket`, which is no process: the cf-api offers
them as the wicket lists them (`GET /api/apps`), so `cf marketplace` follows the wicket's
config.

- `cf create-service keyvalue <store> <name>` makes a namespace, `cf/<instance guid>`, of the
  store; `cf bind-service <app> <name> [-c '{"identifier":"<id>"}']` lets the app open it at the
  identifier (default `""`, what `store::open("")` opens). An app binds one instance per
  identifier (a second is refused, 422). Renaming the instance or moving the app's routes leaves
  the data where it is
- `cf create-service wasi <capability> <name>` and a binding grant the capability; a network
  one (`inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp`) `cli` too
- as the rest of the sandbox, a binding (and an unbinding) applies at the app's next start,
  restart, restage or deployment. A manifest's `services:` (a name, or `name` with `parameters`
  and `binding_name`) binds what the app is not bound to yet, as `cf bind-service`
- `cf env` shows the bindings in `VCAP_SERVICES` (by offering; a keyvalue binding's
  credentials are `store`, `namespace` and `identifier`, a wasi one's `capabilities`)
- instance and binding operations complete at once. `cf delete-service` is refused while the
  instance is bound (as on CF); a keyvalue instance deleted has its data deleted once no app
  maps its namespace any more -- no started app's snapshot, no route of the wicket, a draining
  one included (`DELETE /api/keyvalue/<store>/<namespace>`, tried at the deletion and after each
  listing of the wicket). `cf purge-service-instance` drops the bindings with the instance
- `cf rename-service` and `cf update-service -t` apply at once; a plan change, instance
  parameters, service keys, user-provided instances and sharing are refused (422)

## Limits

Refused with why (422): route paths, ports and wildcards, private and internal domains, docker
images, tasks, ssh, sidecars, process types but `web`, route services, environment variables
(`cf set-env`, a manifest's `env:`), user-provided services, service keys. One owner (`cf`) per
wicket: two cf-apis must not share one.

The integration test (`tests/cli.rs`) runs the OSS CLI -- `SLUICE_CF_CLI`, else `cf8` on the
PATH -- against the cf-api and a wicket in local mode on a flume, building the wicket and its
example guests from `../sluice-wicket-wasi` and the flume from `../sluice-flume` (or taking
`SLUICE_WICKET_WASI`, `SLUICE_WICKET_GUESTS` and `SLUICE_FLUME`):

```text
gh release download v8.19.0 -R cloudfoundry/cli -p 'cf8-cli_8.19.0_macosarm.tgz'
tar xzf cf8-cli_8.19.0_macosarm.tgz
SLUICE_CF_CLI=$PWD/cf8 cargo test
```
