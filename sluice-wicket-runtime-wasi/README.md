# sluice-wicket-runtime-wasi

> **Experimental / proof of concept.** The API, layout, and behavior are
> unstable and may change or disappear; the limitations listed at the bottom
> are intentional PoC scope.

A **wasm platform on the sluice tunnel**: connects to a sluice server like the
Java client, but instead of relaying to TCP upstreams it terminates HTTP on
the tunnel and hands every request to a **wasi:http 0.3 component**
(`wasi:http/handler@0.3.0`, async `handle`). The proto is compiled directly
from `sluice-proto` — a single source of truth with the Java modules.

```
sluice server --tunnel--> sluice-wicket-runtime-wasi --hyper(h1/h2c)--> wasi:http handler component
```

## Layout

Two Cargo workspaces:

- `.` — `sluice-wicket-runtime-wasi`, the tunnel-side wasm host (wasmtime + hyper), laid out as
  `sluice-flume`: `src/lib.rs` wires the modules, `src/main.rs` reads the command line.
  The tunnel client (node streams, flow control, the drain) and the live access log's feed
  are `../sluice-wicket-tunnel`'s, shared with `sluice-wicket-runtime-firecracker`; the
  console's gate, its page's style and script helpers and its directory's layout are
  `../sluice-wicket-console`'s, shared with it too
- `examples/` — sample handler components (`wasm32-wasip3`), a separate
  workspace pinned to a nightly toolchain by `rust-toolchain.toml`
  (wasm32-wasip3 ships rust-std on nightly only; rustup installs it on the
  first build). Each example's README says what it demonstrates and which
  capabilities its route needs. The CI publishes them with the image, as wasm OCI artifacts
  (`wkg oci push`): `ghcr.io/making/sluice/sluice-wicket-runtime-wasi/examples/<example>`
  (`latest` / `<sha>`)
- `wit/` — vendored wasi 0.3 (from wasmtime v50) and `wasi:keyvalue@0.2.0-draft2`
  (`deps/keyvalue`, WebAssembly/wasi-keyvalue main `aa972c8`); each example declares its
  own world inline, so a new example is just a new directory under `examples/`

## Run

```text
cargo build
(cd examples && cargo build --release)   # -> examples/target/wasm32-wasip3/release/<example>.wasm
# or the published ones (see Layout)
wkg oci pull ghcr.io/making/sluice/sluice-wicket-runtime-wasi/examples/hello -o hello.wasm

# sluice server as in the root README, then:
target/debug/sluice-wicket-runtime-wasi --server grpc://127.0.0.1:8001 --token SECRET \
    --wasm demo.local=examples/target/wasm32-wasip3/release/hello.wasm
# or with the routes in a file, reloaded on change (see Configuration file)
target/debug/sluice-wicket-runtime-wasi --config wicket-runtime-wasi.yaml
# or deploying them from a page, by dropping the .wasm files on it (see Console)
target/debug/sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090
```

`cargo test` builds the examples the same way (through the
rustup `cargo` proxy, so rustup must be installed), and runs `redis-server` (with TLS, as
Homebrew's and Debian's are) for the redis store. `--no-default-features` builds without
the redis store.

### Local mode

`--listen <host:port>` serves the routes over plain TCP as well -- http/1.1 and h2c, the
same handlers, sandbox, pools, budgets and reloads -- and without `--server` it is all the
wicket does: a local loop like `wasmtime serve`, with no sluice server.

```text
target/debug/sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 \
    --wasm 'counter.local=examples/target/wasm32-wasip3/release/counter.wasm?wasi=keyvalue'
curl 127.0.0.1:8080/
```

A request goes to the route of its host (the `Host` header, or the h2 authority; the port
left out). A single route given by `--wasm`, on a wicket without `--console`, takes any host: not
a route of the config file or the console, whose routes come and go. An unknown host is
answered 404. Both `--server` and `--listen` serve the tunnel and the listener at once.
`--max-connections` bounds the listener's connections too, apart from the tunnel's; TCP keepalive
(60s idle, then 3 probes 10s apart) ends one whose
client is gone without a FIN or reset, and a client quiet for 60s -- sending no request head on a
new or an idle connection, reading nothing of an answer -- loses one that is there (an h2 one is told
to go by a GOAWAY first, 60s before it is cut); a request in progress goes by its `budget-ms`. All
taken, one with no request in progress gives way to the next, the source holding the most first (an h2
client told by a GOAWAY); with none, the next waits in the backlog
([details](../.kb/connection-limits.md#sluice-wicket-runtime-wasis---listen)). Without `--server` nothing drains: SIGTERM / SIGINT end
the process at once.

The image `ghcr.io/making/sluice/sluice-wicket-runtime-wasi` (`latest` / `<sha>`, amd64 /
aarch64) is published on every push to `main` changing the wicket; it takes the same
arguments, with the components mounted:

```text
docker run --rm --pull always -v $PWD/examples/target/wasm32-wasip3/release:/wasm:ro \
    ghcr.io/making/sluice/sluice-wicket-runtime-wasi \
    --server grpc://host.docker.internal:8001 --token SECRET --wasm demo.local=/wasm/hello.wasm
```

Its working directory is `/data`, where the [keyvalue](#key-value-stores-experimental) store
is created without a `--config` file (`/data/keyvalue.db`): mount a volume there
(`-v wicket-data:/data`) to keep it, or the stores die with the container.

`--help` lists the options. Each option but `--wasm` also reads
`SLUICE_WICKET_RUNTIME_WASI_<NAME>` from the environment (upper case, `-` as `_`, e.g.
`SLUICE_WICKET_RUNTIME_WASI_TOKEN`; `SLUICE_WICKET_RUNTIME_WASI_INSECURE=true`; another variable of that
prefix, a misspelled one, is refused at the start), and takes its value as
`--name value` or `--name=value`.
A token on the command line is in `/proc/<pid>/cmdline`, which every user of the host
reads: give the tunnel's in the environment, the config file or a file
(`--token-file <path>`, `token-file`).

`--version` prints the build: version, git commit (with `dirty` when tracked files had
uncommitted changes) and build time, embedded by `build.rs` (`sluice-build-info`) when the
build script last ran. Without git the build still succeeds and reports `commit unknown`,
unless `SLUICE_GIT_COMMIT` names the commit (the Dockerfile's `GIT_COMMIT` build arg). The
wicket logs the same line at startup and sends it in its `Advertise`, so the server console
shows it on the wicket's row.

`--server` also accepts `grpcs://` (TLS). Verification uses `--ca-cert <file>` (PEM, may
hold a chain), the webPKI roots when omitted, or nothing at all with `--insecure`
(self-signed setups; the `sluice.insecure` equivalent):

```text
target/debug/sluice-wicket-runtime-wasi --server grpcs://127.0.0.1:8001 --ca-cert ca.pem ...
target/debug/sluice-wicket-runtime-wasi --server grpcs://127.0.0.1:8001 --insecure ...
```

`--client-id <id>` pins the client identity sent as `x-sluice-id` on every tunnel
stream; when omitted a random UUID is used, matching the Java client's
`sluice.client.id`. Re-registering with the same id replaces the previous
registration on the server.

`--listen <host:port>` serves the routes over plain TCP too (see [Local mode](#local-mode));
`--keyvalue-file <path>` moves the keyvalue store (see
[Key-value stores](#key-value-stores-experimental)); `--console <host:port>` serves a page
deploying components as routes (see [Console](#console)); `--flume <url>` feeds the live
access log (see [Live access log](#live-access-log)).

`--max-connections N` (default 256) caps the connections open at once over every
node stream; a CONNECT beyond it is answered with the ERROR `connection limit reached`
and not served. A windowed connection buffers at most its 1MiB receive window of
request bytes, coalesced into 16KiB segments whatever the size of the frames (plus at
most two partly used segments and the overhead of 80 segments), so the windows total
about N x 1.04MiB (265MiB by default), and its outbound queue at most 256KiB of
response DATA (N x 256KiB in total; an ended connection keeps its slot until its
queued frames left). Guest instances are bounded by `--max-instances` instead.

`--tunnel-stall-timeout SECONDS` (default 60, `0` = off) gives a node stream up once
connection frames waited that long with gRPC taking none of them (a server that stopped
reading while it keeps the connection alive): the queued frames are dropped, which frees
the slots of the ended connections they belong to, the stream's connections fail, and the
session reconnects (`sluice.client.tunnel-stall-timeout` of the Java client). A frame taken
restarts the wait; control frames do not count.

`--max-instances N` (default 64) caps the guest instances alive at once over every route,
so guest linear memory stays within N x the largest `memory-mib` (16GiB at the defaults),
and sizes wasmtime's instance pool (see [Design notes](#design-notes)).
Every instance holds one of the N slots until it is gone: a request's, or a pooled unit's
for the unit's lifetime (see [Instance pool](#instance-pool)); a spare unit (`keep-ms`) gives
its own up to a request waiting for one. A request finding no slot
waits for one within its `budget-ms` (freed slots go to the waiting requests in arrival
order), and is answered 503 (`wasm instance limit reached`) when the budget runs out
first; admitted late, it runs only for what is left of its budget. One connection holds at most half the slots (at least one):
an h2 connection announces that many concurrent streams, and a stream reset while its
guest still runs keeps its slot until the instance is gone, so the connection's next
stream waits for it.

Multiple `--wasm host=<locator>` routes per process. The locator is a bare
path, `file://<path>`, or `http(s)://<url>` (fetched when its route is new, and on
SIGHUP; see [Reloading routes](#reloading-routes)); the
advertised target is `wasm:<locator>` (plus the non-default options) and
further schemes slot into the same resolver. A `?query` suffix sets the
route's resource limits and WASI capabilities (`wasi`, see [Capabilities](#capabilities)), applied to every
request's instance: `budget-ms` is the request's wall-clock budget, covering
the wait for an instance slot, instantiation, handler and response relay
(exceeded -> 504, or a truncated body once streaming, or 503 while still
waiting for a slot; defaults 10000) and `memory-mib` the per-instance linear
memory cap in MiB (exceeded -> guest trap, 500; defaults 256). `warm=N`
keeps `N` warm instances per route (see [Instance pool](#instance-pool)).
`instances=N` caps the instance slots the route holds at once, its pooled
units included; unset, the route is bounded by `--max-instances` alone. A
request finding the route at its cap waits for one of the route's slots within
its budget (503 when it runs out, as above), holding no `--max-instances` slot
meanwhile: requests waiting on a route's cap take no slot from the other
routes. The cap reserves nothing: only `warm` does. It must be at least 1 and
at least `warm` (a startup error otherwise); above `--max-instances` it is no
error, that limit bounding it. With a cap equal to `warm`, a
request finding every unit busy waits for one instead of instantiating per
request. `keep-ms=N` keeps the instance of a request beyond `warm` as a spare unit,
ended once it served none for N ms (see [Instance pool](#instance-pool)); unset, none is
kept. `concurrency=N` (default 1) has an instance take up to N requests at once, `memory-mib`
and `instances` then counting instances, not requests, and `budget-ms` staying each request's
(see [Concurrency](#concurrency)). `files=<dir>` preopens the directory read-only at `/` for the route's instances
(`wasi:filesystem`, linked for it without `cli`), relative to the working directory; a
directory that is not there fails the route's load, and its files are read as they are
(nothing is watched). The console's page takes no `files`: its directories are the
wicket's to name. A literal `?` in a file path needs URL-escaping.

```text
--wasm demo.local=hello.wasm?budget-ms=5000&memory-mib=64
--wasm demo.local=hello.wasm?warm=8
--wasm batch.local=batch.wasm?instances=4
--wasm api.local=api.wasm?warm=1&instances=8&keep-ms=60000
--wasm cf.local=cf.wasm?warm=1&instances=4&concurrency=16
--wasm site.local=site.wasm?files=public     # a static site (sluice-wicket-site)
curl -H 'Host: demo.local' http://127.0.0.1:8000/hello
```

A host may be routed once, over `--wasm` and the config file together (a startup or
reload error otherwise).

### Configuration file

`--config <file>` reads the settings from a YAML file. Every flag has its key; a flag
overrides the environment (`SLUICE_WICKET_RUNTIME_WASI_<NAME>`), the environment the file, the
file a default. Without `--config` the flags alone apply, as
before. With it, `server` or `listen` is the one setting required: the routes may be empty
at startup and added by a reload.

```yaml
server: grpc://127.0.0.1:8001
token: ${SLUICE_TOKEN}          # an environment variable
# token-file: /run/secrets/token  # or a file holding it
client-id: ${HOSTNAME:-wicket}  # with a default
# insecure: true
# ca-cert: ca.pem               # relative to this file
max-connections: 256
max-instances: 64
tunnel-stall-timeout: 60s
drain-grace: 20s
evacuation-grace: 0             # see Shutdown: 60s serves on while the apps move elsewhere
watch-interval: 1s              # 0 = no polling, SIGHUP only
# listen: 127.0.0.1:8080        # local mode, see Local mode
# health: 127.0.0.1:8081        # see Readiness probe
# console: 127.0.0.1:9090       # see Console
# console-token: ${CONSOLE_TOKEN}
# console-dir: deployed         # relative to this file
# oidc-issuer: https://idp.example.com/dex   # the console's browsers sign in through it
# oidc-client-id: wicket
# oidc-client-secret: ${OIDC_CLIENT_SECRET}
# oidc-claim: [email=*@example.com, groups=devs]
# oidc-scopes: openid,email,profile,groups
# oidc-ca-cert: idp-ca.pem      # relative to this file
# flume: grpc://127.0.0.1:8002  # see Live access log
# flume-token: ${FLUME_TOKEN}
# flume-ca-cert: flume-ca.pem   # relative to this file
# flume-insecure: true

keyvalue:                       # optional: one sqlite store, keyvalue.db next to this file
  default: local                # optional with one store
  stores:
    local:
      type: sqlite
      path: keyvalue.db         # relative to this file
      # max-key-bytes: 1Ki      # the defaults (every store)
      # max-value-bytes: 1Mi
      # max-namespace-bytes: 64Mi
    shared:
      type: redis
      url: rediss://:${REDIS_PASSWORD}@redis.example:6380/0   # redis://, rediss://, redis+unix://
      # ca-cert: redis-ca.pem   # the CA of a rediss:// certificate; default: the webPKI roots
      # prefix: ""              # the defaults
      # timeout: 5s

routes:
  - host: demo.local
    component: hello.wasm       # a path relative to this file, file:// or http(s)://
    sandbox:
      env:                      # the guest's environment variables (wasi:cli/environment)
        GREETING: hello
        API_KEY: ${DEMO_API_KEY}  # a secret: kept out of the file
  - host: relay.local
    component: relay.wasm
    sandbox:                    # the --wasm query options; unset keys keep their defaults
      wasi: [cli, inherit-network]
      budget: 250ms             # budget-ms
      memory: 64Mi              # memory-mib: a whole number of MiB (Ki/Mi/Gi, K/M/G)
      warm: 2
      instances: 4
      keep: 60s                 # keep-ms
      concurrency: 4
  - host: counter.local
    component: counter.wasm
    sandbox:
      wasi: [keyvalue]
      keyvalue:                 # optional: identifier -> store[/namespace]
        "": local               # the namespace defaults to the host
        sessions: shared/sessions
  - host: site.local
    component: site.wasm
    files: public               # read-only at / (files= of the flag), relative to this file
```

- durations take `ms`, `s`, `m` or `h` (`tunnel-stall-timeout`, `drain-grace`,
  `evacuation-grace` and `watch-interval` also a bare `0` = off); the flags keep whole seconds
- sizes take a whole number with `Ki`, `Mi`, `Gi` (or `K`, `M`, `G`); `memory` must be
  whole MiB, the keyvalue limits may be a bare number of bytes
- `${NAME}` in a plain (unquoted) scalar is the environment variable `NAME`, with the
  docker compose forms (`${NAME:-default}`, `${NAME?message}`, ...); an unset `${NAME}`
  without a default is an error, `$${` a literal `${`, and a quoted scalar is never
  expanded
- an unknown key or a value that does not parse fails, naming the line, or the route and
  key (`routes[1]: sandbox.memory: ...`), never the file's text
- a route's `env` is what its guest's `wasi:cli/environment` answers, each scalar as text (a
  name not empty, without `=` or a control character; 128KiB in all). It may hold secrets:
  the advertised target carries a digest of it (keyed by the process: another env, another
  target, and no value in it), the console and the logs its names only. `--wasm` and the
  console's page set none
- the `--wasm` routes follow the file's

### Reloading routes

The routes are reloaded without a restart: their files are polled every
`--watch-interval` (`watch-interval`, default 1s) -- the config file and every
component file the routes read -- and SIGHUP reloads at once. A change is reloaded once
a poll found it unchanged since the previous one, so a file still being written is not
read half way. Without `--config`, the `--wasm` routes stay as given and their
component files are reloaded.

A reload reads the routes again and loads every component before it changes anything:
an invalid config, a component that does not compile, a missing file, a failed fetch,
pools past `--max-instances` or a host routed twice reject the whole reload, logged
(`routes reload (file change) failed; keeping generation N: ...`), and the routes
stay as they were until a file changes again or SIGHUP. Otherwise
(`routes reloaded (file change), generation N: added ...; removed ...; retargeted
...; updated ...; adjusted ...`):

- a route whose component (by sha256) and sandbox did not change keeps its handler
  and its warm pool. An `http(s)` component is fetched again only on SIGHUP
- a component changed on disk serves the next request, of a connection already open
  too; a request in flight finishes on the one it started with. Its target is
  unchanged, so nothing is advertised again
- a sandbox changed in `warm`, `instances`, `keep-ms` or `concurrency` alone keeps its handler, what runs on it
  included ([instance pool](#instance-pool)): a route's goes on under the target naming the new
  numbers, an app's target names none of them (`adjusted`), so nothing is advertised
- an added or removed host, or a changed sandbox (a new target; its env included), is
  advertised again on every node stream. A removed target keeps being served -- a
  connection the server routed before it took the new routes included -- until every
  stream acknowledged them; then its connections shut down gracefully (http/1.1 once the
  request in flight is answered, h2 with GOAWAY after its streams) and a CONNECT to it
  fails
- the replaced handlers' pools close: idle units end at once, busy ones after their
  request, and a new pool's units take the slots as they free (requests take the
  per-request path meanwhile)

The console's edits go through the same reload (see [Console](#console)). Only the routes
(with their keyvalue maps) are reloaded: another setting changed in the file -- the
`keyvalue` stores included -- is logged as such and applies on a restart. Removing every route needs a server from this version on (an
older one ignores an advertise without routes and keeps the old ones).

### Console

`--console <host:port>` (`console`) serves a page and a JSON API deploying components as
routes at once, without a restart: drop a `.wasm` file on the page (or choose one), give it a
host and a sandbox, and it is served -- on the tunnel and on `--listen` alike. Dropped on a
route of the console, a file replaces its component; renamed, a route moves to another host
with its component and sandbox. The routes of the config file and the `--wasm` flags are
listed too, read-only, and so are the [apps](#apps) a controller deploys through the API: hosts
served by one component in one sandbox. It takes 64 connections at once; TCP keepalive ends one
whose client is gone, and a client that leaves one waiting for 60s, not sending a request or not
reading its answer, loses it. All taken, one without a request the gate let through gives way to the
next, the source holding the most first
([details](../.kb/connection-limits.md#the-wickets-consoles-and-sluice-wicket-cf-api)); with none,
the next waits in the backlog.

```text
target/debug/sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090
# open http://127.0.0.1:9090/ and drop hello.wasm on it: served as hello.localhost
curl http://hello.localhost:8080/   # *.localhost is the loopback, in browsers too
```

A deploy is a reload (see [Reloading routes](#reloading-routes)) of the routes as they would
be with it, and it is kept only when they load: a component that does not compile, a host the
config file or a flag routes, pools past `--max-instances` refuse it with the reason, and the
routes stay as they were. The console's hosts are hosts like any other: routing one in the
config file too fails that reload (`configured twice: by the config file and by the
console`). An error names a component of the console by its path in `--console-dir`
(`wasm route 'counter.localhost': component components/0954…e8/counter.wasm imports
wasi:keyvalue/store@0.2.0-draft2: the route needs wasi=keyvalue`), one of the config file or a
flag by its file.

The console keeps its routes and apps in `--console-dir` (`console-dir`, default `deployed`
next to the config file, else in the working directory: `/data/deployed` in the image) and
serves them again after a restart:

```text
deployed/routes.yaml                      the routes and the apps, in the config file's route shape
deployed/components/<sha256>/<name>.wasm  the components they name, never rewritten
deployed/uploads/<sha256>.wasm            the components uploaded by digest, for an app to name
```

`routes.yaml` is written once a change loaded -- for its owner only (0600): it holds the apps'
env -- and a component no route or app names is removed; an upload, 10 minutes after it (an
app naming it links it below `components/`). A component's path names its digest, so other
content is another target: the route is retargeted, and its old target drains as a removed
one does. Without `--console` the directory is not read: the console's routes are served with
the console only. One wicket per directory.

`--console-token <token>` (`console-token`, `SLUICE_WICKET_RUNTIME_WASI_CONSOLE_TOKEN`) is asked of
every request: the browser prompts for it (HTTP Basic, any user name, the token as the
password), an API client sends `Authorization: Bearer <token>`. Without one, and without a
provider (below), the console must listen on a loopback address (a startup error otherwise),
and answers only requests for an IP address or `localhost`, so a DNS rebinding page cannot
reach it. An edit from another origin is refused. The console speaks plain HTTP: reached from
another host, put it behind TLS (a proxy passing the `Host` header on, and `X-Forwarded-Proto`:
an edit's `Origin` is checked against the host, the sign-in's callback is made from both).

#### Sign-in through an OpenID Provider

With `--oidc-issuer` (`oidc-issuer`) a browser signs in through the provider instead of
typing the token (which API clients keep sending as `Bearer`, the cf-api among them):

| option | |
|---|---|
| `--oidc-issuer <url>` | the provider; its discovery document is at `<url>/.well-known/openid-configuration`, found at the first sign-in |
| `--oidc-client-id <id>` | the client registered with it, whose redirect URI is `<console URL>/oauth2/callback` (required with `--oidc-issuer`) |
| `--oidc-client-secret <secret>` | its secret; none = a public client (PKCE only) |
| `--oidc-claim <claim>=<pattern>` | who may sign in: the ID token's claim must hold (a list claim: contain) the pattern, `*` matching any run of characters; repeatable (or comma-separated; a list in the config file). One claim's patterns are alternatives, distinct claims must all hold. At least one rule is required (`email=*` lets anyone the provider knows in) |
| `--oidc-scopes <list>` | the scopes asked for, comma-separated (default `openid,email,profile`; Dex sends the `groups` claim for the scope `groups`) |
| `--oidc-ca-cert <path>` | a PEM CA the provider's certificate is verified against (default the webPKI roots) |

A page request without a session is sent to `/login`, which runs the authorization code flow
(PKCE, a nonce) and whose callback, `/oauth2/callback`, exchanges the code for the ID token --
taken as it comes from the provider's token endpoint (over TLS, against the client's secret;
OpenID Connect Core 3.1.3.7, item 6) once `iss`, `aud`, `exp` and the nonce check out -- and
applies the `--oidc-claim` rules (`403` naming the claim that failed). The session is a cookie
sealed with a key drawn at startup (`HttpOnly`, `SameSite=Lax`, `Secure` behind
`X-Forwarded-Proto: https`), good for 8 hours; a restart signs every browser out. The page
names the user (`preferred_username`, else `email`, else `sub`) and links `/logout`. An API
request without a session or the token is answered `401` (no Basic prompt then: the page is
reloaded to sign in again). The cf-api's [Docker Compose](../sluice-wicket-cf-api/README.md#docker-compose)
sample runs the console and the cf-api on one Dex.

In a container the console listens on all addresses, to be reached through a published port,
so it takes a token. With Docker Compose (`compose.yaml`; the console's routes and the
keyvalue store kept in a volume):

```yaml
services:
  wicket:
    image: ghcr.io/making/sluice/sluice-wicket-runtime-wasi
    pull_policy: always
    command: --listen 0.0.0.0:8080 --console 0.0.0.0:9090 --health 127.0.0.1:8081
    environment:
      SLUICE_WICKET_RUNTIME_WASI_CONSOLE_TOKEN: ${CONSOLE_TOKEN:?set CONSOLE_TOKEN}
      # through a sluice server too (see Run):
      # SLUICE_WICKET_RUNTIME_WASI_SERVER: grpc://host.docker.internal:8001
      # SLUICE_WICKET_RUNTIME_WASI_TOKEN: ${SLUICE_TOKEN:?set SLUICE_TOKEN}
    ports:
    - "8080:8080"            # --listen, on the same port: the page links it
    - "127.0.0.1:9090:9090"  # the console, from this host only
    volumes:
    - wicket-data:/data      # deployed/ (the console's routes) and keyvalue.db
    # with a server, or --evacuation-grace: above them, see Shutdown (drain)
    # stop_grace_period: 30s
    healthcheck:             # see Readiness probe
      test: [CMD, /usr/local/bin/sluice-wicket-runtime-wasi, --probe, 127.0.0.1:8081]
      interval: 5s
volumes:
  wicket-data:
```

```text
CONSOLE_TOKEN=s3cret docker compose up -d
# open http://127.0.0.1:9090/ (any user name, s3cret as the password) and drop hello.wasm on it
curl http://hello.localhost:8080/
```

The API the page uses; `<options>` are the `--wasm` route options, the whole sandbox (one left
out takes its default):

| request | |
|---|---|
| `GET /api/routes[?after=<revision>]` | every route and the last failed reload of the files, as JSON; with `after` the current `revision`, held until they change (25s at most) |
| `PUT /api/routes/<host>?name=<file>&<options>` | deploys the body, a component of 128MiB at most, to the host (replacing the console's route of the host) |
| `PATCH /api/routes/<host>?<options>` | gives the console's route of the host another sandbox |
| `PATCH /api/routes/<host>?host=<to>` | moves the console's route of the host to `<to>` as it is (the route is renamed); `host=` takes no other option |
| `DELETE /api/routes/<host>` | removes the console's route of the host |

```text
curl -T hello.wasm -H 'Authorization: Bearer SECRET' \
    'http://127.0.0.1:9090/api/routes/hello.localhost?name=hello.wasm&wasi=cli,inherit-network'
{"generation":2,"message":"added hello.localhost"}
```

An edit is answered 400 (a bad host name, option or body), 404 (no route of the console), 409
(a host the config file, a flag or an app routes; for a move, any routed host), 413, 422 (the
routes did not load with it; the reason follows) or 503 (draining). A host is dot-separated
labels of `a-z`, `0-9` and `-`. A `wasi=keyvalue` route gets the default keyvalue map: the
default store, the host as the namespace. The namespace follows a move, so a moved route
starts with no data; what it stored stays under the old host.

The listing names each route's `origin` (`config`, `flag`, `console`, `api`), its advertised
`target` (the route's identity on the server: the component's absolute URL and the sandbox),
its component and sandbox (its `env` by the names), and the wicket's `keyvalue_stores`
(`name`, `type`, `default`, `shared`: which store it is when the wickets of other hosts may
reach it too, null for one of the host's own; see [Key-value stores](#key-value-stores-experimental)).

#### Apps

An app is hosts served by one component in one sandbox, its keyvalue map named, put and
removed by its owner: the API of a controller keeping the desired state of its apps (as
`sluice-wicket-cf-api` does). Its component is uploaded by digest first; the app names it:

| request | |
|---|---|
| `PUT /api/components/<sha256>` | keeps the body, a component of 128MiB at most, under its digest (400 when the body has another): `{"sha256": .., "bytes": ..}` |
| `PUT /api/files/<sha256>` | unpacks the body, a tar of an app's files (1GiB at most), under its digest, as the component's: `{"sha256": .., "bytes": ..}`; 400 naming what of the tar is refused |
| `GET /api/apps[?after=<revision>]` | the apps, as `/api/routes` (held with `after`): `{"revision", "generation", "wicket", "budget", "capabilities", "keyvalue_stores", "apps": [..]}`; `capabilities` and `keyvalue_stores` name what a sandbox may hold; `wicket` which wicket this is: `id`, made at its first start and kept in the console directory (`wicket-id`), `node`, its flume node (`wicket:<client-id>`, null without `--flume`), `build`; `budget` `max_instances` (`--max-instances`) and `instances`, the slots the routes' pools hold for good. An app needing more is refused 422 with `"no_room": true`. Told to stop with an evacuation bound, `"leaving": {"since", "until", "evacuated": [<owner>, ..]}` too (both listings; see [Shutdown](#shutdown-drain)) |
| `PUT /api/apps/<name>` | serves the app of the body (JSON, below) |
| `DELETE /api/apps/<name>?owner=<owner>` | removes the app of `owner` |
| `GET /api/keyvalue/<store>/<namespace>` | the routes mapping the namespace, one being drained included (`"a route being drained"`): `{"store", "namespace", "mapped_by": [..]}`; 404 for an unknown store. What a controller asks every wicket before it deletes a shared store's namespace through one |
| `DELETE /api/keyvalue/<store>/<namespace>` | deletes the namespace's keys (its usage too): `{"store", "namespace", "keys": <deleted>}`. Refused (409) while a route maps it, one being drained included; 404 for an unknown store. The namespace may hold `/` (percent-encoded or not) |
| `PUT /api/evacuated/<owner>` | a wicket leaving: `owner` moved what of its apps it could, those left staying until the drain, no longer waited for: the `leaving` of the listing; 409 unless leaving |

```json
{
  "owner": "cf",
  "hosts": ["counter.localhost", "count.localhost"],
  "component": {"sha256": "0954dc11…e8", "name": "counter.wasm"},
  "files": {"sha256": "7a1c…04"},
  "sandbox": {"wasi": ["keyvalue"], "budget-ms": 10000, "memory-mib": 256, "warm": 1,
              "instances": null, "keep-ms": null, "concurrency": 1,
              "keyvalue": {"": "local/cf/5d2f-guid"},
              "env": {"GREETING": "hello"}},
  "tag": "3f1c…9a"
}
```

```text
SHA=$(shasum -a 256 counter.wasm | cut -d' ' -f1)
curl -T counter.wasm -H 'Authorization: Bearer SECRET' http://127.0.0.1:9090/api/components/$SHA
curl -T app.json -H 'Authorization: Bearer SECRET' http://127.0.0.1:9090/api/apps/counter
{"generation":3,"message":"added count.localhost, counter.localhost"}
```

- the sandbox is the route options of `--wasm` in JSON, keyed by their names (`wasi` a list,
  `budget-ms`, `memory-mib`, `warm`, `instances`, `keep-ms`, `concurrency`), each left out taking
  its default, and the keyvalue map:
  with `keyvalue` granted, each identifier to `store/namespace` in full (an app has no one
  host to default the namespace to), the stores those of `keyvalue_stores`; and its `env`,
  `name -> value` as a config file route's (strings). The listings name the variables only
  (`"env": ["GREETING"]`, left out without one), never a value
- `files` (optional) are the app's files, its instances' read-only at `/` (as `files=` of a
  route): a tar uploaded by digest first, of regular files and directories only (a link, a
  device or a name out of the root fails the upload), each name once, 100000 entries and
  1GiB unpacked at most; a file keeps the tar's time. A component importing
  `wasi:filesystem` gets no app without them, nor `wasi=cli`. Files no app names are
  removed once `UPLOAD_GRACE` (10 minutes) passed since their upload. `sluice-wicket-cf-api`
  serves a static site so, with [sluice-wicket-site](../sluice-wicket-site/README.md)
- `tag` (optional; 1 to 128 printable ASCII chars but the space) is opaque to the wicket:
  kept and listed as given, so a controller tells what it put, an env it cannot read back
  included (`sluice-wicket-cf-api` tags each start's snapshot with a random tag). A tag alone
  changes no route
- a PUT is one reload, as a deploy: every host of the app or none, kept only when the routes
  load; the same app again changes nothing, and an app keeps its `deployed` time while its
  component and sandbox stay. An app listed by `GET /api/apps` is a body its PUT takes (less
  `name`, `deployed`, `alive` and `advertised`, and the env's values); a PUT is 1MiB at most
- `alive` in a listing: its instances alive, lowest first, a warm one's, a spare one's and a
  request's own while it runs, `{"index": 0, "since": 1793000000}`: the [index](#live-access-log) and its start in seconds
  since the epoch, a replacement's its own
- `advertised` in a listing: whether the server of every tunnel stream took the app's routes as
  they are, sending them connections (true in local mode): a controller moving an app here
  removes it from the wicket it leaves once they are
- an app's routes share a handler, and never with another app (its target names it,
  `…/counter.wasm?app=counter&…`): its pool, its `instances` cap and its guests' output stay
  its own. Its target names none of `warm`, `instances`, `keep-ms` and `concurrency`: a PUT changing them is
  [no change of what runs](#instance-pool), and advertises nothing
- answered as the console's edits, and 409 for a host another source, the page or another app
  routes, for another owner's app, and for a component or files no upload kept (`"missing":
  ["<sha256>", ..]`, every digest missing); a 422 names the host whose route did not load
  (`"host"`; none when no one route failed, e.g. pools past `--max-instances`)
- the page lists an app's routes read-only (`origin: "api"`, with `app` and `owner`), and its
  per-host endpoints refuse their hosts (409)
- an app and an owner are named by 1 to 100 of `A-Z`, `a-z`, `0-9`, `.`, `_` and `-`, starting
  with a letter or a digit; a component's file name by those, ending in `.wasm`
- the data of a namespace no app maps any more stays until deleted (`DELETE
  /api/keyvalue/...`): a controller deletes it with what owned it (`sluice-wicket-cf-api`: a
  service instance)

### Capabilities

The `wasi` route option grants WASI capabilities, named after the `-S`
options of `wasmtime serve` (v50), so a component runs here with the flags it
runs with there. Without it a route gets the serve default: `wasi:http` (incoming and
outgoing) plus the p3 `cli` / `clocks` / `random` interfaces. A component
importing anything else fails at startup.

| `wasi=` | `wasmtime serve` | effect |
|---|---|---|
| `http` | `-Shttp` | outgoing requests via `wasi:http/client`; always linked as in serve, so accepted but no effect. Ungated: a guest reaches any address the wicket host can, internal networks included |
| `cli` | `-Scli` | links the full wasi p3 set (sockets, filesystem, ...) |
| `inherit-network` | `-Sinherit-network` | sockets may use every address; implies `tcp` + `udp` |
| `tcp` / `udp` | `-Stcp` / `-Sudp` | allows the protocol (every address stays denied without `inherit-network`) |
| `allow-ip-name-lookup` | `-Sallow-ip-name-lookup` | allows `wasi:sockets/ip-name-lookup` |
| `keyvalue` | `-Skeyvalue` | links `wasi:keyvalue@0.2.0-draft2` (see [Key-value stores](#key-value-stores-experimental)) |

The network capabilities require `cli` (sockets are not linked otherwise) and
are enforced by `wasmtime-wasi` on every socket call: a denied call returns
`access-denied` to the guest. Unknown names are a startup error, and so is a component
importing what its route does not link, named with the capability it needs
(`imports wasi:keyvalue/store@0.2.0-draft2: the route needs wasi=keyvalue`).

```text
--wasm 'api.local=app.wasm?wasi=cli,inherit-network'
```

Guest stdout / stderr always go to the wicket's stdout / stderr as whole
lines, prefixed with the component and the id of the store writing them
(`stdout [app.wasm#12] :: ...`: a request's, or a pooled unit's for all its requests), as
`wasmtime serve` does; with a flume they reach the [live access log](#live-access-log) too.

### Live access log

`--flume <grpc://host:port | grpcs://host:port>` with `--flume-token <token>` feeds the
live access log of a sluice-flume as a sluice node feeds its own (see
[Live access log](../README.md#live-access-log)), so the consoles watching the flume and the
cf-api (`cf logs`) see the wicket's lines:

| kind | fields |
|---|---|
| `stdout` / `stderr` | `component`, `store` (the prefix's id), `instance`, `line` |
| `response` | `method`, `path`, `http` (`1.1`, `2`), `status`, `durationMs`, `bytes` (of the body), `remote`, `userAgent`, `instance` |

- the route key of a line is the host of its request: the `Host` (`:authority`), lowercase,
  without its port. A pooled unit's output names the request it was written during (empty
  before its first); taking several at once (`concurrency`), the one whose task wrote it
  (wasmtime's task groups: a task the request's handler spawned is its too), a line written
  outside any (the guest's own background) the request started last
- `instance` is the index of the instance writing the line or serving the request among
  its route's instances alive: a pooled unit's `0..warm` (its replacement keeps it), a
  per-request instance's or a spare unit's the lowest free from `warm` up. A request no instance served (no
  route, none within its budget, its client gone while it waited) has none
- `remote` is the last entry of the request's `X-Forwarded-For` (the node's peer), the
  client's `ip:port` on `--listen`; `durationMs` runs from the request's arrival to the end
  of its response body; a client gone before the response head is logged with status 499
- the wicket is the flume's node `wicket:<client-id>`: pin `--client-id` to stay one node
  across restarts
- as a node's: the feed runs while the flume asks for it; the last 512 lines are kept for its
  backfill, up to 4096 wait for a slow flume, past them lines are dropped and reach it as a
  gap; a flume down costs a warning per 5 minutes, redialed with a backoff of 1s to 30s
- a `grpcs://` flume is verified with `--flume-ca-cert <file>` (PEM), the webPKI roots by
  default, or not at all with `--flume-insecure`; in the config file `flume`,
  `flume-token`, `flume-ca-cert`, `flume-insecure` (read at startup)
- the feed is `sluice-wicket-tunnel`'s (`flume`): `sluice-wicket-runtime-firecracker` feeds
  its VMs' output through it (its README, "Output and the live access log")

### Key-value stores (experimental)

`wasi=keyvalue` gives the guest `wasi:keyvalue@0.2.0-draft2` (`store`, `atomics`, `batch`;
not the `watcher` export), the newest WASI keyvalue there is. The interface follows WASI
keyvalue as it moves: an async WASI 0.3 version replaces draft2 once it exists, and the
wicket's guest interface changes with it. A `0.2.0-draft` component (the version
`wasmtime serve -Skeyvalue` links) does not link here; `wasmtime serve` in turn does not
link draft2, so a draft2 guest runs locally in [local mode](#local-mode).

The store behind it is the host's choice and invisible to the guest. A route maps the
identifiers its guest may `open` to a store and a namespace in it (`sandbox.keyvalue` in
the config file); the default map, and the only one `--wasm` gives, is `""` -> the default
store, the route's host as the namespace. Routes are thus isolated unless their maps name
one namespace. An identifier the map lacks is `no-such-store`. The map is part of the
advertised target (`?keyvalue=:local/counter.local&wasi=keyvalue`), as the rest of the
sandbox: a changed map is a new target.

The stores are process-wide (the `keyvalue` section; a change applies on a restart). Without
the section there is one, `local`: a `sqlite` file `keyvalue.db` next to the config file, or
in the working directory without one; `--keyvalue-file` (`SLUICE_WICKET_RUNTIME_WASI_KEYVALUE_FILE`)
moves it. A store is opened when a route first maps to it: a wicket without a keyvalue
route creates no file. A map naming an unknown store fails the startup or the reload.

- `sqlite`: one SQLite file, the table `kv(namespace, key, value)`, inspected and edited with
  the `sqlite3` CLI (`sqlite3 keyvalue.db "SELECT namespace, key, CAST(value AS TEXT) FROM
  kv"`). WAL with `synchronous=NORMAL`: a power loss may drop the last commits, never
  corrupts the file. Several wickets on one host may share the file (they wait up to 5s
  for each other's lock); sharing across hosts is the `redis` store's job. `list-keys`
  pages hold up to 100 keys in key order, the cursor the last key of a page. A namespace
  holds at most `max-namespace-bytes` (default 64Mi) of keys and values, so a guest cannot
  fill the disk: a write past it fails and changes nothing. A purge deletes a namespace's
  rows and its usage in one transaction
- `redis`: the keys of a Redis or Valkey server (6 or later: the connection speaks RESP3),
  shared by the wickets on every host reaching it. A key is `<prefix><namespace>:<key>`,
  a `:` or `\` in the namespace escaped by `\` so no namespace reaches another's keys
  (`redis-cli --scan --pattern 'counter.local:*'`). `increment` is `INCRBY`, a `swap` a
  script, the batches `MGET` / `MSET` / `DEL`: each atomic. `list-keys` is one `SCAN` a
  page, the cursor SCAN's: a page may be empty before the last, and a key may come twice.
  No namespace bound: the server's `maxmemory` bounds it. A purge is `SCAN` + `DEL` of the
  namespace's keys, a batch at a time (not atomic: a write meanwhile may stay). A
  `rediss://` server's certificate is verified against `ca-cert`, else the webPKI roots.
  The connection opens on the first call and again once lost: a server down fails the calls
  with why (logged), not the startup. Each call answers within `timeout` or fails; one
  timing out has the next call open a new connection
- which store a store is, as listed (`shared`): none for a `sqlite` store or a `redis` one on
  a unix socket, a file or server of the host; else the server as its `url` names it, without
  the credentials, and the prefix (`redis://cache:6379/0?prefix=wicket:`), so two wickets
  naming one server alike list one store (`sluice-wicket-cf-api` spreads an app over wickets
  sharing its stores so). A loopback address names each host's own server: wickets of several
  hosts name it by an address they share
- every store: a key past `max-key-bytes` (default 1Ki) and a value past `max-value-bytes`
  (default 1Mi) are rejected before the store sees them

`increment` adds to the decimal ASCII of an i64 (absent: 0), as Redis `INCRBY`; any other
value is an error. A `cas` holds the value read when it began, and `swap` writes only while
the key still holds it; a lost race returns `cas-failed` with a handle on the current value.
Every error the guest sees is `other(message)` (draft2 has no finer case); a store failure
is logged (`[keyvalue] local/counter.local set failed: ...`).

The draft2 functions are not `async`: a call blocks its instance -- other tasks of the same
guest, a streaming response body among them -- while the store answers; other requests do
not wait. A pooled unit keeping a bucket or cas handle across requests leaves its resource
table dirty and is retired, so a guest opens its buckets per request (cheap: a table entry).

## Design notes

- the tunnel stream is served by `hyper` (http/1.1 and h2c, sniffed from the
  first bytes) through an `AsyncRead`/`AsyncWrite` adapter over the frame
  channels; no loopback sockets
- per-connection flow control as in `.kb/tunnel-flow-control.md`: the wicket announces a
  1MiB receive window per connection (fixed: it does not grow or shrink it)
  and grants back what hyper read; its writes send only against the server's
  grants, the server's initial window (64KiB) and what it grants beyond. A
  write without credit tells the server once per wait (WindowUpdate
  `blocked`), so the server grows its window up to 1MiB, and a server
  reclaiming the credit past its initial window (`reclaim_above`) gets it
  back (`released`). A guest slow to read a
  request body holds back only its own connection, and frames for the other
  connections keep flowing. A server without the window gets the old behavior
  (a connection whose guest does not read stalls the whole stream)
- transport flow control: the gRPC connection announces a fixed HTTP/2 window
  (stream and connection alike, adaptive window off) of twice what the
  connections may have in flight, `--max-connections` x 1MiB, capped at the
  HTTP/2 maximum (512MiB by default; logged at startup as `gRPC flow control
  window`). The tunnel is one stream, so at hyper's defaults (2MiB per stream)
  everything the server sends for every connection shared 2MiB per round trip.
  The window is a ceiling on what the server may send, not a buffer: the
  connections' windows bound what is held
- outbound frames are scheduled as by the Java sender: control frames
  (Advertise, WindowUpdate) first, then one frame per turn from each
  connection with frames queued: another connection's DATA, CLOSE or ERROR
  waits for one frame of each busy connection, not behind its backlog. A
  connection queues at most 16 DATA frames (up to 16KiB each); only its own
  writer waits beyond that
- per request: `wasmtime_wasi_http::p3::Request::from_http` -> fresh instance
  from a pre-linked `InstancePre` -> `Service.handle`; the store runs in a
  detached task and relays response body frames to hyper through a bounded
  channel, so responses stream and a slow client backpressures the guest
- every request's store carries an epoch deadline (`budget-ms`) and a memory
  limiter (`memory-mib`); the shared engine's epoch is incremented by a
  dedicated thread, so a guest busy-loop cannot starve its own budget. The
  epoch deadline only interrupts running wasm: a host-side watchdog outside
  the store ends a request suspended past its budget (e.g. on an upstream
  that never answers) and one whose client left -- before the response
  head, or mid-body (once a declared `content-length` is satisfied, the body
  counts as done: hyper drops it there itself, trailers owed or not; a
  bodiless response -- HEAD, 1xx, 204, 304 -- is drained behind the scenes,
  never a departure)
- over the tunnel only the server's ERROR counts as the client leaving. Its
  CLOSE (the public client's FIN) is a half-close: an http/1.1 request already
  received is still answered, and one whose guest stalls is held until the
  server's ERROR (the server probes a client that ended its side) or the budget
- on `--listen` the wicket does the server's part for http/1.1: a client's reset
  ends its request at once, and a client that ended its side while a response
  head is owed gets a `100 Continue` each second, at most four: a client gone
  answers with a reset, one that only half-closed reads them and its response.
  http/1.0 gets none, and once a response began nothing can be added to it: such
  a client that closed is noticed by a write, else its budget ends the request.
  An h2 connection ends with its client's FIN or reset
  ([details](../.kb/wicket-wasi-findings.md#a-client-gone-from---listen))
- instances come from wasmtime's pooling allocator, a slot of each kind per
  `--max-instances` instance (4 linear memories and a GC heap, 16 tables of 100k elements,
  32 core instances, 8 fiber stacks), so an instantiate never finds the pool exhausted; a
  component needing more is rejected at load. A memory slot reserves 4GiB of address space
  and a 32MiB guard, no memory (1.3TiB at the default 64 instances). An ended instance's
  memory is reset in place: with `PAGEMAP_SCAN` (Linux 6.7+) the pages it wrote, up to 4MiB
  a memory or table, are copied back and stay resident, the rest is released (`madvise`).
  Where the address space cannot be reserved (e.g. `ulimit -v`), instances are allocated on
  demand; the startup log names the allocator
- the runtime is tokio's default, a worker per core. Pooled, no memory map call is left per
  request; on demand, each instance's memory and fiber stack are mapped and unmapped, and past a
  few workers the CPU goes to the kernel's memory map lock and TLB shootdowns: the rate falls
  ([measured](../.kb/wicket-wasi-findings.md#instance-allocation-and-workers))
- guests are ordinary wasm components exporting `wasi:http/handler@0.3.0`;
  the linked wasi surface follows `wasmtime serve` (see `wasi` above) for
  p3 only. wasi p2 is unsupported and never linked (unlike serve's `-Scli`):
  a component importing any p2 interface is rejected at startup
- routes advertise the component locator plus its canonical
  non-default options (`wasm:file://...?wasi=cli,inherit-network`; the server
  passes `wasm:` targets through verbatim as the dial address — oci / s3
  resolvers slot into the same form), so routes sharing a component with
  different options stay distinct, and routes (hosts) with the same component
  and options share one handler: one pool, one `instances` cap. A route's env
  is in its target as `env=<digest>`: HMAC-SHA256 under a key the process
  draws at startup, so the server never sees a value, nor a digest to test a
  guess against (another process, another digest: a restart re-advertises
  such a route under another target)

### Instance pool

`warm=N` (opt-in, per route) keeps `N` warm `(Store, Service)` pairs. Each is
a detached task that runs `Store::run_concurrent` once (it consumes the
store) with a job loop inside, so reuse cannot hand instances
back: a unit serves `concurrency` requests at once (one by default) and the pool size times
that is the warm concurrency. The units take requests in turn, the one with the fewest in
flight first and of those the one idle longest, as
the gorouter balances an app's instances; each has an index of `0..N`, kept by
its replacement (the `instance` of its lines in the
[live access log](#live-access-log)). A unit is offered once instantiated; a
request beyond the stock, or on a route without `warm`, uses the per-request
path above while an instance slot is free (and the route's `instances` cap
allows), so the pool only ever improves the warm latency
([measured](../.kb/wicket-wasi-findings.md#a-pooled-units-latency)). Each unit holds an
instance slot of `--max-instances`, and one of its route's cap, for its
lifetime (a retired unit's replacement takes them over); the pools of all
routes together may not exceed the limit, which is checked at startup and on
every reload. With no slot free, a request waits for whichever comes first, a
unit with room or a free slot.

`keep-ms=N` lets the pool grow with the requests: a request finding no unit with room starts a
spare one in place of its per-request instance (on its slot, an index from `warm` up, as a
per-request instance's), which serves it and stays. `warm` is the floor (`0`: down to none):
the units take requests the pool's own first, then the spare that went idle last, so the
other spares age out; one ends once it served none for N ms, and gives its
slot up to a request or unit of any route waiting for one, an idle spare at once, a busy one
after its request (a spare reserves none: the startup and reload check counts `warm` alone).
A spare is never replaced. The route's `instances` cap bounds the spares with
the rest; unset, `--max-instances` does. Worth it for a guest whose start costs more than a
hello's: one writing an 8MiB heap answers in 1.6ms cold against 30us pooled
([measured](../.kb/wicket-wasi-findings.md#a-pooled-units-latency)).

A reload changing `warm`, `instances`, `keep-ms` or `concurrency` keeps what runs: the units
within the new `warm` serve on with what they hold, those missing start, those past it end, an
idle one at once and a busy one after its requests, as do the spares in the way of the new
`warm`'s indices and those idle past the new `keep-ms` (every one without it). A request open
goes on past a lower cap, which bounds the next ones, and a unit past a lower `concurrency`
takes none until below it.

Reuse assumes stateless / reentrant-safe guests: module state legitimately
persists across a unit's requests, so a misbehaving guest poisons its own
unit only. A unit is retired and replaced when its request ends in a trap, a
failed conversion, an interrupted relay, an overrun budget (re-armed per
request) or a client leaving mid-request (as above; logged, like every
retire), and when it leaves the resource table non-empty or grows its linear
memory past half of `memory-mib` since its last request ended. Wasm memory never shrinks, so a pooled
route holds up to
`memory-mib` per unit, `N` and its spares; the half-cap rule recycles a guest leaking per request
before it hits the cap, while one that settled below it stays warm. Guest
stdio is prefixed with the per-store id, so all requests a pooled unit
serves share that unit's single id.

### Concurrency

`concurrency=N` has a store take up to N requests at once: wasi:http 0.3's handler is an
async export, which the component model lets one instance run concurrently (a guest setting
backpressure takes them in turn, the others waiting in the store within their budgets). A unit
with room takes the next request; one more starts another store, a unit of the pool or else a
spare one (as with `keep-ms`; without it, ended once it serves none), within the route's
`instances`, which counts stores. So an instance is what CF and Cloud Run call one:

- module state is shared by the requests in flight: opt in for a guest safe so (default 1)
- `memory-mib` caps the store, its requests' together, and the half-cap rule is checked at
  each request's end; the resource table only when none is in flight
- `budget-ms` stays each request's. wasmtime 50 cancels no guest task, so a request past its
  budget is answered at its deadline (504, or a truncated body) and given up, as is one whose
  client left; its store takes no more requests, ends once the others are done, each within
  its own budget, and is replaced. A guest running wasm past a deadline blocks its store, so
  its epoch deadline yields to the watchdog: those it serves end at their deadlines, a request
  handed to it and never taken gets a 503 (`wasm instance stopped before serving the
  request`) as it stops
- a trap ends every request of the store: a 500, or a truncated body
- a store is one task: its requests share one core. On an M4 Max, keep-alive `GET /` to the
  hello guest at 16 connections served 92k/s on 16 stores (`warm=16`), 89k/s on 4 x 4 and
  40k/s on one store of 16, at the same ~60us of CPU a request; a guest waiting (5ms on a
  timer) served 2.1k/s either way, against 145/s one at a time
  ([measured](../.kb/wicket-wasi-findings.md#concurrency)). Spread a CPU-bound route over
  stores, give one waiting on upstreams the concurrency

## Cluster / reconnect

Same semantics as the Java client (see "Cluster (scale-out)" in the root
README): `--server` is only the bootstrap, asked for the node list
(`ListNodes`) and asked again while no stream is up; one tunnel stream is
kept per node at its public url (`grpc://` / `grpcs://` addresses; the
bootstrap for a node without one), following `MembershipUpdate`. So the
bootstrap may be a load balancer over any node, but each public url must
reach its own node (the wicket warns when a stream reaches another one). A
dropped stream reconnects with jittered exponential backoff (ceiling
1s..30s, each wait drawn from its upper half).

## Readiness probe

`--health <host:port>` (or `health:` in the config file) serves `GET /ready`, asking no
credentials: `200 ready` while the wicket serves, else `503 not ready: <why>`:

- while the routes start (`starting the routes`): it listens before they do
- with `--server`, until every node of the membership has a stream up that took the current
  routes (`no stream up that took the routes of generation N to <node>`), a reload's as well;
  from the drain on (`draining`), not during an [evacuation](#shutdown-drain) before it
- while a route has no instance ready (`no instance ready for <host>`; a wasm route always has
  one, a Firecracker wicket's VMs may not)
- without `--server`, ready once the routes started

`sluice-wicket-runtime-wasi --probe <host:port>` asks it and exits 0 when ready, 1 when not: the
image is distroless, with no HTTP client for a healthcheck. On Kubernetes a `readinessProbe`'s
`httpGet` asks `/ready` itself. There is no liveness endpoint: what a wicket cannot do without
ends it. Updating several wickets one at a time (a StatefulSet's rolling update, `docker compose
up --wait`, a script), the next one stops once the one before is ready again.

## Shutdown (drain)

With `--evacuation-grace SECONDS` (default 0: none) a SIGTERM / SIGINT first evacuates the
apps of the console's API, as Diego evacuates a cell:

- the wicket serves on as before -- its streams up, advertising its routes, `--listen`, the
  console's edits, ready to its probe -- and lists itself leaving (`"leaving"`, its revision
  moved: a controller's long poll answers at once)
- the controllers start each app elsewhere, then remove it from this wicket
  (`sluice-wicket-cf-api` moves each instance to a wicket with room); an owner that moved what
  it could says so (`PUT /api/evacuated/<owner>`), what is left of its apps staying until the
  drain
- once no app of the API is left but those of such owners, or the bound passed (`evacuation
  grace 60s expired with N app(s) left: ...`), it drains as below; without `--server` it ends.
  A second signal ends the evacuation at once, a third the drain
- the routes of the config file, the flags and the page stay as they are: nothing moves them
- its `routes.yaml` keeps what is left: started again, it serves no app that went elsewhere

On SIGTERM / SIGINT (once evacuated) the wicket drains as the Java client does (see "Client
shutdown (drain)" in the root README for what the server does meanwhile):

- every node stream withdraws the routes (`Advertise.withdraw`, the routes repeated);
  no stream reconnects from then on
- the connections open keep being served; once the server acknowledged the withdrawal
  (`AdvertiseAck.withdrawn`) and no connection is left on a stream, the wicket ends it.
  The process exits with 0 once every stream ended
- `--drain-grace SECONDS` (default 20) bounds the wait: the connections still open are
  then closed (`drain grace 20s expired; closing N open connection(s)`). `0` turns the
  drain off: a signal ends the process at once
- the next SIGTERM / SIGINT ends the drain at once
- the [readiness probe](#readiness-probe) answers 503 from the drain on: the first signal's,
  without an evacuation
- a server predating the drain acknowledges the withdrawal without `withdrawn` and keeps
  routing to the wicket: the wicket closes that stream at once

Where nothing drains (`--drain-grace 0`, or no `--server`) the process exits with 0 at once
(once evacuated): its handler ends it, as the init (pid 1) of a container too, which ignores a
signal it has no handler for.

`docker compose stop` (and `up -d` replacing a container) sends SIGKILL 10s after
SIGTERM: set `stop_grace_period` above `--evacuation-grace` and `--drain-grace` together, e.g.
`30s` for the defaults, `90s` with an evacuation of 60s. On Kubernetes,
`terminationGracePeriodSeconds` (30s by default) bounds it the same way.

## Known limitations (PoC scope)

- no mTLS (client certificates) yet
- the epoch budget is absolute (suspended time counts) and granular to the
  10ms tick; a CPU-limiting `fuel` accounting is not there yet. The ticker
  follows a fixed schedule and catches up a late wake-up, but replays at most
  100 ticks (1s) at once: a longer stall (a suspended process) is forgotten, so
  budgets stretch by the excess
- routes without `instances` share the instance slots unbounded: busy ones can
  hold every slot not pooled and delay the others. A route keeps slots of its
  own only through `warm`, or by capping the other routes below
  `--max-instances` together
- guest async tasks live within `wit-bindgen`'s runtime; the `spawn`
  caveats about task lifetime apply
- a reload changing a route's sandbox past `warm`, `instances`, `keep-ms` and `concurrency` gives it a new target with its
  own `instances` cap: until the old target's requests ended, both caps count, so the route may
  hold up to twice its cap
- wasi:keyvalue is draft2, without the `watcher` export, and its calls block the guest's
  instance (see [Key-value stores](#key-value-stores-experimental))
- file changes are seen by modification time, size and inode: a component rewritten
  in place with the same size within the file system's time granularity is missed
  until SIGHUP
- the console has no TLS of its own, its page sets no keyvalue map but the default one (an
  app names its own) and no env, and it keeps its directory to one wicket (nothing locks it)
- an env is in the clear in the config file (unless read from the environment, `${NAME}`)
  and in `routes.yaml`: their file permissions keep it

## Build note

`tonic-prost-build`'s generated `connect` convenience constructor collides
with the `Tunnel/Connect` rpc (E0592), so the build disables it
(`build_transport(false)`) and the client is constructed from a channel.
