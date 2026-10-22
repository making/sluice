# sluice-wicket-wasi

> **Experimental / proof of concept.** The API, layout, and behavior are
> unstable and may change or disappear; the limitations listed at the bottom
> are intentional PoC scope.

A **wasm platform on the sluice tunnel**: connects to a sluice server like the
Java client, but instead of relaying to TCP upstreams it terminates HTTP on
the tunnel and hands every request to a **wasi:http 0.3 component**
(`wasi:http/handler@0.3.0`, async `handle`). The proto is compiled directly
from `sluice-proto` — a single source of truth with the Java modules.

```
sluice server --tunnel--> sluice-wicket-wasi --hyper(h1/h2c)--> wasi:http handler component
```

## Layout

Two Cargo workspaces:

- `.` — `sluice-wicket-wasi`, the tunnel-side wasm host (wasmtime + hyper), laid out as
  `sluice-flume`: `src/lib.rs` wires the modules, `src/main.rs` reads the command line
- `examples/` — sample handler components (`wasm32-wasip3`), a separate
  workspace pinned to a nightly toolchain by `rust-toolchain.toml`
  (wasm32-wasip3 ships rust-std on nightly only; rustup installs it on the
  first build). Each example's README says what it demonstrates and which
  capabilities its route needs
- `wit/` — vendored wasi 0.3 (from wasmtime v50) and `wasi:keyvalue@0.2.0-draft2`
  (`deps/keyvalue`, WebAssembly/wasi-keyvalue main `aa972c8`); each example declares its
  own world inline, so a new example is just a new directory under `examples/`

## Run

```text
cargo build
(cd examples && cargo build --release)   # -> examples/target/wasm32-wasip3/release/<example>.wasm

# sluice server as in the root README, then:
target/debug/sluice-wicket-wasi --server grpc://127.0.0.1:8001 --token SECRET \
    --wasm demo.local=examples/target/wasm32-wasip3/release/hello.wasm
# or with the routes in a file, reloaded on change (see Configuration file)
target/debug/sluice-wicket-wasi --config wicket-wasi.yaml
```

`cargo test` builds the examples the same way (through the
rustup `cargo` proxy, so rustup must be installed).

### Local mode

`--listen <host:port>` serves the routes over plain TCP as well -- http/1.1 and h2c, the
same handlers, sandbox, pools, budgets and reloads -- and without `--server` it is all the
wicket does: a local loop like `wasmtime serve`, with no sluice server.

```text
target/debug/sluice-wicket-wasi --listen 127.0.0.1:8080 \
    --wasm 'counter.local=examples/target/wasm32-wasip3/release/counter.wasm?wasi=keyvalue'
curl 127.0.0.1:8080/
```

A request goes to the route of its host (the `Host` header, or the h2 authority; the port
left out), or, with a single route, to that route whatever its host. An unknown host is
answered 404. Both `--server` and `--listen` serve the tunnel and the listener at once.
`--max-connections` bounds the listener's connections too, apart from the tunnel's (one
beyond waits in the backlog). Without `--server` nothing drains: SIGTERM / SIGINT end the
process at once.

The image `ghcr.io/making/sluice/sluice-wicket-wasi` (`latest` / `<sha>`, amd64 /
aarch64) is published on every push to `main` changing the wicket; it takes the same
arguments, with the components mounted:

```text
docker run --rm --pull always -v $PWD/examples/target/wasm32-wasip3/release:/wasm:ro \
    ghcr.io/making/sluice/sluice-wicket-wasi \
    --server grpc://host.docker.internal:8001 --token SECRET --wasm demo.local=/wasm/hello.wasm
```

Its working directory is `/data`, where the [keyvalue](#key-value-stores-experimental) store
is created without a `--config` file (`/data/keyvalue.db`): mount a volume there
(`-v wicket-data:/data`) to keep it, or the stores die with the container.

`--help` lists the options. Each option but `--wasm` also reads
`SLUICE_WICKET_WASI_<NAME>` from the environment (upper case, `-` as `_`, e.g.
`SLUICE_WICKET_WASI_TOKEN`; `SLUICE_WICKET_WASI_INSECURE=true`), and takes its value as
`--name value` or `--name=value`.

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
target/debug/sluice-wicket-wasi --server grpcs://127.0.0.1:8001 --ca-cert ca.pem ...
target/debug/sluice-wicket-wasi --server grpcs://127.0.0.1:8001 --insecure ...
```

`--client-id <id>` pins the client identity sent as `x-sluice-id` on every tunnel
stream; when omitted a random UUID is used, matching the Java client's
`sluice.client.id`. Re-registering with the same id replaces the previous
registration on the server.

`--listen <host:port>` serves the routes over plain TCP too (see [Local mode](#local-mode));
`--keyvalue-file <path>` moves the keyvalue store (see
[Key-value stores](#key-value-stores-experimental)).

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
so guest linear memory stays within N x the largest `memory-mib` (16GiB at the defaults).
Every instance holds one of the N slots until it is gone: a request's, or a pooled unit's
for the unit's lifetime (see [Instance pool](#instance-pool)). A request finding no slot
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
memory cap in MiB (exceeded -> guest trap, 500; defaults 256). `pool=N`
keeps `N` warm instances per route (see [Instance pool](#instance-pool)).
`instances=N` caps the instance slots the route holds at once, its pooled
units included; unset, the route is bounded by `--max-instances` alone. A
request finding the route at its cap waits for one of the route's slots within
its budget (503 when it runs out, as above), holding no `--max-instances` slot
meanwhile: requests waiting on a route's cap take no slot from the other
routes. The cap reserves nothing: only `pool` does. It must be at least 1 and
at least `pool` (a startup error otherwise). With a cap equal to `pool`, a
request finding every unit busy waits for one instead of instantiating per
request. A literal `?` in a file path needs URL-escaping.

```text
--wasm demo.local=hello.wasm?budget-ms=5000&memory-mib=64
--wasm demo.local=hello.wasm?pool=8
--wasm batch.local=batch.wasm?instances=4
curl -H 'Host: demo.local' http://127.0.0.1:8000/hello
```

A host may be routed once, over `--wasm` and the config file together (a startup or
reload error otherwise).

### Configuration file

`--config <file>` reads the settings from a YAML file. Every flag has its key; a flag
overrides the environment (`SLUICE_WICKET_WASI_<NAME>`), the environment the file, the
file a default. Without `--config` the flags alone apply, as
before. With it, `server` or `listen` is the one setting required: the routes may be empty
at startup and added by a reload.

```yaml
server: grpc://127.0.0.1:8001
token: ${SLUICE_TOKEN}          # an environment variable
client-id: ${HOSTNAME:-wicket}  # with a default
# insecure: true
# ca-cert: ca.pem               # relative to this file
max-connections: 256
max-instances: 64
tunnel-stall-timeout: 60s
drain-grace: 20s
watch-interval: 1s              # 0 = no polling, SIGHUP only
# listen: 127.0.0.1:8080        # local mode, see Local mode

keyvalue:                       # optional: one sqlite store, keyvalue.db next to this file
  default: local                # optional with one store
  stores:
    local:
      type: sqlite
      path: keyvalue.db         # relative to this file
      # max-key-bytes: 1Ki      # the defaults
      # max-value-bytes: 1Mi
      # max-namespace-bytes: 64Mi

routes:
  - host: demo.local
    component: hello.wasm       # a path relative to this file, file:// or http(s)://
  - host: relay.local
    component: relay.wasm
    sandbox:                    # the --wasm query options; unset keys keep their defaults
      wasi: [cli, inherit-network]
      budget: 250ms             # budget-ms
      memory: 64Mi              # memory-mib: a whole number of MiB (Ki/Mi/Gi, K/M/G)
      pool: 2
      instances: 4
  - host: counter.local
    component: counter.wasm
    sandbox:
      wasi: [keyvalue]
      keyvalue:                 # optional: identifier -> store[/namespace]
        "": local               # the namespace defaults to the host
        sessions: local/shared-sessions
```

- durations take `ms`, `s`, `m` or `h` (`tunnel-stall-timeout`, `drain-grace` and
  `watch-interval` also a bare `0` = off); the flags keep whole seconds
- sizes take a whole number with `Ki`, `Mi`, `Gi` (or `K`, `M`, `G`); `memory` must be
  whole MiB, the keyvalue limits may be a bare number of bytes
- `${NAME}` in a plain (unquoted) scalar is the environment variable `NAME`, with the
  docker compose forms (`${NAME:-default}`, `${NAME?message}`, ...); an unset `${NAME}`
  without a default is an error, `$${` a literal `${`, and a quoted scalar is never
  expanded
- an unknown key or a value that does not parse fails, naming the line, or the route and
  key (`routes[1]: sandbox.memory: ...`)
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
...; updated ...`):

- a route whose component (by sha256) and sandbox did not change keeps its handler
  and its warm pool. An `http(s)` component is fetched again only on SIGHUP
- a component changed on disk serves the next request, of a connection already open
  too; a request in flight finishes on the one it started with. Its target is
  unchanged, so nothing is advertised again
- an added or removed host, or a changed sandbox (a new target), is advertised again
  on every node stream. A removed target keeps being served -- a connection the server
  routed before it took the new routes included -- until every stream acknowledged
  them; then its connections shut down gracefully (http/1.1 once the request in flight
  is answered, h2 with GOAWAY after its streams) and a CONNECT to it fails
- the replaced handlers' pools close: idle units end at once, busy ones after their
  request, and a new pool's units take the slots as they free (requests take the
  per-request path meanwhile)

Only the routes (with their keyvalue maps) are reloaded: another setting changed in the
file -- the `keyvalue` stores included -- is logged as such and applies on a restart. Removing every route needs a server from this version on (an
older one ignores an advertise without routes and keeps the old ones).

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
lines, prefixed with the component and a request id
(`stdout [app.wasm#12] :: ...`), as `wasmtime serve` does.

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
in the working directory without one; `--keyvalue-file` (`SLUICE_WICKET_WASI_KEYVALUE_FILE`)
moves it. A store is opened when a route first maps to it: a wicket without a keyvalue
route creates no file. A map naming an unknown store fails the startup or the reload.

- `sqlite`: one SQLite file, the table `kv(namespace, key, value)`, inspected and edited with
  the `sqlite3` CLI (`sqlite3 keyvalue.db "SELECT namespace, key, CAST(value AS TEXT) FROM
  kv"`). WAL with `synchronous=NORMAL`: a power loss may drop the last commits, never
  corrupts the file. Several wickets on one host may share the file (they wait up to 5s
  for each other's lock); sharing across hosts is a networked store's job. `list-keys`
  pages hold up to 100 keys in key order, the cursor the last key of a page. A namespace
  holds at most `max-namespace-bytes` (default 64Mi) of keys and values, so a guest cannot
  fill the disk: a write past it fails and changes nothing
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
  connections' windows bound what is held. A unit test sends 32MiB through a
  proxy that holds the wicket's answers back one round trip at a time: 17
  round trips at hyper's defaults, at most 3 with the window
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
- only the server's ERROR counts as the client leaving. Its CLOSE (the
  public client's FIN) is a half-close: an http/1.1 request already received
  is still answered, and one whose guest stalls is held until the server's
  ERROR or the budget
- guests are ordinary wasm components exporting `wasi:http/handler@0.3.0`;
  the linked wasi surface follows `wasmtime serve` (see `wasi` above) for
  p3 only. wasi p2 is unsupported and never linked (unlike serve's `-Scli`):
  a component importing any p2 interface is rejected at startup
- routes advertise the component locator plus its canonical
  non-default options (`wasm:file://...?wasi=cli,inherit-network`; the server
  passes `wasm:` targets through verbatim as the dial address — oci / s3
  resolvers slot into the same form), so routes sharing a component with
  different options stay distinct, and routes (hosts) with the same component
  and options share one handler: one pool, one `instances` cap

### Instance pool

`pool=N` (opt-in, per route) keeps `N` warm `(Store, Service)` pairs. Each is
a detached task that runs `Store::run_concurrent` once (it consumes the
store) with a per-request job loop inside, so reuse cannot hand instances
back: a unit serves one request at a time and the pool size is the warm
concurrency. A unit is offered once instantiated; a request beyond the
stock, or on a route without `pool`, uses the per-request path above while
an instance slot is free (and the route's `instances` cap allows), so the
pool only ever improves the warm latency (hello guest, dev profile: ~380us
cold vs ~120us pooled per request). Each unit holds an instance slot of
`--max-instances`, and one of its route's cap, for its lifetime (a retired
unit's replacement takes them over); the pools of all routes together may not
exceed the limit, which is checked at startup and on every reload. With no slot
free, a request waits for whichever comes first, an idle unit or a free slot.

Reuse assumes stateless / reentrant-safe guests: module state legitimately
persists across a unit's requests, so a misbehaving guest poisons its own
unit only. A unit is retired and replaced when its request ends in a trap, a
failed conversion, an interrupted relay, an overrun budget (re-armed per
request) or a client leaving mid-request (as above; logged, like every
retire), and when it leaves the resource table non-empty or grows its linear
memory past half of `memory-mib`. Wasm memory never shrinks, so a pooled
route holds up to
`N` x `memory-mib`; the half-cap rule recycles a guest leaking per request
before it hits the cap, while one that settled below it stays warm. Guest
stdio is prefixed with the per-store id, so all requests a pooled unit
serves share that unit's single id.

## Cluster / reconnect

Same semantics as the Java client (see "Cluster (scale-out)" in the root
README): `--server` is only the bootstrap -- the node list is learned via
`ListNodes` / `MembershipUpdate` and one tunnel stream is kept per node
(`grpc://` / `grpcs://` addresses). A dropped stream reconnects with
jittered exponential backoff (ceiling 1s..30s, each wait drawn from
its upper half); exactly one stream per node is kept, so a
duplicate is closed on the client side before the server drops it.

## Shutdown (drain)

On SIGTERM / SIGINT the wicket drains as the Java client does (see "Client shutdown
(drain)" in the root README for what the server does meanwhile):

- every node stream withdraws the routes (`Advertise.withdraw`, the routes repeated);
  no stream reconnects from then on
- the connections open keep being served; once the server acknowledged the withdrawal
  (`AdvertiseAck.withdrawn`) and no connection is left on a stream, the wicket ends it.
  The process exits with 0 once every stream ended
- `--drain-grace SECONDS` (default 20) bounds the wait: the connections still open are
  then closed (`drain grace 20s expired; closing N open connection(s)`). `0` turns the
  drain off: no signal handler is installed, a signal ends the process at once
- a second SIGTERM / SIGINT ends the drain at once
- a server predating the drain acknowledges the withdrawal without `withdrawn` and keeps
  routing to the wicket: the wicket closes that stream at once

`docker compose stop` (and `up -d` replacing a container) sends SIGKILL 10s after
SIGTERM: set `stop_grace_period` above `--drain-grace`, e.g. `30s` for the default. On
Kubernetes, `terminationGracePeriodSeconds` (30s by default) bounds it the same way.

## Known limitations (PoC scope)

- no mTLS (client certificates) yet
- the epoch budget is absolute (suspended time counts) and granular to the
  10ms tick; a CPU-limiting `fuel` accounting is not there yet. The ticker
  follows a fixed schedule and catches up a late wake-up, but replays at most
  100 ticks (1s) at once: a longer stall (a suspended process) is forgotten, so
  budgets stretch by the excess
- routes without `instances` share the instance slots unbounded: busy ones can
  hold every slot not pooled and delay the others. A route keeps slots of its
  own only through `pool`, or by capping the other routes below
  `--max-instances` together
- guest async tasks live within `wit-bindgen`'s runtime; the `spawn`
  caveats about task lifetime apply
- a reload changing a route's sandbox gives it a new target with its own `instances`
  cap: until the old target's requests ended, both caps count, so the route may hold
  up to twice its cap
- wasi:keyvalue is draft2, without the `watcher` export, and its calls block the guest's
  instance (see [Key-value stores](#key-value-stores-experimental)); `sqlite` is the only
  store yet
- file changes are seen by modification time, size and inode: a component rewritten
  in place with the same size within the file system's time granularity is missed
  until SIGHUP

## Build note

`tonic-prost-build`'s generated `connect` convenience constructor collides
with the `Tunnel/Connect` rpc (E0592), so the build disables it
(`build_transport(false)`) and the client is constructed from a channel.
