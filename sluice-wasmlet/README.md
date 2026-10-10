# sluice-wasmlet

> **Experimental / proof of concept.** The API, layout, and behavior are
> unstable and may change or disappear; the limitations listed at the bottom
> are intentional PoC scope.

A **wasm platform on the sluice tunnel**: connects to a sluice server like the
Java client, but instead of relaying to TCP upstreams it terminates HTTP on
the tunnel and hands every request to a **wasi:http 0.3 component**
(`wasi:http/handler@0.3.0`, async `handle`). The proto is compiled directly
from `sluice-proto` — a single source of truth with the Java modules.

```
sluice server --tunnel--> sluice-wasmlet --hyper(h1/h2c)--> wasi:http handler component
```

## Layout

Two Cargo workspaces:

- `wasmlet/` — `sluice-wasmlet`, the tunnel-side wasm host (wasmtime + hyper)
- `examples/` — sample handler components (`wasm32-wasip3`), a separate
  workspace pinned to a nightly toolchain by `rust-toolchain.toml`
  (wasm32-wasip3 ships rust-std on nightly only; rustup installs it on the
  first build). Each example's README says what it demonstrates and which
  capabilities its route needs
- `wit/` — vendored wasi 0.3 (from wasmtime v49); each example declares its
  own world inline, so a new example is just a new directory under `examples/`

## Run

```text
cargo build -p sluice-wasmlet
(cd examples && cargo build --release)   # -> examples/target/wasm32-wasip3/release/<example>.wasm

# sluice server as in the root README, then:
target/debug/sluice-wasmlet --server grpc://127.0.0.1:8001 --token SECRET \
    --wasm demo.local=examples/target/wasm32-wasip3/release/hello.wasm
```

`cargo test -p sluice-wasmlet` builds the examples the same way (through the
rustup `cargo` proxy, so rustup must be installed).

`--server` also accepts `grpcs://` (TLS). Verification uses `--ca-cert <file>` (PEM, may
hold a chain), the webPKI roots when omitted, or nothing at all with `--insecure`
(self-signed setups; the `sluice.insecure` equivalent):

```text
target/debug/sluice-wasmlet --server grpcs://127.0.0.1:8001 --ca-cert ca.pem ...
target/debug/sluice-wasmlet --server grpcs://127.0.0.1:8001 --insecure ...
```

Multiple `--wasm host=<locator>` routes per process. The locator is a bare
path, `file://<path>`, or `http(s)://<url>` (fetched once at startup); the
advertised target is `wasm:<locator>` (plus the non-default options) and
further schemes slot into the same resolver. A `?query` suffix sets the
route's resource limits and WASI capabilities (`wasi`, see [Capabilities](#capabilities)), applied to every
request's instance: `budget-ms` is the request's wall-clock budget, covering
instantiation, handler and response relay (exceeded -> 504, or a truncated
body once streaming; defaults 10000) and `memory-mib` the per-instance linear
memory cap in MiB (exceeded -> guest trap, 500; defaults 256). `pool=N`
keeps `N` warm instances per route (see [Instance pool](#instance-pool)). A literal `?`
in a file path needs URL-escaping.

```text
--wasm demo.local=hello.wasm?budget-ms=5000&memory-mib=64
--wasm demo.local=hello.wasm?pool=8
curl -H 'Host: demo.local' http://127.0.0.1:8000/hello
```

### Capabilities

The `wasi` route option grants WASI capabilities, named after the `-S`
options of `wasmtime serve` (v49), so a component runs here with the flags it
runs with there. Without it a route gets the serve default: `wasi:http` (incoming and
outgoing) plus the p3 `cli` / `clocks` / `random` interfaces. A component
importing anything else fails at startup.

| `wasi=` | `wasmtime serve` | effect |
|---|---|---|
| `http` | `-Shttp` | outgoing requests via `wasi:http/client`; always linked as in serve, so accepted but no effect |
| `cli` | `-Scli` | links the full wasi p3 set (sockets, filesystem, ...) |
| `inherit-network` | `-Sinherit-network` | sockets may use every address; implies `tcp` + `udp` |
| `tcp` / `udp` | `-Stcp` / `-Sudp` | allows the protocol (every address stays denied without `inherit-network`) |
| `allow-ip-name-lookup` | `-Sallow-ip-name-lookup` | allows `wasi:sockets/ip-name-lookup` |

The network capabilities require `cli` (sockets are not linked otherwise) and
are enforced by `wasmtime-wasi` on every socket call: a denied call returns
`access-denied` to the guest. Unknown names are a startup error.

```text
--wasm 'api.local=app.wasm?wasi=cli,inherit-network'
```

Guest stdout / stderr always go to the wasmlet's stdout / stderr as whole
lines, prefixed with the component and a request id
(`stdout [app.wasm#12] :: ...`), as `wasmtime serve` does.

## Design notes

- the tunnel stream is served by `hyper` (http/1.1 and h2c, sniffed from the
  first bytes) through an `AsyncRead`/`AsyncWrite` adapter over the frame
  channels; no loopback sockets
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
- guests are ordinary wasm components exporting `wasi:http/handler@0.3.0`;
  the linked wasi surface follows `wasmtime serve` (see `wasi` above) for
  p3 only. wasi p2 is unsupported and never linked (unlike serve's `-Scli`):
  a component importing any p2 interface is rejected at startup
- `--wasm` routes advertise the component locator plus its canonical
  non-default options (`wasm:file://...?wasi=cli,inherit-network`; the server
  passes `wasm:` targets through verbatim as the dial address — oci / s3
  resolvers slot into the same form), so routes sharing a component with
  different options stay distinct

### Instance pool

`pool=N` (opt-in, per route) keeps `N` warm `(Store, Service)` pairs. Each is
a detached task that runs `Store::run_concurrent` once (it consumes the
store) with a per-request job loop inside, so reuse cannot hand instances
back: a unit serves one request at a time and the pool size is the warm
concurrency. A unit is offered once instantiated; a request beyond the
stock, or on a route without `pool`, uses the per-request path above, so the
pool only ever improves the warm latency (hello guest, dev profile: ~380us
cold vs ~120us pooled per request).

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
exponential backoff (1s..30s); exactly one stream per node is kept, so a
duplicate is closed on the client side before the server drops it.

## Known limitations (PoC scope)

- no mTLS (client certificates) yet
- the epoch budget is absolute (suspended time counts) and granular to the
  10ms tick; a CPU-limiting `fuel` accounting or per-route concurrency caps
  are not there yet
- guest async tasks live within `wit-bindgen`'s runtime; the `spawn`
  caveats about task lifetime apply

## Build note

`tonic-prost-build`'s generated `connect` convenience constructor collides
with the `Tunnel/Connect` rpc (E0592), so the build disables it
(`build_transport(false)`) and the client is constructed from a channel.
