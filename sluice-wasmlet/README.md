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

Cargo workspace:

- `wasmlet/` — `sluice-wasmlet`, the tunnel-side wasm host (wasmtime + hyper)
- `examples/hello/` — the sample handler component (`wasm32-wasip2`)
- `wit/` — vendored `wasi:http@0.3.0` (from wasmtime v49) + the guest world

## Run

```text
cargo build -p sluice-wasmlet
cargo build --release -p hello --target wasm32-wasip2

# sluice server as in the root README, then:
target/debug/sluice-wasmlet --server grpc://127.0.0.1:8001 --token SECRET \
    --wasm demo.local=target/wasm32-wasip2/release/hello.wasm
```

`--server` also accepts `grpcs://` (TLS). Verification uses `--ca-cert <file>` (PEM, may
hold a chain), the webPKI roots when omitted, or nothing at all with `--insecure`
(self-signed setups; the `sluice.insecure` equivalent):

```text
target/debug/sluice-wasmlet --server grpcs://127.0.0.1:8001 --ca-cert ca.pem ...
target/debug/sluice-wasmlet --server grpcs://127.0.0.1:8001 --insecure ...
```

Multiple `--wasm host=<locator>` routes per process. The locator is a bare
path, `file://<path>`, or `http(s)://<url>` (fetched once at startup); the
advertised target is `wasm:<locator>` and further schemes slot into the same
resolver.

```text
curl -H 'Host: demo.local' http://127.0.0.1:8000/hello
curl --http2-prior-knowledge -H 'Host: demo.local' http://127.0.0.1:8000/hello
curl -H 'Host: demo.local' -d data http://127.0.0.1:8000/echo
curl -H 'Host: demo.local' http://127.0.0.1:8000/panic   # guest trap -> 500, tunnel survives
```

The sample handler echoes method / path / body size; `/panic` panics to
demonstrate guest isolation.

## Design notes

- the tunnel stream is served by `hyper` (http/1.1 and h2c, sniffed from the
  first bytes) through an `AsyncRead`/`AsyncWrite` adapter over the frame
  channels; no loopback sockets
- per request: `wasmtime_wasi_http::p3::Request::from_http` -> fresh instance
  -> `Service.handle`; response body buffered (PoC)
- guests are ordinary wasm components: the host links wasi p2 (rust std
  imports of the `wasm32-wasip2` target) + p3 (the 0.3 world), same as
  wasmtime's own p3 test suite
- `--wasm` routes advertise the component locator (`wasm:file://...`; the
  server passes `wasm:` targets through verbatim as the dial address — oci /
  s3 resolvers slot into the same form)

## Cluster / reconnect

Same semantics as the Java client (see "Cluster (scale-out)" in the root
README): `--server` is only the bootstrap -- the node list is learned via
`ListNodes` / `MembershipUpdate` and one tunnel stream is kept per node
(`grpc://` / `grpcs://` addresses). A dropped stream reconnects with
exponential backoff (1s..30s); exactly one stream per node is kept, so a
duplicate is closed on the client side before the server drops it.

## Known limitations (PoC scope)

- no mTLS (client certificates) yet
- buffered responses, one instance per request, no epoch-based CPU/memory
  limits yet
- guest async tasks live within `wit-bindgen`'s runtime; the `spawn`
  caveats about task lifetime apply

## Build note

`tonic-prost-build`'s generated `connect` convenience constructor collides
with the `Tunnel/Connect` rpc (E0592), so the build disables it
(`build_transport(false)`) and the client is constructed from a channel.
