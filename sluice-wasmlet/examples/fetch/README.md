# fetch

Outgoing http from a guest: `GET /?url=<http(s) url>` calls the url through
`wasi:http/client` and returns its status and body. Needs no capability:
outgoing http is linked for every route, as under `wasmtime serve` (the
`-S http` surface of `wasmtime run`).

```text
cargo build --release -p fetch   # from examples/
sluice-wasmlet ... --wasm fetch.local=examples/target/wasm32-wasip3/release/fetch.wasm
```

```text
curl -H 'Host: fetch.local' 'http://127.0.0.1:8000/?url=https://example.com/'
curl -H 'Host: fetch.local' 'http://127.0.0.1:8000/?url=http://nonexistent.invalid/'   # 502 with the ErrorCode
```

The url is taken verbatim up to the end of the query, so it may carry its own
query string.
