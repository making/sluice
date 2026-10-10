# relay

Raw TCP from a guest and the network capabilities: `GET /?addr=<ip>:<port>`
opens a `wasi:sockets` TCP connection, sends an HTTP/1.0 GET and returns the
raw reply. Needs `wasi=cli,inherit-network`.

```text
cargo build --release -p relay   # from examples/
sluice-wasmlet ... \
    --wasm 'relay.local=examples/target/wasm32-wasip3/release/relay.wasm?wasi=cli,inherit-network' \
    --wasm 'denied.local=examples/target/wasm32-wasip3/release/relay.wasm?wasi=cli'
```

```text
curl -H 'Host: relay.local' 'http://127.0.0.1:8000/?addr=127.0.0.1:31080'    # 200, upstream reply
curl -H 'Host: denied.local' 'http://127.0.0.1:8000/?addr=127.0.0.1:31080'   # 502, AccessDenied
```

Without `cli` the component does not load at all (wasi:sockets is not
linked); with `cli` but without `inherit-network` every address is denied.
`127.0.0.1:31080` is the sample upstream of the root README.
