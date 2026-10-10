# hello

The baseline handler: echoes the request and demonstrates the sandbox. Needs
no capability.

```text
cargo build --release -p hello   # from examples/
sluice-wasmlet ... --wasm 'demo.local=examples/target/wasm32-wasip3/release/hello.wasm?budget-ms=3000&memory-mib=64'
```

| request | demonstrates |
|---|---|
| any path (`-d data` for a body) | echo of method / path / body size, h1 and h2c |
| `/stream` | response streaming: 10 MiB in paced chunks |
| `/log` | guest stdout / stderr in the wasmlet log, line-prefixed |
| `/panic` | guest trap -> 500; the tunnel and later requests survive |
| `/spin` | CPU busy loop interrupted by `budget-ms` -> 504 |
| `/balloon` | allocation denied by `memory-mib` -> 500 |

```text
curl -H 'Host: demo.local' http://127.0.0.1:8000/hello
curl --http2-prior-knowledge -H 'Host: demo.local' http://127.0.0.1:8000/hello
curl -H 'Host: demo.local' -d data http://127.0.0.1:8000/echo
curl -H 'Host: demo.local' http://127.0.0.1:8000/stream -o /dev/null
curl -H 'Host: demo.local' http://127.0.0.1:8000/panic
```
