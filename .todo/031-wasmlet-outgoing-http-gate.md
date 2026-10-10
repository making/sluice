Difficulty: Medium

# sluice-wasmlet: decide whether outgoing http gets a capability gate

Today every route links `wasi:http/client` and `default_send_request` serves it
(`NoHooks`), as under `wasmtime serve`: any guest can reach any address the
wasmlet host can, internal networks included (SSRF surface). `wasi=http` is
accepted for `-S http` parity but changes nothing (`Caps::HTTP` in
`wasm_host.rs`).

Options:

- keep serve parity (status quo), document the exposure
- make `http` a real gate: link `wasi:http/client` only when granted (linker
  level, like `cli`) -- breaks parity with serve for http-calling components
- finer: a `WasiHttpHooks::send_request` that checks the authority against a
  per-route allow-list (e.g. `http-allow=host:port,...`), denying with
  `ErrorCode::HttpRequestDenied`

Decide with the user before implementing. Acceptance for any gate: the `fetch`
example's tests cover granted and denied routes; README `### Capabilities` row
for `http` updated.
