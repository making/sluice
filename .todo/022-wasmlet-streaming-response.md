Difficulty: Medium

# sluice-wasmlet: stream response bodies instead of buffering

`wasmlet/src/wasm_host.rs` `respond()` collects the whole guest response into memory
(`BodyExt::collect`) before returning it from the hyper service. Stream it:

- `wasimoto_wasi_http::p3` `Response::into_http` returns `UnsyncBoxBody` whose
  `GuestBody` owns the store; the open question is keeping the store driven (and
  `run_concurrent` alive) while hyper polls the body after the service future returned
- likely shape: spawn the per-request task via `store.run_concurrent` and hand hyper a
  body that keeps the accessor alive; watch out for !Send (`UnsyncBoxBody`) vs hyper's
  executor needs on h2
- bounded memory: size guard / backpressure from the tunnel channel already exists
  (`CHUNK`), keep it

Acceptance: large streaming response (e.g. guest writing 10MB in chunks) proxies with
bounded client memory; `curl` sees progressive bytes.

Also covers removing the response-buffering note in `sluice-wasmlet/README.md`.
