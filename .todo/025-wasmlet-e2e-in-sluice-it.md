Difficulty: High

# Automated E2E for sluice-wasmlet in sluice-it

The PoC was verified by hand only; nothing runs it in `./mvnw verify`.

- `sluice-it` gets a test that: builds/uses a prebuilt hello component (commit the
  `hello.wasm` artifact — it is 128KB and stable — or build via cargo if available,
  skipped when the toolchain is missing), starts the server + `sluice-wasmlet`
  binary, and asserts h1 / h2c / POST-echo / panic-isolation through the data plane
- the example guests now build with a pinned nightly (`sluice-wasmlet/examples`,
  wasm32-wasip3), which favors committing the artifacts over building in CI; the
  `relay` guest + `?wasi=cli,inherit-network` route covers the capability path,
  `fetch` the outgoing http path
- the rust binary path needs to be parameterized (env or property; default
  `../sluice-wasmlet/target/debug/sluice-wasmlet`), tests skip gracefully when the
  binary is absent so pure-Java builds stay green
- panic isolation assertion: `/panic` returns 500 and a subsequent request succeeds
  on the same route (the sandbox regression guard)
- reuse `TestPorts.freePort()` for any listener; the client needs no inbound port

Acceptance: `./mvnw verify` covers the wasm path; CI without rust still passes
(skipped, not failed).
