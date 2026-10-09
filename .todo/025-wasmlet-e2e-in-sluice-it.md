Difficulty: High

# Automated E2E for sluice-wasmlet in sluice-it

The PoC was verified by hand only; nothing runs it in `./mvnw verify`.

- `sluice-it` gets a test that: builds/uses a prebuilt hello component (commit the
  `hello.wasm` artifact — it is 128KB and stable — or build via cargo if available,
  skipped when the toolchain is missing), starts the server + `sluice-wasmlet`
  binary, and asserts h1 / h2c / POST-echo / panic-isolation through the data plane
- the rust binary path needs to be parameterized (env or property; default
  `../sluice-wasmlet/target/debug/sluice-wasmlet`), tests skip gracefully when the
  binary is absent so pure-Java builds stay green
- panic isolation assertion: `/panic` returns 500 and a subsequent request succeeds
  on the same route (the sandbox regression guard)
- reuse `TestPorts.freePort()` for any listener; the client needs no inbound port

Acceptance: `./mvnw verify` covers the wasm path; CI without rust still passes
(skipped, not failed).
