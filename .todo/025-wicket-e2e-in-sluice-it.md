Difficulty: High

# Automated E2E for sluice-wicket-wasi in sluice-it

The PoC was verified by hand only; nothing runs it in `./mvnw verify`.

- `sluice-it` gets a test that: builds/uses a prebuilt hello component (commit the
  `hello.wasm` artifact — it is 128KB and stable — or build via cargo if available,
  skipped when the toolchain is missing), starts the server + `sluice-wicket-wasi`
  binary, and asserts h1 / h2c / POST-echo / panic-isolation through the data plane
- the example guests now build with a pinned nightly (`sluice-wicket-wasi/examples`,
  wasm32-wasip3), which favors committing the artifacts over building in CI; the
  `relay` guest + `?wasi=cli,inherit-network` route covers the capability path,
  `fetch` the outgoing http path
- the rust binary path needs to be parameterized (env or property; default
  `../sluice-wicket-wasi/target/debug/sluice-wicket-wasi`), tests skip gracefully when the
  binary is absent so pure-Java builds stay green
- panic isolation assertion: `/panic` returns 500 and a subsequent request succeeds
  on the same route (the sandbox regression guard)
- reuse `TestPorts.freePort()` for any listener; the client needs no inbound port

- per-connection flow control (047): the head-of-line scenario against the real
  server -- a large POST to `hello` `/stall` (the guest reads just past 1MiB, then
  stalls) while a GET on another connection answers at once (before 047: only after
  the stalled request's budget). Also h2c over the windowed path (a 10MiB `/stream`
  arrives complete): 047 checked both by hand only, its Rust tests use http/1.1
- build reporting (084): the wicket's row on `/console` shows the build its
  `--version` prints (`ConsolePeerBuildE2ETest` covers the server side with a
  wicket-shaped raw peer only), and the wicket logs the server's build from the
  ack (`[<key>] server build: sluice-server ...`); both are untested against the
  real binary today

- tunnel stall watchdog (074): `--tunnel-stall-timeout` against the real server is
  untested; the Rust test (`a_server_that_stops_reading_has_the_stream_given_up_at_the_stall_timeout`)
  drives the binary's node task against a raw h2 server that stops reading. Against the
  real server: a wicket whose stream the server stops reading gives it up and
  reconnects

- console (`--console`): the page was checked by hand in Chrome only; `tests/local.rs`
  covers its API. A Playwright test (as `ConsoleE2ETest`) against the real binary: a
  component chosen in the file chooser deploys and serves; one dropped on a console row
  replaces its component (a synthetic `DataTransfer` drop); a refused deploy (`counter`
  without `keyvalue`) shows the reason and the routes stay; the sandbox editor saves, and
  refuses instances below the pool; remove asks first; a config route has no actions; a
  core module is stopped before the upload; a broken config file shows the reload-failure
  banner and the fix clears it

Acceptance: `./mvnw verify` covers the wasm path; CI without rust still passes
(skipped, not failed).
