# sluice

TCP tunnel over a single gRPC bidirectional stream; the HTTP reverse proxy is its first consumer.

## Layout

Maven multi-module. Build + test with `./mvnw verify` (JDK 25, a Rust toolchain and `protoc`
required: `sluice-it` builds `sluice-flume` with cargo). Always run
the reactor build from the root: `./mvnw -pl <module>` resolves the other modules from
stale `~/.m2` snapshots and fails with misleading `NoSuchMethodError`s.

- `sluice-proto`: proto + generated stubs + shared tunnel machinery (`am.ik.sluice.tunnel`: VirtualConnection / SessionSender / SocketRelay)
- `sluice-server`: gRPC control plane (8001) + raw TCP data plane (`sluice.data-port`, 8000) + actuator and management console `/console` (8081)
- `sluice-client`: tunnel client. grpc:// / grpcs:// (TLS verification skip via `sluice.insecure`)
- `sluice-it`: full stack E2E tests (real server + client apps in one JVM); E2E tests live here, not in the app modules
- `sluice-flume` (Rust, own Cargo workspace): the live access log's collector; nodes feed it, consoles watch it (`flume.proto`). Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
- `sluice-wicket-wasi` (Rust, own Cargo workspace, laid out as sluice-flume): a tunnel client serving each route with a wasi:http component (wasmtime); its guest `examples/` are a separate workspace on a pinned nightly. Same gates
- `sluice-build-info` (Rust, own Cargo workspace): build script dependency of sluice-flume and sluice-wicket-wasi embedding their build (git commit, dirty, build time; `SLUICE_GIT_COMMIT` where there is no work tree, as in the Docker builds). Same gates
- `sluice-example-upstream`: minimal sample upstream (`It works`, http/1.1 + h2c) for manual checks

The data plane relays raw bytes (the request head is inspected for routing; every request head gets the `X-Forwarded-*` headers, and with `rewrite-host=true` its host is rewritten on the wire). Honoring half-close (CLOSE frame) is essential for keep-alive / WebSocket passthrough.

## Conventions

- Coding standards: user-level skills (java-code-standards / spring-code-standards / java-package-structure / java-testing-standards). spring-javaformat validate is wired into the build.
- `application.properties`: keys in alphabetical order; a comment stays directly above the key it describes.
- gRPC 1.83.x quirks: `StreamObserver` / `ClientCallStreamObserver` live in `io.grpc.stub`. The client onReady handler may only be set inside `ClientResponseObserver#beforeStart`.
- Boot 4.1: `HealthIndicator` is in `org.springframework.boot.health.contributor`.
- wasmtime 49 (`sluice-wicket-wasi`): a timer raced inside the `run_concurrent` closure may never fire; put timeouts / watchdogs outside the call (see `supervise` in `wasm_host.rs`).
- Docker images via buildpack (`spring-boot:build-image`); no Dockerfile, except the Rust components (`sluice-flume/Dockerfile`, built from the repository root).
- Console: Mustache + htmx 4 (same setup as blog-frontend-htmx: vendored `htmx.min.js`, `{{#src}}` content-hashed URLs from `WebConfig`, `compression-maven-plugin` .br/.gz). htmx 4 inherits nothing implicitly (`:inherited`). The drawing geometry lives in `FlowDrawing`; templates stay logic-less.
- Native image (`-Pnative`, client/server): console view records are rendered via Mustache reflection -- when adding/renaming a record under `console/web`, register it in `sluice-server/src/main/resources/META-INF/native-image/am.ik.sluice/sluice-server/reflect-config.json`.
- Tests that need a port before the server starts take it from `TestPorts.freePort()` (sluice-it), never by probing `new ServerSocket(0)` and closing it: the OS may hand that port out again.
- `sluice-it` classes run in 6 surefire forks at once (`-Dsluice.it.forks=N`), one class at a time per fork: a test must not depend on a quiet machine or on state outside its JVM (ports only via `TestPorts`, which reserves them across JVMs).
- Console E2E tests use Playwright (`ConsoleE2ETest`); the first run downloads Chromium.
- Tests needing a flume run `sluice-flume` as a child process via `Flume.start()` (sluice-it), one per class; it exits with the JVM (stdin EOF).
- `.todo` numbers come from `.todo/claim-number.sh`.

## Knowledge base

README.md is for users; detailed behavior (flow control, connection limits, h2 demux, metrics) lives in `.kb/<topic>.md`. Keep the README summary in sync when a `.kb` file changes.

## Manual E2E check

See "Run" in README.md.


## Open work

See `.todo/`. On completion, append a row to `.todo/history/YYYY-MM.md` (see existing rows for the format).
