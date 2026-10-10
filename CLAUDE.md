# sluice

TCP tunnel over a single gRPC bidirectional stream; the HTTP reverse proxy is its first consumer.

## Layout

Maven multi-module. Build + test with `./mvnw verify` (JDK 25 required). Always run
the reactor build from the root: `./mvnw -pl <module>` resolves the other modules from
stale `~/.m2` snapshots and fails with misleading `NoSuchMethodError`s.

- `sluice-proto`: proto + generated stubs + shared tunnel machinery (`am.ik.sluice.tunnel`: VirtualConnection / SessionSender / SocketRelay)
- `sluice-server`: gRPC control plane (8001) + raw TCP data plane (`sluice.data-port`, 8000) + actuator and management console `/console` (8081)
- `sluice-client`: tunnel client. grpc:// / grpcs:// (TLS verification skip via `sluice.insecure`)
- `sluice-it`: full stack E2E tests (real server + client apps in one JVM); E2E tests live here, not in the app modules
- `sluice-example-upstream`: minimal sample upstream (`It works`, http/1.1 + h2c) for manual checks

The data plane relays raw bytes (the request head is inspected for routing; with `preserve-host=false` every request head is rewritten on the wire). Honoring half-close (CLOSE frame) is essential for keep-alive / WebSocket passthrough.

## Conventions

- Coding standards: user-level skills (java-code-standards / spring-code-standards / java-package-structure / java-testing-standards). spring-javaformat validate is wired into the build.
- `application.properties`: keys in alphabetical order; a comment stays directly above the key it describes.
- gRPC 1.83.x quirks: `StreamObserver` / `ClientCallStreamObserver` live in `io.grpc.stub`. The client onReady handler may only be set inside `ClientResponseObserver#beforeStart`.
- Boot 4.1: `HealthIndicator` is in `org.springframework.boot.health.contributor`.
- wasmtime 49 (`sluice-wasmlet`): a timer raced inside the `run_concurrent` closure may never fire; put timeouts / watchdogs outside the call (see `supervise` in `wasm_host.rs`).
- Docker images via buildpack (`spring-boot:build-image`); no Dockerfile.
- Console: Mustache + htmx 4 (same setup as blog-frontend-htmx: vendored `htmx.min.js`, `{{#src}}` content-hashed URLs from `WebConfig`, `compression-maven-plugin` .br/.gz). htmx 4 inherits nothing implicitly (`:inherited`). The drawing geometry lives in `FlowDrawing`; templates stay logic-less.
- Native image (`-Pnative`, client/server): console view records are rendered via Mustache reflection -- when adding/renaming a record under `console/web`, register it in `sluice-server/src/main/resources/META-INF/native-image/am.ik.sluice/sluice-server/reflect-config.json`.
- Tests that need a port before the server starts take it from `TestPorts.freePort()` (sluice-it), never by probing `new ServerSocket(0)` and closing it: the OS may hand that port out again.
- Console E2E tests use Playwright (`ConsoleE2ETest`); the first run downloads Chromium.
- `.todo` numbers come from `.todo/claim-number.sh`.

## Manual E2E check

See "Run" in README.md.


## Open work

See `.todo/`. On completion, append a row to `.todo/history/YYYY-MM.md` (see existing rows for the format).
