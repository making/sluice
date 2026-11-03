# sluice

TCP tunnel over a single gRPC bidirectional stream; the HTTP reverse proxy is its first consumer.

## Layout

Maven multi-module. Build + test with `./mvnw verify` (JDK 25, a Rust toolchain, `cc` and `protoc`
required: `sluice-it` builds `sluice-flume` with cargo). Always run
the reactor build from the root: `./mvnw -pl <module>` resolves the other modules from
stale `~/.m2` snapshots and fails with misleading `NoSuchMethodError`s.

- `sluice-proto`: proto + generated stubs + shared tunnel machinery (`am.ik.sluice.tunnel`: VirtualConnection / SessionSender / SocketRelay)
- `sluice-server`: gRPC control plane (8001) + raw TCP data plane (`sluice.data-port`, 8000) + actuator and management console `/console` (8081)
- `sluice-client`: tunnel client. grpc:// / grpcs:// (TLS verification skip via `sluice.insecure`)
- `sluice-it`: full stack E2E tests (real server + client apps in one JVM); E2E tests live here, not in the app modules
- `sluice-flume` (Rust, own Cargo workspace): the live access log's collector; nodes and wickets feed it, consoles and the cf-api watch it (`flume.proto`). Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
- `sluice-wicket-runtime-wasi` (Rust, own Cargo workspace, laid out as sluice-flume): a tunnel client serving each route with a wasi:http component (wasmtime); its guest `examples/` are a separate workspace on a pinned nightly. Same gates; its tests run `redis-server` (with TLS) as a child process
- `sluice-wicket-cf-api` (Rust, own Cargo workspace, laid out as sluice-flume): a Cloud Foundry API facade (CC v3 / UAA / log-cache, the subset the OSS `cf` CLI v8 uses) deploying apps to one wicket through its app API. Same gates; its integration test runs the OSS CLI (`SLUICE_CF_CLI`, else `cf8` on the PATH; CI installs 8.19.0) against the cf-api and a wicket it builds from `sluice-wicket-runtime-wasi`, on a flume it builds from `sluice-flume`, and where `/dev/kvm` is usable (`sg kvm -c 'cargo test'`) from `sluice-wicket-runtime-firecracker`, then itself in a VM of that wicket. `demo/record.sh` re-records the README's demo video with VHS (`demo/README.md`)
- `sluice-wicket-runtime-firecracker` (Rust, own Cargo workspace): a tunnel client serving each route with Firecracker microVMs over vsock, VMs booted (`mode=boot`, `vms=N`), or clones of a snapshot (one per connection, reused, or pooled), within a VM / memory budget, reloaded from `--config`, the VMs' output on its stdout / stderr and a flume, their egress through it to the destinations a route allows (`egress.rs`); jail mode (`--jail-ids`, started as root) jails each VM (`jail.rs`) from a root keeper (`keeper.rs`) running the wicket unprivileged: keep the keeper single-threaded (it forks each VM's jail); `guest/` is the VMs' `/init` (musl, its own workspace: keep `guest/src/wire.rs` a copy of `src/wire.rs`), `scripts/cnb-rootfs.sh` turns an OCI / CNB image into a rootfs. Same gates (and the guest's); its VM test (`tests/vm.rs`) runs where `/dev/kvm` is usable (`sg kvm -c 'cargo test'` until the `kvm` group is in the login) and is skipped elsewhere, the CI's aarch64 runner among them (on an M3 or later Mac, `scripts/lima.sh` runs it on real aarch64, in a nested virtualization VM); `SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1` runs it jailed (through `sudo -n`). `--console` deploys uploaded rootfs images as routes, and apps (`console.rs`, `deployed.rs`), an OCI image's rootfs made on the wicket with `--guest` (`image.rs`, `rootfs.rs`), an app's one booted VM with a persistent disk (`disk.rs`: one writer, a `flock` going with its `firecracker`; a reload stops the old VM first). Never fork the wicket per VM (`vm.rs`: a fork holds its memory map lock); findings in `.kb/wicket-firecracker-findings.md`
- `sluice-wicket-oidc` (Rust, own Cargo workspace): the OpenID Provider client (discovery, the authorization code flow with PKCE, the claim rules, a session sealer) of the wickets' consoles and sluice-wicket-cf-api, with the mock provider their tests run (feature `mock`). Gates as sluice-flume's, with `--all-features`
- `sluice-wicket-console` (Rust, own Cargo workspace): what the consoles of sluice-wicket-runtime-wasi and sluice-wicket-runtime-firecracker share: the gate (token, OpenID sign-in, loopback only, no foreign origin), the server and the page's shared files (`console.css`, fonts, `kit.js` the scripts import), the bodies (an upload streamed to a file under its digest), the directory's layout (`store`), the long poll, the options. Same gates
- `sluice-wicket-registry` (Rust, own Cargo workspace): the OCI distribution client (references, manifests, blobs, registries' challenges) sluice-wicket-cf-api and sluice-wicket-runtime-firecracker pull images with. Same gates
- `sluice-wicket-server` (Rust, own Cargo workspace): what the listeners of sluice-wicket-runtime-wasi, sluice-wicket-runtime-firecracker and sluice-wicket-cf-api have alike: the accept loop with its connections' slots and TCP keepalive (`accept`), an HTTP/1.1 connection no client holds by going quiet (`http`). Same gates
- `sluice-wicket-tunnel` (Rust, own Cargo workspace): the tunnel client of sluice-wicket-runtime-wasi and sluice-wicket-runtime-firecracker (ListNodes fan-out, node sessions, per-connection flow control, the connection limit, the drain), their live access log's feed to a sluice-flume (`flume`), and their local mode's routing by host (`listener`); a wicket supplies its route table and how a connection is served (`routes::Table` / `routes::Binding`). Its test doubles (a tunnel server, a peer feeding the dispatch, a flume) are the wickets' too (feature `testing`). Gates as sluice-flume's, with `--all-features`
- `sluice-build-info` (Rust, own Cargo workspace): build script dependency of sluice-flume, sluice-wicket-runtime-wasi and sluice-wicket-cf-api embedding their build (git commit, dirty, build time; `SLUICE_GIT_COMMIT` where there is no work tree, as in the Docker builds). Same gates
- `sluice-example-upstream`: minimal sample upstream (`It works`, http/1.1 + h2c) for manual checks

The data plane relays raw bytes (the request head is inspected for routing; every request head gets the `X-Forwarded-*` headers, and with `rewrite-host=true` its host is rewritten on the wire). Honoring half-close (CLOSE frame) is essential for keep-alive / WebSocket passthrough.

## Conventions

- Coding standards: user-level skills (java-code-standards / spring-code-standards / java-package-structure / java-testing-standards). spring-javaformat validate is wired into the build.
- `application.properties`: keys in alphabetical order; a comment stays directly above the key it describes.
- gRPC 1.83.x quirks: `StreamObserver` / `ClientCallStreamObserver` live in `io.grpc.stub`. The client onReady handler may only be set inside `ClientResponseObserver#beforeStart`.
- Boot 4.1: `HealthIndicator` is in `org.springframework.boot.health.contributor`.
- wasmtime 50 (`sluice-wicket-runtime-wasi`; seen on 49): a timer raced inside the `run_concurrent` closure may never fire; put timeouts / watchdogs outside the call (see `supervise` in `wasm_host.rs`).
- Docker images via buildpack (`spring-boot:build-image`); no Dockerfile, except the Rust components (`sluice-flume/Dockerfile`, built from the repository root).
- Console: Mustache + htmx 4 (same setup as blog-frontend-htmx: vendored `htmx.min.js`, `{{#src}}` content-hashed URLs from `WebConfig`, `compression-maven-plugin` .br/.gz). htmx 4 inherits nothing implicitly (`:inherited`). The drawing geometry lives in `FlowDrawing`; templates stay logic-less.
- Native image (`-Pnative`, client/server): console view records are rendered via Mustache reflection -- when adding/renaming a record under `console/web`, register it in `sluice-server/src/main/resources/META-INF/native-image/am.ik.sluice/sluice-server/reflect-config.json`. Build args go in `META-INF/native-image/am.ik.sluice/<module>-netty/native-image.properties`: Spring AOT overwrites the one under `am.ik.sluice/<module>`. `./mvnw -pl sluice-client -am -PnativeTest test -Dtest=<class>` runs a test on a native image (CI runs `DirectBufferReleaseTest` so).
- Tests that need a port before the server starts take it from `TestPorts.freePort()` (sluice-it), never by probing `new ServerSocket(0)` and closing it: the OS may hand that port out again.
- `sluice-it` classes run in 6 surefire forks at once (`-Dsluice.it.forks=N`), one class at a time per fork: a test must not depend on a quiet machine or on state outside its JVM (ports only via `TestPorts`, which reserves them across JVMs).
- Console E2E tests use Playwright (`ConsoleE2ETest`); the first run downloads Chromium. Launch it through `Chromium.launch` (sluice-it), never `playwright.chromium().launch()`: a browser that sees the host's interfaces fails a request with `net::ERR_NETWORK_CHANGED` when another fork's container starts or stops.
- Tests needing a flume run `sluice-flume` as a child process via `Flume.start()` (sluice-it), one per class; it exits with the JVM (stdin EOF).
- `.todo` numbers come from `.todo/claim-number.sh`.

## Knowledge base

README.md is for users; detailed behavior (flow control, connection limits, h2 demux, metrics) lives in `.kb/<topic>.md`. Keep the README summary in sync when a `.kb` file changes.

## Manual E2E check

See "Run" in README.md.


## Open work

See `.todo/`. On completion, append a row to `.todo/history/YYYY-MM.md` (see existing rows for the format).
