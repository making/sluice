Difficulty: High

# Live access log in the console via sluice-flume

The console shows the access log lines of the whole cluster as they happen. A small Rust gRPC
server, `sluice-flume` (a flume is the water channel logs ride down), collects the lines from
every node while someone watches; the console of any node relays them to the browser over SSE.
Optional: without `sluice.live-log.url` the feature is off and costs nothing.

```
each node ──bidi gRPC Feed (node dials out)──> sluice-flume (in-memory ring, one global seq)
                                                    ^
browser ──SSE──> console of any node ──server-streaming gRPC Watch──┘
```

## Why

The cluster runs behind a load balancer, so a request is logged by one node only; a node-local
view shows about 1/N of the traffic, and which node depends on where the console request landed.
Considered and rejected: a peer mesh (`WatchAccessLog` on every node; N*(N-1) streams, a
per-node cursor vector, peers must reach each other's publicUrl) and RabbitMQ Streams (a broker to
operate for a best-effort view, publishes even with no viewer, logs at rest on the broker).

## Scope

- Proto: new `sluice-proto/src/main/proto/sluice/v1/flume.proto`, `tunnel.proto` untouched. Rough
  shape (additive-only evolution: nodes and flume deploy independently):
  - `rpc Feed(stream FeedMessage) returns (stream FeedControl)`: node -> flume hello (node id),
    events, gap (dropped count); flume -> node start (filter) / stop.
  - `rpc Watch(WatchRequest) returns (stream WatchEvent)`: filter (route key, types, node id),
    `after` seq; events carry seq, node id, node time and the fields; plus gap and node
    connected / disconnected.
- `AccessLogger`: build one structured event per line (record) and hand it to both the
  `sluice.access` logger and the tap. The log output stays byte-identical (pinned by
  `AccessLoggerTest` / `AccessLogE2ETest`). Only lines past the policy and the rate limit reach
  the tap (what you see = what is logged).
- Node side (`sluice-server`):
  - A tap with a local ring (~500) for the backfill on start; publish = `offer` to a bounded
    queue, never blocking a relay thread; overflow is dropped and counted (gap event). No viewer =
    near zero cost.
  - Feed sender: dials `sluice.live-log.url` (`grpc://` / `grpcs://`, insecure / TLS bundle as
    `sluice.cluster`), token `sluice.live-log.token`, respects `onReady`, reconnects with backoff.
    Server startup and the data plane never depend on the flume being up.
  - On/off decided in the bean body, no `@ConditionalOn*` (native image).
- `sluice-flume` (Rust, own Cargo workspace at `sluice-flume/`, not inside `sluice-wicket-wasi`):
  - tonic 0.14 + prost, proto compiled from `sluice-proto` in `build.rs` (as the wicket does).
  - tokio `broadcast` for the fan-out (`RecvError::Lagged(n)` -> gap event), `VecDeque` ring with
    one global seq; tells nodes to start / stop feeding as watchers come and go.
  - Bearer token interceptor, TLS (`tls-ring`), `tonic-health`.
  - Port / token / TLS by CLI or env; prints a `listening` line on stdout once bound;
    `--exit-on-stdin-close` so a test's child process dies with its JVM.
  - Dockerfile (static musl binary on distroless static). Image build in `deploy.yaml` for amd64
    / aarch64 on the existing native runners plus the manifest. Verifying the image (smoke test)
    is out of scope.
- Console:
  - A live access log view; SSE endpoint (`SseEmitter`, virtual threads), heartbeat comment every
    ~15s, `X-Accel-Buffering: no`. The SSE event id = the flume seq, so `Last-Event-ID` resumes
    from the flume ring even when the reconnect lands on another node.
  - Filters (route, type, node) as query parameters, applied in the flume.
  - Rows rendered server side (Mustache fragment). The vendored htmx 4.0.0 has no SSE handling:
    vendor the htmx 4 SSE extension or a small `EventSource` in `console.js`.
  - Pause, follow scroll, a DOM row cap; the gap count and disconnected nodes shown. With the
    feature off, a hint naming `sluice.live-log.url`.
  - Register new view records in `reflect-config.json`.
- CI: `ci.yaml` gets the Rust toolchain + cargo cache, paths include `sluice-flume/**`; the flume's
  `cargo test` / fmt / clippy run there. Coordinate with 032 (wicket Rust CI): one Rust job for
  both is fine.

## Tests

- Rust: integration tests in the crate with a tonic client (feed -> watch, lagged -> gap, resume
  by `after`, start / stop on watcher count, token rejected).
- `sluice-it`: the flume runs as a child process. exec-maven-plugin runs
  `cargo build --manifest-path ../sluice-flume/Cargo.toml` (debug) before the tests and passes the
  binary path as a system property; a missing Rust toolchain fails the build (no silent skip).
  Port from `TestPorts.freePort()`, stdin kept open (exit on EOF), wait for the `listening` line,
  one flume per test class (`@BeforeAll` / `@AfterAll`).
- Extend before adding: `AccessLoggerTest` (tap receives exactly the logged lines, overflow
  counted), `ConsoleE2ETest` (rows appear, filter, pause), a two-node case on the setup of the
  existing cluster E2E (`RouteGateE2ETest`): a request served by node 2 shows in node 1's console
  with its node id.

## Docs

- `.kb/live-access-log.md` (topology, drop semantics, ordering, resume, the console session is
  per node so the SSE reconnect needs a sticky console, ingress buffering / idle timeout) and the
  README summary.
- CLAUDE.md: Layout gains `sluice-flume`; the build needs JDK 25 and Rust; "Docker images via
  buildpack; no Dockerfile" gains the exception "Rust components use a Dockerfile".

## Acceptance

- Two nodes + flume: a request through either node shows in the console of each within a second,
  tagged with its node; a reconnect resumes without duplicates; a flume down leaves the data plane
  and the server startup unaffected and the console says so.
- No viewer: nodes send nothing to the flume beyond the idle stream.

Related: 165 (peer credential split) is independent; the flume has its own token.
