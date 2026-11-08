Difficulty: Medium

# sluice-flume: export the lines it receives over OTLP/HTTP (protobuf)

The flume keeps the lines of every node and wicket in memory only, and only while someone watches. Add an
opt-in exporter sending every line it receives to an OTLP/HTTP logs endpoint (`application/x-protobuf` only:
no OTLP/gRPC, no JSON).

## Design

### A standing watcher

The exporter is a watcher of the hub like a console's view, registered at the start with its own filter and
never unregistered: `Hub::watch(filter, None)` and the same `service::relay` into a bounded mpsc, consumed by
the exporter task instead of a gRPC stream. So:

- every node and wicket the filter covers is told to start feeding when it connects and is never told to stop
  (`rebuild` on connect reads `state.watchers`). The "nothing sent without a viewer" property no longer holds
  with the exporter on: document it.
- the steering union covers the exporter's filter: a console's narrower filter no longer narrows what the
  nodes send.
- backpressure is the existing one: a slow or unreachable endpoint blocks the relay, the broadcast lags, and
  `catch_up` resumes from the ring; lines the ring no longer holds arrive as a `BEHIND` gap. The ring
  (`--ring-size` / `--ring-bytes`) is the exporter's buffer: no second queue.
- exported are the lines the nodes' `sluice.access` logger emits: past the access log settings and the rate
  limit, the `ratelimit` summaries included. Note it in the docs (default rate limit is 10 lines / 10s per
  kind per node).

### Mapping (one `ExportLogsServiceRequest` per batch)

- `ResourceLogs` per node id in the batch. Resource attributes: `service.name` (`--otlp-service-name`,
  default `sluice`), `service.instance.id` = the node id (`wicket:<client-id>` for a wicket), then
  `--otlp-resource-attributes` (`k=v,...`, as `OTEL_RESOURCE_ATTRIBUTES`; a key given there wins).
- One `ScopeLogs`: scope name `sluice.access`, version the flume's `CARGO_PKG_VERSION`.
- `LogRecord` per `AccessEvent`:
  - `time_unix_nano` = `time_unix_millis * 1_000_000`; `observed_time_unix_nano` = when the exporter took it
  - `severity_number` INFO (9), `severity_text` `INFO`
  - `event_name` = `sluice.access.<kind>`
  - attributes: `sluice.kind`, `sluice.route_key` (when non-empty), `sluice.node_seq` (int), each field as
    `sluice.<name>` (string, verbatim: no type guessing, so a key keeps one type across records). A field
    named like a fixed one keeps the fixed one. `stdout` / `stderr`: also `log.iostream` (semconv).
  - body (string): `stdout` / `stderr` the `line` field; other kinds the logfmt line rebuilt from the fields
    (`name=value`, the value quoted as the node's `AccessEvent.quoted` does when it is empty or holds a space,
    `"`, `\`, `=` or a control character; the wire does not carry which fields the node quoted, so the body
    may differ from the node's log in quoting only)
- A gap (`NODE_DROPPED` / `BEHIND`) becomes a `LogRecord` too: severity WARN (13), `event_name`
  `sluice.gap`, attributes `sluice.gap.cause` (`node_dropped` / `behind`), `sluice.gap.dropped` (int), body
  `<node> dropped N lines` / `up to N lines missed`; a `BEHIND` gap under a resource with the flume's own
  `service.instance.id` (`sluice-flume`), a node's under the node's.
- `NodeState` and `WatchHello` are not exported.

### Transport

- `POST` to `--otlp-endpoint`: a URL whose path is empty or `/` gets `/v1/logs` appended (the base URL as
  `OTEL_EXPORTER_OTLP_ENDPOINT` takes it); another path is used as is. `http://` / `https://`.
- `Content-Type: application/x-protobuf`; `--otlp-compression gzip|none` (default `gzip`: every OTLP
  receiver takes it, and log lines shrink several times).
- `--otlp-headers` (`k=v,...`, values percent-decoded, as `OTEL_EXPORTER_OTLP_HEADERS`) /
  `--otlp-headers-file` (the same from a file, for a secret such as `Authorization=Bearer ...`; as
  `--token-file`).
- TLS: rustls on ring (tonic's `tls-ring` already pulls it), webpki roots, `--otlp-ca <pem>` replacing them;
  `--otlp-client-cert` / `--otlp-client-key` (PEM) for mTLS, together or neither.
- Client: reqwest 0.13 (`default-features = false`, `rustls-no-provider`), as sluice-wicket-oidc.
- Only `SLUICE_FLUME_<NAME>` configures it (`OTLP_ENDPOINT` etc. through `config.rs`'s rule); the `OTEL_*`
  variables are not read: one configuration surface, and `check_env` stays the guard against misspellings.

### Batching, retry, drops

- A batch closes at `--otlp-batch-size` records (512) or 1 MiB encoded (`prost::Message::encoded_len`), or
  `--otlp-batch-delay` (1s) after its first record. One request in flight: per node the order is the node's.
- Timeout per request 10s.
- Retryable (connection error, timeout, 429, 502, 503, 504): the same batch again after a backoff of 1s
  doubling to 30s with jitter, or the `Retry-After` seconds. Retried without limit: the ring meanwhile keeps
  the newest lines, and what it loses comes out as one `BEHIND` gap record once the endpoint is back.
- Any other non-2xx (400, 401, 403, 404, 413, ...): the batch is dropped and counted.
- 2xx with `partial_success.rejected_log_records > 0`: counted, `error_message` logged.
- Delivery is at least once: a batch the endpoint took whose response was lost is sent again.
- stderr: the state changes only (`otlp: exporting to <url>`, the first failure with its status / error,
  `otlp: recovered after N attempts`), plus at most one line per minute summing dropped / rejected records.
  The startup line names the endpoint (`otlp <url>` / `otlp none`).
- Shutdown (SIGTERM / SIGINT / stdin EOF): the pending batch is sent with a 5s deadline, then the flume exits;
  `serve` gets the exporter's join handle into its shutdown path.

### Filter

`--otlp-kinds`, `--otlp-route-keys`, `--otlp-node-ids` (comma lists, default empty = all) form the standing
watcher's `AccessFilter`: the nodes then send only what it or a console asks for. Checked by `Hub::check`
(`MAX_FILTER_ENTRIES`).

### Proto

Vendor the four files `opentelemetry/proto/{common,resource,logs}/v1/*.proto` and
`collector/logs/v1/logs_service.proto` of one opentelemetry-proto release (the newest, with
`LogRecord.event_name`) under `sluice-flume/proto/`, with its version and the Apache-2.0 notice in a README
there; `build.rs` compiles them as messages only (`build_server(false).build_client(false)` for that set).
Not the `opentelemetry-proto` crate: it pins its own prost / tonic and pulls the SDK. Add the directory to the
Dockerfile's `COPY`s and `sluice_build_info::embed`'s paths, and to `rust.yaml`'s flume path filter.

## Code

- `src/export.rs` (new): `OtlpSettings` (parsed), the task (batch / send / retry), the mapping.
- `src/lib.rs`: `Settings.otlp: Option<OtlpSettings>`; `serve` starts the exporter after the hub, flushes on
  shutdown.
- `src/config.rs`: the options in `VALUED`, `USAGE`, parse errors (endpoint not http(s), a header without
  `=`, cert / key alone, an unknown compression).
- `Cargo.toml`: reqwest, flate2 (gzip), rustls (ring, for the provider install as sluice-wicket-oidc does).

## Tests (as few as reach it)

- `config.rs`: extend `options_override_the_environment_which_overrides_the_defaults` with the OTLP options
  (env and flags), and `rejects_what_it_cannot_serve_with` with their errors.
- `tests/flume.rs`: one test with a fake OTLP/HTTP receiver (a hyper http1 server, dev-dependency) decoding
  gzip + `ExportLogsServiceRequest`: a feed connecting with no console watching gets a `start`; its events and
  a gap arrive as records under its resource with the mapping above (event name, attributes, body, times);
  the receiver answers the first request `503` + `Retry-After: 0` and the same batch arrives again, once, with
  nothing duplicated after it; a shutdown sends a batch not yet full.
- No sluice-it test: the node side is unchanged.

## Docs

- `.kb/live-access-log.md`: an "OTLP export" section (options, mapping, the always-feeding consequence, drops,
  at least once).
- README "Live access log": one line pointing at it, as the `.kb` file changes.
- CLAUDE.md `sluice-flume` entry: "and exports them over OTLP/HTTP when configured".

## Out of scope

OTLP/gRPC, OTLP/JSON, metrics or traces, persistence beyond the ring, semconv HTTP attributes
(`http.request.method`, ...): the field names are the node's and evolve with it.
