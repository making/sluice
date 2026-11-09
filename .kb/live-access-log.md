# Live access log

Detailed behavior behind README "Live access log": the console shows the access log lines
of the whole cluster as the nodes log them.

## Topology

```
each node ──Feed (bidi gRPC, the node dials out)──> sluice-flume (ring, one global seq)
                                                          ^
browser ──SSE──> console of any node ──Watch (server-streaming gRPC)──┘
```

Behind a load balancer each request is logged by one node only, so a node-local view
would show about 1/N of the traffic. `sluice-flume` (Rust, `sluice-flume/`) collects the
lines of every node in memory while someone watches; the console of any node relays them.
Nothing is stored at rest: the flume keeps the newest `--ring-size` lines (10000) within
`--ring-bytes` (64 MiB) for backfill and resume, and loses them on restart.

```text
cargo build --release --manifest-path sluice-flume/Cargo.toml
sluice-flume/target/release/sluice-flume --token FLUME_SECRET            # grpc://0.0.0.0:8002
# or: docker run -p 8002:8002 -e SLUICE_FLUME_TOKEN=FLUME_SECRET ghcr.io/making/sluice/sluice-flume

# every node
--sluice.live-log.url=grpc://flume-host:8002 --sluice.live-log.token=FLUME_SECRET
```

Every flume option is also an environment variable `SLUICE_FLUME_<NAME>` (`--help`; another
variable of that prefix, a misspelled one, is refused at the start):
`--host`, `--port` (8002), `--token` / `--token-file` (required), `--tls-cert` /
`--tls-key` (PEM, serves `grpcs://`), `--ring-size`, `--ring-bytes` (64 MiB: the most those
lines may hold), `--backfill` (200: the newest lines a new view begins with),
`--exit-on-stdin-close`. `--version` prints its build (version, git
commit, build time); the `listening on ...` line it prints once bound starts with the same,
and every view's `WatchHello` carries it, shown next to the page's state (a flume predating
that shows `Unknown (older build)`). It serves the gRPC health service (`sluice.v1.Flume`)
without the token. Nodes dial a `grpcs://` flume verified against
`sluice.live-log.tls-bundle`, else the JVM trust store; `sluice.live-log.insecure=true`
skips the verification.

## When nodes send

A node holds one feed stream open while it runs, redialed with a backoff of 1s doubling to
30s whenever it ends. The server starts and serves whether or not the flume is up; a flume
down costs a warning per 5 minutes.

Without a viewer a node sends nothing beyond the idle stream (and the gRPC keepalive). When
a view opens, the flume tells the nodes it covers to start, with the union of the filters
of the views covering each node; when the last view of a node closes, it tells the node to
stop. On start a node first sends its recent lines (a ring of the last 512) the flume does
not have yet -- the flume remembers the last line it got from each node's run -- then each
line as it is logged.

Only lines past the access log settings and the rate limit reach the live access log, the
`type=ratelimit` summaries included: it shows exactly what the `sluice.access` logger
emits. With the access log off for a route, its lines do not show either.

## Drops

The relay threads never wait for the live access log. A node queues at most 4096 lines for
its feed; a line past a full queue is dropped and counted, and the node reports the count
as a gap (`<node> dropped N lines`). A view reading slower than the lines arrive (a slow
browser holds the flume back through gRPC and SSE flow control) catches up from the
flume's ring; lines the ring no longer holds are reported as `Up to N lines missed`. The
count is exact without a filter and an upper bound with one. The page sums both in
`Missed`.

## Ordering and resume

The flume numbers every line it receives with one sequence (its run's `epoch` plus a
counter); a view shows them in that order, which is per node the node's order, across
nodes the order they reached the flume. The time column is the node's clock, shown with its date in the browser's time zone (the
zone on hover).

The SSE event id is the flume's position (`<epoch>-<seq>`). A browser reconnecting sends
it as `Last-Event-ID` and resumes right after it from the flume's ring -- on any node, as
the position is the flume's -- without repeating a line. Pause closes the stream and resume
reopens it after the last row the same way. A position of an earlier flume run starts the
view over with the note that the flume restarted.

## Wickets and the cf-api

```
wicket ──Feed (as a node)──> sluice-flume <──Watch── sluice-wicket-cf-api <──log-cache read── cf logs
```

A wicket (`sluice-wicket-runtime-wasi --flume URL --flume-token TOKEN`) feeds as a node does -- the
same start and stop, ring of 512, queue of 4096 and gaps -- as the node
`wicket:<client-id>`, with kinds of its own. A line's route key is the host of its request
(`Host` / `:authority`, lowercase, without the port):

| kind | fields |
|---|---|
| `response` | `method`, `path`, `http`, `status`, `durationMs`, `bytes`, `remote`, `userAgent`, `instance` |
| `stdout` / `stderr` | `component`, `store` (the id of the wicket's log prefix), `instance`, `line` |

- `response` is logged once the response body ended or the client left, with status 499
  when it left before the head; `durationMs` runs from the request's arrival. `remote` is
  the last `X-Forwarded-For` entry (the node's peer), the TCP peer on `--listen`.
- A guest's output belongs to the request its store serves: a store serves one request,
  a pooled unit one at a time, so the attribution is exact. A pooled unit's output before
  its first request has an empty route key.
- `instance` is the index of the instance among its route's instances alive, as CF numbers
  an app's: a pooled unit's `0..warm`, kept by its replacement, and a per-request
  instance's the lowest free from `warm` up while its store lives. The units take requests
  in turn, the one idle longest first, as the gorouter's round-robin does. A `response`
  no instance served (no route, none within its budget, its client gone while it waited)
  has none.
- The console shows a wicket's rows like a node's (the route column is the host) and
  names it `wicket:<client-id>` in the node strip; the kind filter lists its kinds. The
  node selector starts with the cluster's nodes and adds a wicket once the flume reports
  it; a node or route the page's address names stays a choice across a reload.

`sluice-wicket-runtime-firecracker --flume URL --flume-token TOKEN` feeds the same way (the
feed is `sluice-wicket-tunnel`'s) the output of the processes in its VMs, `stdout` /
`stderr` with the fields `vm` (`vm-<index>` a booted VM, `template` a cloned route's
template, `vm-<n>` its clones), `instance` and `line`; the route key is the first host of
the VM's route, as the VMs of one target serve all its hosts. `vm` names the VM in the
wicket's run; `instance` is its index among the route's VMs alive:

- a template's is 0: no other VM of the route lives with it
- a VM holds the lowest index free until it is gone, in every mode but the template's: a pool's
  (booted or cloned) from its start, another clone from its restore, warm or serving, an index below
  the route's `instances`. A pool's `warm` VMs start as `0` to `warm - 1`, one in the place of a VM that
  failed holds its index, one beyond `warm` the next free. A pool shrinking may leave a gap: `keep-ms` kills the VM
  idle longest, of any index
- a reload starting a route's VMs anew numbers them from 0, beside the old VMs draining,
  as a rolling deployment's new process does on CF

A clone's lines are its own: the
template's are taken before the snapshot, what a clone holds of them at its restore is
discarded. The lines a VM's own buffer dropped (4096 lines waiting for the wicket) reach
the flume as a gap. It relays bytes, not requests: no `response` lines.

The cf-api (`sluice-wicket-cf-api --flume URL --flume-token TOKEN`) watches the hosts of
its apps' routes with the kinds `response`, `stdout` and `stderr`, and adds each line to
the log of the app holding its host, which `cf logs` shows as Cloud Foundry does, the
`instance` as the output's instance (`APP/PROC/WEB/1`) and the request's `app_index`
(`-` for none; `RTR/0` is the router, the wicket):

```text
[APP/PROC/WEB/1] OUT hello stdout: Method::Get /log body_bytes=0
[APP/PROC/WEB/1] ERR hello stderr: Method::Get /log
[RTR/0] OUT hello.localhost - [2026-10-30T01:17:47.123Z] "GET /log HTTP/1.1" 200 - 58 "-" "curl/8.7.1" "10.0.0.1" "-" response_time:0.000522 app_id:"<guid>" app_index:"1"
```

- `cf logs` names what `cf app` lists, `0` to `instances - 1` of the web process (its entries
  are the instances alive, the `alive` of the wicket's `GET /api/apps` taken the same way):
  the cf-api takes the `instance` modulo `instances`, knowing them as the wicket does not. With `cf scale -i 3`, `warm` is 3 and the route's cap 3 x `concurrency`: requests one
  after another rotate over instances 0 to 2; one arriving while all three are busy runs on
  an instance of its own, the wicket's index 3 or above, named as one of 0 to 2 (the requests
  an instance takes at once). On a Firecracker wicket it is its VMs: a pool of
  `cf scale -i 3` names 0 to 2 and nothing above, as they are. Over several wickets each
  wicket's `instance` is its own, the line's `WatchRecord.node_id` unread: each wicket's
  first is `0` (`cf app` maps a wicket's index to the app's, through the indices it holds).
- The watch resumes past its cursor (kept in `state.json`) when the hosts change or the
  stream drops. Without a host it holds no watch: an empty filter passes every line.
- Its startup line names the flume (`flume none` without `--flume`); a watch opened for
  other hosts than the last one said, or after a failure was reported (`cf logs: the watch
  of the flume ... ended`), logs `cf logs: watching the flume <url> for <hosts>`. The hosts
  are matched exactly: a line whose route key is none of them reaches no app.
- Lines lost become an `LGR` ERR line in the log of every app watched, as they may be any
  app's: a wicket's drops, `Up to N lines missed` when the watch fell behind, a flume
  restart. A node's drops are never an app's lines and are left out.
- The lines are kept in memory, 1000 per app, stamped when they reach the cf-api: the
  order `cf logs` streams them in. While the cf-api watches, the nodes and the wicket keep
  feeding, and `cf logs --recent` shows what arrived since.
- After a flume restart the wicket sends again the lines of its ring the new flume lacks,
  so an app's log may repeat some.

## OTLP export

`--otlp-endpoint https://collector.example:4318` has the flume export every line it receives
to an OTLP/HTTP logs endpoint (`application/x-protobuf` only: no OTLP/gRPC, no OTLP/JSON).
A URL without a path, or with `/`, gets `/v1/logs` appended; another path is used as is.
`--otlp-headers` (values percent-decoded, as `OTEL_EXPORTER_OTLP_HEADERS`) or
`--otlp-headers-file` (one a line or a comma list) carries the authentication, `--otlp-ca` pins the endpoint's TLS (else
the webPKI roots; `--otlp-client-cert` / `--otlp-client-key` for mTLS), `--otlp-compression`
gzip (default) or none, `--otlp-batch-size` (512), `--otlp-batch-delay` (1000 ms), and
`--otlp-kinds` / `--otlp-route-keys` / `--otlp-node-ids` bound what goes out. Every option
is `SLUICE_FLUME_<NAME>` too, like the others; the `OTEL_*` variables are not read.

The exporter is a standing watcher of the flume, as a console's view is, and never goes
away. With it on, every node (and wicket) the filter covers feeds whether or not a console
watches -- nothing is sent without a viewer no longer holds -- and a console's narrower
filter no longer narrows what a node sends. Exported are the lines the nodes'
`sluice.access` logger emits, past the access log settings and the rate limit (the
`ratelimit` summaries included; the default rate limit is 10 lines / 10 s per kind per
node).

One `ExportLogsServiceRequest` per batch:

- a `ResourceLogs` per node: `service.name` (`--otlp-service-name`, default `sluice`) and
  `service.instance.id` its id (`wicket:<client-id>` for a wicket), then
  `--otlp-resource-attributes` (`k=v,...`, values percent-decoded as
  `OTEL_RESOURCE_ATTRIBUTES`; a key given there wins);
- one `ScopeLogs`, name `sluice.access` at the flume's version;
- a `LogRecord` per line: `time_unix_nano` the node's time, `observed_time_unix_nano` when
  the flume took it, severity INFO, `event_name` `sluice.access.<kind>`, attributes
  `sluice.kind`, `sluice.route_key`, `sluice.node_seq` and each field as `sluice.<name>`
  (a string: the wire carries no type), `stdout` / `stderr` also `log.iostream`, the body
  its `line` or, for the other kinds, the logfmt line rebuilt from the fields (a value that
  is empty or holds a space, `"`, `\`, `=` or a control character quoted as the node
  quotes it; the wire does not carry which it quoted, so the body may differ in quoting);
- a gap as a record: WARN, `event_name` `sluice.gap`, `sluice.gap.cause` (`node_dropped` /
  `behind`), `sluice.gap.dropped`, body `<node> dropped N lines` / `up to N lines missed`,
  a `behind` gap under `service.instance.id` `sluice-flume`.

A batch closes at 512 records, 1 MiB encoded or 1 s after its first line; one request is in
flight and a node's records keep its order. A connection error, a timeout, 429, 502, 503 or
504 sends the same batch again after 1 s doubling to 30 s (or `Retry-After`), without
limit: the ring is the exporter's buffer, and what it loses meanwhile comes out as one
`behind` gap once the endpoint is back. Any other non-2xx drops the batch; a
`partial_success` rejection is counted. Delivery is at least once. On SIGTERM, SIGINT or
stdin EOF what the flume took and the exporter has not sent (the pending batch and the
ring's lines past it) goes out within 5 s, then the flume exits. stderr shows the
endpoint at the start, the first failure with its status, the recovery, and at most one
line a minute summing dropped lines and rejected records.

OTLP/gRPC, OTLP/JSON, metrics and traces, persistence beyond the ring and semconv HTTP
attributes (`http.request.method`, ...) are out: the field names are the nodes' and evolve
with them.

## Deployment notes

- The console session is per node: a browser's SSE reconnect must reach the node it signed
  in to (a sticky console ingress), or it is sent to the login page.
- The stream must not be buffered: the response carries `X-Accel-Buffering: no` (nginx)
  and `Cache-Control: no-store`; other proxies need buffering off for
  `/console/access-log/stream`.
- An idle stream carries an SSE comment every 15s, below the usual 60s proxy idle
  timeouts. The flume pings idle connections every 30s and drops one silent for 10s more,
  so a node or console gone without a word is marked disconnected.
- With `sluice.live-log.url` unset the page names the setting and the server keeps no
  lines.
