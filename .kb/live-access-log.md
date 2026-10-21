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
Nothing is stored at rest: the flume keeps the newest `--ring-size` lines (10000) for
backfill and resume, and loses them on restart.

```text
cargo build --release --manifest-path sluice-flume/Cargo.toml
sluice-flume/target/release/sluice-flume --token FLUME_SECRET            # grpc://0.0.0.0:8002
# or: docker run -p 8002:8002 -e SLUICE_FLUME_TOKEN=FLUME_SECRET ghcr.io/making/sluice/sluice-flume

# every node
--sluice.live-log.url=grpc://flume-host:8002 --sluice.live-log.token=FLUME_SECRET
```

Every flume option is also an environment variable `SLUICE_FLUME_<NAME>` (`--help`):
`--host`, `--port` (8002), `--token` / `--token-file` (required), `--tls-cert` /
`--tls-key` (PEM, serves `grpcs://`), `--ring-size`, `--backfill` (200: the newest lines a
new view begins with), `--exit-on-stdin-close`. It prints a `listening on ...` line once
bound and serves the gRPC health service (`sluice.v1.Flume`) without the token. Nodes dial
a `grpcs://` flume verified against `sluice.live-log.tls-bundle`, else the JVM trust store;
`sluice.live-log.insecure=true` skips the verification.

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
nodes the order they reached the flume. The time column is the node's clock.

The SSE event id is the flume's position (`<epoch>-<seq>`). A browser reconnecting sends
it as `Last-Event-ID` and resumes right after it from the flume's ring -- on any node, as
the position is the flume's -- without repeating a line. Pause closes the stream and resume
reopens it after the last row the same way. A position of an earlier flume run starts the
view over with the note that the flume restarted.

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
