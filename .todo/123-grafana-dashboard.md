Difficulty: Medium

# Grafana dashboard for the server's and the client's Prometheus metrics

Requested by the user.

## Scope

- A Grafana dashboard (JSON, provisionable; e.g. `dashboards/sluice.json`) over what
  `/actuator/prometheus` exposes on the server (8081) and the client (9001), as listed in
  README "Health / metrics". Template variables for the datasource, the instance / pod
  and, where the metric has it, the route.
- Rows, at least:
  - Tunnel: connected sessions, `sluice_tunnel_bytes_total` rate per route and direction,
    window updates, stall seconds / stall failures, advertise timeouts, reconnects
    (client), sessions without a build.
  - Data plane: connections limit / admitted / rejected by reason, idle and stall closes.
  - h2 demux: queued bytes per route, budget vs used, windows withheld, streams refused /
    reset / misdirected by reason, legs failed.
  - Client: connection limit / admitted / rejected.
  - JVM / process basics (heap, GC, threads, CPU) kept short.
- Metric names, tags and units are taken from the code (Micrometer meter names and tags),
  not from memory; every panel's query must return data against a running stack.
- README: how to import / provision it.

## Verification

- Run the server, a client and an upstream locally with Prometheus and Grafana (docker
  compose under the artefacts or a script; not committed unless useful), drive traffic
  (h1, h2 with several streams, a refused stream, a rejected connection), import the
  dashboard and check each panel shows the series it claims (screenshot per row).
- A cheap automated guard if worthwhile: a test that every metric name the dashboard
  queries exists in a scrape of the running stack (sluice-it can scrape
  `/actuator/prometheus`), so a renamed meter breaks the build instead of a panel.
