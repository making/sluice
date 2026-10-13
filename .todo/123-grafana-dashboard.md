Difficulty: Medium

# Grafana dashboard for the server's and the client's Prometheus metrics

Requested by the user.

## Scope

- A Grafana dashboard (JSON, provisionable; e.g. `dashboards/sluice.json`) over what
  `/actuator/prometheus` exposes on the server (8081) and the client (9001), as listed in
  README "Health / metrics".
- Parameterize it as the user's Spring Boot dashboard does (reference, requested by the
  user): https://raw.githubusercontent.com/making/k8s-gitops/refs/heads/main/lemon/platform/grafana/dashboard/spring-boot.json
  - Variables, chained: `datasource` (type datasource, prometheus); `app`
    (`label_values(jvm_memory_used_bytes,app)`); `cluster`
    (`label_values(jvm_memory_used_bytes{app="$app"},cluster)`); `instance`, `namespace`,
    `pod` (multi, include all, each narrowed by the ones before it). Add `route` (multi,
    include all) from the sluice meters that carry it.
  - Every query selects `{app=~"$app", cluster=~"$cluster", instance=~"$instance",
    namespace=~"$namespace", pod=~"$pod"}` and uses `"uid": "$datasource"`, so the same
    JSON serves the server and the client (picked by `app`).
  - Find where `app` / `cluster` come from in that setup (scrape relabeling or
    `management.metrics.tags.*`) and state in README what labels the dashboard expects;
    the local verification stack must attach them the same way.
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
