Difficulty: Low

# Connection overflow (503 past the limit): untested proxy-protocol path, dashboard panels

Left open by 133 (2b9e8b1).

## To do

- `proxy-protocol=true`: an overflow connection goes through the same `transport()` as the
  normal path (the PROXY header read first), but no test shows it. Add one (TLS h1 behind a
  PROXY v1/v2 header past the limit gets 503 with `Retry-After`, the access log line carries
  the PROXY source address), then state it in README "Connection limits".
- An undecodable h2 head in the overflow path is closed without an answer: test it and
  mention it next to the other no-answer cases in README.
- Grafana dashboard (`dashboards/sluice.json`): add `sluice_data_connections_overflow`
  (gauge) and `sluice_data_connections_overflow_answered_total{protocol}` to the data plane
  row, and `rejected{reason="h2-limit"}` / `sluice_data_connections_h2_declined_total` (132)
  if missing; `GrafanaDashboardE2ETest` must keep passing (it checks every queried metric is
  scraped -- make sure the new meters are registered at startup or the test drives them).
