Difficulty: Medium

# Wickets: a host without a route answers 404 once every app is deleted

On a Firecracker wicket with the cf-api as an app, after `cf delete` of the last app the only route
left is the cf-api's. A sole route takes any host, so the deleted app's host answers 200 with the
cf-api's root document, not 404:

```text
cf delete hello -r -f
curl -s -o /dev/null -w '%{http_code}\n' http://hello.localhost:8080/   # 200; 404 while another app remains
```

The same holds for any app that is the last one on a wicket: its route answers every host. Both
wickets' READMEs document it under `--listen`, as what a local loop on one route wants.

- `host_route` in `sluice-wicket-tunnel/src/routes.rs` (`[sole] => Some(sole)`, its test
  `a_host_takes_its_route_or_the_sole_one` in `listener.rs`)
- `Routes::sole` in `sluice-wicket-runtime-firecracker/src/routes.rs`, used by its `listener.rs`
  before reading anything: a guest of a sole route may speak first, and a connection that is not
  HTTP reaches it. That case must stay, for a route given on the command line (`--vm`,
  `scripts/try.sh`)
- the wasi wicket's `--wasm` local loop relies on the same table

The likely shape: a route from the console (an app) always matches by host; only a route from the
command line or the config file may be the sole one. Decide whether the config file's counts.

Tests: `the_cf_cli_deploys_to_a_firecracker_wicket` in `sluice-wicket-cf-api/tests/cli.rs` deletes
its last apps with the cf-api as an app: assert the 404 there. Update `--listen` in both wickets'
READMEs, and `demo/README.md`, whose tape keeps an app so as to show the 404.
