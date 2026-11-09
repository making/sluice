Difficulty: Medium

# sluice-wicket-runtime-wasi / sluice-wicket-runtime-firecracker / sluice-wicket-tunnel: say when a wicket serves

Two wickets updated one at a time (252): the second may stop only once the first serves again. A wicket
restarted starts every route before its console listens and before its tunnel connects (wasi `lib.rs`: "Fetches /
prewarms every route's component (fail fast)"; Firecracker: `Reloader::start` before `serve`), and a route that
does not start ends its start. Nothing outside tells when it serves: the console answering says the routes
started, not that the server routes to it. By hand, the server's console and the logs tell; a StatefulSet's
rolling update, a Compose `healthcheck` or a script needs a probe.

## To do

- Both wickets: an endpoint answering ready once the routes started and, with `--server`, every node of the
  membership has a stream up that acknowledged the current generation (sluice-wicket-tunnel holds both:
  `Routes::acknowledged`, `Shared::up`); not ready from the drain on (and while leaving, 257). In local mode,
  once the routes started and the listener is bound
- Where: the console's listener (its gate asks a token or a sign-in, and it may listen on the loopback only)
  with a path left open, or a listener of its own (`--health <addr>`); a liveness beside it or not. Decide
- Docs: both wickets' READMEs (Shutdown (drain), a probe in their Docker samples), the update procedure in the
  cf-api's README (252)

## Tests

sluice-wicket-tunnel against its test server (`session::testing`): not ready before the server acknowledged,
ready after, not ready once the drain began. Each wicket's endpoint in its own tests.
