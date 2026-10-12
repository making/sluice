Difficulty: Medium

# Report client / wasmlet build versions to the server

Do before 054: its precondition ("no pre-046 client and no pre-047 wasmlet is deployed")
is unverifiable today. The server's version is visible (`/actuator/info`), but the server
cannot tell which build a connected client or wasmlet runs, and the wasmlet has no build
identification at all (`Cargo.toml` version only, no commit, no `--version`).

## Scope

- Proto: a build descriptor the peer sends in `Advertise` (e.g. `PeerBuild build = 3`:
  implementation `java-client` / `wasmlet`, version, git commit, build time). The server
  sends its own in `AdvertiseAck` so a client can log which server it reached. Peers that
  predate the field send nothing; the server shows them as "unknown (older build)".
- Java client: fill it from the existing `build-info` / `git.properties`
  (`BuildProperties` / `GitProperties`); log the server's build on the ack.
- wasmlet: `build.rs` embeds the git commit (and dirty flag) and build time as env vars
  (`git` unavailable -> "unknown", the build must not fail); add `--version`; log the
  build at startup; send it in `Advertise`.
- Server: keep the peer build on `TunnelSession`; log it at session open.
- Console (required by the user): each connected client / wasmlet row on the server's
  `/console` shows its build -- implementation, version and short git commit (build time
  on hover or in a detail cell). Add it to `ConsoleView.Client` and the template; register
  the record change in `reflect-config.json` so the native image renders it too.
- Legacy visibility, the direct check for 054: mark sessions whose `Advertise` has
  `connection_window == 0` as legacy in the console, and expose a gauge of legacy
  sessions (a wasmlet's legacy state shows the same way).

## Tests (watch each fail first)

- The server records and renders the build a client and a wasmlet send; a peer sending
  no build renders as unknown.
- Console E2E (`sluice-it`, real server + client): the client's version and commit appear
  in its row on `/console`, matching the client's own `BuildProperties` / `GitProperties`.
- The client logs the server's build from the ack.
- wasmlet: `--version` prints the embedded commit; a build without git still succeeds.
- A legacy-window session counts in the gauge and is marked in the console; a windowed
  one does not.

## After this lands

Update 054's precondition: deploy this, then check the console / gauge shows no legacy
and no unknown-build sessions before removing the legacy mode.
