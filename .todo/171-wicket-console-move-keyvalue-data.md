Difficulty: Medium

# wicket-wasi console: what a move does with the route's keyvalue data

A console route with `wasi=keyvalue` gets the default keyvalue map: the default store, the
host as the namespace (`Deployed::specs` in `deployed.rs`, `Stores::default_map`). A move
(`PATCH /api/routes/<host>?host=<to>`, the page's Rename...) changes the host, so the
namespace follows it: the moved route starts with no data, and what it stored stays under
the old host (moving back finds it again). The page's note warns before the move and the
README states it; nothing moves or deletes data.

Decision deferred: whether the data follows the route, and how.

- keep as is: the namespace is a function of the host, as for every route, and the data is
  never touched. A user who wants the data along has no way to take it
- pin the namespace on a move: the moved route keeps `<store>/<old host>` as an explicit
  map. The manifest refuses a keyvalue map today (`read()` in `deployed.rs`: what the
  console never writes), so `routes.yaml` and the console's sandbox need one; the page shows
  the pin (the sandbox cell lists `identifier -> store/namespace` already) and needs a way
  back to the default. Whether a later deploy or sandbox edit keeps the pin is part of it.
  Todo 172 gives the app API explicit maps, which the pin can build on
- move the data in the store: sqlite updates `kv` and its `usage` accounting (the triggers
  do not move bytes between namespaces) and checks `max-namespace-bytes`; redis renames
  every `<prefix><namespace>:<key>`, not atomic with the reload. Data already under the new
  host collides; a namespace another route maps explicitly moves under it; a move the reload
  refuses must move the data back

## Acceptance

- the chosen behavior is pinned by `the_consoles_edits_are_kept_only_when_they_load`
  (`routes.rs`) and the console test in `tests/local.rs`
- the page's note on a keyvalue route's move and the README Console section say what
  happens to the data
