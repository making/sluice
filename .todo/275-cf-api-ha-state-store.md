Difficulty: High

# sluice-wicket-cf-api: the state in a store several instances share

First of 275-284 (several cf-api instances, updated one at a time). The others need this one.
285-287 are what one instance gains without it, done first.

## What holds it to one instance

- `store.rs`: the state is one `Mutex<State>` in the process, the only truth; `state.json` is its
  copy, written whole on every `Store::update`. A second process on the directory reads it once
  and overwrites the other's changes from then on
- `Store::open` takes the directory as its own: it removes every `.upload` / `.tmp` / `.droplet`
  (another instance's upload under way), fails every `STAGING` build
  (`fail_unfinished_builds`), settles the jobs' waits (`settle_unfinished_waits`), places the
  indices again by its own `--wicket`s (`follow_wickets`) and renames the domains
  (`follow_domains`)
- `Store::update` runs `State::place` over all apps under the lock, with the room of what this
  process observed (`Wickets::budgets`, `full`, `leaving`, `gone`: not kept)
- `Store::changed` is a `Notify` in the process: the controller and the flume watch wake on
  this process's changes only
- `State::outcomes` and most of `placement::Wickets` are `#[serde(skip)]`: what `cf app` shows
  exists in the process that listed the wickets (278 shares it)
- on a Firecracker wicket the directory is a disk with one writer (`disk.rs`: a `flock`)

## Design (to confirm)

`Store` becomes an interface over a backend; the handlers keep `read` / `update` closures, an
update a transaction retried on a conflict (so its closure must be free of effects: check every
caller, the ones that return `Vec<Blob>` to delete or spawn inside included).

- backends: the file (today's, for one instance, no new dependency) and PostgreSQL
  (`--state postgres://...`): recommended over an embedded Raft (membership across VMs replaced
  one at a time, a disk each) and over the wickets' keyvalue stores (no transaction over several
  keys). To decide before anything else; 277-280 lean on its transactions, its `LISTEN/NOTIFY`
  and a lease row
- records, not one document: a row per entity (app, route, package, build, droplet, deployment,
  job, instance, binding, broker, role, ...) with a version, so two edits of two apps do not
  conflict and an update writes what it changed, not 10^3 builds. An invariant across entities
  (a route's host unique, an app's name unique in its space, `homes`) by a constraint or in the
  transaction
- no copy in memory that answers for the truth: a read is a snapshot of the backend (measure
  `cf apps` with 10^3 apps; a cache invalidated by the change feed if needed)
- `changed` from the backend's change feed, for every instance
- placement out of `update`: an edit writes the wanted count; the indices are placed by the
  leader (278), or in the transaction from room kept in the store. Decide with 278
- nothing at `open` that takes the directory or the state as this process's own: the sweeps and
  the fails move to 276 and 280, `follow_wickets` / `follow_domains` to 283
- a `state.json` is imported once (`--state` given, the store empty, the file there)

## One instance stays as it is

The rule of every item of 275-284: at its end, one instance on the file backend works as today
with no new flag, dependency or service (no PostgreSQL, bucket or flume), and reads the
`--data-dir` the release before wrote. `tests/cli.rs` as it is, on the file backend, is the
gate of each item; what an item adds for several instances is tested beside it, not in its
place.

Here: the file backend stays one process, refusing a second (287). Whether it keeps `state.json` as one document or takes the
records' layout, a state of today is read.

## Tests

The store's own tests run against both backends (PostgreSQL as a child process or a container,
as `redis-server` in sluice-wicket-runtime-wasi's); two `Store`s on one backend: concurrent
updates of one app (one retried, none lost), of two apps (no conflict), `changed` reaching the
other. The CLI test of 284 runs the rest.
