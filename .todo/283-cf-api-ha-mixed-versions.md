Difficulty: Medium

# sluice-wicket-cf-api: two versions and two configurations on one state

Needs 275. Part of 275-284.

## What holds it to one instance

A rolling update runs the old and the new build side by side, and each instance comes with its
own flags:

- `State` is read with serde's defaults and written whole: a field the old build does not know
  is dropped at its next write; `State::migrate` rewrites the old shapes at a start, under the
  old build's feet; `skip_serializing` fields vanish
- the configuration changes the state at a start: `follow_wickets` places again every index on
  a wicket this instance's `--wicket`s do not name, `follow_domains` renames the domains,
  `State::new` takes `--org` / `--space`. Two instances differing (an update adding a wicket)
  move the apps back and forth
- `check_wickets` (`distinct`) and `stacks_of` run per instance at a start
- settings that must agree are per process: `--wicket-token`, the admins and claim rules
  (`roles::Policy`), the editors, `--cnb-builder`, `--flume`, `--wicket-away-grace`, the
  wicket's app API as each build speaks it (a new body field put by the new build, put back
  without it by the old)

## Design (to confirm)

- a schema version in the store (275): a build refuses to start on a version newer than it
  knows, and writes none; a change of shape goes in two releases (the new build reads both and
  writes the old until every instance is new, then a migration, by the leader, once all
  instances report the build: an instances table with build and heartbeat). Rows keep the
  fields a build does not know (a JSON column merged, not replaced)
- the topology in the store, not in each instance's flags: the wickets (name, URL), the domains
  and the default org / space are records, seeded from the flags of the first instance and
  changed by an admin call (or by the leader from its own flags, the others only warning of a
  difference: simpler, decide). `follow_wickets` runs when the record changes, once
- the rest compared at a start against what the other instances registered: a difference in
  what must agree is refused (or logged, for what an update is meant to change: the list of
  each)
- only the leader writes to the wickets (278), and the leader is the oldest build while builds
  differ, or the new build puts nothing the old does not know until all are new: decide; the
  first keeps the wickets' apps from flapping between two bodies
- `GET /v3/info` names the instance's build (it does); the instances table is what an operator
  reads for the update's progress

## One instance (275's rule)

On the file backend the flags are the topology, as today: a restart with other `--wicket`s or
`--domain`s follows them at once, with no admin call.

## Tests

Store tests: a row written by a "newer" shape survives an update by an older one; a start on a
newer schema refused. Two instances with different `--wicket`s: no index moves until the
topology record changes. A previous release's binary beside the current one in the CLI test of
284, once there is a release to take.

## From 275

- `sluice_cf_meta.layout` is the rows' layout (1): a build refuses a store of a newer one
  (`postgres::SCHEMA`). Nothing moves it yet
- a row of a kind a build does not know is left alone (`records::known`); one it cannot read is
  logged and left out of its copy; a field it does not know is dropped when it writes the row
  again (the body is the entity's JSON, written whole): the merge is to do
- with `--state`, `follow_domains` and `follow_wickets` run at every start in a transaction, by
  the flags of the instance starting (`Store::open`); `State::migrate` only on the `state.json`
  an empty store takes