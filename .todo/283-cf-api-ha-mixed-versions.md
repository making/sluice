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

## From 279

- `sluice_cf_lease.address` (the holder's `--instance-url`) is written by `take`: a build
  predating it takes the lease and leaves the address of the holder before, the others sending
  their edits to an instance that answers them not made, until 20s passed (503). The instances'
  `--instance-url` and `--instance-ca-cert` are settings that must agree in kind (all or none)

## From 278

- a build before the lease (277 and older) reconciles the wickets regardless of it: never one
  beside a 278 or later on a store. The layout stays 1: the new rows (`outcome`, `wicket`, `store`
  of `crate::records`) are kinds an older build leaves alone, `sluice_cf_lease` a table it does not
  read. The schema version of this item is where to refuse such a build
- the wickets fence the writes per owner, by the epoch of a store's lease: two stores' cf-apis on
  one wicket (both owner `cf`) refuse each other's

## From 280

- a build before 280 stages, drives the brokers' operations and deletes an app after its unbinds
  in the process that took the request, whatever the lease says: never one beside a 280 or later
  on a store (two would drive one). The schema version of this item is where to refuse it
- `Operation::undo` and `Staging::taken` are fields an older build drops when it writes the row
  again: an undo owed is then never sent

## From 281

- the logs are one through the flume: `--flume` must name one flume on every instance, and
  several instances without it each hold a log of their own (refuse at a start, as planned)
- a flume predating `WatchRequest.oldest` and `WatchRecord.time_unix_nanos` (additive fields)
  begins a watch with its newest lines and stamps none: each instance then stamps a line with its
  own clock, and the instances' timestamps differ. Update the flume before the cf-api
- a build that does not know `cf-log` / `cf-walk` (one before 281) neither feeds nor takes them:
  its log lacks the others' lines and the others lack its own
- the `single` row `flume` (the cursor of builds before 281) is read by no build now and left in
  the store: drop it with the schema's next migration
