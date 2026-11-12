Difficulty: High

# sluice-wicket-cf-api: one instance at a time reconciles the wickets

Needs 275. Part of 275-284; 279, 280, 281 and 282 build on its lease.

## What holds it to one instance

- `Controller` takes itself for the only writer of the owner's apps on the wickets: `watch`
  lists each wicket and puts / deletes the difference, `confirm` tells a wicket back from a
  crash what to serve (`PUT /api/confirmed/cf`), `evacuate` / `relocate` / `respread` move
  indices, `PUT /api/evacuated/cf` ends an evacuation, the purges delete keyvalue data. Two
  instances doing so put against each other, move an index twice, confirm from states read at
  two times
- its coordination is in the process: `held` and `shares` (`Locks`), `did_not_start`, `listed`,
  `strangers`, `evacuations`, `respreading`, `down`, `placed_again`, `purging`
- what it observed is in the process: `State::outcomes`, `placement::Wickets` (budgets, `full`,
  `leaving`, `nodes`, `stores`, gone), `Noted`. `cf app`, `cf services` (the wickets' offers)
  and the placement answer from it
- `controller.resume()` (deployments, moves) runs at every start: a second instance takes up
  what the first is doing
- the wickets take a put from anyone holding the token: nothing stops an instance that lost
  its turn (paused, partitioned) from putting what it read before

## Design (to confirm)

- a lease in the store (275: a row with a holder, an epoch and an expiry, renewed; PostgreSQL's
  clock, not the instances'): its holder, the leader, runs `watch`, the deployments' steps
  (`deployments.rs`), the moves, the purges, the placement (275) and the sweeps of 276 / 277.
  A leader that cannot renew stops them before the expiry; the next takes them up from the
  state as a restart does today (`resume`, `resume_moves`)
- fencing at the wickets: the app API takes the owner's epoch (a header) on every write
  (`PUT` / `DELETE /api/apps`, `/api/confirmed`, `/api/evacuated`, `/api/keyvalue`) and refuses
  one below the highest it saw (kept in the console's directory). In sluice-wicket-console and
  both runtimes; an owner sending none is taken as today. Uploads by digest need none
- what the leader observed, for the others: kept in the store as it changes (outcomes, budgets,
  offers, away), or every instance lists the wickets read-only (`GET /api/apps?after=` is a long
  poll any number may hold) and only the leader writes. The second keeps the store quiet and an
  answer fresh; decide by what `Store::update`'s placement needs (275)
- `did_not_start`, `strangers`, `down`, the evacuations: the leader's, rebuilt at a takeover
  from a listing (check each: `did_not_start` not kept means a share that failed is put once
  more by a new leader; say so or keep it)
- a takeover is a start to the wickets: `held-ms` and `MAX_PAUSE` hold only if the lease's
  expiry plus a listing stays below `HELD` (15s). Bound the lease by it

## One instance (275's rule)

On the file backend the lease is the directory's `flock`: this process leads from its start,
with no wait for an expiry. It sends no epoch, or one a wicket predating the fencing ignores:
a new cf-api on an old wicket and an old one on a new wicket both work.

## Tests

`controller::tests` with two controllers on one store and the mock wicket: one leads, the other
puts nothing; the leader stopped (and one only paused past its lease), the other takes a
deployment and a move under way to their end; a put with an old epoch refused by the wicket
(the wickets' own tests, both runtimes).

## From 276

Every instance sweeps the blobs (`Store::keep_swept`), one at a time deleting: `State::sweep`'s
`by` is a lease of its own (an instance and the seconds since the epoch of its clock, stale after
`SWEEP_STALE`). The leader alone is to sweep, under the lease here (the store's clock); what a
droplet's naming checks (`sweep.rounds`, `Store::unswept`) stays.

## From 277

The logins need no leader: the expired sessions and passcodes go as new ones are made
(`postgres.rs`: `put_session`, `put_passcode`), in the statement making them, and every
instance reads the keys on its own (`uaa.rs`: `KEYS_EVERY`). Nothing of them to move under the
lease.

## From 275

- `Store::update` is one transaction after another over every instance (the lock of
  `sluice_cf_meta`'s row), `Store::changed` wakes on any instance's commit, `Store::sync` brings
  the copy to the store's revision. The lease is a row written through it; for the store's clock,
  a statement of its own in `postgres.rs` (`now()`), not a field of the state
- the placement stayed in the transaction: `State::place` runs in `Store::update`, on the room
  the committing process observed (`Wickets::budgets`, `full`, `leaving`, `gone`, not kept). Two
  instances place by two views of the room: decide here whether the leader alone places (the
  others writing the count) or the room is kept
- `Store::note` (what is not kept) is one process's: `outcomes` and `placement::Wickets` are
  not in the rows. It panics in a debug build when it changes a list of the state