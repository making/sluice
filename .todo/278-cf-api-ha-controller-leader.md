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

## Tests

`controller::tests` with two controllers on one store and the mock wicket: one leads, the other
puts nothing; the leader stopped (and one only paused past its lease), the other takes a
deployment and a move under way to their end; a put with an old epoch refused by the wicket
(the wickets' own tests, both runtimes).
