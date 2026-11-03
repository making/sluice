Difficulty: High

# sluice-wicket-cf-api: `cf scale -i N` is the upper limit on both wickets

The user's decision: `cf scale` sets the upper limit on the wasi wicket too, and annotations how an instance
lives. Today only a Firecracker app's cloned modes have it:

- wasi: `instances` is the route's `pool` (warm instances), the limit the annotation
  `wicket.sluice.ik.am/instances`, unset unbounded (`controller.rs`, `Controller::desired`). `-i 1` with
  `warm: 0` runs any number of instances while `cf app` says `1/1`. The Firecracker app refuses that
  annotation saying "`cf scale -i` caps them"
- Firecracker, none annotated: `-i N` is a fixed size, a floor as much as a limit, and 1 to 2 is no change of
  count: `shared` to `pool`, the VM replaced, a snapshot's memory taken

Do:

- wasi: the route's `instances` from the process's `instances`, `pool` from `warm`. The annotation
  `wicket.sluice.ik.am/instances` goes, and an app holding it gets what the README's upgrade note says
- `cf logs`' index then stays within `0` to `N - 1` on wasi too (`.kb/live-access-log.md`: "one of its own the
  lowest free from `pool` up")

To settle, for the long term rather than the smallest change:

- **what 1 means where an instance is per request or per connection**. A CF instance takes many requests at
  once; a wasi instance or a `per-connection` / `reuse` VM takes one. `cf push` without `instances` gives
  1, so a literal limit serves one request at a time and the next waits (on Firecracker up to `wait-ms`, 10s,
  then 502). Options: the literal limit, said by the README and by the error; a larger count for a push
  naming none, shown as such by `cf app`; an operator's default. A limit `cf app` does not show is not an
  option: it is the mismatch this item removes. The README's migration "annotate it `per-connection` and
  `cf scale -i 1`" is one connection at a time as it stands, and is rewritten either way
- `warm`'s default on each wicket once N is a limit, and what a wasi `-i N` keeps warm on the upgrade
- whether the unannotated Firecracker mapping stays (1 `shared`, more a fixed `pool`), and if so that
  `cf scale` across 1 and 2 says what it replaces
- the options a limit makes matter: `wait-ms`, `conns`, and `prime`, none of them annotations today
- what a wicket's route without a limit means in the cf-api's listing comparison (`same`), so an app
  upgraded is not restarted twice

The test: `vm_sandbox`'s table and `desired` in `controller.rs`, the scale steps of `tests/cli.rs` on both
wickets, seen failing first. The cf-api's README tables, "How long a VM lives" and the upgrade notes in step.
Mind todo 214 (the scale's check) and todo 216 (`warm`'s default), which touch the same lines.
