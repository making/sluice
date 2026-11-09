Difficulty: High

# sluice-wicket-cf-api: several wickets, an app's instances spread over them

What two wickets updated one at a time need, the apps serving on: the cf-api drives several wickets and puts an
app's instances on distinct ones, so `cf push -i 2` runs on two wickets. The rest is there: a wicket stopping
drains (its routes withdrawn on every node) and the sluice server sends their hosts to the other wicket serving
them; a wicket restarted starts the routes of its `routes.yaml` before it advertises them. Neither needs the
cf-api. 253 adds what the cf-api knows of each wicket; 254-258 build on this.

## Now: one wicket throughout

- `--wicket <url>` / `--wicket-token` name one console (`Settings.wicket`); `Controller` holds one `Wicket`,
  one `watch()` long poll, one `offer` and `runtime`, `did_not_start` and `strangers` by guid alone
- `State.wicket` holds one `Outcome` per app, `State.removals` the guids to remove from the wicket;
  `purge_pending` sends a purge once
- An app is one route of `instances` N; `api.rs` `stats` lists the one wicket (`"host": "wicket"`),
  `edit_at_once` and `trial_put` put once per app

## To do

- Configuration: wickets by name (`--wicket <name>=<url>`, repeatable, and its env form; `--wicket <url>` stays
  the one-wicket form), their token (one for all, or one each: decide). One runtime for all: a wicket listing
  another is logged and placed on by none
- Placement by index: each index of the web process on a wicket, kept in the state; distinct wickets while
  there are some, then the wicket holding the fewest of the app's (253 weighs room). Stable: a scale up places
  the new indices so, a scale down drops the highest (as CF) and removes the app from a wicket left with none.
  A state of one wicket migrates to the first wicket configured; an index on a wicket no longer configured is
  placed again at the start (logged)
- A wicket's body: `instances` its indices' count, `warm` those of them below the annotated `warm` (the warm
  ones are the lowest, as a wasm wicket's units are `0..warm`); the rest as now
- Per wicket: the watch, `fix`, the outcome, `did_not_start`, the removals, the strangers, the purges (through
  each wicket; 255 refines a shared store's). An app the state knows but does not place on a wicket is removed
  from it, not left as a stranger
- Edits at once (`cf scale -i`, routes, annotations): tried on every wicket they touch, those put before going
  back on a refusal (as `edit_at_once` does across apps). A start, restart or restage puts each wicket's share;
  a wicket refusing or away shows on its own indices
- Staging's `PUT /api/images` on one wicket, the others making it at their first put (`missing`): one guest
  makes one digest (two guests flip it: 254)
- `offer`: what every wicket offers (the keyvalue stores' sharing is 255's)
- `cf app`: every wicket's entries by index, `host` the wicket's name; a wicket away: its indices `STARTING`;
  refusing: `CRASHED` with its reason
- Docs: README (the diagram, Options, Mapping, Instances, Behavior, Limits), the module docs of `lib.rs` and
  `controller.rs`:
  - several wickets are reached through a sluice server, each advertising its own apps' hosts; a wicket's
    `--listen` serves its own apps only and drains nothing (a signal ends it at once), so local mode stays one
    wicket. A console directory per wicket (a volume each; a StatefulSet on Kubernetes)
  - updating the wickets one at a time: an app at `-i 2` or more serves on, one at `-i 1` answers 503 until
    its wicket is back (257); the default `smallest-client-id` makes the other wicket a standby, `round-robin`
    shares the load (260 weighs it); `stop_grace_period` above `--drain-grace` (a Firecracker wicket's 40s);
    the next wicket once the first serves again (a route that does not start ends a wicket's start; 261 tells
    when it serves); a keyvalue app on a store both share (`local` is a file of each host: 255); no image
    pushed while Firecracker wickets of two guests run (254); the cf-api beside the wickets, not an app of one
  - a Compose sample: two wickets, a sluice server, the cf-api

## Tests

Extend `tests/cli.rs` `the_cf_cli_deploys_to_the_wicket` with a second wicket rather than adding a test: `cf push
-i 2` puts an instance on each wicket (each listing the app at `instances: 1`) and each wicket's listener answers;
`cf app` shows `#0` and `#1` running on two hosts; one wicket stopped, `cf app` shows its index `STARTING`, and
restarted, nothing is put again; `cf scale -i 3` gives one wicket two, `cf scale -i 1` removes the app from the
other. The placement as unit tests in `controller.rs`. Todo 232 (`k/k`) reads the same entries. The check
through a sluice server (no request failing across a wicket's restart) needs the wickets in sluice-it: todo 025.
