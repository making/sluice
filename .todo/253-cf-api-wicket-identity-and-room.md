Difficulty: Medium

# sluice-wicket-cf-api: what the cf-api knows of each wicket (identity, room, its log lines)

After 252, which places by count and takes the wickets as configured.

## Now

- The apps listing (`GET /api/apps`, both wickets) names neither the wicket nor its budget (the Firecracker
  wicket's `/api/routes` has the budget): two URLs reaching one wicket (which defeats the spread), a wicket
  replaced behind its URL, and where there is room cannot be told
- `cf logs`: `flume::listed` names a line's `instance` modulo N, one wicket's; a request's `RTR` line is index 0
  ("the wicket is the one router"); the flume's `WatchRecord.node_id` is not read

## To do

- Both wickets: the apps listing names the wicket -- an id made at its first start and kept in its console
  directory, its flume node (`wicket:<client-id>`, drawn per run unless `--client-id` pins it), its build -- and
  its budget: `--max-instances`, a Firecracker wicket's `--max-memory-mib`, what is taken. Decide what the
  cf-api does with a wicket predating them
- Two names of one id are refused; an id changed behind a name is logged, its apps put again as on a lost
  directory today
- Placement by room: ties broken by room (the budget less what the apps placed there take); a refusal for room
  (422 without `did_not_start`) passes the index to the next wicket. An index no wicket has room for: decide
  (Diego takes the scale and shows the instance down, "insufficient resources")
- `cf logs`: a line's wicket by `node_id`, its index by that wicket's indices; decide the `RTR` line's index; a
  wicket's dropped lines (`LGR`) in the logs of its apps alone. The `CELL` lines name the wicket
- Docs: README, `.kb/live-access-log.md` (Wickets and the cf-api)

## Tests

The CLI test's two wickets: `cf logs` names `#0` and `#1` by the wicket that wrote each line; a second name for
one wicket's console is refused at the start; with one wicket's budget nearly taken, a new app's indices go to
the other. The log line mapping and the placement by room as unit tests.
