Difficulty: Medium

# sluice-wicket-cf-api: an app's log the same on every instance

Needs 275, 278. Part of 275-284.

## What holds it to one instance

- `logs.rs`: a ring per app in the process. The cf-api's own lines (`STG`, `API`, `CELL`) are in
  the ring of the instance that wrote them: `cf push` polls the staging log through log-cache's
  read API and may reach another, `cf logs --recent` shows a different log each time
- a line's timestamp is "unique and ascending within an app" by a counter of the process; the
  CLI walks by `start_time`, so two instances' rings skip or repeat lines
- `flume.rs`: one watch, its cursor kept in the state (`State::flume`, written at a stop):
  several instances would each resume from the last one's cursor and write it back
- `Ring`'s reader bookkeeping (`read_out`: a reader taken as following) is of the process
- without `--flume` there is no log but the process's own

## Design (to confirm)

The flume is the log's bus:

- the cf-api feeds its own lines to the flume (`Feed`, as a node of its own; `flume.proto` and
  sluice-flume take a source that is neither a node's access log nor a wicket's guest: check
  what a line needs to carry the app's guid and `STG` / `API` / `CELL`), and every instance
  watches all of it, the rings filled the same on each. Timestamps are the flume's order
  (its cursor), not a process's counter
- no cursor in the state: an instance starting watches from the lines the flume has at hand
  (`WatchRequest.after` absent: only the newest; a watch from the oldest at hand needs a flume
  change), so its ring is as the others' within the flume's retention
- several cf-api instances require `--flume` (refused at a start otherwise, 283); one instance
  without it stays as today
- `LGR` lines (lost) are per watch: they may differ between instances; say so in the README

Alternative: the own lines as rows of the store (275), the apps' from the flume. A table that
grows by every staging line; only if the flume cannot carry them.

## Tests

`flume::tests` with two watches on one flume (sluice-wicket-tunnel's test flume): a line added
on one instance read from the other, in one order on both; the CLI test of 284 reads a push's
staging log through the instance that did not stage.
