Difficulty: High

# sluice-wicket-cf-api: a partition runs an app's moved indices twice

The cf-api takes a wicket that has not answered for `--wicket-away-grace` for gone and moves its
indices to the other wickets (README, "A wicket gone"). Cut off from the cf-api only (a partition),
the wicket still reaches the sluice server: it keeps advertising and serving them, and they run
twice, there and on the wickets they went to, until the cf-api reaches it again and removes them.
An app of `-i 1` (a singleton: a scheduler, a consumer of a queue) then runs as two.

Today this is documented, not handled.

## To do

- Decide who settles "gone": the cf-api alone cannot tell a dead wicket from one it is cut off from.
  Weigh at least:
  - a second opinion before moving: the sluice server (does the wicket hold a node session?) or the
    flume (is it still feeding?); moving only once neither sees it, at the cost of a dependency on them
  - a lease the wicket fences itself with: the cf-api's listing renews it, the wicket stops serving
    the cf-api's apps once it runs out, and the grace exceeds it; at the cost of the apps going dark
    on every wicket while the cf-api is down (today they keep serving), unless the lease stays
    renewed some other way
- Keep a wicket really gone moving its indices within the grace as now

## Tests

First, a test failing without it: the CLI test (or the controller against fake wickets) cutting the
cf-api off a wicket that keeps serving; past the grace, no index of an app runs on two wickets.
