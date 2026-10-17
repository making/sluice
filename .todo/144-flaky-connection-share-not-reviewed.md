Difficulty: Low

# H2DemuxConnectionShareE2ETest: held-at-share test fails under load

Seen 2026-10-17 while working on 142: `aConnectionHeldAtItsShareIsNotReviewedOnTheBudgetReleasesOfOthers`
failed twice (once in a full `./mvnw verify`, once run alone on a loaded machine), then passed
three times. After the other connection released, `budgetUsed` stayed about 66KB above the
value captured before.

## Work

- Dump the timeline of the failing run (budget meter, per-connection queued and charged
  credit, when the other connection's release lands) before probing.
- Decide whether the assertion races a release still in flight (test bug) or the budget
  really keeps bytes it should have returned (production bug). Fix accordingly; prefer
  removing the test if other tests already cover its behavior.
