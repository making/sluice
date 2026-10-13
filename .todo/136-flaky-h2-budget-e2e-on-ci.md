Difficulty: Medium

# H2DemuxBudgetE2ETest fails on CI: the budget is not exhausted, a third connection not withheld

Seen 2026-10-14 on two CI runs of main, before and after the connection reservations of 132
(the relay's flow and budget code is the same in both):

- https://github.com/making/sluice/actions/runs/38425901598 (0ef50c0, before 132):
  `newStreamIsRefusedOnlyOnAConnectionPastItsAllowance` (30s) and
  `withheldWindowIsGrantedOnceOtherConnectionsFreeTheBudget` (61s).
- https://github.com/making/sluice/actions/runs/38442133073 (e21dee6):
  `withheldWindowIsGrantedOnceOtherConnectionsFreeTheBudget` (line 361,
  `waiting.awaitWithheld()` not within 1 minute) and
  `clientsGoneWhileWithheldReleaseTheBudget` (`awaitExhausted`, budget never reached within
  30s).

a9d7491 ("Exhaust the budget whatever the tunnel absorbs") came between them and did not
fix it. Locally (macOS, 6 forks) the class passes. In both failing runs the relays of the
failed tests then log `sluice-h2n-client did not end within PT30S` and `a leg of route
bud.* finished with ~2.5MiB bytes accounted`: the gated upstreams stay closed until the
test's `finally`, so the legs' writers block on the tunnel (expected after the failure, but
confirm it is only a consequence).

## To do

- Dump the timeline of a failing run first (budget used, each relay's queued bytes and
  withheld state, the pumps' sent bytes, what the tunnel absorbed) -- e.g. log them from
  the test while awaiting -- to see whether the stalled connections' bytes end up in the
  tunnel (Linux socket buffer autotuning on the runner) instead of the relays, or a relay
  is never reviewed.
- Then make the exhaustion independent of what the tunnel absorbs (as a9d7491 meant to), or
  assert on what the relays hold rather than on timing.
