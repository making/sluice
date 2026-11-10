Difficulty: High

# sluice-wicket-cf-api: a rolling / canary deployment of a droplet of another stack

Needs 267. There, a deployment whose droplet's stack differs from the app's is refused: a
deployment swaps a wicket's share in place (`deployments.rs`), and a wicket runs one stack. CC
deploys a droplet of another stack as any other (the new process's instances placed by its stack),
so `cf push --strategy rolling` from a component to an image keeps answering there; this lifts the
refusal.

## Design (to confirm)

A step moves indices instead of swapping shares, as an evacuation does (`App::moving`,
`Controller::relocate`): its `max_in_flight` indices (a canary's target) each started on a wicket
of the new stack with room, serving the new snapshot, then out of the old wicket's share once the
new one serves it (`State::moved`); an old wicket left with no index leaves `Rolling::pending`, its
share removed. Deployed once none is pending.

To decide:

- no wicket of the new stack with room for a step's index: `ACTIVE` with the reason as its error,
  as a share refused, going on as room comes
- a cancel: back on the snapshot before, the moved indices moving back the same way
  (make-before-break), not placed again at once
- a supersede or a start meanwhile; a restart of the cf-api mid-step (`resume`, the moves in the
  state)
- a canary's first step: one index moved, not a wicket's share

## Tests

Extend `deployments::tests::a_deployment_goes_a_wicket_at_a_time` (wickets of two stacks) and the
KVM CLI test of 267: a rolling push from a component to an image, requests answered all along, and
a canary canceled.
