Difficulty: High

# sluice-wicket-cf-api: builds, broker operations and jobs outliving their instance

Needs 275, 276, 278. Part of 275-284. A build and an app's delete go on from what the state
keeps at a start of one instance (`Api::take_up`, `staging::run`: a build of source followed on
from `Staging::building`); this gives that to another instance while the first is gone.

## What holds it to one instance

Work under way is a task of the process that took the request, and a start of the cf-api takes
every such work for dead:

- a build (`api.rs`: `tokio::spawn(staging::run)`): a start takes up every `STAGING` build
  (`Api::take_up`). With several instances, one starting runs again the builds the others run
- a broker operation (`operations.rs`): `Operations::driving` is a set of the process, `kick`
  at a start drives every operation under way: two instances drive one (the requests are
  idempotent, the orphan mitigation and the job's settling are not meant twice)
- an app's delete after its unbinds (`Api::take_up`) is taken up by every instance starting: two
  delete one
- a deployment's step and a move are the leader's (278)
- a docker package's pull (`registry.rs`) and a rootfs made by a wicket (`PUT /api/images`) run
  in the request or the build's task

## Design (to confirm)

Each piece of work is a row with an owner and a lease (the instance's id, renewed while it
runs); nothing is failed or taken at a start:

- a build whose owner's lease expired is taken by another instance, not failed, where it can go
  on: a CNB build lives on the wicket (`GET /api/builds/<id>?after=<n>` from the line kept, its
  droplet by digest), a rootfs of an image is made again by its pinned reference, a zip's
  detection is run again from the package (276). The staging log of the first owner is in the
  shared log (281). One that cannot go on fails as today, by whoever takes it
- a broker operation is driven by its owner; taken over after its lease, resuming at the poll
  (`Operation` holds the broker's operation id and what was sent). The orphan mitigation once:
  its step kept before it is sent
- the jobs' waits settle from the operations' ends in the store, by whoever ends them
- a stop hands over (282): the instance releases its leases, the others take them at once

Simpler alternative to weigh: all of it on the leader (one lease, 278), the handlers only
writing the row. A build then runs away from the instance holding the upload, which 276 allows;
a takeover is the leader's. Fewer leases, but a leader's stop interrupts every build at once.

## One instance (275's rule)

On the file backend a start still means all work of the run before is ownerless (the `flock`
says so): it is taken up or failed at once, with no wait for a lease to expire. A build that
cannot go on fails with today's message.

## Tests

`staging` / `operations` tests with two instances on one store and the mock wicket / broker
(`tests/broker`): the owner stopped mid-build and mid-poll, the other finishing both, the job
`COMPLETE` once, the broker called for an undo once; an instance starting beside a build under
way fails nothing.

## From 275

With `--state`, `fail_unfinished_builds` and `settle_unfinished_waits` run only on the
`state.json` an empty store takes (`store::kept`), not at every start: a start fails nothing of
the store. `Api::take_up`, `Operations::kick` and `Controller::resume` still run at every
instance's start.