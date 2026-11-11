Difficulty: Medium

# sluice-wicket-cf-api: a restart takes up the builds and jobs under way

Of one instance, for its upgrades today; 280 builds the ownership between several on it. Needs
nothing (285 is its other half: a stop that ends no request).

A start fails what the run before left: every `STAGING` build (`State::fail_unfinished_builds`:
"cut short by a restart of the cf-api: stage again") and the jobs waiting
(`State::settle_unfinished_waits`: an app's delete after its unbinds). So an upgrade of the
cf-api fails the `cf push` under way. The broker operations (`Operations::kick`), the
deployments (`Controller::resume`) and the moves are taken up already.

## Design (to confirm)

A build keeps what its next step needs, and a start goes on from there (`staging::run` in
steps the state names), failing only what cannot:

- a CNB build lives on its wicket: keep the wicket and the build's id (`Wicket::start_build`)
  with the build; a start reads on (`GET /api/builds/<id>?after=<n>`, `n` kept as the lines
  are taken) to its droplet. The wicket no longer knowing it (it restarted too): the source is
  still its layer by digest, built again
- a zip's detection and a site's packing: run again from the package, which is kept
- an image: its manifest read again; a rootfs made by a wicket (`PUT /api/images`) asked again
  by the pinned reference (the wicket keeps what it made by digest)
- the staging log is the process's ring (`logs.rs`), gone with the stop: the CLI polling it
  after the restart sees the lines from there on. Say so in the log (`STG`: taken up after a
  restart), or keep a build's lines with it: decide by what `cf push` prints
- the jobs' waits: an app's delete goes on from the unbinds left (`api.rs`: the task spawned at
  the delete), not failed
- what still fails at a start, with today's message: a package whose upload was under way

## Tests

Extend the staging tests (the mock wicket): a build cut at each step, the state opened again,
ends `STAGED` with the same droplet; an app's delete cut between two unbinds completes. The CLI
test: the cf-api restarted during a CNB `cf push` (KVM) and a zip's: the push succeeds.
