Difficulty: High

# sluice-wicket-cf-api: several instances deployed, updated one at a time, and tested so

Needs 275-283: the last of them.

## What holds it to one instance

- the documented deployment (README "Operating it") is one VM of a Firecracker wicket on a
  `disk`: one writer (`disk.rs`), and an update stops the old VM before the new one boots (a
  boot long of no API, `.kb/wicket-cf-api-findings.md`)
- a booted app's `instances=N` on a wicket are N VMs of one image on one host: no disk each,
  and a reload replaces them how? (all at once, or one at a time: read `deployed.rs` /
  `routes.rs`); the cf-api cannot be an app of itself (`owner` any but `cf`)
- the wicket holding it is a single point too: instances on several wickets, each advertising
  the cf-api's host to the sluice server, are spread by the server over the wickets
- `tests/cli.rs` runs one cf-api; `demo/` and the Dockerfile assume `/data`

## Design (to confirm)

- the instances hold nothing: no `disk`, `--state` / `--blobs` / `--flume` in `env`, the store
  and the bucket in `egress`. `--data-dir` stays for one instance and for scratch
- two ways to run them, both documented with images that were run:
  1. an app of the Firecracker wickets' consoles, one instance per wicket on two or more
     wickets, the same hosts: the sluice server spreads the requests; an update is the app put
     again on one wicket at a time, each after the last is ready (282's `/healthz`; the wicket
     starts the new VM before it stops the old where there is no disk: needs that of the
     wicket if a reload stops first)
  2. plain processes / containers behind any load balancer, `SIGTERM` and `/healthz`
- no session affinity needed (277, 279, 281); say what an operator sees when the store is away
  (reads? none: 503), and when no leader is (reads served, edits 503 until one)
- `sluice-wicket-cf-api/ha.md` (what several instances share, apart from the README, which
  points to it from "Several instances"): the deployment, the update's steps, the sizing
  (instances' memory, the store's and the bucket's), a backup (the store's and the bucket's, no
  longer the disk's); `.kb/wicket-cf-api-findings.md`: measured: a leader's takeover, an
  instance's update with `cf push` under way, requests answered throughout

## Tests

`tests/cli.rs`: the existing flow against two cf-apis on one PostgreSQL and one bucket behind a
round-robin relay of the test (each CLI request to the next instance), not a second copy of the
test: the same assertions, the single-instance run kept on the file backend for the parts only
it has. Added to it:

- the leader killed (SIGKILL) during a rolling deployment and during a CNB build: both end
- each instance stopped and started in turn (SIGTERM) during `cf push`: no CLI error, the apps
  answering all along
- where `/dev/kvm` is usable: the instances as VMs of two Firecracker wickets, updated one at a
  time

CI: PostgreSQL and the S3 mock for the job running it.

## From 275

A request is answered 503 (`CF-ServiceUnavailable`) while the store does not answer within 5s
(`Store::sync`), `/healthz` too; a change under way waits for it. The store's tests run
`postgres:18-alpine` in a container (`tests/support/postgres_server.rs`, or
`SLUICE_CF_API_TEST_POSTGRES`): the CLI test's PostgreSQL is that one.
## From 280

`controller::tests::the_next_instance_takes_up_what_the_one_leading_left` ends a build of source,
a broker's operation and an app's delete in the next leader's turn, against the mock wicket and
broker, the first killed or paused. The CLI test's kill during a CNB build is the same takeover
with the real wicket; what the unit test cannot show is `cf push` reading the staging log across
it (281).

## From 281

- `flume::tests::the_instances_on_a_flume_hold_one_log` runs two instances' logs on one flume in
  process; what is left for the CLI test: `cf push` reading the staging log through the instance
  that did not stage, and across a takeover (the lines before it are in the next leader's log)
- each instance needs a `--data-dir` of its own for its flume node id (`flume-node`): an
  `emptyDir` makes a new node per pod, which the flume lists as disconnected until its restart

## From 282

- a stop of the leader lets go of the lease first (after its edits under way, 2s at most), the
  requests answered meanwhile, an edit still under way sent to the next: `ha.md` "A stop".
  `lib::tests::an_edit_on_any_instance_is_made_by_the_one_reconciling_the_wickets` stops the
  leader with an edit under way; left for the CLI test: `cf push` across a stop of the instance
  it talks to, which needs the relay to send a refused connection to the other instance
- who takes an instance out of the routing: a Firecracker wicket's stop of a VM takes it out of
  its route's pool first (`Route::stopping`: `Pool::halt`, then the stop notice), its open
  connections going on until the process ends; but new connections of that route then fail at
  that wicket (no other VM started in its place) while it still advertises the host: an update
  one wicket at a time needs the new VM up before the old is taken out, or the host withdrawn
  from the server first. The listener closes at the stop's begin: a balancer must send a
  refused connection to another instance, or take the instance out before the signal (a
  Kubernetes `preStop` sleep); no lame duck period was added
- the last write of a stop is bounded (the grace, 1s past it at least): a store not answering
  no longer holds the process

## From 283

- an update's rules are in `ha.md` "An update": the oldest build (`postgres::SCHEMA`) leads, the
  topology is the store's until every instance has the new flags, `--blobs` and `--flume` are
  required beside another instance (the two cf-apis of the CLI test need both)
- left for the CLI test: a previous release's binary beside the current one, once there is a
  release to take; an update adding a `--wicket` (no index moves until the last instance of the
  flags before stopped). `instances::tests` covers the rules, `store::tests::
  two_instances_change_one_state` the topology and the layout's move against PostgreSQL
- the update's steps to document: the flume first; each instance stopped and started with the new
  build and flags; `sluice_cf_instances` is the progress; the layout's move is the point of no
  return (a backup of the store before the last instance is updated)
