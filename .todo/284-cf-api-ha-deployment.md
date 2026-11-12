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
- the README: "Several instances" beside "Several wickets", the update's steps, the sizing
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