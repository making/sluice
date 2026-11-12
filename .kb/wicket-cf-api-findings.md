# sluice-wicket-cf-api: findings

## As an app of a Firecracker wicket

The cf-api in a VM of the wicket, on its disk (its README,
[Operating it](../sluice-wicket-cf-api/README.md#operating-it)):

- `memory-mib`: an upload, a package and a droplet go through files, not memory: 128 MiB took a push of a
  150 MiB zip
- an upgrade: the new one boots on the disk: 0.7-0.8s of no API. A new image that never listens is
  refused (422, 11.5s), one whose process exits sooner (`--listen` on a port it may not bind: 1.55s)
- its VM killed: the API back in 782ms, an app answering throughout (16 of 16 requests), its state
  there

## The state's store

`--state` (PostgreSQL) beside the directory's `state.json`. Decided against the alternatives:

- a row per entity, but one lock for every change (the revision's row, `SELECT ... FOR UPDATE`),
  not a version per row: the changes are closures over the whole state (`State::place` over all
  apps, a droplet's blob dropped when no app's droplet names it, a name unique in its space), so
  two of them on two rows are not independent, and a version per row would let through what one
  process's lock never did. Under the one lock a change runs once, on the state as it is, and is
  tried again only for a connection lost; what a row per entity gives is the write (the rows
  changed) and the others' catch-up (the rows of a later revision)
- the reads from a copy in memory, not from the store: every handler reads through closures
  over `&State`. The copy is
  never ahead (a change is in it once committed) and a request first asks for the revision, so
  it sees what any instance answered before it

Measured on one host (release build, PostgreSQL 18 in a container with its default `fsync`,
through Docker's port proxy; 1000 apps, each with a route, a package, a build and a droplet:
5000 rows):

| | `state.json` | PostgreSQL |
|---|---|---|
| an edit of one app (median / p99) | 27.7 / 64.4 ms | 8.2 / 17.7 ms |
| a request's revision query | none | 0.25 / 0.31 ms |
| a read of every app (`cf apps`' walk) | 8 us | 8 us |
| the 5000 rows written at once | 48 ms | 205 ms |
| a second instance's start (all rows read) | | 127 ms |
| an edit seen by the other instance | | 10.4 / 15.0 ms |
| an edit, two instances editing at once | | 8.4 ms each |

An edit in PostgreSQL is its commit plus a copy of the state (7 ms of the 8: the change runs on
a copy, compared with the one before for the rows to write); the file's is the whole document
written and synced. Both grow with the state, not with the change; no cache of the reads is
needed.

## The blobs' bucket

`--blobs` (an S3 bucket) beside the directory's files. Decided:

- one put per object, no multipart upload: an upload is 160 MiB at most and a droplet bounded,
  both far below a put's 5 GiB, so nothing unfinished is ever in the bucket to sweep. The
  payload's digest is signed (the bucket checks what it got): a droplet's is its name, a
  package's is taken as it is copied out of the upload
- the six calls (put, get, head, delete, list, each signed) on `reqwest`, not an SDK
- deletion by a sweep, not by the handler that dropped the blob: another instance may be reading
  it (a put to a wicket, 11 minutes at most), so it goes once a sweeper saw it unnamed for 30
  minutes, by its own clock, not the bucket's `LastModified`. Listing the bucket against the
  state also finds what was made and never named
- a digest made again while its blob is swept: age alone cannot tell (listed unnamed, put again,
  named, then deleted). A sweep deletes between two changes of the state, its begin (counted,
  `sweep.rounds`) and its end, one instance at a time; a droplet is named in a change that
  checks no sweep began since its blobs were seen kept, with none under way then, else they are
  looked for again and what is gone staged again. A sweep sends its last delete 60s after its
  begin, each given 30s; one begun 10 minutes ago is taken for cut short and another instance
  sweeps. An instance paused between that check and its delete for 10 minutes would still delete:
  the bucket takes no fencing
- the directory deletes what a change dropped at once, by the same begin and end: its reader
  holds the file
- the sweep takes the state for all that names a blob: two states on one bucket and prefix
  delete each other's

Measured on one host (release build, two instances on one PostgreSQL 18 and one bucket, both in
containers; a wasi wicket): a `cf push` of a 140 MiB zip (a component and random bytes) through
one instance, the peak resident memory of the two:

| the bucket | the push | peak resident memory |
|---|---|---|
| Versity S3 Gateway 1.10.0 over a directory | 10.3s | 19 and 21 MiB |
| RustFS 1.0.5 | 11.2s | 19 and 21 MiB |

On both: an app pushed through one instance without a start and started through the
other is staged from the bucket's package and its droplet put to the wicket from the bucket.

## A stop

The release binary on the host (`--wicket` unreachable), SIGTERM to its exit: idle, 11-13ms. A
package upload under way (12 MB at 4 MB/s, SIGTERM 1s in): answered 200 at 3.0s, the process gone
2.05s after the signal; a new connection refused meanwhile. With `--stop-grace 1` and the upload at
1 MB/s: gone after 1.02s, the upload cut (`stopped past --stop-grace: 1 connection(s), 1
request(s) and 0 put(s) cut`). So an upgrade's gap is the 0.7-0.8s boot above plus what the
requests under way take, `--stop-grace` (8s) at most; not measured again in a VM.

What the CLI (8.19.0) does in that gap: a request refused (`connect: connection refused`) is sent
3 times in 35ms in all, then `FAILED` (`CF_TRACE=true cf curl /v3/apps` against a closed port). So
a `cf push` across a restart of the cf-api gets through only when none of its requests falls in
the gap: the staging the cf-api takes up ends `STAGED` either way, and a CLI test of a push across
a restart would pass by its timing alone, which is why none is written; the unit tests cut a
staging at each step instead (`staging::tests::a_staging_cut_short_is_taken_up`).

## A start that never listens

An app's process living and not listening takes its start `boot-ms` (the health check's timeout, 60s
by default). Before, the edit lock and the wicket's reload both waited for it: `cf set-env` of another
app was seen past a 20s client timeout. With one such start under way (40s timeout), of another app:
`cf set-env` 41ms, `cf restart` 797ms.

`ghcr.io/categolj/blog-frontend-htmx:jvm` (Tomcat on 8081 of its own, `-m 1G -t 60`), a9e77bd, with
`nginx-unprivileged:alpine` as the other app: `cf set-env` 37ms and `cf restart` 819ms with its Tomcat
up and the app `starting` (38ms, 803ms with nothing starting); the `cf push` refused 61s after its
start. Its start is not tried again as the revision moves (45s, two edits), once at the cf-api's start
(`cf set-env` 40ms, `cf restart` 787ms meanwhile). A `cf restart` given SIGINT at 12s: `crashed` is
recorded 62s after its start. With `SERVER_PORT=8080` the app runs (`cf restart` 12.9s), its `/` 500
for want of an egress.

A disk's VM replaced and a reload of the wicket's files or on SIGHUP start beside the edits as well:
the wicket's reloader awaits no boot. A pool's fill failing for room is no `did_not_start`: tried
again as the routes change. On SIGHUP (`port: 9999`, `boot-ms: 60000`): a PUT of another app 0.75s
meanwhile; a second SIGHUP with the file restored takes over at once.

## Who logs in

Neither `--oidc-issuer` nor `--any-credentials`: no start, on a host as in a VM. A flag, not
finding out it is in a VM: what the loopback check misses is any relay to the loopback (a
Firecracker wicket's route, a sluice client, a proxy), and a VM's marker would name the first
alone. On the wicket's side, a route served to loopback peers only would refuse the README's setup
too: a port Docker publishes does not come from its loopback, nor a tunnel's connection.

## Security groups

A guest takes 1024 bytes of rules (`vm::MAX_EGRESS`), so that is the bound of a port range too,
with no count of its own: 64 rules of `0.0.0.0/0:10000` (1023 bytes), 53 of `127.0.0.2/31:40000`.

- 54 rules (1020 bytes, a listener each): `cf restart` of the sample image took 784-795ms, 779ms
  with no rule: no cost to bound
- `ghcr.io/categolj/blog-frontend-htmx:jvm` (`SERVER_PORT=8080`) with `entry-api.ik.am:443`: `/`
  answered 500 (`UnresolvedAddressException`) before, 200 after, 0.30-0.32s once warm, 1.86s the
  first time
- a range's rule on the process's port made the wicket take `/init`'s listener for the process:
  `/init` probes `127.0.0.1:PORT`, so the app showed `running` with no process. `/init` starts no
  process there now
- the CLI (8.19.0) validates no `destination`: a name, its case kept, reaches the cf-api

## Service brokers

What the CLI (8.19.0) does with them, against the stub broker of `tests/broker`:

- `cf create-service`, `bind-service`, `update-service`, `delete-service` poll the job and stop at
  `POLLING` (`... in progress`) unless `--wait`; a broker's operation done within the CLI's first
  poll reads as done at once, so the test holds the broker (`Broker::hold`) to see one under way
- of a `FAILED` job the CLI prints the first error's `detail` alone, so the broker's description
  goes there (`... failed: Service broker error (status 400): ...`)
- `cf marketplace` asks with `space_guids`, `cf create-service` takes the plan by name without:
  the visibility is checked at the create too

## Deployments

What the CLI (8.19.0) does with them (`cf push`, `restart`, `restage --strategy rolling|canary`):

- it polls `GET /v3/deployments/<guid>` every 3s (fixed), the first 1ms after creating it, until
  `FINALIZED` / `DEPLOYED` (a canary's `ACTIVE` / `PAUSED` too), reading no instance meanwhile: a
  deployment done within that 1ms returns at once, any other takes 3s more. Then it waits for an
  instance `RUNNING` (or none) as a start does
- at `CF_STARTUP_TIMEOUT` (minutes, a float, 5 by default) it cancels the deployment itself and
  fails (`Start app timeout`): a deployment stopped by a refusal is canceled so, 5 minutes on
- `--no-wait` polls the stats of the deployment's `new_processes` instead, ending at an instance
  `RUNNING`; none listed, at once
- `-i`, `-m`, `-k`, `-l` given with `--strategy` go in the deployment's `options` (`web_instances`,
  `memory_in_mb`, ...), not in the manifest the push applies first
- `cf app`, `cancel-deployment` and `continue-deployment` find the deployment as
  `GET /v3/deployments?app_guids=<guid>&status_values=ACTIVE&order_by=-created_at&per_page=1`;
  `cf app` shows it (`Active deployment with status ...`) only when it is `ACTIVE`

## Evacuation

Two wasi wickets (debug builds, `--evacuation-grace 60`) through a sluice server
(`smallest-client-id`: every request to the wicket stopped until its apps left it), the cf-api
placing an app of `-i 1` and one of `-i 2`, a client sending a request every ~4ms through the
server, a connection each, both hosts in turn. The wicket holding both told to stop (SIGTERM):

- it exited 0.53s after the signal (3 runs): both apps started on the other wicket, its servers
  taking their routes (the cf-api looking every 100ms), out of its share and removed from it, then
  its drain (the server acknowledging the withdrawal at once)
- no request failed: 0 of 1383-1418 a run. The leaving wicket's removal waits until the
  replacement lists the app `advertised`: every server of its streams acknowledged the routes
  holding the host. Without that wait (the removal right after the replacement's put returned)
  none failed either, 4 runs exiting in 0.42-0.48s: the replacement's advertise leaves as its put
  returns, the removal a few round trips of the cf-api's later. Under load a stream may hold its
  advertise behind the data it sends, which the wait covers
- back, the `-i 2` app's second index moved back to it

## A wicket gone

The same setup (`--wicket-away-grace 5`, a request every ~5ms through the server, a connection
each), an app of `-i 1` on the first wicket, killed (SIGKILL):

- the requests failed from the kill until the app answered on the other wicket 5.47s on (992 of
  2141): the bound, the first failed listing seen at once (the long poll's connection reset), the
  move a put and the servers' acknowledgement
- changed meanwhile (`cf set-env`, `cf restart`: its new env on the other wicket), the killed
  wicket started again: 0 of 1027 requests answered by its old version. It held the app until
  the cf-api, listing it at once, removed it
- an app deleted with its wicket killed, the wicket started again 0.5s on: 0 of 1688 requests
  answered. Without the hold (seen through a server the same day): the deleted app served 8s,
  until the cf-api's next listing, whose pause grew to 30s then

## Stacks and the CLI

What the OSS CLI (8.19.0) does with a stack, seen 2026-11-10 against wickets of both runtimes:

- `cf push -s <stack>` with `--docker-image` is refused by the CLI itself (`cannot be used
  together`): an image's app is pinned by a manifest, whose `docker:` with `stack:` passes it
- `cf push -s <stack>` with a zip reaches the app's `lifecycle.data.stack`
- `cf app` shows the stack of the current droplet: empty before staging

## A build's last lines

The OSS CLI (8.19.0) follows the staging log through log-cache's `Walk` 2s behind
(`WithWalkDelay(2*time.Second)`, polled every 250ms when there is nothing new,
`actor/sharedaction/logging.go`), and stops at the first `GET /v3/builds/<guid>` answering
`STAGED` or `FAILED`: the lines of the build's last 2s, the cf-api's `Staged ...` or `Staging
failed: ...` among them, never reach `cf push`'s output (`cf logs --recent` has them). The
pruning is the client's: no read carries an `end_time`, each starts after the last line printed
(`start_time`, go-log-cache v2.0.7 `walk.go`). So the cf-api records a build's end once the
reader walking its log reads from past its last line (5s at most), 1s for one to start, no
wait without one. Seen on an arm64 Lima VM before the success waited too: a cached build of the
test's sample buildpack ended within 2s of its first lines, and `cf push` showed none of them.

## The demo's GIF

2.5MB ([demo/README.md](../sluice-wicket-cf-api/demo/README.md)).
