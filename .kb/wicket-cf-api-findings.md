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

## The logins' store

With `--state`, the key, the sessions and the passcodes in tables of their own
(`sluice_cf_keys`, `sluice_cf_sessions`, `sluice_cf_passcodes`), not records of the state.
Decided against the alternatives:

- not of the state: a login or a refresh would be a revision, taking the lock every change
  waits for, waking every instance's watchers (`Store::changed`) and copying the state with
  10000 sessions in it for each change. Nothing of the state depends on a login. Each of their
  statements is a transaction of its own (a refresh: one `DELETE ... RETURNING` feeding the
  `INSERT` of the next session, so one of two at once gets it and none is lost between), on a
  connection of theirs, pipelined
- what a request reads of its token's login (the ID token's claims, for `--group-role` and the
  editors) is one query by the session's key, sent with the revision's: not cached, as a
  refresh on another instance ends it
- the sign-in under way at the provider in a cookie, not a row: nothing to sweep, no bound to
  keep (`MAX_PENDING` was a process's), and the callback is then the browser's that began it,
  which a row by `state` alone does not say. Sealed (HMAC), not encrypted: the PKCE verifier in
  it is the browser's own secret. The key sealing is derived from the tokens' key, so what is
  sealed is no token's signature. The wickets' consoles keep theirs in the process
  (`Provider::begin`)
- a key's `made` is when it signs from, the instances reading the keys every minute: a key
  replacing another is known to all before any signs with it, with no reload on a `kid`
  unknown (a request without a login could ask for it at will)
- the removal of the expired sessions and of those past the bound passes the rows another
  statement holds (`FOR UPDATE SKIP LOCKED`): two logins at once never wait for each other's

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

## The reconciling of the wickets

One instance at a time reconciles the wickets ([ha.md](../sluice-wicket-cf-api/ha.md#the-reconciling-of-the-wickets)).
Decided against the alternatives:

- the lease a table of its own (`sluice_cf_lease`), not a record of the state: a renewal every 2s
  is no revision (no notice to every instance, no copy, no commit row), on a connection of its
  own, never behind a change. Its take holds the revision's lock, so a change sees the lease as it
  is until its commit: the store refuses a change of the leader alone once another took it
- an epoch is the store's clock in milliseconds at least (`GREATEST(epoch + 1, ...)`): a store
  restored from a backup, or a new one, goes on above what the wickets saw, which would refuse
  every write of a lower one, after their restarts too (`epochs.json`)
- what the leader saw of the wickets kept in the store, not listed again by every instance: every
  instance places the apps' indices in its own changes (`State::place`, by the room the budgets,
  `full`, leaving and gone leave), and a share the wicket refused shows in no listing, which
  `cf app` and a start's wait for `RUNNING` on another instance need. A row per app and wicket
  and per wicket, written as one changes, the noted ones of 100ms in one change: a listing's
  answers of a wicket come back as one write
- who writes them: whoever saw them, unless its turn ended meanwhile, what it saw then forgotten:
  a leader cut off notes every share unreachable, which it never writes over the next one's
- `did_not_start` not kept: a takeover is a start, which tries such a share once more, as a
  restart of one instance does. Kept, each failed share's body (its env with it) would be in the
  store for a try saved once a takeover. A wicket gone in the turn before is evacuated at once,
  not after another `--wicket-away-grace`; the evacuations' and respreads' bookkeeping and the
  strangers' lines are learned again
- a wicket keeps the highest epoch per owner (two owners are two controllers), and holds an
  owner's fence until the write is handed to its reloader: the writes reach the routes in the
  order admitted. One without the header is taken as before, both ways (an old cf-api, an old
  wicket ignoring the header)
- a sweep's marker carries its turn's epoch: one of a turn before the lease's is over at once,
  not 10 minutes on (`SWEEP_STALE`), as its instance stops deleting when its turn ends

The bound: the lease (10s), a take's pause (1s) and a listing's (`MAX_PAUSE`, 2s) stay within
`HELD` (15s), asserted at compile time: a wicket started again as the leader went holds its apps
until the next one confirms them.

Measured on one host (release build; PostgreSQL 18 in a container through
Docker's port proxy; a wasi wicket in local mode; two instances, no app), 3 runs each:

| the leader | the other leads | its `/healthz` 200 |
|---|---|---|
| killed (SIGKILL), 2.5s after one leads | 9.42-9.49s | 16-17ms later |
| stopped (SIGTERM): the lease let go of | 0.44-0.49s | 16-17ms later |

A kill's is the lease's lapse (10s after the last renewal, at most 2s before the kill) and a
take's pause; a stop's the take's pause alone.

## An edit on any instance

The leader makes the edits the wickets take at once, the others sending them to it
([ha.md](../sluice-wicket-cf-api/ha.md#an-edit-on-any-instance)). Decided against the
alternatives:

- the leader applies, not "the handler commits what is wanted and the leader converges": `cf
  map-route` and `cf scale` print a wicket's refusal (422) and change nothing, which a commit ahead
  of the wickets cannot. Tried by the instance taking it, beside the leader's watch, a trial put
  was undone by the leader before its commit, or a commit landed after the leader's last listing
- the request sent whole to the leader's own listener, not an endpoint between instances with a
  key of the store: the leader authenticates the token as from any client, and a handler is the
  same code on either path. What goes there is told by method and path (`api::edit`): the handlers
  holding an app or a route. A request sent on (`sluice-sent-on`) is sent no further; one not
  leading answers it 503 `sluice-not-made`, which changed nothing
- one writer, held two ways: an instance without a turn sends no write of an app (`Wicket`'s
  `NO_TURN`, where before it sent one without an epoch, taken by the wicket), and an edit's commit
  is `Store::lead`, refused once another took the lease. So a turn ending under an edit (a pause
  past the lease, 282's release before the requests end) leaves nothing half made: the commit
  is before the takeover, whose first listing sees it, or not at all, the request going to the
  next. A handler missing from `api::edit` answers 503 on a follower, never a write beside the
  leader's
- the request held for a takeover, not answered 503 at once: cf CLI v8.19.0 sends a `GET` answered
  503 three times within 10ms and a `POST` once, `Retry-After` unread (measured against a stub)
- an answer lost with the leader: sent again where twice is once (the CLI does so itself for all
  but a `POST`); a deployment's `POST`s not (a second `continue` is a step further), 502
- the lease's `address` is written by `take`: a build predating it takes the lease leaving the
  address of the holder before (283)

Measured on one host (release build, loopback; PostgreSQL 18 in a container, `fsync=off`; the
test's mock wicket; `cf scale -i` of one app, 80 each):

| the request reaches | median | p90 |
|---|---|---|
| the leader | 3.7-4.2ms | 4.2-4.5ms |
| another instance | 5.9-6.5ms | 6.2-6.8ms |

The hop is 2.2ms, on a connection kept.

## The work outliving a request

A build, a broker's operation and an app's delete after its unbinds are work of the turn of the
instance reconciling the wickets ([ha.md](../sluice-wicket-cf-api/ha.md#the-work-outliving-a-request)).
Decided against the alternatives:

- the lease of the wickets, not an owner and a lease per build and operation (an instances table
  with a heartbeat, each row naming its owner, taken once the owner's lapsed). The turn already
  is what such an owner needs: a task cancelled at its end (`Controller::spawn`), a change the
  store refuses once another took the lease (`Store::lead`), a takeover that is a start. A second
  kind of owner would be a second takeover to reason about, and an app's delete is the leader's
  either way (it writes the wickets). What it costs: the staging of every build runs on one
  instance, and a leader's end cuts all of them at once, each taken up within the takeover (a
  build of source goes on at the wicket meanwhile). An upload reaches the build through the
  bucket on any instance, the one that took it too (`Store::local`): no copy is saved by staging
  where it was uploaded
- the request writes the row, the leader's follower takes it up from the state's changes
  (`Controller::follow`), on the same instance too: one path, and nothing is told between
  instances but through the store. The request's wait for a build (15s) is on the state's changes
- every change of the work is `Store::lead`, not only its end: a paused instance's tasks run on
  until its lease's loop is polled, and may send what is idempotent (a broker's request, a
  build's follow) but change nothing. What is not idempotent comes after a change that is
  refused: the wicket forgets a build once the store keeps the staging's end (before, a crash
  between the two built again from scratch), and an undo at the broker is sent once the store
  keeps the operation failed and to be undone (`Operation::undo`), where before it was a detached
  task of the process deciding it
- the undo at least once, not exactly once: a turn ending between the undo sent and the store
  told has the next send it again; a deprovision or an unbind of what is gone is a `410`
- an app's delete is due once none of its job's waits is an operation under way, not when "each
  binding it listed was unbound": a binding gone another way (its instance purged) no longer
  leaves the job waiting until a restart
- no line of a staging is carried over by the store: with a flume every instance holds them
  ([The logs of several instances](#the-logs-of-several-instances))

## The logs of several instances

The flume is the bus of the cf-api's own lines too
([Live access log](live-access-log.md#wickets-and-the-cf-api)). Decided against the alternatives:

- the own lines as rows of the store: a table growing by every staging line, a second source to
  merge with the flume's in one order. The flume already orders the apps' lines for every watcher
- the flume's time as a line's timestamp, not each instance's clock or counter: the CLI walks by
  `start_time`, so two instances must name a line by one number. An `LGR` line is the watch's
  own, placed one nanosecond after the last line taken: a flume line can only collide with it by
  being taken within that nanosecond
- no cursor in the store: an instance starting watches from the oldest line at hand
  (`WatchRequest.oldest`), and holds what the others do within the flume's ring (10000 lines,
  64 MiB by default; 1000 per app kept)
- with `--flume` one instance's own lines go round the flume as several instances' do: one path.
  The cost is a flume away: the own lines wait in the tap (512) where before they were in the
  ring at once
- a staging's end waits for its own last line to come round (`run`, `n`) before it looks at the
  readers, 5s at most as before
- the reader of a staging's log is told through the flume too (`cf-walk`), by an instance not
  reconciling the wickets while the app has a build `STAGING`: 4 lines a second per `cf push`
  for the time of the staging, none otherwise. Against a call to the leader's `--instance-url`:
  nothing is told between instances but through the store and the flume

## An update of several instances

Two builds and two sets of flags on one store
([ha.md](../sluice-wicket-cf-api/ha.md#an-update)). Decided against the alternatives:

- the oldest build reconciles the wickets, against a new build writing as the old one until all
  are new: the second needs every build to keep the bodies of the one before (the wickets' app
  API, the rows), the first only an order of the builds. The order is the newest layout a build
  reads (`postgres::SCHEMA`), not its build time: an old source built again is newer by the
  clock. So `SCHEMA` moves with what an older build would undo, not with the rows' shape alone
- the layout moved by the one reconciling the wickets once every instance says it reads it,
  under the revision's lock; a build checks the layout in every transaction, so one that does not
  say itself (before layout 2) is refused at its next change. Layout 2 changes no row: it is the
  fence against those builds (they reconcile the wickets whatever the lease says, 278 and 280)
- the topology the store's, taken from flags when an instance having them takes the lease, which
  needs them to be the store's already or every instance's. Against the flags of whoever leads:
  the apps go back and forth as the lease passes between two sets of flags. Against a record an
  admin call changes: the wickets' clients (URL, token) of every instance made anew at run time,
  and flags that then say nothing. The wickets' URLs stay each instance's
- an instance says itself on the lease's connection, as often and for as long as the lease:
  one killed is off the store when its lease lapsed, no later
- what a build does not read of a row kept as the pair (the body taken, the body as this build
  writes what it read), only of the rows where the two differ; merged into the body at the
  write. A list's items are told apart by position: kept only while the list is as it was read
- the rows kept whole and those of what the wickets were seen doing compared as this build
  writes them, not as they were taken: two builds writing one value in two ways do not write it
  over each other at every change
- a start refused for `--blobs` and `--flume` only: without them several are wrong in silence (a
  package one instance cannot read, a log per instance). The rest is logged: an update changes it
- a start refused is tried for the lease's time first: a supervisor starting an instance again
  after a kill finds its row still there

## A stop

The release binary on the host (`--wicket` unreachable), SIGTERM to its exit: idle, 11-13ms. A
package upload under way (12 MB at 4 MB/s, SIGTERM 1s in): answered 200 at 3.0s, the process gone
2.05s after the signal; a new connection refused meanwhile. With `--stop-grace 1` and the upload at
1 MB/s: gone after 1.02s, the upload cut (`stopped past --stop-grace: 1 connection(s), 1
request(s) and 0 put(s) cut`). So an upgrade's gap is the 0.7-0.8s boot above plus what the
requests under way take, `--stop-grace` (8s) at most; not measured again in a VM.

Of several instances the one reconciling the wickets lets go of the lease as the stop begins,
not at its end ([ha.md](../sluice-wicket-cf-api/ha.md#a-stop)). Decided against the alternatives:

- at the stop's end, the requests answered first: the others' edits are held for as long as the
  slowest request of the one stopping (an upload, a start waiting for a health check),
  `--stop-grace` at most, and found its listener closed all that time
- at once, without waiting for the edits it makes: each would be refused at its commit and made
  again by the next leader, its trial already at the wickets; and the only instance of a store
  would make none it has under way. Waiting 2s (`lease::EDITS_END`) lets the short ones end in
  their turn, and holds the others' that much longer at most
- an instance that let go of the lease follows it on (`Postgres::held`) and takes it no more: its
  edits under way need the next leader's `--instance-url`

Measured as [the takeover](#the-reconciling-of-the-wickets) (two instances, no app), the leader
stopped with a request under way whose body ends 3s later, 3 runs: the other leads 0.01-0.03s
after the signal (its tries, every second, fell there: 1s at most), the request is answered 2.96s
after it and the process gone 2.97-2.98s after it. Before, the other led once the request was
answered.

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
wait without one. The reader may read from another instance: that one feeds the read to the
flume ([The logs of several instances](#the-logs-of-several-instances)). Seen on an arm64 Lima VM before the success waited too: a cached build of the
test's sample buildpack ended within 2s of its first lines, and `cf push` showed none of them.

## The demo's GIF

2.5MB ([demo/README.md](../sluice-wicket-cf-api/demo/README.md)).
