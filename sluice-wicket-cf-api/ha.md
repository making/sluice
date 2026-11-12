# sluice-wicket-cf-api: several instances

What several instances of the [cf-api](README.md) share, in place of the files of one
instance's `--data-dir`: the state and the logins in PostgreSQL (`--state`), the packages and
the droplets in an S3 bucket (`--blobs`); and who reconciles the wickets, one instance at a time
([The reconciling of the wickets](#the-reconciling-of-the-wickets)). An edit is still tried
against the wickets by the instance taking it: only the one reconciling them is ready, and a
balancer checking `/healthz` sends it every request, the others standing by. The options are in
the README's [Options](README.md#options).

## The state in PostgreSQL

`--state postgres://user:password@host:5432/database?sslmode=require` keeps the state in PostgreSQL in place of
`state.json`: the store several instances are to share.

- its tables (`sluice_cf_records`: a row per organization, app, route, package, ..., its JSON as
  `state.json` has it; `sluice_cf_meta`: the revision; `sluice_cf_commits`: who committed each)
  are made at the first start, in the schema of the user's `search_path`
- an empty store takes `--data-dir`'s `state.json`, once (`STATE: ... taken into the store`); the
  file stays as it was and is written no more. Nothing writes a store back to a file
- a change is a transaction holding the revision's lock: it sees every change before it, of any
  instance, and writes the rows it changed. It is answered once committed; while the store does
  not answer a change waits for it, and a new request is answered 503 (`/healthz` too)
- the reads are answered from a copy in memory, brought to the store's revision at each
  request's start (one query) and as the others commit (`LISTEN`)
- give the URL `?sslmode=require`: the state holds the apps' credentials. The server is then
  verified, its certificate and its host name, against `--state-ca-cert`, else the webPKI roots.
  Without it (libpq's default `prefer`) a server with TLS is verified the same, and one without
  is gone on with, the start saying so (`STATE: the connection to the store is not over TLS`)

Run against PostgreSQL 18; what it costs is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-states-store).

## The logins in the store

With `--state` the [logins](README.md#login) are the store's too, in place of `--data-dir`'s
`jwt.key` and `sessions.json`: a token of one instance is good on every other, with no session
affinity.

- the key the tokens are signed with is a row (`sluice_cf_keys`), made by the first instance to
  start. A store with none takes `--data-dir`'s `jwt.key` and the logins of its `sessions.json`,
  once (`UAA: ... taken into the store`): the tokens issued before stay good. The files stay as
  they were and are written no more
- a login is a row (`sluice_cf_sessions`): a refresh token is good once among all instances, one
  of two refreshes at once getting the new pair. The expired go as logins are made, 10000 kept
  at most, those expiring last
- a passcode of `cf login --sso` is a row (`sluice_cf_passcodes`): shown by one instance, taken
  by any, once; 1000 wait at most
- the sign-in under way at the provider is no row: the browser keeps it between `/uaa/passcode`
  and `/uaa/callback`, in a cookie sealed with a key of the tokens' key's (10 minutes, the
  callback's alone, `HttpOnly`), so any instance finishes it, and only for the browser that
  began it. Without `--state` too
- the expiries are by the store's clock; a token's own (12 hours, 30 days) by the clock of the
  instance verifying it: keep them in step
- a token names its key (`kid`) and each instance reads the keys every minute, so a key can be
  replaced with no login lost: a row added with the next `seq` and a `made` some minutes ahead
  signs from then on, every instance knowing it by then; the one before verifies for 30 days
  more (a refresh token's life)
- while the store does not answer, a login and a refresh are answered 503, as any request

## The blobs in a bucket

`--blobs s3://bucket/prefix` keeps the packages and the droplets in an S3 bucket in place of
`--data-dir`'s files, under the same keys (`packages/<guid>.zip`, `droplets/<sha256>.wasm`,
`droplets/<sha256>.tar`): the blobs several instances are to share, with `--state`.

- the bucket is of any S3 at `--blobs-endpoint`, addressed by path (`<endpoint>/<bucket>/<key>`),
  or without it of AWS in `--blobs-region`. The requests are signed (Signature Version 4) with
  `--blobs-access-key-id` and `--blobs-secret-access-key`: static keys that may get, put, delete
  and list below the prefix. An `https://` endpoint is verified against `--blobs-ca-cert`, else
  the webPKI roots
- an upload, and a droplet as it is staged, are files of `--data-dir` first (the instance's own),
  then put in one request: an object is there whole or not at all. A package is unpacked from a
  copy in `--data-dir`, a droplet goes to a wicket as a stream: neither is held in memory
- a blob the state names no more (an older package, a droplet neither current nor the one
  before) is deleted 30 to 35 minutes later, another instance reading it as it may be; one never
  named (a staging cut short) likewise. In the directory the first goes at once
- the bucket holds what `disk-mib` is sized for in the directory, and what was dropped in the
  last 35 minutes. A bucket and prefix are one state's: instances on another state delete what
  theirs does not name
- as an app of a Firecracker wicket, its `egress` names the endpoint

Run against RustFS 1.0.5 (`rustfs/rustfs`) and Versity S3 Gateway 1.10.0 (`versity/versitygw`,
its POSIX backend); what was decided
and measured is in [.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-blobs-bucket).

## The reconciling of the wickets

With `--state` one instance at a time reconciles the wickets: the holder of a lease, a row of the
store (`sluice_cf_lease`) it renews every 2s for 10s, by the store's clock. Its turn is the
watches of the wickets, the confirmations of what a wicket holds at its start, the evacuations
and the moves, the purges, the deployments' steps and the sweeps of the bucket. The others take
the lease once it lapses, trying every second; a stop lets go of it, the next taking it at once.
Without `--state` the one process of `--data-dir` leads from its start.

- each take begins a turn of a new epoch, the store's clock in milliseconds at least: above every
  one before, of a store restored from a backup too. The turn's writes to a wicket's app API carry
  it (`sluice-epoch`), and the wicket refuses one below the highest it saw of the owner, after a
  restart of it too: an instance paused or cut off past its lease puts nothing over what the next
  one put. A write without an epoch (a cf-api predating it, or of `--data-dir`) is taken as
  before, and a wicket predating it takes any
- the store refuses a change only the leader makes (a deployment's step, a move placed, a purge
  done) once another took the lease, which ends the turn; a holder that cannot renew ends it 9s
  after the last renewal it sent that held
- what the leader saw of the wickets is the store's, a row each, written as it is seen: their
  answers of each app's share, their budgets, offers, the wickets leaving or gone. Every instance
  answers `cf app` and places the apps' indices by it; the others list a wicket for a request
  only (the process stats)
- a takeover is a start of the cf-api to the wickets: the next leader takes up the deployments and
  the moves under way from the state, lists every wicket and confirms what each holds. What only
  a process knew is learned again: a share whose VMs did not start is tried once more. The lease,
  a take's pause and a listing stay within the 15s a wicket started again holds its apps
- `/healthz`: 200 of the leader once it listed every wicket in its turn; 503 of the others
  (`another instance reconciles the wickets`)

What was decided and measured is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-reconciling-of-the-wickets).
