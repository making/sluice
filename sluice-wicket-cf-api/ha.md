# sluice-wicket-cf-api: several instances

What several instances of the [cf-api](README.md) share, in place of the files of one
instance's `--data-dir`: the state and the logins in PostgreSQL (`--state`), the packages and
the droplets in an S3 bucket (`--blobs`); and who reconciles the wickets, one instance at a time
([The reconciling of the wickets](#the-reconciling-of-the-wickets)), which makes the edits the
wickets take at once for every instance ([An edit on any instance](#an-edit-on-any-instance)) and
does the work that outlives a request
([The work outliving a request](#the-work-outliving-a-request)); and the apps' logs, one on every
instance through the flume ([The logs](#the-logs)); and what holds while their builds and flags
differ ([An update](#an-update)). Then how they are run ([The deployment](#the-deployment)),
updated ([Updating them](#updating-them)), sized and backed up
([Sizing and backup](#sizing-and-backup)).
The options are in the README's [Options](README.md#options).

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
- an upload's zip (written as its body comes), and a droplet as it is staged, are files of
  `--data-dir` first (the instance's own), then put in one request: an object is there whole or
  not at all. A package is unpacked from the bucket, 8 MiB of it at a time, a droplet goes to a
  wicket as a stream: neither is held in memory, nor a second time in `--data-dir`
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
and the moves, the purges, the deployments' steps, the sweeps of the bucket, and
[the work outliving a request](#the-work-outliving-a-request). The others take
the lease once it lapses, trying every second; [a stop](#a-stop) lets go of it, the next taking
it at once.
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
  only (the process stats). A request that is no `GET` is answered once what it saw is written:
  the share a wicket refused a `cf push` is `CRASHED` on whichever instance the CLI asks next
- a takeover is a start of the cf-api to the wickets: the next leader takes up the deployments and
  the moves under way from the state, lists every wicket and confirms what each holds. What only
  a process knew is learned again: a share whose VMs did not start is tried once more. The lease,
  a take's pause and a listing stay within the 15s a wicket started again holds its apps
- `/healthz`: 200 of the leader once it listed every wicket in its turn; of the others once the
  lease names the leader's `--instance-url`

What was decided and measured is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-reconciling-of-the-wickets).

## An edit on any instance

An edit the wickets take at once (routes, `cf scale -i`, a start, stop or restart, a manifest, a
deployment and its cancel and continue, the app's name and annotations), and a deletion of what
they serve (an app, a route, a space, an organization), is made by the instance reconciling the
wickets: tried against them, committed and answered as by one instance, a wicket's refusal a 422
that changes nothing. Any other request (the reads, the uploads and the builds, the services, the
logins) is answered by the instance it reaches.

- give each instance `--instance-url http(s)://host:port`, where the others reach its listener
  and no other instance's; the lease names its holder's. Another instance sends the request
  there as it came, with its token, and relays the answer. The request's `Host` is the URL's:
  what routes to the instance (a wicket, a sluice server, an ingress) routes by it; the host
  the client asked goes beside it (`sluice-sent-for`), so the links are the API's as the client
  reaches it. `https://` is verified against `--instance-ca-cert`, else the webPKI roots. The
  instances are one another's clients only: no key between them, the token is checked again
- an instance writes no app to a wicket outside its turn, and the edit's commit is refused by the
  store once another took the lease: an edit is made in one turn or not at all, and then goes to
  the next leader
- while no instance takes it (a takeover: within 11s of a kill, within 3s of a stop) the request
  is held, 20s at most, then answered 503 with `Retry-After`. The cf CLI tries a 503 of a `GET` again
  at once and of a `POST` not at all
- the leader gone with an edit under way (a kill, the network), its answer lost: the request is
  sent again to the next, which a second time changes nothing more; but a deployment's `POST`s
  (create, cancel, continue), answered 502: `cf app` tells whether it was made
- a leader without `--instance-url` takes no edit of the others: they are not ready (`/healthz`
  503) while it leads, and a balancer checking it sends that one every request

What was decided and measured is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#an-edit-on-any-instance).

## The work outliving a request

A build (`cf push`, `cf restage`), a broker's operation (a provision, an update, a deprovision, a
bind, an unbind) and an app's delete waiting for its unbinds go on after the request that asked
for them is answered. The request writes what is asked to the store, on whichever instance it
reaches; the instance reconciling the wickets does it, in its turn, and the next one goes on from
what the store keeps of it. There is no lease per build or operation: the one of the wickets.

- a start fails nothing and takes nothing: an instance starting beside a build under way leaves
  it to the one whose turn it is
- the turn over (a stop, a kill, a pause past the lease), the next takes each up as a restart of
  one instance does: a build of source a wicket runs is followed on from the line the store kept
  (built again only where that wicket knows it no more), a zip is staged again from the package,
  an image's manifest read again and its rootfs asked of the wicket, which keeps it by its digest;
  a broker's operation is sent again while the broker has not taken it (the requests are
  idempotent) and polled on once it has; an app is deleted once its bindings are unbound
- what the work changes of the state is made in the turn alone, refused by the store once another
  took the lease: an instance paused past its lease ends no build, no operation and no job, and
  has no wicket forget a build. A job is settled once, by the change that ends what it waits for
- a provision or a bind whose outcome is unknown is undone at the broker (orphan mitigation) only
  once the store keeps that it failed and is to be undone: an instance whose turn ended sends no
  undo. A turn ending before the broker answered the undo, the next sends it again
- one taking a build up says so in the staging log (`Staging taken up ...`) and logs on; the
  lines before are in every instance's log ([The logs](#the-logs))
- `cf push` waits 15s for a build on the instance it reached, then polls it, on any

What was decided is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-work-outliving-a-request).

## A stop

An instance stopped (SIGTERM, SIGINT, stdin's end) loses no request it took: its listener closes,
the requests under way are answered within `--stop-grace` (8s), each connection closed after its
answer ([Behavior](README.md#behavior)). What several add:

- the one reconciling the wickets lets go of the lease first, once the edits it is making ended,
  2s at most: the next takes it within a second, while the requests of the one stopping are still
  answered. The turn's work under way (a put to a wicket, a build's staging, a broker's
  operation) is cut there and taken up by the next, as after a kill
- an edit still under way then is not committed (the store refuses it), is sent to the next
  leader and its answer relayed, as an edit of any other instance: made once, answered by the
  one that took the request
- the others' edits are held meanwhile and made by the next leader: none is sent to an instance
  that stops, whose listener is closed
- `/healthz` answers 503 from the stop on, on the connections it still has; a new connection is
  refused. Whoever routes to the instances takes it out by either, or before the signal: a
  request sent to the closed listener was not taken, and is safe to send to another instance
- the last instance of a store stopped, an edit longer than those 2s is not made (the request
  cut at `--stop-grace`): no instance takes the lease

What was decided and measured is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#a-stop).

## The logs

With `--flume` on every instance (one flume), an app's log is the same on each: `cf logs` and the
staging log of `cf push` read from any, and go on from another.

- an instance feeds its own lines (`STG`, `API`, `CELL`) to the flume, as the node `cf-api:<id>`
  (the id made in its `--data-dir`, `flume-node`: a directory per instance), and takes every
  instance's from its watch, with the apps' own. A line's timestamp is the flume's, the order the
  flume's
- an instance starting takes the lines the flume has at hand (`--ring-size`, 10000 by default),
  the newest 1000 of each app: what the others hold, but what the flume let go of
- the lines lost (`LGR`) are each instance's watch's own: they may differ between instances
- while the flume is away the instances' own lines reach no log (they are on each one's stdout);
  the newest 512 of each instance come once it is back, the rest are told as lost
- a staging's end waits for the reader of its log on whichever instance it reads
- without `--flume` a log is the instance's alone: for one instance

What was decided is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-logs-of-several-instances).

## An update

An update one instance at a time runs two builds, and two sets of flags, on one store. Each
instance is a row of it (`sluice_cf_instances`), said every 2s for 10s and removed at its stop:

```sql
SELECT id, build, reads, address, started FROM sluice_cf_instances WHERE expires > now();
```

is the update's progress (`reads`: the newest rows' layout the build reads); `GET /v3/info` names
the build of the instance answering.

- the instance reconciling the wickets is of the oldest build on the store: what it puts to the
  wickets and writes to the store, every instance reads. A newer one waits (`LEASE: not taken by
  this instance: ...`), and one leading lets go of the lease for an older one come beside it.
  The last older one stopped, a new one takes over as after any stop
- what a row holds that a build does not read (a field of a newer one) is kept when that build
  writes the row; a row of a kind it does not know is left alone
- the rows' layout is the store's (`sluice_cf_meta.layout`): the instance reconciling the
  wickets moves it to its build's once every instance reads that one (`STATE: the store's layout
  is 2 now`). A build reads the layouts before its own; it does not open a store of a newer one
  (`--state: of a newer sluice-wicket-cf-api`): from the move on, the way back to the build before
  is the store's backup
- the wickets' names and the domains (`--wicket`, `--domain`) are the store's, not each
  instance's: a new store takes the flags of its first instance. Other flags become the store's
  when an instance having them takes the lease, which it does once every instance on the store
  has them: until then the instances of the flags before reconcile the wickets and the apps stay
  where they are; then the apps are placed by the new flags, once (logged, as at one instance's
  start). While the flags of no instance are the store's and they are not all alike, none takes
  the lease: the edits are answered 503
- several share the bucket and the flume: an instance without `--blobs` or `--flume`, or of
  another bucket or prefix, beside another on the store does not start (after 10s, in which an
  instance killed is off the store)
- the rest is to be alike, and said by each instance where another's differs (`INSTANCES: the
  instance ... on the store has otherwise: its build (...), --admin`): `--admin` (with
  `--group-role` and `--oidc-groups-claim`), `--client-credentials`, `--cnb-builder` and the
  build's size, the flume's URL, `--instance-url` (given or not), `--oidc-issuer` (its client
  and claims), `--security-group-editor`, `--service-broker-editor`, the wickets' URLs,
  `--wicket-away-grace`, `--wicket-token`. Compared as each build shows them: between two
  builds one may be said to differ that does not
- update the flume before the cf-api: an older one stamps no line and begins a watch at its
  newest lines, each instance then stamping with its own clock
- a build before the instances said themselves (layout 1) is never one beside a later one: the
  store refuses it once its layout moved

What was decided is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#an-update-of-several-instances).

## The deployment

Two or more instances, alike but for `--instance-url` and `--data-dir`, behind whatever spreads
the requests over them: no session affinity, any instance takes any request.

- an instance holds nothing to keep: `--state`, `--blobs` and `--flume` are required beside
  another instance. Its `--data-dir` is scratch (an upload and a droplet while they are made)
  and its id among the flume's nodes (`flume-node`): one that does not outlive the instance
  makes it a new node at each start, the ones before listed as disconnected until the flume
  forgets them (its `--forget-after`, an hour)
- what routes to them sends a connection refused to another instance (the listener of one
  stopping is closed, [A stop](#a-stop)), and takes out one whose `/healthz` is not 200. A
  request cut with no answer (an instance killed) is safe to send again when it is a `GET`
- the store away, every instance answers 503 (`CF-ServiceUnavailable`), `/healthz` too: the
  API is down, the apps serve on, and it is back with the store, no instance restarted. No
  instance reconciling the wickets (a takeover, flags not alike), the reads are answered and
  an edit is held, then 503 ([An edit on any instance](#an-edit-on-any-instance))

### Behind a load balancer

Processes or containers, each with a listener the balancer and the other instances reach. As
in the README's [Docker Compose](README.md#docker-compose) sample: in its `compose.yaml`, the
services below in place of `cf-api`, the volumes `postgres`, `s3`, `cf-api-a` and `cf-api-b` in
place of `cf-api`; `haproxy.cfg` next to it; in `.env`, `POSTGRES_PASSWORD`, `S3_ACCESS_KEY_ID`
and `S3_SECRET_ACCESS_KEY` too (`openssl rand -hex 16` each).

```yaml
  postgres:                         # the state and the logins
    image: postgres:18-alpine
    environment:
      POSTGRES_USER: cf
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD}
    healthcheck:
      test: [CMD, pg_isready, -U, cf]
      interval: 2s
    volumes:
      - postgres:/var/lib/postgresql
    restart: unless-stopped
  s3:                               # the packages and the droplets
    image: rustfs/rustfs:1.0.5
    environment:
      RUSTFS_ACCESS_KEY: ${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID}
      RUSTFS_SECRET_KEY: ${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY}
    volumes:
      - s3:/data
    restart: unless-stopped
  bucket:                           # made once: the cf-api makes none
    image: amazon/aws-cli:2.31.35
    command: [s3api, create-bucket, --bucket, cf-blobs, --endpoint-url, "http://s3:9000"]
    environment:
      AWS_ACCESS_KEY_ID: ${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: us-east-1
    depends_on: [s3]
    restart: on-failure
  cf-api-a: &cf-api
    image: ghcr.io/making/sluice/sluice-wicket-cf-api
    pull_policy: always
    command: [--listen, 0.0.0.0:9091, --instance-url, http://cf-api-a:9091, --wicket, http://wicket:9090, --flume, grpc://flume:8002, --blobs, s3://cf-blobs, --blobs-endpoint, http://s3:9000, --oidc-issuer, http://dex.localhost:5556/dex]
    environment:
      SLUICE_WICKET_CF_API_STATE: postgres://cf:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD}@postgres:5432/cf
      SLUICE_WICKET_CF_API_BLOBS_ACCESS_KEY_ID: ${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID}
      SLUICE_WICKET_CF_API_BLOBS_SECRET_ACCESS_KEY: ${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY}
      SLUICE_WICKET_CF_API_WICKET_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      SLUICE_WICKET_CF_API_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      SLUICE_WICKET_CF_API_DOMAIN: ${APPS_DOMAIN:-localhost}
      SLUICE_WICKET_CF_API_OIDC_CLIENT_ID: sluice
      SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      SLUICE_WICKET_CF_API_OIDC_CLAIM: email=admin@example.com
      SLUICE_WICKET_CF_API_ADMIN: email=admin@example.com
    volumes:
      - cf-api-a:/data              # its scratch and its id among the flume's nodes: one each
    depends_on:
      postgres: {condition: service_healthy}
      bucket: {condition: service_completed_successfully}
      wicket: {condition: service_started}
      dex: {condition: service_started}
    restart: unless-stopped
  cf-api-b:
    <<: *cf-api
    command: [--listen, 0.0.0.0:9091, --instance-url, http://cf-api-b:9091, --wicket, http://wicket:9090, --flume, grpc://flume:8002, --blobs, s3://cf-blobs, --blobs-endpoint, http://s3:9000, --oidc-issuer, http://dex.localhost:5556/dex]
    volumes:
      - cf-api-b:/data
  cf-api:                           # the balancer: what the CLI targets
    image: haproxy:3.4-alpine
    ports:
      - "127.0.0.1:9091:9091"
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    depends_on: [cf-api-a, cf-api-b]
    restart: unless-stopped
```

```text
# haproxy.cfg, next to compose.yaml
defaults
    mode http
    timeout connect 2s
    timeout client 15m          # an upload, a build waited for
    timeout server 15m
    retries 2
    option redispatch 1         # a connection refused, or not made, goes to the other instance
resolvers docker                # an instance recreated may come back at another address
    nameserver dns 127.0.0.11:53
    hold valid 2s
frontend cf
    bind :9091
    default_backend cf-api
backend cf-api
    option httpchk GET /healthz # 503 from an instance's stop on, and while it is not ready
    default-server check inter 1s fall 2 rise 1 resolvers docker init-addr last,libc,none
    server a cf-api-a:9091
    server b cf-api-b:9091
```

```text
docker compose up -d
cf login -a http://127.0.0.1:9091 --sso    # the code one instance shows, any takes
cf push hello -p hello/ -i 2
# who is on the store, and who reconciles the wickets
docker compose exec postgres psql -U cf -c \
    "SELECT build, address, started FROM sluice_cf_instances WHERE expires > now()" -c \
    "SELECT address FROM sluice_cf_lease WHERE expires > now()"
```

This PostgreSQL takes no TLS (the instances say so at their start) and the bucket's endpoint is
plain HTTP: on a network that is not yours alone, `?sslmode=require` and an `https://` endpoint
([The state in PostgreSQL](#the-state-in-postgresql), [The blobs in a bucket](#the-blobs-in-a-bucket)).

### As apps of Firecracker wickets

An instance a VM of each of two or more wickets, put through each wicket's console as the one
of the README's [Run as an app of the wicket](README.md#run-as-an-app-of-the-wicket), with no
`disk`: the wickets advertise the API's host to a sluice server, which spreads its requests
over them, and a wicket down takes one instance with it, not the API.

```text
export TOKEN=$(openssl rand -hex 16) FLUME_TOKEN=$(openssl rand -hex 16) SERVER_TOKEN=$(openssl rand -hex 16)
export POSTGRES_PASSWORD=$(openssl rand -hex 16) S3_ACCESS_KEY_ID=cf S3_SECRET_ACCESS_KEY=$(openssl rand -hex 16)
docker network create sluice
docker run -d --name flume --network sluice -e SLUICE_FLUME_TOKEN=$FLUME_TOKEN ghcr.io/making/sluice/sluice-flume
docker run -d --name postgres --network sluice -e POSTGRES_USER=cf -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -v postgres:/var/lib/postgresql postgres:18-alpine
docker run -d --name s3 --network sluice -e RUSTFS_ACCESS_KEY=$S3_ACCESS_KEY_ID \
    -e RUSTFS_SECRET_KEY=$S3_SECRET_ACCESS_KEY -v s3:/data rustfs/rustfs:1.0.5
docker run --rm --network sluice -e AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY \
    -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.31.35 s3api create-bucket --bucket cf-blobs --endpoint-url http://s3:9000
docker run -d --name server --network sluice -p 127.0.0.1:8000:8000 ghcr.io/making/sluice/sluice-server:native \
    --sluice.token=$SERVER_TOKEN
for w in a b; do                           # wicket-a's console on 9090, wicket-b's on 9190
  [ $w = a ] && port=9090 || port=9190
  docker run -d --name wicket-$w --network sluice --device /dev/kvm --group-add $(stat -c %g /dev/kvm) \
      --tmpfs /run/sluice --stop-timeout 40 -v wicket-$w:/data -p 127.0.0.1:$port:9090 \
      -e SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN=$SERVER_TOKEN \
      -e SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE_TOKEN=$TOKEN \
      -e SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_TOKEN=$FLUME_TOKEN \
      ghcr.io/making/sluice/sluice-wicket-runtime-firecracker \
      --server grpc://server:8001 --listen 0.0.0.0:8080 --console 0.0.0.0:9090 --flume grpc://flume:8002
done
```

The instance of a wicket, by `put.sh a 9090` and `put.sh b 9190`:

```text
#!/usr/bin/env bash
# put.sh <a|b> <console port>: the cf-api as an app of that wicket
w=$1; console=http://127.0.0.1:$2
[ $w = a ] && peer=b || peer=a
sha256=$(curl -sS -X PUT -H "Authorization: Bearer $TOKEN" $console/api/images \
    -d '{"image": "ghcr.io/making/sluice/sluice-wicket-cf-api"}' | jq -r .sha256)
curl -sS -X PUT -H "Authorization: Bearer $TOKEN" $console/api/apps/cf-api -d @- <<JSON
{
  "owner": "ops",
  "hosts": ["cf.localhost", "wicket-$w"],
  "component": {"sha256": "$sha256", "name": "sluice-wicket-cf-api.ext4"},
  "sandbox": {
    "memory-mib": 1024,
    "egress": ["wicket-a:9090", "wicket-b:9090", "wicket-$peer:8080", "flume:8002", "postgres:5432", "s3:9000"],
    "env": {
      "SLUICE_WICKET_CF_API_ANY_CREDENTIALS": "true",
      "SLUICE_WICKET_CF_API_DATA_DIR": "/tmp/cf-api",
      "SLUICE_WICKET_CF_API_INSTANCE_URL": "http://wicket-$w:8080",
      "SLUICE_WICKET_CF_API_WICKET": "a=http://wicket-a:9090,b=http://wicket-b:9090",
      "SLUICE_WICKET_CF_API_WICKET_TOKEN": "$TOKEN",
      "SLUICE_WICKET_CF_API_FLUME": "grpc://flume:8002",
      "SLUICE_WICKET_CF_API_FLUME_TOKEN": "$FLUME_TOKEN",
      "SLUICE_WICKET_CF_API_STATE": "postgres://cf:$POSTGRES_PASSWORD@postgres:5432/cf",
      "SLUICE_WICKET_CF_API_BLOBS": "s3://cf-blobs",
      "SLUICE_WICKET_CF_API_BLOBS_ENDPOINT": "http://s3:9000",
      "SLUICE_WICKET_CF_API_BLOBS_ACCESS_KEY_ID": "$S3_ACCESS_KEY_ID",
      "SLUICE_WICKET_CF_API_BLOBS_SECRET_ACCESS_KEY": "$S3_SECRET_ACCESS_KEY"
    }
  }
}
JSON
```

```text
cf api http://cf.localhost:8000            # through the server, either wicket's instance
cf login -u me -p x                        # any credentials: see the README's Operating it
cf push web --docker-image ghcr.io/traefik/whoami:v1.11.0 -i 2    # a VM on each wicket
curl http://web.localhost:8000/
curl -H 'Host: wicket-a' http://127.0.0.1:8000/healthz            # one instance's: ready
```

- **no `disk`**: the rootfs is read-only, so `--data-dir` is on `/tmp`, a tmpfs of half the
  VM's memory ([Sizing and backup](#sizing-and-backup)), gone with the VM: each start is a new
  node of the flume, the one before forgotten an hour later. Without a disk the wicket starts the new VM before it tells the old one
  to stop ([Updating them](#updating-them))
- **`--instance-url`** is the wicket's `--listen` (beside `--server`), in the other's
  `egress`, and the wicket's name a host of the app there: an edit sent on says `Host:
  wicket-a`, which only that wicket serves
- **`egress`** names the store, the bucket, the flume, every wicket's console and the other
  instances' listeners. A CA of your own for the store or the bucket (`--state-ca-cert`,
  `--blobs-ca-cert`) is a file of the image: one built on the cf-api's with it added
- **who reaches it**, a provider in place of any credentials, and the wickets' own updates:
  as [Operating it](README.md#operating-it) says of one instance. A wicket restarting, the
  API answers through the other's instance

## Updating them

One instance at a time, the rules of [An update](#an-update) holding meanwhile:

1. the flume first, when it is updated too
2. a backup of the store, when the new build moves its layout
   ([Sizing and backup](#sizing-and-backup)): from the move on there is no way back but it
3. each instance stopped (SIGTERM) and started with the new build and flags, the next once
   this one's `/healthz` is 200. `sluice_cf_instances` is the progress; the last of the old
   build stopped, a new one reconciles the wickets and moves the layout

```text
# behind the balancer
docker compose pull cf-api-a               # the image of both
docker compose up -d --no-deps --force-recreate cf-api-a
docker compose exec cf-api wget -qO- http://cf-api-a:9091/healthz    # ready
docker compose up -d --no-deps --force-recreate cf-api-b
docker compose exec cf-api wget -qO- http://cf-api-b:9091/healthz
# as apps of the wickets: the app put again (PUT /api/images makes the new image's rootfs)
./put.sh a 9090
curl -H 'Host: wicket-a' http://127.0.0.1:8000/healthz               # ready
./put.sh b 9190
curl -H 'Host: wicket-b' http://127.0.0.1:8000/healthz
```

A wicket puts the app again by starting the new VM, serving the next connection from it, then
telling the old one's process to stop: no boot long without that instance.

An instance stopping answers the requests it took and lets go of the lease first
([A stop](#a-stop)): the CLI sees nothing of it, a `cf push` under way neither. What was
measured of both deployments, an update, a kill and the store away is in
[.kb/wicket-cf-api-findings.md](../.kb/wicket-cf-api-findings.md#the-deployment-of-several-instances).

## Sizing and backup

- **an instance**: some MiB of memory idle, some 20 pushing a large zip: an upload, a package
  and a droplet go through files of `--data-dir`: with `--blobs` the zip while it is
  uploaded, then what staging makes of it (the component, a site's or the source's files as
  a tar), one at a time. On a tmpfs (a VM without a disk) those are memory, and `/tmp` is
  half the VM's: `memory-mib` is above twice the larger of the two
- **the store**: a row per entity, a few hundred bytes each, an app's 2 kB; 1000 apps with
  their routes, packages, builds and droplets are 5000 rows. An instance holds 4 or 5
  connections
- **the bucket**: the newest package of each app, its current droplet and the one before, and
  what was dropped in the last 35 minutes ([The blobs in a bucket](#the-blobs-in-a-bucket))
- **a backup** is the store's, then the bucket's: `pg_dump` (or the server's own), then a copy
  of the bucket made within the 30 minutes after it, which then holds every blob the dump
  names (a blob goes 30 minutes after the state named it last, no sooner). Restored, the wickets are
  brought to it as at any start, an app it does not know left serving and logged; a turn's
  epoch is above every one before, whatever the backup's age. No disk of an instance is part
  of it

