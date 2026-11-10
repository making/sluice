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

## The demo's GIF

2.5MB ([demo/README.md](../sluice-wicket-cf-api/demo/README.md)).
