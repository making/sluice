# sluice-wicket-cf-api: findings

## As an app of a Firecracker wicket

The cf-api in a VM of the wicket, on its disk (its README,
[Operating it](../sluice-wicket-cf-api/README.md#operating-it)):

- `mem`: an upload, a package and a droplet go through files, not memory: 128 MiB took a push of a
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

## The demo's GIF

2.5MB ([demo/README.md](../sluice-wicket-cf-api/demo/README.md)).
