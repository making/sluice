# sluice-wicket-cf-api: findings

## As an app of a Firecracker wicket

The cf-api in a VM of the wicket, on its disk (its README,
[Operating it](../sluice-wicket-cf-api/README.md#operating-it)):

- `mem`: an upload, a package and a droplet go through files, not memory: 128 MiB took a push of a
  150 MiB zip
- an upgrade: the new one boots on the disk: 0.7-0.8s of no API. A new image that does not start is
  refused (422, 11.5s)

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
