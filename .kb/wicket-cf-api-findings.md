# sluice-wicket-cf-api: findings

## As an app of a Firecracker wicket

The cf-api in a VM of the wicket, on its disk (its README,
[Operating it](../sluice-wicket-cf-api/README.md#operating-it)):

- `mem`: an upload, a package and a droplet go through files, not memory: 128 MiB took a push of a
  150 MiB zip
- an upgrade: the new one boots on the disk: 0.7-0.8s of no API. A new image that does not start is
  refused (422, 11.5s)

## The demo's GIF

2.5MB ([demo/README.md](../sluice-wicket-cf-api/demo/README.md)).
