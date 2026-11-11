Difficulty: Medium

# sluice-wicket-cf-api: the packages and droplets in a store several instances share

Needs 275. Part of 275-284.

## What holds it to one instance

- `packages/<guid>.zip`, `droplets/<sha256>.wasm|.tar` are files of `--data-dir`
  (`Store::package_path`, `droplet_path`, `files_path`): an upload reaches one instance, the
  build, a start or a move of an index (`Wicket::upload` of the file) may run on another
- `make_by_rename` / `make_digested` / `keep_files` are renames within the directory; the spool
  (`Store::spool`) is in it too
- `Store::delete(&[Blob])` removes at once what the state no longer names: another instance may
  be uploading that droplet to a wicket, or have just made the same digest again
- `Store::open` removes the unfinished files of every instance (275)
- the README's sizing (`disk-mib`: a zip's droplet kept for each app) is of the one disk

## Design (to confirm)

A blob store interface: the directory (one instance) and an S3-compatible bucket
(`--blobs s3://...`), by the same keys.

- an upload is spooled on the instance's own scratch (not the shared store), then put under its
  key: a package by guid, a droplet and a site's files by digest (an object is complete or
  absent: a multipart upload completed, no rename)
- reads are streams: staging unpacks a package from the store (a local copy while it does), a
  put to a wicket streams the droplet (`Wicket::upload`, `GET /api/layers` pieces); memory stays
  below a zip's size (`.kb/wicket-cf-api-findings.md`: 128 MiB took 150 MiB)
- deletion after the state: a blob the state no longer names is removed by the leader (278)
  once older than a grace, not by the handler that dropped it; a digest made again meanwhile
  stays. The sweep of unfinished uploads likewise, by age
- credentials and endpoint by env, the bucket's TLS; the image's `egress` names it

## Tests

The blob tests against both backends (an S3 mock in process or MinIO as a child process); a
package uploaded through one `Store` staged through another; a droplet dropped while it is read
is still read.
