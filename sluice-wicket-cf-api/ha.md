# sluice-wicket-cf-api: several instances

What several instances of the [cf-api](README.md) share, in place of the files of one
instance's `--data-dir`: the state in PostgreSQL (`--state`), the packages and the droplets in
an S3 bucket (`--blobs`). The logins and the reconciling of the wickets are one process's still:
run one instance on them. The options are in the README's [Options](README.md#options).

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
