# rontolisp's Shiori on a wasi wicket through the cf-api

A full web app with a database of its own, deployed with `cf push`: [Shiori](https://github.com/making/rontolisp/tree/develop/examples/db/shiori),
a reading journal (cl-postgres, cl-mustache, tiny-routes, htmx 4) compiled by rontolisp into one wasi:http 0.3
component of 6.1MB, talking to PostgreSQL over wasi:sockets and reading where it is from `DATABASE_URL`.

Measured 2026-11-09: an Apple silicon Mac, OrbStack, the cf-api README's [Docker Compose](../sluice-wicket-cf-api/README.md#docker-compose)
(cf-api `8f73939`, wicket `6de06c4`), PostgreSQL 18.6, rontolisp 0.1.0-SNAPSHOT (`6686836`, the native
executable), the OSS `cf` 8.19.0.

## Steps

1. The README's four files, with PostgreSQL added to `compose.yaml` (its volume under `volumes:` too) and its
   password to `.env`. It is reached on the Compose network alone, by its service name:

   ```yaml
     postgres:
       image: postgres:18-alpine
       pull_policy: always
       environment:
         POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD}
         POSTGRES_INITDB_ARGS: -c password_encryption=md5
       volumes:
         - postgres:/var/lib/postgresql
       restart: unless-stopped
   ```

   ```text
   POSTGRES_PASSWORD=5f0c1d2e3a4b5c6d7e8f90a1b2c3d4e5     # openssl rand -hex 16
   ```

   - md5, not the default SCRAM-SHA-256: Shiori opens a connection per request, and a SCRAM login costs the
     guest 0.7s (`authentication=698.039 ms` in the server's log; 1.18s a page against 0.47s). PostgreSQL 18
     deprecates md5 and warns once at initdb. It goes in `POSTGRES_INITDB_ARGS`: initdb stores the password,
     and the image's entrypoint takes the `host` rule's method from the setting (`host all all all md5`);
     `command: [postgres, -c, password_encryption=md5]` makes the rule md5 but leaves the password a SCRAM
     one, which the md5 rule then logs in with
   - the image from 18 keeps its data in `/var/lib/postgresql/18/docker`: the volume is `/var/lib/postgresql`

   ```text
   docker compose up -d
   cf login -a http://127.0.0.1:9091 --sso      # admin@example.com / secret at Dex
   ```

2. The component, in a directory of its own (`cf push` takes a directory or a zip), from a rontolisp checkout:

   ```text
   rontolisp examples/db/shiori/shiori.lisp -o shiori/shiori.wasm --component --optimize   # 7s
   ```

3. `shiori/manifest.yml`, the services it names, the push:

   ```yaml
   applications:
   - name: shiori
     instances: 2
     memory: 256M
     metadata:
       annotations:
         wicket.sluice.ik.am/budget-ms: "60000"   # the first start: see below
     services:
     - shiori-network
     - shiori-dns
   ```

   ```text
   cf create-service wasi inherit-network shiori-network
   cf create-service wasi allow-ip-name-lookup shiori-dns
   cd shiori && cf push --no-start
   cf set-env shiori DATABASE_URL "postgresql://postgres:<POSTGRES_PASSWORD>@postgres:5432/postgres"
   cf start shiori
   open http://shiori.localhost:28000/
   ```

   `DATABASE_URL` is set apart from the manifest so the password is in no file pushed; the app reads that
   variable alone, not `VCAP_SERVICES`, so a user-provided service does not reach it.

The first request took 15.6s (the schema and eighteen sample books), the other instance's first 1.8s (the
schema statements again), then 0.49s a page (see [Latency](#latency)). On the README's
[Run](../sluice-wicket-cf-api/README.md#run) (the binaries on the macOS host, PostgreSQL 17 on trust) the
forms, the same-origin check (`403` to a cross-site POST), htmx's in-place updates in a browser (the CDN's
htmx under the app's CSP, no console error), `cf scale -i 3`, `cf logs` and a rolling `cf push` under load
(400 requests, all `200`) worked, a page in about 0.1s.

## Capabilities

| bound | without it |
|---|---|
| `inherit-network` | `tcp-connect: cannot connect to ...`: every address is denied. It implies `tcp`, which adds nothing |
| `allow-ip-name-lookup` | the same for a host by name (`postgres`); an IP literal and `localhost`, which rontolisp parses itself, need none |

`inherit-network` allows every address, the wicket's console and the cf-api's included: on a wasi wicket the
security groups have no effect.

## The first start

Shiori's top level creates the schema and the sample data, and a component's top level runs in the first
request of each instance, under that request's `budget-ms` (10s by default). On an empty database the
default budget is overrun: the store is stopped, the transaction rolled back, and the next request starts
over, so it never finishes (`504` after 10s, every time), while `cf app` shows the instances running. The
annotation above lets it finish; it applies at the next start (`cf restart`), not at once.

## Latency

| | `/books` (6 statements) | first start |
|---|---|---|
| SCRAM-SHA-256 | 1.18s | 17.9s |
| md5 | 0.47s | 15.6s |
| md5, delayed ACK off on PostgreSQL's side (`ip route change ... quickack 1` in its network namespace) | 0.025s | 0.24s |
| a static file (`/favicon.svg`) | 0.0015s | |

The guest's TCP socket has no `TCP_NODELAY` (wasmtime-wasi sets none; wasi:sockets has no option for it), and
cl-postgres writes a statement's messages in more than one write: each waits on PostgreSQL's delayed ACK,
84ms from a `parse` to its `bind` and 43ms to the next `parse` in the server's statement log.

## When the database is not there

A wrong `DATABASE_URL` fails the top level: every request, the static files included, answers `500` with
`wasm handler failed: wasm trap: ...` and the backtrace as its body; `cf logs` has `Unhandled condition:
Database error: tcp-connect: cannot connect to ...`, and `cf app` shows the instances running.
