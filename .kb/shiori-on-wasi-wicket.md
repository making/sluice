# rontolisp's Shiori on a wasi wicket through the cf-api

A full web app with a database of its own, deployed with `cf push`: [Shiori](https://github.com/making/rontolisp/tree/develop/examples/db/shiori),
a reading journal (cl-postgres, cl-mustache, tiny-routes, htmx 4) compiled by rontolisp into one wasi:http 0.3
component of 6.1MB, talking to PostgreSQL over wasi:sockets and reading where it is from `VCAP_SERVICES`: the
credentials' `uri` of a service bound to it and tagged `postgresql`, a user-provided one here, a
[service broker's](#a-service-brokers-postgresql-in-its-place) later (else `DATABASE_URL`).

Measured 2026-11-09: an Apple silicon Mac, OrbStack, the cf-api README's [Docker Compose](../sluice-wicket-cf-api/README.md#docker-compose)
(cf-api `8f73939`, wicket `6de06c4`), PostgreSQL 18.6, rontolisp 0.1.0-SNAPSHOT (`6686836`, the native
executable; Shiori from `04cea9c`, which reads `VCAP_SERVICES`), the OSS `cf` 8.19.0. The steps, the
broker's instance, [The first start](#the-first-start) and [Latency](#latency) again with rontolisp
`bdcb55eb9` (the jar built from source) and cf-api `126ed35`.

## Steps

1. The files of the [appendix](#appendix-the-files), in a directory of their own:
   the README's [Docker Compose](../sluice-wicket-cf-api/README.md#docker-compose) with PostgreSQL added, reached
   on the Compose network alone by its service name, without Redis (the keyvalue store is the default
   `local`; Shiori uses none), the wicket's and Dex's configuration inlined (`configs:`).

   - the default SCRAM-SHA-256: Shiori opens a connection per request, and a login costs the guest about
     55ms (see [Latency](#latency))
   - the image from 18 keeps its data in `/var/lib/postgresql/18/docker`: the volume is `/var/lib/postgresql`

   ```text
   docker compose up -d
   cf login -a http://127.0.0.1:9091 --sso      # admin@example.com / secret at Dex
   ```

2. The component, beside the manifest in `shiori/` (`cf push` takes a directory or a zip), from a rontolisp
   checkout at `bdcb55eb9` or later:

   ```text
   rontolisp examples/db/shiori/shiori.lisp -o <the directory>/shiori/shiori.wasm --component --optimize   # 7s
   ```

   The snapshot's native executables of 2026-11-09 00:38 UTC predate it, and a SCRAM login there costs the
   guest 0.7s: until they are rebuilt, `./mvnw -DskipTests package` and `java -jar
   target/rontolisp-0.1.0-SNAPSHOT-exec.jar` in place of `rontolisp` (11s).

3. The services `shiori/manifest.yml` names, the push:

   ```text
   cf create-service wasi inherit-network shiori-network
   cf create-service wasi allow-ip-name-lookup shiori-dns
   cf create-user-provided-service shiori-db -t postgresql \
     -p '{"uri":"postgres://postgres:5f0c1d2e3a4b5c6d7e8f90a1b2c3d4e5@postgres:5432/postgres"}'
   cd shiori && cf push
   open http://shiori.localhost:28000/
   ```

   The `uri`'s password is `.env`'s `POSTGRES_PASSWORD`: change both together. `cf env shiori` shows it
   under `user-provided`, tagged `postgresql`.

The first request took 0.32s (the schema and eighteen sample books), the other instance's first 0.28s (the
schema statements again), then 0.077s a page (see [Latency](#latency)). On the README's
[Run](../sluice-wicket-cf-api/README.md#run) (the binaries on the macOS host, PostgreSQL 17 on trust) the
forms, the same-origin check (`403` to a cross-site POST), htmx's in-place updates in a browser (the CDN's
htmx under the app's CSP, no console error), `cf scale -i 3`, `cf logs` and a rolling `cf push` under load
(400 requests, all `200`) worked, a page in about 0.1s.

## A service broker's PostgreSQL in its place

The app looks for the tag, not the offering's name: a broker's instance bound under the same name takes the
user-provided one's place with no change to the app or its manifest, once its offering (or plan) is tagged
`postgresql` and its bindings' credentials hold a `uri`. The appendix's `broker.py` is one to try it with: a
stand-in, synchronous, of an offering `postgresql-db` whose every instance is the same database, started under
the Compose profile `broker`. Registering a broker takes the cf-api's `--service-broker-editor` (the
appendix's `compose.yaml` names admin@example.com):

```text
docker compose --profile broker up -d broker
cf create-service-broker db-broker broker <BROKER_PASSWORD> http://broker:8080   # .env's
cf enable-service-access postgresql-db
cf unbind-service shiori shiori-db && cf delete-service shiori-db -f
cf create-service postgresql-db small shiori-db
cf bind-service shiori shiori-db
cf restart shiori              # a binding applies at the next start
```

Between the unbind and the restart the app answers `500` (no database). After it, `VCAP_SERVICES` holds the
binding under `postgresql-db`, tagged `postgresql` and `relational`, and Shiori serves from it. `docker
compose --profile broker down -v` removes the broker too: without the profile, its container and the
network are left.

## Capabilities

| bound | without it |
|---|---|
| `inherit-network` | `tcp-connect: cannot connect to ...`: every address is denied. It implies `tcp`, which adds nothing |
| `allow-ip-name-lookup` | the same for a host by name (`postgres`); an IP literal and `localhost`, which rontolisp parses itself, need none |

`inherit-network` allows every address, the wicket's console and the cf-api's included: on a wasi wicket the
security groups have no effect.

## The first start

Shiori's top level creates the schema and the sample data, and a component's top level runs in the first
request of each instance, under that request's `budget-ms` (10s by default). Shiori's takes 0.32s on an
empty database. A top level overrunning the budget never finishes: the store is stopped, the transaction
rolled back, and the next request starts over (`504` after 10s, every time), while `cf app` shows the
instances running. Shiori's did with rontolisp `6686836` (15.6s, see [Latency](#latency)); the
annotation `wicket.sluice.ik.am/budget-ms` in the manifest's `metadata.annotations` raises the budget, at
the next start (`cf restart`), not at once.

## Latency

| | `/books` (6 statements) | first start |
|---|---|---|
| SCRAM-SHA-256 | 0.077s | 0.32s |
| md5 (`POSTGRES_INITDB_ARGS: -c password_encryption=md5`) | 0.024s | 0.15s |
| a static file (`/favicon.svg`) | 0.0008s | |
| SCRAM-SHA-256, rontolisp `6686836` | 1.18s | 17.9s |
| md5, rontolisp `6686836` | 0.47s | 15.6s |

A SCRAM login costs the guest about 55ms (`authentication=` in the server's `log_connections` log; 0.7s
with `6686836`). md5 saves it, but PostgreSQL 18 deprecates md5. Set it in `POSTGRES_INITDB_ARGS`: initdb
stores the password, and the image's entrypoint takes the `host` rule's method from the setting;
`command: [postgres, -c, password_encryption=md5]` makes the rule md5 but leaves the password a SCRAM one.

The guest's TCP socket has no `TCP_NODELAY` (wasmtime-wasi sets none; wasi:sockets has no option for it).
With rontolisp `6686836` cl-postgres wrote a statement's messages in more than one write, each waiting on
PostgreSQL's delayed ACK: 84ms from a `parse` to its `bind` and 43ms to the next `parse` in the server's
statement log, most of its rows above. rontolisp buffers a connection's writes since `442dca5f2`.

## When the database is not there

A wrong `DATABASE_URL` fails the top level: every request, the static files included, answers `500` with
`wasm handler failed: wasm trap: ...` and the backtrace as its body; `cf logs` has `Unhandled condition:
Database error: tcp-connect: cannot connect to ...`, and `cf app` shows the instances running.

## Appendix: the files

Run as they are on 2026-11-09 (Docker Compose v5.5.0), from `docker compose up -d` to Shiori's first page and
on to the broker's instance in place of the user-provided one. The wicket's and Dex's configuration are in
`compose.yaml` (`configs:`); `shiori.wasm` is step 2's.

```text
compose.yaml
.env
broker.py
shiori/
  manifest.yml
  shiori.wasm
```

`compose.yaml`:

```yaml
# compose.yaml: Shiori's stack, the wicket's and Dex's configuration inlined; .env holds the secrets
services:
  postgres:
    image: postgres:18-alpine
    pull_policy: always
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD}
    volumes:
      - postgres:/var/lib/postgresql
    restart: unless-stopped
  flume:                            # the apps' output and requests in cf logs
    image: ghcr.io/making/sluice/sluice-flume
    pull_policy: always
    environment:
      SLUICE_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
    restart: unless-stopped
  dex:                              # the OpenID Provider cf login --sso goes through
    image: ghcr.io/dexidp/dex:v2.46.1
    pull_policy: always
    command: [dex, serve, /etc/dex/dex.yaml]
    configs:
      - source: dex
        target: /etc/dex/dex.yaml
    networks:
      default:
        aliases: [dex.localhost]    # the issuer's host, as the browser reaches it on the loopback
    ports:
      - "127.0.0.1:5556:5556"
    restart: unless-stopped
  wicket:
    image: ghcr.io/making/sluice/sluice-wicket-runtime-wasi
    pull_policy: always
    command: [--config, /etc/sluice/wicket.yaml]
    configs:
      - source: wicket
        target: /etc/sluice/wicket.yaml
    ports:
      - "28000:28000"               # the apps
      - "127.0.0.1:9090:9090"       # the console, from this host only
    volumes:
      - wicket:/data
    depends_on: [dex]
    restart: unless-stopped
  broker:                           # a stand-in service broker: docker compose --profile broker up -d
    image: python:3-alpine
    pull_policy: always
    profiles: [broker]
    command: [python, -I, /broker.py]
    environment:
      BROKER_USERNAME: broker
      BROKER_PASSWORD: ${BROKER_PASSWORD:?BROKER_PASSWORD}
      DATABASE_URI: postgres://postgres:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD}@postgres:5432/postgres
    volumes:
      - ./broker.py:/broker.py:ro
    restart: unless-stopped
  cf-api:
    image: ghcr.io/making/sluice/sluice-wicket-cf-api
    pull_policy: always
    command: [--listen, 0.0.0.0:9091, --wicket, http://wicket:9090, --flume, grpc://flume:8002, --oidc-issuer, http://dex.localhost:5556/dex]
    environment:
      SLUICE_WICKET_CF_API_WICKET_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      SLUICE_WICKET_CF_API_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      SLUICE_WICKET_CF_API_DOMAIN: ${APPS_DOMAIN:-localhost}
      SLUICE_WICKET_CF_API_OIDC_CLIENT_ID: sluice
      SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      SLUICE_WICKET_CF_API_OIDC_CLAIM: email=admin@example.com    # who may log in
      SLUICE_WICKET_CF_API_ADMIN: email=admin@example.com    # who is an admin
      SLUICE_WICKET_CF_API_SERVICE_BROKER_EDITOR: email=admin@example.com    # who registers service brokers
    ports:
      - "127.0.0.1:9091:9091"       # the API, from this host only
    volumes:
      - cf-api:/data
    depends_on: [wicket, dex]
    restart: unless-stopped
configs:
  wicket:
    content: |
      listen: 0.0.0.0:28000
      console: 0.0.0.0:9090
      console-token: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      console-dir: /data/deployed
      oidc-issuer: http://dex.localhost:5556/dex
      oidc-client-id: sluice
      oidc-client-secret: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      oidc-claim: email=admin@example.com
      flume: grpc://flume:8002
      flume-token: ${FLUME_TOKEN:?FLUME_TOKEN}
  dex:
    content: |
      issuer: http://dex.localhost:5556/dex
      storage:
        type: memory
      web:
        http: 0.0.0.0:5556
      oauth2:
        skipApprovalScreen: true
      staticClients:
        - id: sluice
          name: sluice
          secret: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
          redirectURIs:
            - http://127.0.0.1:9091/uaa/callback
            - http://127.0.0.1:9090/oauth2/callback
      enablePasswordDB: true
      staticPasswords:
        - email: admin@example.com     # password "secret"
          username: admin
          preferredUsername: admin
          userID: 1c2a0b3e-0000-4000-8000-000000000001
          hash: "${ADMIN_PASSWORD_HASH:?ADMIN_PASSWORD_HASH}"
volumes:
  postgres:
  wicket:
  cf-api:
```

`.env`:

```text
# .env, next to compose.yaml: these values work as they are; change the secrets (openssl rand
# -hex 16 each) and the password before anyone else can reach the host
POSTGRES_PASSWORD=5f0c1d2e3a4b5c6d7e8f90a1b2c3d4e5
FLUME_TOKEN=d6edd1ade98c42dd49b77a7ba4906881
CONSOLE_TOKEN=92190bcaf0277ff738ee4162d89c9eeb
BROKER_PASSWORD=4e8a1c7f2b9d3e6a0c5f8b1d7e2a9c4f
OIDC_CLIENT_SECRET=13915f086b88d15379e27e6c3aa4a59f
# admin@example.com's password, bcrypt: htpasswd -bnBC 10 "" <password> | tr -d ':\n'
# (single-quoted: $ is Compose's otherwise); this one is "secret"
ADMIN_PASSWORD_HASH='$2y$10$cYEVeVIzJX6QWDQcjRUaNe1RHrXamph8BdMRzlR/LS7msAe83oJOi'
# a name whose wildcard DNS reaches the host (default localhost)
# APPS_DOMAIN=apps.example.com
```

`broker.py`:

```python
# broker.py, next to compose.yaml: a stand-in Open Service Broker (API 2.17), synchronous, of one
# offering tagged postgresql whose every instance is the same database: a binding's credentials are
# DATABASE_URI. Enough to swap a user-provided service for a broker's; it provisions nothing.
import base64
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

URI = os.environ["DATABASE_URI"]
AUTH = "Basic " + base64.b64encode(
    f"{os.environ['BROKER_USERNAME']}:{os.environ['BROKER_PASSWORD']}".encode()).decode()
CATALOG = {"services": [{
    "id": "7d3e5c1a-0000-4000-8000-000000000001",
    "name": "postgresql-db",
    "description": "a PostgreSQL database",
    "bindable": True,
    "tags": ["postgresql", "relational"],
    "plans": [{"id": "7d3e5c1a-0000-4000-8000-000000000002", "name": "small",
               "description": "the shared database", "free": True}],
}]}


class Broker(BaseHTTPRequestHandler):
    def answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def serve(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.headers.get("Authorization") != AUTH:
            return self.answer(401, {})
        path = self.path.split("?")[0]
        if self.command == "GET" and path == "/v2/catalog":
            return self.answer(200, CATALOG)
        if not path.startswith("/v2/service_instances/"):
            return self.answer(404, {})
        if self.command == "PUT" and "/service_bindings/" in path:
            return self.answer(201, {"credentials": {"uri": URI}})
        if self.command in ("PUT", "PATCH"):
            return self.answer(201 if self.command == "PUT" else 200, {})
        if self.command == "DELETE":
            return self.answer(200, {})
        return self.answer(404, {})

    do_GET = do_PUT = do_PATCH = do_DELETE = serve


ThreadingHTTPServer(("0.0.0.0", 8080), Broker).serve_forever()
```

`shiori/manifest.yml`:

```yaml
applications:
- name: shiori
  instances: 2
  memory: 256M
  services:
  - shiori-network
  - shiori-dns
  - shiori-db
```
