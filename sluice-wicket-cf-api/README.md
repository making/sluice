# sluice-wicket-cf-api

A Cloud Foundry API facade for sluice wickets: the subset of the Cloud Controller v3 API, the
UAA token endpoints and log-cache's read API the stock `cf` CLI uses, on one origin. It keeps the
desired state of its apps and has its wickets serve them through each wicket's app API, an app's
instances spread over them ([Several wickets](#several-wickets)), so routes, scaling, rolling
updates, stop and start and `cf logs` work as the CLI expects. Users log in through an OpenID
Provider (`cf login --sso`; see [Login](#login)), or, with `--any-credentials`, on the loopback
with any credentials.

What `cf push` deploys depends on the wicket, whose listing names its runtime:

| | [On a wasi wicket](#on-a-wasi-wicket) | [On a Firecracker wicket](#on-a-firecracker-wicket) |
|---|---|---|
| the wicket | [sluice-wicket-runtime-wasi](../sluice-wicket-runtime-wasi/README.md#apps) | [sluice-wicket-runtime-firecracker](../sluice-wicket-runtime-firecracker/README.md#apps) |
| an app | a wasm component: its bits, or an image in an OCI registry; or a [static site](#static-sites): html, css, js | a container image, a CNB image say, or a rootfs in a zip: microVMs |
| the cf-api | a process or a container next to the wicket | an app of the wicket, in a VM with a disk, or next to it |
| services | the wicket's keyvalue stores and capabilities; [user-provided](#user-provided-services); a [service broker](#service-brokers)'s | [user-provided](#user-provided-services); a [service broker](#service-brokers)'s |
| `cf logs` | the app's output and requests | the process's output |

The rest holds for both: [Several wickets](#several-wickets), [Options](#options),
[Mapping](#mapping), [Behavior](#behavior), [Login](#login), [Limits](#limits), [Tests](#tests).

```text
cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> wickets <--tunnel-- sluice server
                                             ^                            |
                                             +--Watch-- sluice-flume <--Feed  (cf logs, optional)
```

The `cf` here is the OSS CLI ([cloudfoundry/cli](https://github.com/cloudfoundry/cli) v8, whose
archive holds it as `cf` and `cf8`); the Tanzu distribution (`cf` 10.x) reads `/v3/info` and
refuses any other platform.

## On a wasi wicket

A wasm component deployed with `cf push`, from its bits or an image in an OCI registry
(`--docker-image`), with the wicket's keyvalue stores and capabilities as
[services](#services); or a directory of html, css and js, a [static site](#static-sites).

[![cf login --sso through Dex, cf push, routes, scale, environment variables, a keyvalue service and cf logs against a wicket in local mode](demo/sluice-cf-demo.gif)](demo/sluice-cf-demo.mp4)

The GIF links to the [MP4](demo/sluice-cf-demo.mp4); both are recorded by `demo/record.sh`
([demo/README.md](demo/README.md)).

### Run

```text
cargo build --release                                # needs protoc, and git (the build embeds the commit)
# it embeds sluice-wicket-site, the guest of static sites, built on the toolchain it pins
# (rustup installs it), or the one SLUICE_WICKET_SITE_WASM names (as the Docker build)

# optional, for the apps' output and requests in cf logs: a flume (see sluice-flume)
sluice-flume --token FLUME_SECRET                     # grpc://0.0.0.0:8002

# the wicket, serving on 8080 with its console on 9090 (see its README)
sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

# the cf-api, on http://127.0.0.1:9091
target/release/sluice-wicket-cf-api --wicket http://127.0.0.1:9090 --wicket-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET --any-credentials

cf login -a http://127.0.0.1:9091 -u me -p x   # any credentials: --any-credentials (see Login)
cf push hello -p hello/                        # hello/ holds hello.wasm (a wasi:http 0.3 component)
curl http://hello.localhost:8080/              # the wicket's --listen; *.localhost is the loopback
cf push site -p public/                        # public/ holds index.html: a static site
curl http://site.localhost:8080/
# an image (see Images): the wicket's hello example, as its CI publishes it
cf push img --docker-image ghcr.io/making/sluice/sluice-wicket-runtime-wasi/examples/hello
cf map-route hello localhost --hostname hi
cf scale hello -i 2
cf push hello -p hello/ --strategy rolling     # a wicket at a time, swapped in place
cf logs hello --recent
cf marketplace                                 # the wicket's keyvalue stores and capabilities
cf create-service keyvalue local kv            # a namespace of the store local
cf push counter -p counter/ --no-start         # counter/ holds counter.wasm, a wasi:keyvalue component
cf bind-service counter kv
cf start counter                               # a binding applies at the next start
```

The options are in [Options](#options).

### Docker Compose

The images on one host, with Redis holding the apps' keyvalue data and [Dex](https://dexidp.io/)
as the OpenID Provider of the cf-api and of the wicket's console alike, its users in its
configuration file. The apps are published on 28000 (8080 and its usual stand-ins are often taken), the
cf-api (9091), the console (9090) and Dex (5556) on the host's loopback: `cf` and the browser run on that host (Docker Engine on
Linux, Docker Desktop or OrbStack on macOS) or through an SSH tunnel
(`ssh -L 9091:127.0.0.1:9091 -L 9090:127.0.0.1:9090 -L 5556:127.0.0.1:5556 -L 28000:127.0.0.1:28000 <host>`). Dex is
`dex.localhost` for everyone: the loopback to the browser (`*.localhost`), the service's alias
on the Compose network to the cf-api and the wicket, so the ID tokens' issuer is one URL.

Four files in one directory, as below: `compose.yaml`, `wicket.yaml`, `dex.yaml` and `.env`.

```yaml
# compose.yaml; .env holds the secrets and APPS_DOMAIN
services:
  redis:
    image: redis:7-alpine
    pull_policy: always
    command: [redis-server, --requirepass, "${REDIS_PASSWORD:?REDIS_PASSWORD}", --appendonly, "yes"]
    volumes:
      - redis:/data
    restart: unless-stopped
  flume:
    image: ghcr.io/making/sluice/sluice-flume
    pull_policy: always
    environment:
      SLUICE_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
    restart: unless-stopped
  dex:
    image: ghcr.io/dexidp/dex:v2.46.1
    pull_policy: always
    command: [dex, serve, /etc/dex/dex.yaml]
    environment:                    # what dex.yaml's secretEnv and hashFromEnv read
      OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      ADMIN_PASSWORD_HASH: ${ADMIN_PASSWORD_HASH:?ADMIN_PASSWORD_HASH}
    networks:
      default:
        aliases: [dex.localhost]    # the issuer's host, as the browser reaches it on the loopback
    ports:
      - "127.0.0.1:5556:5556"
    volumes:
      - ./dex.yaml:/etc/dex/dex.yaml:ro
    restart: unless-stopped
  wicket:
    image: ghcr.io/making/sluice/sluice-wicket-runtime-wasi
    pull_policy: always
    command: [--config, /etc/sluice/wicket.yaml]
    environment:                    # what wicket.yaml's ${...} take
      CONSOLE_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      REDIS_PASSWORD: ${REDIS_PASSWORD:?REDIS_PASSWORD}
      OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
    ports:
      - "28000:28000"               # the apps: --listen, on the same port (the console links it)
      - "127.0.0.1:9090:9090"       # the console, from this host only
    volumes:
      - ./wicket.yaml:/etc/sluice/wicket.yaml:ro
      - wicket:/data    # the apps and their components (deployed/)
    depends_on: [redis, dex]
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
    ports:
      - "127.0.0.1:9091:9091"       # the API, from this host only
    volumes:
      - cf-api:/data    # the desired state, packages and droplets (cf-api/)
    depends_on: [wicket, dex]
    restart: unless-stopped
volumes:
  redis:
  wicket:
  cf-api:
```

```yaml
# wicket.yaml, next to compose.yaml (see the wicket's Configuration file); ${NAME} is the
# wicket's environment
listen: 0.0.0.0:28000           # the apps; with server and token also through a sluice server
console: 0.0.0.0:9090           # published on the host's loopback only
console-token: ${CONSOLE_TOKEN}  # the cf-api's
console-dir: /data/deployed     # in the volume, not next to this file
oidc-issuer: http://dex.localhost:5556/dex  # the browser signs in through Dex
oidc-client-id: sluice
oidc-client-secret: ${OIDC_CLIENT_SECRET}
oidc-claim: email=admin@example.com       # who may sign in
flume: grpc://flume:8002
flume-token: ${FLUME_TOKEN}
keyvalue:
  stores:
    redis:                      # the plan of cf create-service keyvalue
      type: redis
      url: redis://:${REDIS_PASSWORD}@redis:6379/0
```

```yaml
# dex.yaml, next to compose.yaml: the OpenID Provider (https://dexidp.io/docs/), its users
# in this file
issuer: http://dex.localhost:5556/dex
storage:
  type: memory
web:
  http: 0.0.0.0:5556
oauth2:
  skipApprovalScreen: true
staticClients:
  - id: sluice                  # the cf-api's and the console's oidc-client-id
    name: sluice
    secretEnv: OIDC_CLIENT_SECRET
    redirectURIs:
      - http://127.0.0.1:9091/uaa/callback      # the cf-api
      - http://127.0.0.1:9090/oauth2/callback   # the wicket's console
enablePasswordDB: true
staticPasswords:                # the users: Dex asks for the email and the password
  - email: admin@example.com
    username: admin
    preferredUsername: admin    # the cf user name
    userID: 1c2a0b3e-0000-4000-8000-000000000001
    hashFromEnv: ADMIN_PASSWORD_HASH
```

```text
# .env, next to compose.yaml: these values work as they are; change the secrets (openssl rand
# -hex 16 each) and the password before anyone else can reach the host
FLUME_TOKEN=d6edd1ade98c42dd49b77a7ba4906881
CONSOLE_TOKEN=92190bcaf0277ff738ee4162d89c9eeb
REDIS_PASSWORD=1b2ee329d0ec1fa3127d57206537d7a7
OIDC_CLIENT_SECRET=13915f086b88d15379e27e6c3aa4a59f
# admin@example.com's password, bcrypt: htpasswd -bnBC 10 "" <password> | tr -d ':\n'
# (single-quoted: $ is Compose's otherwise); this one is "secret"
ADMIN_PASSWORD_HASH='$2y$10$cYEVeVIzJX6QWDQcjRUaNe1RHrXamph8BdMRzlR/LS7msAe83oJOi'
# a name whose wildcard DNS reaches the host (default localhost)
# APPS_DOMAIN=apps.example.com
```

```text
docker compose up -d
cf login -a http://127.0.0.1:9091 --sso    # open the URL it prints, log in at Dex (admin@example.com / secret), paste the code
cf push hello -p hello/                    # hello/ holds hello.wasm, as in Run
# in a browser, http://127.0.0.1:9090/: the wicket's console, signed in through Dex too
curl http://hello.localhost:28000/         # with APPS_DOMAIN: http://hello.apps.example.com:28000/
cf create-service keyvalue redis kv        # the namespace cf/<instance guid> in Redis
cf push counter -p counter/ --no-start
cf bind-service counter kv
cf start counter
curl http://counter.localhost:28000/       # hits 1, 2, ... in Redis: cf/<instance guid>:hits
```

`docker compose down` keeps the volumes: the next `up` serves the apps again, with their data
(Redis keeps it in its append-only file) and the logins (`sessions.json` in the cf-api's). For
other users, add them to `staticPasswords` and to `SLUICE_WICKET_CF_API_OIDC_CLAIM` and
wicket.yaml's `oidc-claim` (`email=admin@example.com,email=dev@example.com`), or give them a
group and allow the group (`groups=devs`, with the scopes `openid,email,profile,groups`), and give
them [roles](#organizations-spaces-and-roles) (`cf set-space-role dev sluice wicket
SpaceDeveloper`, or a group's: `SLUICE_WICKET_CF_API_GROUP_ROLE:
devs=space_developer@sluice/wicket`); a Dex [connector](https://dexidp.io/docs/connectors/)
replaces the static users with GitHub, LDAP, another OpenID Provider and the like.

An app with a database of its own in the same Compose, PostgreSQL beside the wicket: rontolisp's Shiori, with
the capabilities it is bound, what its first start and a page take, in
[.kb/shiori-on-wasi-wicket.md](../.kb/shiori-on-wasi-wicket.md).

### Mapping to components

| CF | wicket |
|---|---|
| package (bits) | the uploaded zip: `cf push` takes a directory or a zip (`-p hello.wasm` fails in the CLI: `not a valid zip file`) |
| package (docker) | an image (`cf push --docker-image`, a manifest's `docker.image`) holding the component as one `application/wasm` layer, as `wkg oci push` and `oras push <ref> hello.wasm:application/wasm` push it (of an index, its `wasm` entry). `--docker-username` with `CF_DOCKER_PASSWORD` are its credentials |
| build | picks the component: the one the web process command names (`-c hello.wasm`, `command:`), else the package's only `.wasm`, else of a package with an `index.html` sluice-wicket-site ([a static site](#static-sites)); of an image, pulls it. It must be a component (`\0asm`, layer 1). Anything else fails the build, naming what it found |
| droplet | that component, by sha256 (`process_types.web` its path; stack `wasip3`, buildpack `wasm`, a static site's `site` with its files); of an image, the image by digest (`cf app`: `docker image:`), `process_types.web` the repository's last segment `.wasm` |
| web process `instances` | [the limit](#instances) N: the route's `instances`, each a wasm instance (a store) taking `concurrency` requests at once, its `warm` N by default. A request no warm instance has room for gets an instance of its own, up to the limit. 0 is [as on CF](#mapping): no app in the wicket. `cf logs` names the instance (`APP/PROC/WEB/<index>`, the requests' `app_index`) within `0` to `instances - 1`, what `cf app` lists: the wicket's index, a warm one's `0` to `warm - 1`, one of its own the lowest free from `warm` up, modulo `instances` |
| web process `memory` | `memory-mib`: what an instance holds, its requests' together |
| annotation `wicket.sluice.ik.am/budget-ms` | `budget-ms`: each request's |
| annotation `wicket.sluice.ik.am/concurrency` | `concurrency`: [the requests an instance takes at once](#instances), at once |
| annotation `wicket.sluice.ik.am/keep-ms` | `keep-ms`: [how long an instance beyond `warm` is kept](#instances) serving none, at once |
| annotation `wicket.sluice.ik.am/warm` | `warm`: [the instances kept up](#instances), at once |
| environment variables (`cf set-env`, a manifest's `env:`), `VCAP_APPLICATION`, `VCAP_SERVICES` | the sandbox's `env`: what the guest's `wasi:cli/environment` answers |
| disk | accepted and shown, no effect |
| security groups | accepted and shown, no effect: a component's network is the capabilities below |
| service offering `keyvalue`, plan `<store>` | a keyvalue store of the wicket (`keyvalue.stores`, else `local`) |
| service offering `wasi`, plan `<capability>` | a capability of the sandbox's `wasi`: `cli`, `inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp` |
| keyvalue service instance | the namespace `cf/<instance guid>` of its plan's store |
| binding to a keyvalue instance | the sandbox's `keyvalue` map: its identifier (parameter `identifier`, default `""`) -> `<store>/cf/<instance guid>`, and the capability `keyvalue` |
| binding to a wasi instance | the plan's capability; a network one `cli` too |
| user-provided service instance | its credentials in `VCAP_SERVICES`, nothing granted ([User-provided services](#user-provided-services)) |

### Static sites

A package of no component, nor a rootfs, with an `index.html` at its root is a static site:
`cf push site -p public/`. Its droplet is the site's component
([sluice-wicket-site](../sluice-wicket-site/README.md),
embedded in the cf-api at its build) and a tar of the package's files, which the wicket
preopens read-only at `/` for it ([the app's `files`](../sluice-wicket-runtime-wasi/README.md#apps)):

- the guest is a sandbox as any app's, of one capability: the files, read-only. It serves
  `GET` and `HEAD` alone, a directory's `index.html`, the site's `404.html` for what is not
  there, never a listing, a symbolic link or a dot file (but `/.well-known/`), with an `ETag`
  (a 304 to `If-None-Match`), `Cache-Control: no-cache` and `X-Content-Type-Options: nosniff`.
  A file streams from the wicket's disk: a site is not bounded by `cf push -m`
- staging leaves out the dot files and directories (but `.well-known/`) and the manifest
  at the root (`manifest.yml`, `manifest.yaml`; the CLI leaves out `.git` and its
  `.cfignore`'s too): a `.env` or a manifest's env never reaches the wicket. A symbolic link
  fails the build; 100000 files and 1GiB at most (the wicket's bounds of a tar)
- the same files make the same tar, however zipped: a push of an unchanged site uploads
  nothing new to the wicket; a file changed is served at the next start, as a component
  (`cf push`, `cf restage`, a rolling deployment)
- the droplet's buildpack is `site` (`cf app`); routes, scale, `cf logs` (its requests), env
  and rolling deployments are as an app's; services have nothing to bind to

### Images

Staging pulls the image itself: the wicket gets the component as from a zip and never reaches
the registry. A registry on the loopback is reached over plain HTTP, any other over HTTPS (the
webPKI roots; a registry's own CA is not taken); asked to authenticate, a pull gets a token from
the registry's realm, anonymous or with the credentials, or answers a basic challenge with them.
An image holds a wasm component, not a container: any other fails the build.

### Services

The wicket's keyvalue stores and capabilities are the plans of the service offerings
`keyvalue` and `wasi` of the broker `sluice-wicket`, which is no process: the cf-api offers
them as the wicket lists them (`GET /api/apps`), so `cf marketplace` follows the wicket's
config.

- `cf create-service keyvalue <store> <name>` makes a namespace, `cf/<instance guid>`, of the
  store; `cf bind-service <app> <name> [-c '{"identifier":"<id>"}']` lets the app open it at the
  identifier (default `""`, what `store::open("")` opens). An app binds one instance per
  identifier (a second is refused, 422). Renaming the instance or moving the app's routes leaves
  the data where it is
- `cf create-service wasi <capability> <name>` and a binding grant the capability; a network
  one (`inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp`) `cli` too
- as the rest of the sandbox, a binding (and an unbinding) applies at the app's next start,
  restart, restage or deployment. A manifest's `services:` (a name, or `name` with `parameters`
  and `binding_name`) binds what the app is not bound to yet, as `cf bind-service`
- `cf env` shows the bindings in `VCAP_SERVICES` (by offering; a keyvalue binding's
  credentials are `store`, `namespace` and `identifier`, a wasi one's `capabilities`)
- instance and binding operations complete at once. `cf delete-service` is refused while the
  instance is bound (as on CF); a keyvalue instance deleted has its data deleted once no app
  maps its namespace any more -- no started app's snapshot, no route of the wicket, a draining
  one included (`DELETE /api/keyvalue/<store>/<namespace>`, tried at the deletion and after each
  listing of the wicket; of [several wickets](#several-wickets), see there).
  `cf purge-service-instance` drops the bindings with the instance
- `cf rename-service` and `cf update-service -t` apply at once; a plan change, instance
  parameters, service keys and sharing are refused (422)

### User-provided services

On either wicket, `cf create-user-provided-service <name> -p '<json>' [-t <tags>]` keeps the
credentials, and `cf bind-service` gives them to the app in `VCAP_SERVICES` (`user-provided`,
as on CF), granting nothing else:

- as a binding, new credentials (`cf update-user-provided-service -p`, which replaces them)
  apply at the app's next start, restart, restage or deployment; `cf env` shows the current
  ones. Binding parameters (`-c`), a syslog drain (`-l`) and a route service (`-r`) are refused
  (422)
- the credentials are a secret, as an environment variable's value: in `state.json` (0600) and
  the guest's env, in no log and no listing of the wicket. 64 KiB of JSON at most; a start
  whose env would be more than the 128 KiB the wicket takes is refused, naming the size
- creating, updating, binding, unbinding and deleting answer at once (no job), as on CF

### Service brokers

On either wicket, any [Open Service Broker API](https://github.com/cloudfoundry/servicebroker/blob/v2.17/spec.md)
broker (2.17) the cf-api reaches offers its plans beside the wicket's:

```text
cf create-service-broker db-broker <user> <password> https://broker.example.com [--space-scoped]
cf enable-service-access db [-p <plan>] [-o sluice]
cf create-service db small db1 -c '{"size": 1}' [--wait]
cf bind-service hello db1 [-c '<json>'] [--wait]
cf restart hello                          # VCAP_SERVICES holds the broker's credentials
```

- **Who registers them.** A broker's URL is any the cf-api reaches, and its catalog decides what
  everyone sees: registering, updating, renaming and deleting brokers, `cf enable-service-access`
  / `disable-service-access` and `cf purge-service-offering` take `--service-broker-editor` (or
  `--service-broker-editor-client`), as the [security groups](#security-groups) do theirs; anyone
  logged in reads them. A global broker's plans are seen by no one until `cf
  enable-service-access` (`cf marketplace` lists none, `cf create-service` is refused); a
  `--space-scoped` one's are the space's at once
- **The catalog.** Fetched and checked as CC does at `cf create-service-broker` and `cf
  update-service-broker` (a problem refuses it, each named in the job's error). An offering or a
  plan dropped from it stays, deactivated and offering no new instance, while it has instances
  (a warning says so)
- **Operations.** Provision, update (`-p`, `-c`, `cf upgrade-service` for a plan's
  `maintenance_info`), deprovision, bind and unbind are sent with `accepts_incomplete=true`, the
  login's `Originating-Identity`, and the parameters as given. A broker answering `202` is polled
  (`last_operation`, as its `Retry-After` asks, else every 5s) until it succeeds, fails or outlives
  the plan's `maximum_polling_duration` (7 days without one): the job is `POLLING` meanwhile, so
  the CLI says `in progress` and returns unless `--wait`; `cf service` shows the last operation.
  One operation at a time per instance or binding (`409`). An operation under way outlives a
  restart of the cf-api, which sends it again or polls on. A provision or bind whose outcome is
  unknown (no answer in 60s, a `5xx`) is undone with a deprovision or unbind, as the
  specification asks
- **Bindings.** The credentials (an async one's fetched once it succeeds) are in `VCAP_SERVICES`
  under the offering's name, with the plan's and the offering's tags, from the app's next start,
  as any binding. A manifest's `services:` waits for its bindings, `cf delete` of an app for its
  unbinds. `cf service --params` and the binding's parameters are the broker's, when its offering
  says they are retrievable
- `cf delete-service` is refused while the instance is bound, `cf delete-service-broker` while
  the broker has instances; `cf purge-service-instance` drops an instance and its bindings
  without asking the broker. The broker's password is in no answer, the credentials in `cf env`
  alone; both are in `state.json` (0600) and in no log. Service keys and sharing are refused (422)

## On a Firecracker wicket

For a host with KVM that should run the wicket and little else: the cf-api is an app of the
[wicket](../sluice-wicket-runtime-firecracker/README.md#docker-image), in a VM of its own, its
state on a [disk](../sluice-wicket-runtime-firecracker/README.md#disks) that outlives the VM, and
`cf push` deploys container images as microVMs. The flume is what brings the apps' output to `cf
logs`: to do without it, leave out its container, `--network`, `--flume`, and its `egress` and
`env` below. The cf-api also runs next to the wicket, with `--wicket` at its console, as in
[Docker Compose](#docker-compose-1).

[![cf login --sso through Dex, cf push of a container image and a root filesystem as microVMs, logs, routes, a pool of VMs, environment variables and a restart of the wicket](demo/sluice-cf-firecracker-demo.gif)](demo/sluice-cf-firecracker-demo.mp4)

The GIF links to the [MP4](demo/sluice-cf-firecracker-demo.mp4); both are recorded on a Mac by
`demo/record-firecracker.sh` ([demo/README.md](demo/README.md#on-a-firecracker-wicket)).

### Run as an app of the wicket

The wicket and the flume, on the host's loopback (see **who reaches it** below):

```text
export TOKEN=$(openssl rand -hex 16) FLUME_TOKEN=$(openssl rand -hex 16)
docker network create sluice
docker run -d --name flume --network sluice --restart unless-stopped \
    -e SLUICE_FLUME_TOKEN=$FLUME_TOKEN ghcr.io/making/sluice/sluice-flume
docker run -d --name wicket --network sluice --restart unless-stopped --device /dev/kvm \
    --group-add $(stat -c %g /dev/kvm) --tmpfs /run/sluice --stop-timeout 40 -v wicket:/data \
    -p 127.0.0.1:8080:8080 -p 127.0.0.1:9090:9090 \
    -e SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE_TOKEN=$TOKEN \
    -e SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_TOKEN=$FLUME_TOKEN \
    ghcr.io/making/sluice/sluice-wicket-runtime-firecracker \
    --listen 0.0.0.0:8080 --console 0.0.0.0:9090 --flume grpc://flume:8002
```

Then the cf-api: its rootfs, made by the wicket of its image, and the app naming it. The wicket
serves on `127.0.0.1:9090` once `docker logs wicket` says `console on`:

```text
sha256=$(curl -sS -X PUT -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9090/api/images \
    -d '{"image": "ghcr.io/making/sluice/sluice-wicket-cf-api"}' | jq -r .sha256)
curl -sS -X PUT -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9090/api/apps/cf-api -d @- <<JSON
{
  "owner": "ops",
  "hosts": ["cf.localhost"],
  "component": {"sha256": "$sha256", "name": "sluice-wicket-cf-api.ext4"},
  "sandbox": {
    "memory-mib": 256,
    "disk": "/data",
    "disk-mib": 4096,
    "egress": ["localhost:9090", "flume:8002"],
    "env": {
      "SLUICE_WICKET_CF_API_ANY_CREDENTIALS": "true",
      "SLUICE_WICKET_CF_API_WICKET": "http://localhost:9090",
      "SLUICE_WICKET_CF_API_WICKET_TOKEN": "$TOKEN",
      "SLUICE_WICKET_CF_API_FLUME": "grpc://flume:8002",
      "SLUICE_WICKET_CF_API_FLUME_TOKEN": "$FLUME_TOKEN"
    }
  }
}
JSON
```

The CLI works as on any wicket, an app being a container image or a rootfs (see
[Mapping to VMs](#mapping-to-vms)):

```text
cf api http://cf.localhost:8080
cf login -u me -p x                              # any credentials: no provider yet

# any image whose process listens on $PORT: the port it exposes, else 8080. The wicket pulls it
# and makes its rootfs, read-only but for /tmp
cf push web --docker-image ghcr.io/traefik/whoami:v1.11.0
curl http://web.localhost:8080/
cf logs web --recent                             # the process's output, from the flume

# or a root filesystem of your own, an ext4 image in a zip: here of the small sample server the
# wicket's image holds
mkdir hello
docker cp wicket:/usr/local/share/sluice-wicket-runtime-firecracker/guest.ext4 hello/hello.ext4
cf push hello -p hello/
curl http://hello.localhost:8080/

cf set-env hello GREETING hi
cf restart hello                                 # the env applies at the next start
curl http://hello.localhost:8080/env             # the sample guest's answer: the env it was given
cf map-route hello localhost --hostname hi       # routes apply at once
curl http://hi.localhost:8080/
cf scale hello -i 2                              # one more VM, cloned from the app's snapshot
cf scale hello -m 256M -f                        # a VM's memory: stops and starts the app
cf push hello -p hello/ --strategy rolling
cf stop hello
cf start hello
cf delete hello -r -f
```

A private image takes `--docker-username me` with `CF_DOCKER_PASSWORD`. The apps and the cf-api's
state come back with the wicket, from the volume:

```text
docker restart wicket
sleep 5                                          # the VMs boot again
cf apps
curl http://web.localhost:8080/
```

### Operating it

- **who reaches it**: `..._ANY_CREDENTIALS` lets whoever reaches the cf-api log in and deploy, and
  that it listens on its VM's loopback protects nothing here: the wicket relays its route to whoever
  reaches `--listen`, or the sluice server with `--server`, saying `Host: cf.localhost`. Keep them on
  the host's loopback as above until it has a provider: `..._OIDC_ISSUER`, `..._OIDC_CLIENT_ID`,
  `..._OIDC_CLIENT_SECRET`, `..._OIDC_CLAIM`, `..._ADMIN` in `env` in place of `..._ANY_CREDENTIALS` (see
  [Login](#login)), the provider's `host:port` in `egress`. Then `-p 8080:8080`, `cf login --sso`.
  With neither it does not start: the PUT is refused (422), `docker logs wicket` saying why
- **jail mode**, for tenants on the wicket: in `docker run`, `-u 0 --cap-add SYS_ADMIN --security-opt
  apparmor=unconfined --security-opt seccomp=unconfined` for `--group-add`, and `--jail-ids
  900000-900127 --jail-user nonroot` (see the wicket's
  [Jail mode](../sluice-wicket-runtime-firecracker/README.md#jail-mode))
- **`owner`** is any but the cf-api's own (`cf`): what it manages is the apps of that one
- **`disk`** at `/data`, the image's working directory: `--data-dir`'s default is on it. Without a
  disk the rootfs is read-only and the cf-api does not start (the PUT is refused). Every write of
  its state is durable when it returns: a kill loses no edit it answered
- **`egress`** names what it reaches, and the env says so by the same name; the wicket connects
  by the name. Any other option is an env of its own (`SLUICE_WICKET_CF_API_DOMAIN=apps.example.com`,
  with `"hosts": ["cf.apps.example.com"]`, for a domain of a wildcard DNS of yours)
- **its listener** stays on the VM's loopback (`127.0.0.1:9091`, the port the image exposes, what
  `/init` relays to). TLS is the sluice server's in front of it (`X-Forwarded-Proto` makes the
  links `https://`): `--tls-cert` here has no host to be routed by on `--listen`, the wicket
  reading a request's `Host`
- **`memory-mib`**: an upload, a package and a droplet go through files, not memory: a `memory-mib` below a zip's
  size takes its push. **`disk-mib`**: an image's droplet is a reference, a zip's is kept as a file
  for each app (that one, the one before, and its package, with the upload while it comes)
- **an upgrade**: `PUT /api/images` of the new image, then this app with its `sha256` (or another
  env): the old VM stops (SIGTERM, all written out), the new one boots on the disk: a boot long of
  no API ([measured](../.kb/wicket-cf-api-findings.md)), the CLI's login still good, the wicket's
  apps served all along. A new image that does not start is refused (422), the old VM started again
- **the wicket's own upgrade**: `docker pull` its image, `docker stop wicket` (SIGTERM: it drains,
  then stops the VMs, the cf-api's all written out to its disk, within `--stop-timeout`: 40, above
  the drain's 20s and a disk's 20s), `docker rm wicket` and the same `docker run`: the apps and the
  cf-api come back from the volume, booting again, a boot long of no app (one wicket has nowhere to
  move them), the cf-api's apps served once it confirms them (15s at most). The cf-api next to
  several wickets instead ([Several wickets](#several-wickets)), each is updated in turn with
  `--evacuation-grace`, its apps moved to the others first:
  `--stop-timeout` above the evacuation, the drain and the disks' 20s together
- **a backup**: [the disk's](../sluice-wicket-runtime-firecracker/README.md#disks), whose file is
  `deployed/disks/ops/cf-api.ext4` in the volume. Lost, the apps serve on, a new cf-api leaves
  them (see Behavior)
- **a crash of the cf-api** (not a stop): the wicket stops its VM and starts another one on the
  disk, [waiting between starts](../sluice-wicket-runtime-firecracker/README.md#a-vm-going-down)
  if it keeps crashing: a boot long of no API. The apps serve on meanwhile

### Docker Compose

The images on one host with KVM (Docker Engine on Linux; on a Mac see [On a Mac](#on-a-mac)), with
[Dex](https://dexidp.io/) as the OpenID Provider of the cf-api and of the wicket's console alike,
its users in its configuration file. The cf-api is a container next to the wicket here, its state
on a volume of its own. The apps are published on 28000 (8080 and its usual stand-ins are often
taken), the cf-api (9091), the console (9090) and Dex (5556) on the host's loopback: `cf` and the
browser run on that host or through an SSH tunnel
(`ssh -L 9091:127.0.0.1:9091 -L 9090:127.0.0.1:9090 -L 5556:127.0.0.1:5556 -L 28000:127.0.0.1:28000 <host>`). Dex is
`dex.localhost` for everyone: the loopback to the browser (`*.localhost`), the service's alias on
the Compose network to the cf-api and the wicket, so the ID tokens' issuer is one URL.

Three files in one directory, as below: `compose.yaml`, `dex.yaml` and `.env`.

```yaml
# compose.yaml; .env holds the secrets, KVM_GID and APPS_DOMAIN
services:
  flume:
    image: ghcr.io/making/sluice/sluice-flume
    pull_policy: always
    environment:
      SLUICE_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
    restart: unless-stopped
  dex:
    image: ghcr.io/dexidp/dex:v2.46.1
    pull_policy: always
    command: [dex, serve, /etc/dex/dex.yaml]
    environment:                    # what dex.yaml's secretEnv and hashFromEnv read
      OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      ADMIN_PASSWORD_HASH: ${ADMIN_PASSWORD_HASH:?ADMIN_PASSWORD_HASH}
    networks:
      default:
        aliases: [dex.localhost]    # the issuer's host, as the browser reaches it on the loopback
    ports:
      - "127.0.0.1:5556:5556"
    volumes:
      - ./dex.yaml:/etc/dex/dex.yaml:ro
    restart: unless-stopped
  wicket:
    image: ghcr.io/making/sluice/sluice-wicket-runtime-firecracker
    pull_policy: always
    command: [--listen, 0.0.0.0:28000, --console, 0.0.0.0:9090, --flume, grpc://flume:8002, --oidc-issuer, http://dex.localhost:5556/dex]
    environment:
      SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}    # the cf-api's
      SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      SLUICE_WICKET_RUNTIME_FIRECRACKER_OIDC_CLIENT_ID: sluice
      SLUICE_WICKET_RUNTIME_FIRECRACKER_OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      SLUICE_WICKET_RUNTIME_FIRECRACKER_OIDC_CLAIM: email=admin@example.com    # who may sign in
    devices:
      - /dev/kvm
    group_add:
      - ${KVM_GID:?KVM_GID}
    tmpfs:
      - /run/sluice                 # the VMs' snapshots stay off the disk
    stop_grace_period: 40s          # the drain's 20s, and 20s for an app's disk
    ports:
      - "28000:28000"               # the apps: --listen, on the same port (the console links it)
      - "127.0.0.1:9090:9090"       # the console, from this host only
    volumes:
      - wicket:/data    # the apps, their root filesystems and disks (deployed/)
    depends_on: [flume, dex]
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
    ports:
      - "127.0.0.1:9091:9091"       # the API, from this host only
    volumes:
      - cf-api:/data    # the desired state, packages and droplets (cf-api/)
    depends_on: [wicket, dex]
    restart: unless-stopped
volumes:
  wicket:
  cf-api:
```

```yaml
# dex.yaml, next to compose.yaml: the OpenID Provider (https://dexidp.io/docs/), its users
# in this file
issuer: http://dex.localhost:5556/dex
storage:
  type: memory
web:
  http: 0.0.0.0:5556
oauth2:
  skipApprovalScreen: true
staticClients:
  - id: sluice                  # the cf-api's and the console's client id
    name: sluice
    secretEnv: OIDC_CLIENT_SECRET
    redirectURIs:
      - http://127.0.0.1:9091/uaa/callback      # the cf-api
      - http://127.0.0.1:9090/oauth2/callback   # the wicket's console
enablePasswordDB: true
staticPasswords:                # the users: Dex asks for the email and the password
  - email: admin@example.com
    username: admin
    preferredUsername: admin    # the cf user name
    userID: 1c2a0b3e-0000-4000-8000-000000000001
    hashFromEnv: ADMIN_PASSWORD_HASH
```

```text
# .env, next to compose.yaml: these values work as they are; change the secrets (openssl rand
# -hex 16 each) and the password before anyone else can reach the host
FLUME_TOKEN=d6edd1ade98c42dd49b77a7ba4906881
CONSOLE_TOKEN=92190bcaf0277ff738ee4162d89c9eeb
OIDC_CLIENT_SECRET=13915f086b88d15379e27e6c3aa4a59f
# admin@example.com's password, bcrypt: htpasswd -bnBC 10 "" <password> | tr -d ':\n'
# (single-quoted: $ is Compose's otherwise); this one is "secret"
ADMIN_PASSWORD_HASH='$2y$10$cYEVeVIzJX6QWDQcjRUaNe1RHrXamph8BdMRzlR/LS7msAe83oJOi'
# a name whose wildcard DNS reaches the host (default localhost)
# APPS_DOMAIN=apps.example.com
```

```text
echo "KVM_GID=$(stat -c %g /dev/kvm)" >> .env    # the group of /dev/kvm, the host's
docker compose up -d
cf login -a http://127.0.0.1:9091 --sso    # open the URL it prints, log in at Dex (admin@example.com / secret), paste the code
cf push web --docker-image ghcr.io/traefik/whoami:v1.11.0
curl http://web.localhost:28000/           # with APPS_DOMAIN: http://web.apps.example.com:28000/
cf logs web --recent
mkdir hello                                # or a root filesystem of your own, here the sample's
docker compose cp wicket:/usr/local/share/sluice-wicket-runtime-firecracker/guest.ext4 hello/hello.ext4
cf push hello -p hello/
curl http://hello.localhost:28000/
cf scale hello -i 2                        # one more VM, cloned from the app's snapshot
# in a browser, http://127.0.0.1:9090/: the wicket's console, signed in through Dex too
```

`docker compose down` keeps the volumes: the next `up` serves the apps again, once their VMs have
booted, with the logins (`sessions.json` in the cf-api's). For other users, add them to
`staticPasswords` and to both `..._OIDC_CLAIM`s (`email=admin@example.com,email=dev@example.com`), or
give them a group and allow the group (`groups=devs`, with the scopes `openid,email,profile,groups`),
and give them [roles](#organizations-spaces-and-roles), as on a wasi wicket; a Dex
[connector](https://dexidp.io/docs/connectors/) replaces the static users with GitHub, LDAP,
another OpenID Provider and the like.

### On a Mac

An Apple silicon Mac (M3 or later) runs this in the Lima VM of the wicket's
[On a Mac](../sluice-wicket-runtime-firecracker/README.md#on-a-mac), started by its `lima.sh`. The
`docker` commands run in the VM, with the same `$TOKEN` and `$FLUME_TOKEN`; `curl` and `cf` run on
the Mac, whose loopback the VM's ports are forwarded to. `docker cp` leaves the sample guest in the
VM, for `limactl copy`. An image must have a `linux/arm64` build: `the image is for amd64, this
wicket's VMs for arm64` fails the build otherwise.

```text
../sluice-wicket-runtime-firecracker/scripts/lima.sh true     # makes and starts the VM
limactl shell sluice-firecracker                              # the first time: newgrp docker in it
```

[Docker Compose](#docker-compose-1) likewise: the VM mounts nothing, so its directory is copied in
(`limactl copy -r . sluice-firecracker:compose`), where `KVM_GID` is added and `docker compose` runs.

### Mapping to VMs

The wicket's listing says `runtime: firecracker`.

| CF | wicket |
|---|---|
| package (docker) | any container image, `--docker-username` with `CF_DOCKER_PASSWORD` its credentials: the build has the wicket make its rootfs (`PUT /api/images`), which pulls it. Listening on `PORT`, as on CF. Spring Boot reads `SERVER_PORT`, not `PORT`: its default fits, an app with another `server.port` takes `cf set-env <app> SERVER_PORT 8080` |
| package (bits) | a zip holding the rootfs, `.ext4` as a component's `.wasm` is picked (160 MiB at most, unpacking to 4 GiB at most: an image is the way for more) |
| droplet | the rootfs by sha256 (stack `firecracker`, buildpack `rootfs`; an image's has neither, as on CF): the wicket keeps an image's, the cf-api its reference pinned to the manifest's digest and, per wicket, the digest it made, and has it made again when the wicket lost it (the same digest, unless the wicket's guest changed) |
| web process `instances` | `instances`, [the limit](#instances) of the app's VMs: `mode=pool`, that many VMs cloned from a snapshot, one too; [annotated](#how-long-a-vm-lives), any mode. 0 is [as on CF](#mapping): no VM and no app in the wicket |
| web process `memory` | `memory-mib`, 1G by default as on CF: less fails a CNB Java image's memory calculator at its start. The snapshot takes what its guest wrote of it on the host's memory (`--tmpfs /run/sluice`; [measured](../.kb/wicket-firecracker-findings.md#a-snapshots-memory)), and at most all of it |
| web process `health_check` `timeout` (`cf push -t`) | `boot-ms`, 60s by default and 180s at most as on CF: how long the process may take to listen (and with an `http` health check, to answer it), in the request that starts it. A process exiting first is refused at once |
| web process `health_check` (`cf push -u`, `cf set-health-check`, the manifest's `health-check-type`, its `-http-endpoint`, `-invocation-timeout` and `-interval`, in seconds) | `http`: the route's [liveness check](../sluice-wicket-runtime-firecracker/README.md#liveness), `liveness`, `liveness-endpoint`, `liveness-timeout-ms` and `liveness-interval-ms`: a VM starts once its endpoint answered 200 within `timeout`, and one failing a probe after is killed and started again, `CRASHED` meanwhile, as Diego restarts an instance at its first failure. `port`, the default, and `process`: a VM starts once its port accepts and is down once it refuses, the wicket's own; a route relays its requests to the port, so `process` is `port` here, and their invocation timeout and interval are kept, unused. Taken at the next start, as on CF (`cf set-health-check` asks for a restart). `-interval` refused with mode `per-connection` or `reuse`: a VM of a connection's own is checked at its start alone, its template's |
| web process `readiness_health_check` (the manifest's `readiness-health-check-type: port` or `http`, its `-http-endpoint`, `-invocation-timeout` and `-interval`, in seconds) | the route's [readiness check](../sluice-wicket-runtime-firecracker/README.md#readiness), `readiness`, `readiness-endpoint`, `readiness-timeout-ms` and `readiness-interval-ms`: a VM takes requests once its check passed and none while it fails, not restarted; the process stats list it `routable` then alone. A wicket whose VMs of the app are all down or failing it leaves the route's advertisement, the server sending the requests to the other wickets serving it. Taken at the next start, as on CF. Refused (`cf push`, the v3 API, an annotation's edit) with mode `per-connection` or `reuse`, and at `warm: 0`: no VM serves ahead of the requests to probe |
| environment variables, `VCAP_APPLICATION`, `VCAP_SERVICES` | the sandbox's `env`: the process's environment, over the image's (its `PORT` is `/init`'s) |
| disk | no effect: the rootfs is read-only and `/tmp` a tmpfs of the VM's memory; a persistent disk is the wicket API's, with no CF counterpart |
| services | [user-provided](#user-provided-services) ones and a [service broker](#service-brokers)'s: the listing offers neither capabilities nor keyvalue stores |
| outbound connections | the [security groups](#security-groups): the route's `egress`. An app reaches no other host |
| `cf logs` | the process's output, naming the VM (`APP/PROC/WEB/<index>`): the lowest index free among the VMs alive, so `0` to `instances - 1` in every mode, modulo `instances` for a VM past a scale down finishing its connections: what `cf app` lists. No `RTR` line: this wicket feeds no requests to the flume |
| annotations | [how long a VM lives](#how-long-a-vm-lives); `budget-ms`, a component's, is refused |

### How long a VM lives

`cf scale -i` sets [the limit](#instances), annotations how long a VM lives below it: the route's
options of the same names ([modes](../sluice-wicket-runtime-firecracker/README.md)), from a
manifest's `metadata` or [the v3 API](#instances).

```yaml
applications:
- name: hello
  instances: 4        # at most 4 VMs
  metadata:
    annotations:
      wicket.sluice.ik.am/mode: reuse
      wicket.sluice.ik.am/keep-ms: 30000
```

| annotation `wicket.sluice.ik.am/` | | taken by |
|---|---|---|
| `mode` | `boot`, `per-connection`, `reuse` or `pool` | |
| `warm` | VMs kept, from 0 to `instances` | `per-connection`, `reuse`, `pool` |
| `keep-ms` | how long a VM serving nothing is kept: a pool's beyond its `warm`, which it needs annotated; a component's instance so ([Instances](#instances)) | `reuse`, `pool`, a component |
| `uses`, `lifetime-ms` | the connections a VM serves, how long it serves | `reuse` |
| `concurrency` | what an instance takes at once: [the limit](#instances) | `pool`, a component |

- none annotated, a pool of a fixed size, of one VM at 1 instance. A pool keeps `warm` VMs, all of
  them by default; at `warm: 0` the first request pays a restore. `boot`
  is `instances` VMs, each booted and never restored from a snapshot, for a process that does not
  take that: what it opened before its snapshot (a database's connections) is gone in a clone
- refused, by `cf push`, the v3 API and `cf scale -i`, naming it: an option its mode does not take,
  none annotated being `pool`, what is no mode or no number, `warm` above the
  instances, a pool's `keep-ms` without a `warm` (a component's too), `instances`, which `cf scale -i` and `concurrency` replace. A wicket not answering, by
  the app's next start
- at once, as `cf scale -i`: the wicket adjusts the running VMs, none restarted. `mode` is what
  a VM is: another one than the running one waits for the next start, the others with it, the
  log saying so, and `cf scale -i` must hold both as the app runs and as annotated, refused
  naming which: `cf restart` undoes a difference. A manifest's `instances` the running app
  cannot take (`cf push` restarts it right after) waits for the next start, the log saying so
- `cf app` lists [the VMs alive](#instances), one coming with its restore and going as these say,
  a VM the wicket keeps and has not up yet `STARTING` (`1/2` just after `cf scale -i 2`),
  `0/0` with none, `CRASHED` with why if a request's restore failed, until a restore succeeds: the
  next request's, a `cf scale`'s at once, or the wicket's own, [1s to 60s apart](../sluice-wicket-runtime-firecracker/README.md#a-vm-going-down)

### Security groups

An app of a Firecracker wicket reaches the destinations of the groups bound to its space, through
the wicket's [egress](../sluice-wicket-runtime-firecracker/README.md#egress), from its next start:

```text
cf create-security-group blog-backend rules.json
cf bind-security-group blog-backend sluice --space wicket
cf restart blog
```

```json
[{"protocol": "tcp", "destination": "entry-api.ik.am", "ports": "443"}]
```

| CF | the wicket's `egress` |
|---|---|
| `destination`: an address, a CIDR | an address, a range; bits past the prefix mean nothing, as to a firewall |
| `destination`: `10.0.0.1-10.0.0.9` | the fewest CIDRs covering it |
| `destination`: a name | a name, which CF does not have: a guest has no name server, so `0.0.0.0/0:443` connects to any address and resolves nothing. The wicket connects to it as its host resolves it |
| `destination`: comma-separated | a rule per destination |
| `ports`: `443`, `80,443`, `8000-8100` | a rule per port |
| `protocol`: `tcp` | what a rule is |
| `protocol`: `udp`, `icmp`, `all` | refused (422): the wicket relays TCP alone |
| `bind-security-group`, `bind-running-security-group` | the apps of the spaces bound (of every space for a global group), at their next start: the `running` snapshot |
| `--lifecycle staging`, `bind-staging-security-group` | shown, no effect: no VM stages |
| `log`, `description` | shown, no effect: the wicket records every connection |

- **Who edits them.** A rule reaches what the wicket's host reaches: `localhost` is the host's, a
  range covers link-local addresses, and on the Compose network `wicket:9090` is the console. So
  editing takes more than logging in, whose `cloud_controller.admin` everyone holds:
  `--security-group-editor` names who, as a claim rule, `--security-group-editor-client` which
  clients; without them nobody may (`403`), with or without a provider. Without a provider the
  claims are `user_name` and `email`, both the name given at `cf login`, which anyone can say: on
  the host's loopback, `--security-group-editor user_name=me`. A rule reads the claims of the
  login, at each edit; an edit is logged with who (`SECURITY GROUPS: alice: POST ...`). Anyone
  logged in reads them
- **How many.** A guest takes 1024 bytes of rules on its kernel's command line: 64 rules of
  `0.0.0.0/0:10000`, 39 of a 20-character name at a 4-digit port. A group past them is refused at the
  edit (`1-65535` always is), groups past them together at the start (`CRASHED`, the wicket's
  reason in `cf push`'s `Error starting instances`)
- **The process's own port.** A rule on the process's port (`PORT`) holds it in the guest. A range's
  rule, `localhost`'s or `127.0.0.1`'s: `/init` starts no process and the start fails, its reason
  in `cf logs` (`port 8080 is the process's own`). Another address's or a name's: a process
  listening at `127.0.0.1` still can, one at every address (`0.0.0.0`, a JVM) cannot: it exits,
  with `/init`'s line in the logs. Only `/init` knows the port (an image's `EXPOSE`), so neither
  the cf-api nor the wicket refuses the rule before

## Several wickets

`--wicket <name>=<url>` names each wicket, repeated: the cf-api spreads an app's instances over
them, so one wicket can stop while the apps serve on -- those of `-i 2` or more on their other
instances, all of them once moved off a wicket told to stop, or gone -- and the wickets be
updated one at a time. Clients reach them through a [sluice server](../README.md), each wicket
advertising the hosts of the apps it runs, the app's instances on it their weight, and the server
spreading a host's requests over the wickets serving it by them (its
[Scale-out](../README.md#scale-out)): `-i 3` over two wickets, 2 of every 3 to the wicket of two.
A wicket's `--listen` serves its own apps only and drains nothing (a signal ends it once
evacuated): local mode stays one wicket.

- **Placement.** Each index of an app's web process is on a wicket, kept in the state: a new
  index goes to a wicket with room for it holding none of the app's while there is one, else to
  one holding the fewest, of them the one with the most room, the first named of those. A
  wicket's room is its budget (`--max-instances`, a Firecracker wicket's `--max-memory-mib`) less
  what its routes not of the cf-api's apps keep and the indices of the started apps placed on it,
  each an instance (a VM's memory too). A scale down drops the highest indices, as on CF, and
  removes the app from a wicket left with none. A wicket runs an app of its indices: their count
  is its `instances`, those below the app's `warm` (the lowest are the warm ones) its `warm`. A
  wicket refusing a share for room has none until its apps change: an edit (`cf scale -i`) is
  placed again without it, a start passes the share's highest index to a wicket with room
  (`CELL: Index 2 placed on the wicket b: the wicket a has no room for it`). No wicket has room:
  an edit is refused with the wicket's reason, a start shows the indices `CRASHED` with it. A
  running index moves only off a wicket leaving or gone, or onto one back (below), each the way
  Diego evacuates a cell: started on its new wicket, then out of its old one's share once the new one
  serves it -- its wicket took it, with a [readiness check](#mapping-to-vms) its VMs are ready,
  and every server it streams to took its routes (no request finds the host advertised by
  neither); not within the health check's timeout, it stays. A state of one wicket has its indices on the
  first wicket named, and the indices of a wicket no name gives any more are placed again at the
  start (logged): a wicket keeps its apps while it keeps its name, and the one wicket of
  `--wicket <url>` is `wicket` (`--wicket wicket=<url>`)
- **Which wicket.** A wicket's apps listing names it: an id made at its first start and kept in
  its console directory (`wicket-id`), its flume node, its build, and its budget. Two names of one
  id (two URLs reaching one console) are refused at the start; one coming to another's id later
  is logged and placed on no more. An id changed behind a name (another console directory) is
  logged, kept in `state.json`, and the apps placed there are put again. A wicket listing none (one
  predating them) is logged once and taken as it is: room enough, its lines' instances as one
  wicket's
- **`cf app`** lists each wicket's instances at their indices, the wicket's name as their `host`
  (`cf curl /v3/apps/<guid>/processes/web/stats`): a wicket away, or holding its share since its
  start (below), shows its indices `STARTING`, one
  refusing its share `CRASHED` with its reason. An edit applying at once (`cf scale -i`, routes,
  annotations) is put on each wicket whose share it changes, and a refusal by one changes nothing;
  a start, restart or restage puts every share at once, each wicket's outcome its own, a
  [deployment](#behavior) a wicket's share at a time. `cf logs` names a line's instance as `cf
  app` does, by the wicket whose flume node fed it: `[APP/PROC/WEB/1]` for the second wicket's
  first instance of an app of `-i 2`. A request's `RTR` index is its wicket's place among the
  `--wicket`s (each a router of its share), its `app_index` the instance. The lines a wicket
  dropped (`LGR`) are in the logs of the apps it holds indices of
- **What they share.** The wickets run apps one way: one listing another runtime than the first
  is logged, and none placed or put on it. They share one `--wicket-token`, not a console
  directory (a volume each; a StatefulSet on Kubernetes). The marketplace is what every wicket
  offers. Firecracker
  wickets make an image's rootfs each, one at staging, the others at their first put of it, the
  cf-api keeping the digest each made: wickets of two guests, as during an update of the wickets
  changing the guest, serve one image as two digests. The cf-api runs beside the wickets, not as
  an app of one, which would stop with it
- **Keyvalue stores.** Each wicket lists its stores, each one of its host (`sqlite`) or shared
  with the wickets of other hosts, by an identity: its server as its `url` names it and its
  prefix (`redis`; see the wicket's [Key-value stores](../sluice-wicket-runtime-wasi/README.md#key-value-stores-experimental)).
  A plan is a store every wicket lists, a shared one by one identity on all: two servers of one
  name are offered by none. A shared store's instance is one namespace for every wicket, so an
  app over two counts once (`redis` in the sample below). A store of each wicket's host holds an
  instance's data on one wicket, its home: where index 0 of the first app whose start maps it
  runs. An app whose start maps one has all its instances there, from that start on: `cf scale
  -i 3` puts the three on that wicket (refused with its reason when it has no room, no index
  passed to another), and `cf bind-service` refuses (422) an instance whose data is on another
  wicket than that of one the app binds already. A wicket no `--wicket` names any more keeps the
  data it holds (logged): the apps start on another without it. A deleted instance's data is
  deleted of a store of each wicket's host through every wicket, each once none of its routes
  maps it; of a shared store through one, once every wicket answers that none of its routes
  maps it (`GET /api/keyvalue/<store>/<namespace>`), a draining one included
- **Evacuation.** A wicket told to stop with an evacuation bound (`--evacuation-grace`, see the
  wicket's [Shutdown](../sluice-wicket-runtime-wasi/README.md#shutdown-drain)) serves on and lists
  itself leaving: it has no room, and each index of the started apps on it moves to a wicket with
  room, as a new one goes (the app's spread kept as far as it goes), every app at once (`CELL:
  Index 0 moves to the wicket a: the wicket b is leaving`, then `Index 0 runs on the wicket a: out
  of the wicket b's share`), the app removed from the leaving one with its last index. Then the
  cf-api tells it what is left stays (`PUT /api/evacuated/cf`), and it drains: an index with
  nowhere to go (no room, its keyvalue data there, refused elsewhere) stays until it stops (`CELL:
  Index 0 stays on the wicket b until it stops: no other wicket has room for it`), as do the
  wicket's own routes and a Firecracker wicket's app on a [disk](../sluice-wicket-runtime-firecracker/README.md#disks).
  Its `routes.yaml` keeps what stayed: back, it serves no app that went elsewhere. A wicket back,
  or a new one, takes the indices that spread an app again the same way round, one app at a time:
  from the wicket holding the most of the app's while that one holds two more (`Index 1 moves to
  the wicket b: the app's instances spread over the wickets again`). The cf-api down, the wicket
  waits out its bound, and the apps of `-i 1` on it answer 503 from its drain until it is back
- **A wicket gone.** A wicket that has not answered for `--wicket-away-grace` (30s by default; 0:
  never), as Diego takes a cell whose presence expired (its host down, a crash loop), is gone
  (`CELL: The wicket b has not answered for 30s: placed on no more`): no room there, and each
  index of the started apps on it moves to a wicket with room as off a wicket leaving (`Index 0
  moves to the wicket a: the wicket b has not answered for 30s`), the app's spread kept as far as
  it goes, removing nothing from it: it answers nothing. An index with nowhere to go stays (`until
  it is back`), tried again as room comes, as long as the wicket does not answer; one refused
  elsewhere stays. Back, the wicket's listing removes what moved away, and it is placed on again
  once it has answered for as long (`CELL: The wicket b has answered for 30s: placed on again`),
  taking the indices that spread an app again: a wicket going and coming within the bound moves
  no app back and forth. The cf-api cut off from a wicket the server still reaches (a partition)
  takes it for gone: the indices moved run twice, there and on the wicket they went to, until the
  cf-api reaches it again and removes them
- **A wicket back** (a crash, a kill, its host's restart) serves the apps of its console directory
  at once, but those of the cf-api: it holds them -- neither advertised nor served on its
  `--listen`, not [ready](../sluice-wicket-runtime-wasi/README.md#readiness-probe) -- until the
  cf-api, listing it again within 2s, says they are as wanted (`PUT /api/confirmed/cf`), puts them
  again or removes them: an app deleted, stopped, scaled off it, moved away or changed while it
  was away takes no request there (each share is put with `"held-ms": 15000`, see the wicket's
  [Apps](../sluice-wicket-runtime-wasi/README.md#apps)). The cf-api down, the wicket serves them
  15s after its start, as they were
- **Updating the wickets**, one at a time: a wicket stopping evacuates (above), then drains
  (`--drain-grace`, 20s), its routes withdrawn on every node, and the server sends their hosts to
  the other wicket; started again, it serves the apps of its console directory before it
  advertises them (a Firecracker wicket each once a VM of it is up and passed its [readiness
  check](#mapping-to-vms)), those of the cf-api once it confirmed them (above), and the cf-api
  puts nothing again but the indices it takes back. Every app moved serves on; without an
  evacuation, one of `-i 1`, or one mapping a store of each wicket's host (all its instances on
  one wicket), answers 503 until its wicket is back. Let a stop take longer than the evacuation
  and the drain together (`stop_grace_period` / `terminationGracePeriodSeconds`: 90s for an
  evacuation of 60s, 20s more for a Firecracker wicket's disks), and update the next wicket once
  the first serves again: its readiness probe (`--health`, see the wicket's [Readiness
  probe](../sluice-wicket-runtime-wasi/README.md#readiness-probe)) says so once every node of the
  server took its routes, each with an instance ready and none held. A route that does not start
  ends the wicket's start. On Kubernetes the cf-api reaches a wicket leaving (a pod terminating, no
  endpoint), or one not ready that holds its apps, by its name in the StatefulSet's headless
  Service with `publishNotReadyAddresses: true`
  alone

The [Docker Compose](#docker-compose) sample with a sluice server and two wickets: in its
`compose.yaml`, the services below in place of `wicket` and `cf-api`, the volumes `wicket-a` and
`wicket-b` in place of `wicket`; `wicket.yaml` as below (the consoles the cf-api's alone: no
sign-in, no port); in `.env`, a `SERVER_TOKEN` too (`openssl rand -hex 16`). The apps keep their
data in the Redis both wickets share.

```yaml
  server:
    image: ghcr.io/making/sluice/sluice-server:native
    pull_policy: always
    command: ["--sluice.token=${SERVER_TOKEN:?SERVER_TOKEN}"]
    ports:
      - "28000:8000"                # the apps, through the server
    restart: unless-stopped
  wicket-a: &wicket
    image: ghcr.io/making/sluice/sluice-wicket-runtime-wasi
    pull_policy: always
    command: [--config, /etc/sluice/wicket.yaml]
    environment:                    # what wicket.yaml's ${...} take
      SERVER_TOKEN: ${SERVER_TOKEN:?SERVER_TOKEN}
      CONSOLE_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      REDIS_PASSWORD: ${REDIS_PASSWORD:?REDIS_PASSWORD}
    stop_grace_period: 90s          # above evacuation-grace and drain-grace: a stop moves, then drains
    healthcheck:                    # ready: the server routes to it (wicket.yaml's health)
      test: [CMD, /usr/local/bin/sluice-wicket-runtime-wasi, --probe, 127.0.0.1:8081]
      interval: 5s
    volumes:
      - ./wicket.yaml:/etc/sluice/wicket.yaml:ro
      - wicket-a:/data              # its console directory: one each
    depends_on: [server, redis]
    restart: unless-stopped
  wicket-b:
    <<: *wicket
    volumes:
      - ./wicket.yaml:/etc/sluice/wicket.yaml:ro
      - wicket-b:/data
  cf-api:
    image: ghcr.io/making/sluice/sluice-wicket-cf-api
    pull_policy: always
    command: [--listen, 0.0.0.0:9091, --wicket, a=http://wicket-a:9090, --wicket, b=http://wicket-b:9090, --flume, grpc://flume:8002, --oidc-issuer, http://dex.localhost:5556/dex]
    environment:
      SLUICE_WICKET_CF_API_WICKET_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      SLUICE_WICKET_CF_API_FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      SLUICE_WICKET_CF_API_DOMAIN: ${APPS_DOMAIN:-localhost}
      SLUICE_WICKET_CF_API_OIDC_CLIENT_ID: sluice
      SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
      SLUICE_WICKET_CF_API_OIDC_CLAIM: email=admin@example.com
      SLUICE_WICKET_CF_API_ADMIN: email=admin@example.com
    ports:
      - "127.0.0.1:9091:9091"
    volumes:
      - cf-api:/data
    depends_on: [wicket-a, wicket-b, dex]
    restart: unless-stopped
```

```yaml
# wicket.yaml, both wickets': the apps through the server
server: grpc://server:8001
token: ${SERVER_TOKEN}
evacuation-grace: 60s           # on a stop, its apps moved to the other wicket first
health: 127.0.0.1:8081
console: 0.0.0.0:9090
console-token: ${CONSOLE_TOKEN}
console-dir: /data/deployed
flume: grpc://flume:8002
flume-token: ${FLUME_TOKEN}
keyvalue:
  stores:
    redis:
      type: redis
      url: redis://:${REDIS_PASSWORD}@redis:6379/0
```

```text
docker compose up -d
cf login -a http://127.0.0.1:9091 --sso
cf push hello -p hello/ -i 2               # an instance on each wicket
curl http://hello.localhost:28000/         # through the server, each wicket taking half
cf curl "/v3/apps/$(cf app hello --guid)/processes/web/stats" | jq -c '.resources[] | [.index, .host]'
# an update, one wicket at a time: each moves its apps to the other and drains (hello goes on
# through the other), and is waited for until it is ready again, taking hello's second back
docker compose up -d --force-recreate --wait wicket-a
docker compose up -d --force-recreate --wait wicket-b
```

## Options

| option | |
|---|---|
| `--listen <host:port>` | bind address (default `127.0.0.1:9091`); with `--any-credentials` a loopback address only |
| `--wicket <name>=<url>` | a wicket's console, `http://host:port`, and its name (letters, digits, `-`, `_`, `.`): repeatable (or comma-separated), an app's instances spread over the wickets ([Several wickets](#several-wickets)). `--wicket <url>` names the one wicket `wicket` |
| `--wicket-token <token>` / `--wicket-token-file <path>` | their `--console-token`, one for all |
| `--wicket-away-grace <secs>` | of several wickets, one not answering that long is gone: placed on no more, the indices on it started on the others, until it has answered as long again ([Several wickets](#several-wickets); default 30, 0 = never) |
| `--domain <name>` | a shared domain; repeatable (or comma-separated): the first takes the default and random routes (default `localhost`). Through a sluice server, a suffix whose wildcard DNS reaches it |
| `--data-dir <path>` | the state and the blobs (default `cf-api`): `state.json` (written by a rename on every change), `jwt.key` and `sessions.json` (the logins), `packages/<guid>.zip` (the newest per app), `droplets/<sha256>.wasm` (the current and the previous per app) |
| `--org <name>` / `--space <name>` | the organization and its space the first start makes (default `sluice` / `wicket`) |
| `--flume <url>` | the sluice-flume the wicket feeds (`grpc://host:port` / `grpcs://host:port`): the apps' output and requests reach `cf logs` from it |
| `--flume-token <token>` / `--flume-token-file <path>` | its bearer token |
| `--flume-ca-cert <path>` / `--flume-insecure` | a `grpcs://` flume verified against this PEM CA (default the webPKI roots), or not at all |
| `--any-credentials` | no provider: whoever reaches the API logs in and deploys (see [Login](#login)). This or `--oidc-issuer` is required |
| `--oidc-issuer <url>` | the OpenID Provider users log in through (`cf login --sso`); its discovery document is at `<url>/.well-known/openid-configuration` |
| `--oidc-client-id <id>` | the client registered with the provider, whose redirect URI is `<API URL>/uaa/callback` (required with `--oidc-issuer`) |
| `--oidc-client-secret <secret>` / `--oidc-client-secret-file <path>` | its secret; none = a public client (PKCE only) |
| `--oidc-claim <claim>=<pattern>` | who may log in: the ID token's claim must hold (a list claim: contain) the pattern, `*` matching any run of characters; repeatable (or comma-separated). One claim's patterns are alternatives, distinct claims must all hold. At least one rule is required with `--oidc-issuer` (`email=*` lets anyone the provider knows in) |
| `--oidc-scopes <list>` | the scopes asked for, comma-separated (default `openid,email,profile`; Dex sends the `groups` claim for the scope `groups`) |
| `--oidc-ca-cert <path>` | a PEM CA the provider's certificate is verified against (default the webPKI roots) |
| `--client-credentials <id>:<secret>` / `--client-credentials-file <path>` | a client of `cf auth <id> <secret> --client-credentials` (CI); repeatable, the file one per line. With `--oidc-issuer` only |
| `--admin <claim>=<pattern>` | who is an [admin](#organizations-spaces-and-roles): as `--security-group-editor`. With `--oidc-issuer` this, `--admin-client` or a group's `--group-role` is required; with `--any-credentials` and none of them, whoever logs in is one |
| `--admin-client <id>` | a client of `--client-credentials` who is an admin; repeatable |
| `--group-role <group>=<role>` | the role a group of the provider gives its members: `admin`, `organization_<role>@<org>` or `space_<role>@<org>/<space>` ([details](#organizations-spaces-and-roles)); repeatable (or comma-separated). With `--oidc-issuer` only |
| `--oidc-groups-claim <claim>` | the ID token's claim listing a user's groups, which `--group-role` reads (default `groups`) |
| `--security-group-editor <claim>=<pattern>` | who may edit the [security groups](#security-groups), nobody without it: as `--oidc-claim`; without `--oidc-issuer` the claims are `user_name` and `email`, both the name anyone logs in with |
| `--security-group-editor-client <id>` | a client of `--client-credentials` who may too; repeatable |
| `--service-broker-editor <claim>=<pattern>` | who may register, update and delete the [service brokers](#service-brokers) and set who sees their plans, nobody without it: as `--security-group-editor` |
| `--service-broker-editor-client <id>` | a client of `--client-credentials` who may too; repeatable |
| `--tls-cert <path>` / `--tls-key <path>` | serve TLS: the certificate chain and its private key, PEM. Without them, behind a TLS proxy, `X-Forwarded-Proto: https` makes the links `https` |
| `--exit-on-stdin-close` | exit when stdin reaches its end (a parent's child process) |

Each option also reads `SLUICE_WICKET_CF_API_<NAME>` from the environment; another variable of that
prefix, a misspelled one, is refused at the start. The image
`ghcr.io/making/sluice/sluice-wicket-cf-api` (`latest` / `<sha>`, built from
`Dockerfile` at the repository root) keeps `/data` as the working directory; with
`--any-credentials` it listens on the loopback, so it is reached from the wicket's network
namespace (`--network host`, a sidecar, `kubectl port-forward`).

## Mapping

What holds on both wickets; a component's and a VM's are in
[Mapping to components](#mapping-to-components) and [Mapping to VMs](#mapping-to-vms).

| CF | wicket |
|---|---|
| app, started, with a droplet and a route | on each wicket holding some of its instances ([Several wickets](#several-wickets)), an app named by its guid, owner `cf`: its routes' hosts, the droplet's component, the sandbox below |
| app stopped, scaled to 0, or without a route | no app in the wickets: a request gets a wicket's 404, as gorouter answers a route with no instance registered. `cf app` shows `0/0` (stopped: `0/N`, each `DOWN`), `cf push` of `instances: 0` waits for none |
| route `<host>.<domain>` | a host: labels of `a-z`, `0-9` and `-`. No path, port or wildcard (422). A route serves one app: a second destination is refused; zero-downtime updates are `--strategy rolling` |
| shared domain | `--domain` |
| instance state | once the wicket took the app, an entry per [instance alive](#instances), `RUNNING`, its `since` its own start, `routable` unless it fails its [readiness check](#mapping-to-vms), for a VM it keeps and has not up yet `STARTING`, and for a VM it keeps gone down or not starting (no room in the wicket's budget, a snapshot gone), or none alive at `warm: 0` with a request's restore failing for anything but room, `CRASHED` with why as `details`, `since` the failure began, until it is up; `CRASHED` with the wicket's refusal as `details` (`cf push` prints it: `Error starting instances: '...'`); `STARTING` while the wicket is unreachable, has not taken the app, or holds it since its start until the cf-api confirms it. These two list an entry per index the wicket holds. Each entry's `host` is its wicket's name: of an index [moving](#several-wickets) to another wicket, the one it leaves until the other serves it |
| deployment (`--strategy rolling`, `canary`) | the app put with the new droplet a wicket's share at a time, each wicket swapping it in place ([Behavior](#behavior)) |
| org / space | the cf-api's own ([Organizations, spaces and roles](#organizations-spaces-and-roles)): the wicket knows none, its hosts are every space's |
| readiness check (`readiness-health-check-*`, a manifest's alone, as on CF) | kept and shown (`cf get-readiness-health-check`, the process's `readiness_health_check`); `process`, the default, on both wickets; `port` and `http` on a Firecracker wicket ([Mapping to VMs](#mapping-to-vms)), refused on a wasm wicket, where an instance takes requests as they come |
| health check (`cf push -u`, `cf set-health-check`, `health-check-*`) | kept and shown (`cf get-health-check`, the process's `health_check`); on a Firecracker wicket the route's liveness check and `timeout` its start's ([Mapping to VMs](#mapping-to-vms)); on a wasm wicket `port` and `process` ask nothing, `http` is refused: an instance takes requests as they come |
| log rate, stack, buildpacks | accepted and shown, no effect |

### Instances

An instance is what CF calls one: it takes many requests at once. Three settings size an app, as
Cloud Run's do:

| setting | what it is | Cloud Run | default |
|---|---|---|---|
| `instances` (`cf scale -i N`) | the most instances alive at once | maximum instances | 1 |
| annotation `wicket.sluice.ik.am/concurrency` | what an instance takes at once: a component's requests, a `pool` VM's connections | concurrency | 200 |
| annotation `wicket.sluice.ik.am/warm` | the instances kept up with no request, from 0 to N | minimum instances | N, as on CF; a VM per connection: 0 |

- **A component's instance** is one wasm instance (a store) taking `concurrency` requests at
  once ([the wicket's](../sluice-wicket-runtime-wasi/README.md#concurrency)), as a CF instance is
  one process: `memory` bounds it, its requests' together, the guest's module state is theirs to
  share, and a trap ends every one of them. `budget-ms` stays each request's: one past it is
  answered 504 at its deadline, and its instance, taking no more, ends once the others are
  done and is replaced. `warm: N` keeps N of them, the route's `warm`. At `-i 1`
  requests are served so:

  | request | served by |
  |---|---|
  | the 1st to the 200th | the warm instance, at once |
  | the 201st | the first of them to end, waited for within its `budget-ms`, then a 503 |

  A request no warm instance has room for, within `instances`, starts one, which takes the next
  ones as the warm do and ends once it serves none; annotated `keep-ms`, it is kept until it
  served none for that long. That pays for a component whose start costs more than a hello's:
  one writing an 8MiB heap answers in 1.6ms cold against 30us warm
  ([measured](../.kb/wicket-wasi-findings.md#a-pooled-units-latency)). Such an instance is named in
  `cf logs` by the lowest index free from `warm` up.

  An instance is one task of the wicket: its requests share a core. A hello component's
  instance serves ~40k requests a second where 16 serve 92k, a component waiting on its
  upstreams as much either way ([measured](../.kb/wicket-wasi-findings.md#concurrency)): `cf scale
  -i` adds cores to a CPU-bound app, `concurrency` room to a waiting one
- **Scale to zero** is `warm: 0`. A component then has a request start an instance (~250us
  against ~130us warm of a hello component, [measured](../.kb/wicket-wasi-findings.md#a-pooled-units-latency)),
  which takes the others arriving while it serves and ends once it serves none, unless
  `keep-ms` keeps it until it served none for that long: an idle app holds none once it passed. A `pool` VM grows to N on demand, and one
  serving none is stopped after `keep-ms` (60s by default), so an idle app holds no VM and its next
  request pays a restore. `reuse` keeps a clone `keep-ms` after its connection, `per-connection`
  none; `boot` keeps its N VMs

```yaml
applications:
- name: hello
  instances: 4        # at most 4 VMs
  metadata:
    annotations:
      wicket.sluice.ik.am/warm: 0          # none when idle
      wicket.sluice.ik.am/keep-ms: 60000   # a VM serving none stops after 60s
```

The CLI has no command for annotations: without a manifest, the v3 API sets them (a string each,
a `null` removing one), at once:

```text
cf curl -X PATCH "/v3/apps/$(cf app hello --guid)" \
  -d '{"metadata": {"annotations": {"wicket.sluice.ik.am/warm": "0"}}}'
cf curl "/v3/apps/$(cf app hello --guid)" | jq .metadata
```

As on CF, a scale keeps what runs. Up, one more instance starts and none of the others is touched:
a VM cloned from the app's snapshot in a restore's time, whatever its boot takes, a component's unit
instantiated. Down, the highest indices take no more requests: a component's unit ends after its
own, a VM after its connections and its `keep-ms` (60s by default; annotated `boot`, none). The server's
route does not change, so a keep-alive connection stays on its VM. That is why a VM is a pool at 1
instance too: the second comes in a restore's time, where `boot` has it take a boot, 12s of a CNB
Java app, `cf scale` not waiting for it ([weighed](../.kb/wicket-firecracker-findings.md#a-scale)).

| | `-i N` limits | at once |
|---|---|---|
| component | N wasm instances (the route's `instances`), each taking `concurrency` requests; `warm` N unless annotated | `N x concurrency` requests |
| `boot` VM | N VMs (`instances`), kept | any |
| `pool` VM | N VMs (`instances`), each taking `concurrency` connections before the pool grows | as `concurrency` has it |
| `per-connection`, `reuse` VM | N VMs (`instances`): the memory is N x `memory-mib`, as CF's quota counts it | N connections |

- **`concurrency`** (`wicket.sluice.ik.am/concurrency`) is from 1, 200 by default, the route's
  `concurrency`. Cloud Run's 80 is where it adds an instance; at `-i 1` there is none to add, so
  the default is where a request is turned away: a CF instance has no such limit, a JVM's request
  threads are 200. `-i 1` with no annotation behaves as a CF app: a component takes 200 requests at
  once, a pool of one VM any number of connections. A component wanting one request at a time per instance
  annotates 1. `boot`, growing none past N, has none to decide and a VM per connection is 1 by
  definition: those modes refuse it
- **It means what each can tell.** A component's counts requests, and is hard: one past the limit
  waits for an instance within its `budget-ms`, then gets a 503. A pool VM's counts connections,
  and is soft: the connections a VM holds before the pool grows, at N VMs one more going to the VM
  holding the fewest. The wicket relays a VM's bytes and sees no requests, so a hard limit would
  let an idle keep-alive connection shut the others out
- **A VM per connection** keeps N literal: a VM is memory, and whoever annotates the mode chose it.
  `cf push` and `cf scale` warn of the count, and the connection finding no room waits for it
  within the route's `wait-ms` (10s), then gets a 502 saying so
- **A limit above the wicket's own** runs, bounded by the wicket: N past `--max-instances`
  is no refusal. N is 1000 at most: each index is placed on a wicket, kept and listed
  ([Several wickets](#several-wickets))
- **`cf app`** lists the instances alive, as the wicket has them at each request: a warm one, a VM
  serving or kept, one started for a request while it serves, each `since` its own start (a unit replaced, a VM
  cloned anew: then; those of one index, as `cf logs` has them below: the lowest's). So `3/3` below a limit of 4, and `0/0`,
  `There are no running instances of this process`, with none: no failure, and `cf push`, `cf restart`
  and `--strategy rolling` finish at it, the CLI's wait ending at an entry `RUNNING` or at no entry.
  So with `boot` they return with the first VM accepting, as on CF, the others coming as they boot,
  none of them refused for one that fails (`CRASHED` at its index, with why, until another one is up
  there, the others serving on). All of them failing is the start's failure.
  The CLI counts the entries, so the limit cannot show as `3/4`: entries for the rest would read
  as failed, and at none alive keep that wait until its timeout. The limit is the process's
  `instances` (`cf create-app-manifest`, `cf curl /v3/apps/<guid>/processes/web`). The VMs the wicket
  keeps up are another matter, coming whatever the requests: `-i 2` of a pool or `boot` shows `1/2`,
  the second `STARTING`, until it is up, unless one is down, which stays the `CRASHED` above
- **`cf logs`** names what `cf app` lists, `0` to `N - 1`: of a component the units alive may
  outnumber N, the work of N instances, and the cf-api names one `index % N`, knowing N as the
  wicket does not; those of one index are one entry. A VM's index is within N already, but for one
  past a scale down finishing its connections, named so too. Over several wickets, each names its
  own: `cf app` maps a wicket's index to the app's, `cf logs` does not

## Behavior

- as on CF, routes and `instances` apply at once, and so do the annotations of how many
  instances and how long ([Instances](#instances)); the droplet and the rest of the sandbox
  (memory, `mode`, `budget-ms`, the service bindings, the environment variables, the security
  groups) at a start, restart, restage or deployment: the running app is the snapshot taken then.
  For `scale -m` the CLI stops and starts the app itself
- environment variables: `cf set-env` / `unset-env`, a manifest's `env:` (merged, as `cf push`
  does) and `cf env` work as on CF. Refused: the names CF reserves (`VCAP_*`, `VMC*`, `PORT`),
  what the wicket refuses (`=` or a control character in a name, a NUL in a value) and more
  than 64KiB in all. The guest gets them with `VCAP_APPLICATION` and `VCAP_SERVICES` (JSON, as
  `cf env` shows them). No log holds a value (`cf logs` says `[PRIVATE DATA HIDDEN]`), nor does
  the wicket's listing, which names the variables only: the app carries a tag drawn at each
  start, which the cf-api compares instead. The data directory's files are its owner's only
  (0600): `state.json` holds the values. They are written by a rename and an `fsync`: on the
  disk when the answer comes
- images (`--docker-image`): staging pins the tag to its manifest's digest, kept with the droplet:
  a restart serves what was staged after the tag moved, a restage takes the tag again. The
  password is in no answer (`***` in the package) and no log; `state.json` holds it. An app is an
  image's after a push of one, a zip's after a push of bits. A build the cf-api's restart cut
  short fails: restage
- an edit the wickets answer at once (map-route, unmap-route, scale -i, a manifest's routes)
  is refused with 422 and the reason of the wicket refusing it, and changes nothing; a start,
  restart or deployment is accepted, and its outcome shows in the process stats and the log
  (`cf logs`)
- a VM's start may take the health check's timeout (`cf push -t`, 60s by default): only that app's
  edits that touch the wicket wait for it (`cf set-env` does not), and a client giving up leaves
  it going, its outcome shown all the same. An app whose VMs did not start on a wicket (`CRASHED`)
  is not tried again there until what is asked of it changes (`cf restart`), the wicket comes
  back or the cf-api restarts; one a wicket refused for another reason (no room, a host taken)
  whenever its revision moves
- a deployment (`--strategy rolling` of `cf push`, `restart`, `restage`) puts the app's new
  snapshot a wicket's share at a time, in the order of the `--wicket`s (as many as
  `--max-in-flight` instances take, a share at least), the others serving the app as it was: each
  wicket swaps its share in place, the app answering all along. The next goes once those put serve
  it: their wicket took it (a Firecracker wicket's VMs started, a wasm wicket's route loaded) and,
  with a [readiness check](#mapping-to-vms), the VMs it keeps are ready. `cf app` shows `Active
  deployment with status DEPLOYING` meanwhile; `cf push` waits for its end. A share refused stops
  it there, active as CF keeps one whose new instances crash: the share's instances `CRASHED` with
  the wicket's reason (the deployment's `status.details.error`), the other wickets on the app as
  it was, until `cf cancel-deployment` puts every share back on it (the CLI cancels it itself
  after `CF_STARTUP_TIMEOUT`, 5 minutes); the share taken later (the wicket's revision moved, the
  cf-api restarted), it goes on. `--strategy canary` pauses once the first wicket's share serves
  it (`--instance-steps`: each step's percentage of the instances) until `cf
  continue-deployment`. A start, restart or stop supersedes one under way, every share put at
  once; so does the next deployment, the wickets it has not reached serving on. `-i` and `-m` of
  the push apply with it; an app serving nothing (stopped, at 0, without a route) starts at once.
  A restart of the cf-api takes it up where it was
- the desired state is the cf-api's: at startup and whenever a wicket's revision moves
  (`GET /api/apps?after=`), it lists that wicket's apps of its owner and puts / deletes the
  difference (the same app again changes nothing: the wicket's generation stays), each wicket
  watched apart. The wickets serve on while the cf-api is down, a wicket started again what it
  holds of the cf-api's 15s after its start; with a wicket down, the apps' indices on it show
  `STARTING` and are put when it is back, and an app deleted meanwhile (or whose removal it
  refused) is removed then, as is one it holds none of any more: the wicket holds what it kept
  until the cf-api, listing it within 2s, confirms what is as wanted, so it serves none of the
  others ([Several wickets](#several-wickets)). Of several wickets, one down for
  `--wicket-away-grace` is gone, its indices started on the others
- an app of its owner the state does not know is left where it is: a new or lost `--data-dir`
  removes nothing. It is logged (`CELL: left in the wicket`), still serves, and keeps its hosts
  (a push of one is refused, naming it): `DELETE /api/apps/<guid>?owner=cf` of the wicket's
  console removes it. The login's key goes with the directory too: the CLI logs in again
- `cf logs` (log-cache `read`, polled by the CLI): an in-memory ring of 1000 lines per app of
  the cf-api's own lines: staging (`STG`), lifecycle (`API`), the wicket's outcome (`CELL`).
  With `--flume`, the app's own too, from the live access log the wicket feeds: its output
  (`APP/PROC/WEB` OUT / ERR), on a wasi wicket its requests (`RTR`, in the gorouter's access log
  format), each naming its instance, and the lines lost on the way (`LGR`). The cf-api
  watches its apps' hosts and resumes past the last line it took (kept in `state.json`); its
  log names the flume on start (`flume none` without one), and the hosts each watch opened for
  (`cf logs: watching the flume <url> for <hosts>`): a request whose `Host` is none of them is in
  no app's log. See [Live access log](../.kb/live-access-log.md#wickets-and-the-cf-api)
- auth: see [Login](#login). Every request but the root document, `/v3`, `/v3/info` and the
  UAA endpoints needs a valid token (log-cache's included), and its holder may what its
  [roles](#organizations-spaces-and-roles) say
- errors are in CC's shape (`{"errors": [{"code", "title", "detail"}]}`); `/v3/info` names
  this API
- connections: 64 at once. TCP keepalive ends one whose client is gone, and a client that leaves
  one waiting for 60s, in its TLS handshake, not sending a request or not reading its answer, loses
  it; the CLI opens another when it needs one. All taken, one without a request with a valid token
  gives way to the next, the source holding the most first
  ([details](../.kb/connection-limits.md#the-wickets-consoles-and-sluice-wicket-cf-api)); with none,
  the next waits in the backlog

## Login

The cf-api is its own UAA: `POST /uaa/oauth/token` answers a JWT (HS256) signed with the key in
`--data-dir` (`jwt.key`), good for 12 hours, with a refresh token good for 30 days and once: a
refresh answers a new pair and the old refresh token is spent (`sessions.json` keeps the logins,
so they outlive a restart). Who gets a token is said at the start, one or the other, or it does
not start:

- with `--any-credentials`, anyone: the password, client_credentials and refresh_token grants
  take any credentials (`cf login -u me -p x`). The listener then binds a loopback address only,
  which guards nothing once something relays to it: a route of a Firecracker wicket it runs in, a
  sluice client, a proxy. The cf-api cannot tell, so the flag is required wherever it runs, and who
  reaches it is yours to keep
- with `--oidc-issuer`, the users an OpenID Provider signs in and the clients configured. The
  listener may bind any address, with TLS of its own (`--tls-cert`, `--tls-key`) or behind a
  TLS proxy:
  - `cf login --sso` asks for a code and names where to get one, `<API URL>/uaa/passcode`
    (`cf login` without `--sso`, and `-u -p`, are refused with that hint). The page sends the
    browser to the provider (authorization code flow with PKCE and a nonce) and the callback,
    `<API URL>/uaa/callback`, exchanges the code for the ID token. The token comes straight from
    the provider's token endpoint, over TLS and against the client's secret, so its claims are
    taken as they come (OpenID Connect Core 3.1.3.7, item 6) once `iss`, `aud`, `exp` and the
    nonce check out; the `--oidc-claim` rules then say who may log in (`403` with the claim
    that failed), and the page shows an 8-character code, good for 5 minutes and once. The CLI
    posts it (`grant_type=password&passcode=`) and gets the cf-api's tokens; the user name is
    the ID token's `preferred_username`, else `email`, else `sub`
  - `cf auth <id> <secret> --client-credentials` (CI) takes a client of `--client-credentials`
  - a refresh of a user's tokens applies the `--oidc-claim` rules again to the ID token's claims
    of the login: a user no longer allowed is refused (`401`: `<user> is no longer allowed
    (...)`) and logged out. The access token stays good until it expires (12 hours at most)
  - the tokens of the open mode are refused once a provider is configured: everyone logs in
    again

The provider needs a client (confidential, or public with PKCE) whose redirect URI is `<API
URL>/uaa/callback` -- the URL `cf api` targets, as the links are made from the request's
`Host` (and `X-Forwarded-Proto`) -- with the scopes `openid`, `email`, `profile` (and `groups`
for Dex's groups); the discovery document must be reachable from the cf-api, which finds it at
the first sign-in and keeps it. Dex, Keycloak, Entra ID, Google and the like serve; the
[Docker Compose](#docker-compose) sample runs Dex with its users in a file.

## Organizations, spaces and roles

`cf create-org`, `create-space`, `target`, `rename-org`, `rename-space`, `delete-org`,
`delete-space`, `set-org-role`, `set-space-role`, `unset-org-role`, `unset-space-role`,
`org-users` and `space-users` work as on CF. The first start makes `--org` and its `--space`; a
state written before there were more takes its apps, routes and service instances into them.

- **Admins.** An admin sees everything, makes the organizations and may do anything in them, but
  edit the [security groups](#security-groups) and the [service brokers](#service-brokers), which
  take their own editors. `--admin` names who (a claim rule, as `--oidc-claim`), `--admin-client`
  which clients, `--group-role <group>=admin` which groups; with `--oidc-issuer` one of them is
  required. With `--any-credentials` and neither `--admin` nor `--admin-client`, whoever logs in is
  an admin; with them, the claims are `user_name` and `email`, both the name given at `cf login`
- **Roles.** As CC's: a space's developer pushes, deletes and binds, and reads the environment
  and the credentials; its supporter starts, stops, scales, stages and maps routes; its auditor
  and manager read the rest. A space's manager renames it and gives its roles; an organization's
  manager does the same for all its spaces, makes and deletes them, and gives the organization's
  roles. A space role needs a role in the organization (the CLI gives `organization_user` first).
  An organization or a space none of one's roles names is not seen (`404`), one seen but not
  allowed is refused (`403`). A role names a user by the name logged in with (through a provider,
  the ID token's `preferred_username`, else `email`, else `sub`), a client by its id; giving one is
  logged (`ORGS: carol: gave dave the role space_developer at <guid>`)
- **A group's roles.** `--group-role <group>=<role>` gives the members of a group of the provider,
  as the ID token's `--oidc-groups-claim` (default `groups`) lists them, a role at each request:
  `admin`, `organization_<manager|billing_manager|auditor|user>@<org>` or
  `space_<developer|supporter|manager|auditor>@<org>/<space>`, by name (a group's name may hold `=`,
  not `,`). They follow the claims of the login (a refresh reads them again), and are not listed
  in `cf org-users` / `space-users`
- **Deleting.** `cf delete-space` deletes its apps (gone from the wicket), routes, service
  instances (a keyvalue one's data, as `cf delete-service`), space-scoped brokers and roles;
  refused while it holds a broker's instance (`cf delete-service` it first). `cf delete-org`
  deletes its spaces so, and takes it off the plans visible to it
- **What is shared.** The shared domains, the stack and the wicket's offerings are every
  space's. A host is one route's, of one space: a route taken in another is refused. A broker's
  plan is visible as `cf enable-service-access` says: to everyone, to the organizations named
  (`-o`), or, a space-scoped broker's, to its space

## Limits

Refused with why (422): route paths, ports and wildcards, private and internal domains, tasks,
ssh, sidecars, process types but `web`, route services, syslog drains, environment variable
groups, service keys, sharing a service instance with another space, an organization made by
other than an admin, more than 1000 instances of an app, a deployment of a revision (there are
none: `cf rollback` finds none). One quota, of no limit. One owner (`cf`) per wicket: two cf-apis
must not share one. The wickets of one cf-api run apps one way, and are told apart by their names
alone: two names of one console behind two URLs defeat the spread. No session affinity (the
gorouter's `JSESSIONID` / `__VCAP_ID__`): a request goes to any instance of its app, so what a
session holds belongs in a service.

## Tests

The integration test (`tests/cli.rs`) runs the OSS CLI -- `SLUICE_CF_CLI`, else `cf8` on the
PATH, never a `cf` that may be the Tanzu one -- against the cf-api and a wicket in local mode
on a flume, then two, building the wicket and its example guests from `../sluice-wicket-runtime-wasi` and the
flume from `../sluice-flume` (or taking `SLUICE_WICKET_RUNTIME_WASI`, `SLUICE_WICKET_GUESTS` and
`SLUICE_FLUME`); the login through a provider runs against a mock of one in the test, with TLS
of the cf-api's own, and the images against a registry in the test (`tests/registry`). Where
`/dev/kvm` is usable, with Firecracker and a guest kernel (as the wicket's VM test finds them), a
second test runs against `../sluice-wicket-runtime-firecracker` (`sg kvm -c 'cargo test'` until
the `kvm` group is in the login), then runs the cf-api itself on it, in a VM with a disk, the flume
and a provider through its egress, across a restart, a wicket killed right after a push and an
upgrade:

```text
gh release download v8.19.0 -R cloudfoundry/cli -p 'cf8-cli_8.19.0_macosarm.tgz'
tar xzf cf8-cli_8.19.0_macosarm.tgz
SLUICE_CF_CLI=$PWD/cf cargo test
```

On an Apple silicon Mac, the Firecracker test in its Lima VM
([On a Mac](../sluice-wicket-runtime-firecracker/README.md#on-a-mac)):

```text
../sluice-wicket-runtime-firecracker/scripts/lima.sh bash -c \
  'cd ../sluice-wicket-cf-api && cargo test --locked --test cli the_cf_cli_deploys_to_a_firecracker_wicket'
```
