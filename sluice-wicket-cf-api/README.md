# sluice-wicket-cf-api

A Cloud Foundry API facade for one sluice wicket: the subset of the Cloud Controller v3 API, the
UAA token endpoints and log-cache's read API the stock `cf` CLI uses, on one origin. It keeps the
desired state of its apps and has the wicket serve them through the wicket's app API, so routes,
scaling, rolling updates, stop and start and `cf logs` work as the CLI expects. Users log in
through an OpenID Provider (`cf login --sso`; see [Login](#login)), or, on the loopback, with any
credentials.

What `cf push` deploys depends on the wicket, whose listing names its runtime:

| | [On a wasi wicket](#on-a-wasi-wicket) | [On a Firecracker wicket](#on-a-firecracker-wicket) |
|---|---|---|
| the wicket | [sluice-wicket-runtime-wasi](../sluice-wicket-runtime-wasi/README.md#apps) | [sluice-wicket-runtime-firecracker](../sluice-wicket-runtime-firecracker/README.md#apps) |
| an app | a wasm component: its bits, or an image in an OCI registry | a container image, a CNB image say, or a rootfs in a zip: microVMs |
| the cf-api | a process or a container next to the wicket | an app of the wicket, in a VM with a disk, or next to it |
| services | the wicket's keyvalue stores and capabilities | none |
| `cf logs` | the app's output and requests | the process's output |

The rest holds for both: [Options](#options), [Mapping](#mapping), [Behavior](#behavior),
[Login](#login), [Limits](#limits), [Tests](#tests).

```text
cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> a wicket <--tunnel-- sluice server
                                             ^                             |
                                             +--Watch-- sluice-flume <--Feed  (cf logs, optional)
```

The `cf` here is the OSS CLI ([cloudfoundry/cli](https://github.com/cloudfoundry/cli) v8, whose
archive holds it as `cf` and `cf8`); the Tanzu distribution (`cf` 10.x) reads `/v3/info` and
refuses any other platform.

## On a wasi wicket

A wasm component deployed with `cf push`, from its bits or an image in an OCI registry
(`--docker-image`), with the wicket's keyvalue stores and capabilities as
[services](#services).

[![cf login --sso through Dex, cf push, routes, scale, environment variables, a keyvalue service and cf logs against a wicket in local mode](demo/sluice-cf-demo.gif)](demo/sluice-cf-demo.mp4)

The GIF links to the [MP4](demo/sluice-cf-demo.mp4); both are recorded by `demo/record.sh`
([demo/README.md](demo/README.md)).

### Run

```text
cargo build --release                                # needs protoc, and git (the build embeds the commit)

# optional, for the apps' output and requests in cf logs: a flume (see sluice-flume)
sluice-flume --token FLUME_SECRET                     # grpc://0.0.0.0:8002

# the wicket, serving on 8080 with its console on 9090 (see its README)
sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

# the cf-api, on http://127.0.0.1:9091
target/release/sluice-wicket-cf-api --wicket http://127.0.0.1:9090 --wicket-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

cf login -a http://127.0.0.1:9091 -u me -p x   # any credentials: no --oidc-issuer (see Login)
cf push hello -p hello/                        # hello/ holds hello.wasm (a wasi:http 0.3 component)
curl http://hello.localhost:8080/              # the wicket's --listen; *.localhost is the loopback
# an image (see Images): the wicket's hello example, as its CI publishes it
cf push img --docker-image ghcr.io/making/sluice/sluice-wicket-runtime-wasi/examples/hello
cf map-route hello localhost --hostname hi
cf scale hello -i 2
cf push hello -p hello/ --strategy rolling     # swapped in place
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
group and allow the group (`groups=devs`, with the scopes `openid,email,profile,groups`); a Dex
[connector](https://dexidp.io/docs/connectors/) replaces the static users with GitHub, LDAP,
another OpenID Provider and the like.

### Mapping to components

| CF | wicket |
|---|---|
| package (bits) | the uploaded zip: `cf push` takes a directory or a zip (`-p hello.wasm` fails in the CLI: `not a valid zip file`) |
| package (docker) | an image (`cf push --docker-image`, a manifest's `docker.image`) holding the component as one `application/wasm` layer, as `wkg oci push` and `oras push <ref> hello.wasm:application/wasm` push it (of an index, its `wasm` entry). `--docker-username` with `CF_DOCKER_PASSWORD` are its credentials |
| build | picks the component: the one the web process command names (`-c hello.wasm`, `command:`), else the package's only `.wasm`; of an image, pulls it. It must be a component (`\0asm`, layer 1). Anything else fails the build, naming what it found |
| droplet | that component, by sha256 (`process_types.web` its path; stack `wasip3`, buildpack `wasm`); of an image, the image by digest (`cf app`: `docker image:`), `process_types.web` the repository's last segment `.wasm` |
| web process `instances` | `pool`: warm instances, taking the requests in turn. A request beyond them gets an instance of its own, as on every route: 0 still serves. `cf logs` names the instance (`APP/PROC/WEB/<index>`, the requests' `app_index`): a warm one's `0` to `instances - 1`, one of its own the lowest index free from `instances` up |
| web process `memory` | `memory-mib` |
| annotations `wicket.sluice.ik.am/budget-ms`, `wicket.sluice.ik.am/instances` | `budget-ms`, `instances` (the route's cap; below `pool` the wicket refuses) |
| environment variables (`cf set-env`, a manifest's `env:`), `VCAP_APPLICATION`, `VCAP_SERVICES` | the sandbox's `env`: what the guest's `wasi:cli/environment` answers |
| disk | accepted and shown, no effect |
| service offering `keyvalue`, plan `<store>` | a keyvalue store of the wicket (`keyvalue.stores`, else `local`) |
| service offering `wasi`, plan `<capability>` | a capability of the sandbox's `wasi`: `cli`, `inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp` |
| keyvalue service instance | the namespace `cf/<instance guid>` of its plan's store |
| binding to a keyvalue instance | the sandbox's `keyvalue` map: its identifier (parameter `identifier`, default `""`) -> `<store>/cf/<instance guid>`, and the capability `keyvalue` |
| binding to a wasi instance | the plan's capability; a network one `cli` too |

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
  listing of the wicket). `cf purge-service-instance` drops the bindings with the instance
- `cf rename-service` and `cf update-service -t` apply at once; a plan change, instance
  parameters, service keys, user-provided instances and sharing are refused (422)

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
    "mem": 256,
    "disk": "/data",
    "disk-mib": 4096,
    "egress": ["localhost:9090", "flume:8002"],
    "env": {
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
cf scale hello -i 2                              # a pool of 2 VMs, cloned from a snapshot
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

- **who reaches it**: without a provider the cf-api takes any credentials, and that it listens on
  its VM's loopback protects nothing here: the wicket relays its route to whoever reaches
  `--listen`, or the sluice server with `--server`, saying `Host: cf.localhost`. Keep them on the
  host's loopback as above until it has a provider: `..._OIDC_ISSUER`, `..._OIDC_CLIENT_ID`,
  `..._OIDC_CLIENT_SECRET`, `..._OIDC_CLAIM` in `env` (see [Login](#login)), the provider's
  `host:port` in `egress`. Then `-p 8080:8080`, `cf login --sso`
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
- **`mem`**: an upload, a package and a droplet go through files, not memory: 128 MiB took a push
  of a 150 MiB zip. **`disk-mib`**: an image's droplet is a reference, a zip's is kept as a file
  for each app (that one, the one before, and its package, with the upload while it comes)
- **an upgrade**: `PUT /api/images` of the new image, then this app with its `sha256` (or another
  env): the old VM stops (SIGTERM, all written out), the new one boots on the disk: 0.7-0.8s of
  no API, the CLI's login still good, the wicket's apps served all along. A new image that does
  not start is refused (422, 11.5s), the old VM started again
- **a backup**: [the disk's](../sluice-wicket-runtime-firecracker/README.md#disks), whose file is
  `deployed/disks/ops/cf-api.ext4` in the volume. Lost, the apps serve on, a new cf-api leaves
  them (see Behavior)
- **a crash of the cf-api** (not a stop) leaves its VM up without it, the wicket's stderr saying
  `guest: /usr/local/bin/sluice-wicket-cf-api exited`: a PUT with another env starts it again.
  The apps serve on meanwhile

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
cf scale hello -i 2                        # a pool of 2 VMs, cloned from a snapshot
# in a browser, http://127.0.0.1:9090/: the wicket's console, signed in through Dex too
```

`docker compose down` keeps the volumes: the next `up` serves the apps again, once their VMs have
booted, with the logins (`sessions.json` in the cf-api's). For other users, add them to
`staticPasswords` and to both `..._OIDC_CLAIM`s (`email=admin@example.com,email=dev@example.com`), or
give them a group and allow the group (`groups=devs`, with the scopes `openid,email,profile,groups`);
a Dex [connector](https://dexidp.io/docs/connectors/) replaces the static users with GitHub, LDAP,
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
| droplet | the rootfs by sha256 (stack `firecracker`, buildpack `rootfs`; an image's has neither, as on CF): the wicket keeps an image's, the cf-api its reference pinned to the manifest's digest, and has it made again when the wicket lost it (the same digest, unless the wicket's guest changed) |
| web process `instances` | 1: a shared VM; more: `mode=pool`, that many VMs cloned from a snapshot; 0: a VM of its own per connection, killed when it ends |
| web process `memory` | `mem`, 1G by default as on CF: less fails a CNB Java image's memory calculator at its start. A pool's snapshot takes that much of the host's memory (`--tmpfs /run/sluice`): `cf scale -m` a small app |
| web process `health_check` `timeout` (`cf push -t`) | `boot-ms`, 60s by default and 180s at most as on CF: how long the process may take to listen, in the request that starts it. A process exiting first is refused at once |
| environment variables, `VCAP_APPLICATION`, `VCAP_SERVICES` | the sandbox's `env`: the process's environment, over the image's (its `PORT` is `/init`'s) |
| disk | no effect: the rootfs is read-only and `/tmp` a tmpfs of the VM's memory; a persistent disk is the wicket API's, with no CF counterpart |
| services | none: the listing offers neither capabilities nor keyvalue stores |
| outbound connections | none: the wicket's `egress` has no CF counterpart here, an app reaches no other host |
| `cf logs` | the process's output, instance 0; no `RTR` line: this wicket feeds no requests to the flume |
| annotations | no effect: a VM's sandbox is its `mode`, `mem`, `vms` and `env` |

## Options

| option | |
|---|---|
| `--listen <host:port>` | bind address (default `127.0.0.1:9091`); without `--oidc-issuer` a loopback address only, as the API takes any credentials then |
| `--wicket <url>` | the wicket's console, `http://host:port` |
| `--wicket-token <token>` / `--wicket-token-file <path>` | its `--console-token` |
| `--domain <name>` | a shared domain; repeatable (or comma-separated): the first takes the default and random routes (default `localhost`). Through a sluice server, a suffix whose wildcard DNS reaches it |
| `--data-dir <path>` | the state and the blobs (default `cf-api`): `state.json` (written by a rename on every change), `jwt.key` and `sessions.json` (the logins), `packages/<guid>.zip` (the newest per app), `droplets/<sha256>.wasm` (the current and the previous per app) |
| `--org <name>` / `--space <name>` | the one organization and space (default `sluice` / `wicket`) |
| `--flume <url>` | the sluice-flume the wicket feeds (`grpc://host:port` / `grpcs://host:port`): the apps' output and requests reach `cf logs` from it |
| `--flume-token <token>` / `--flume-token-file <path>` | its bearer token |
| `--flume-ca-cert <path>` / `--flume-insecure` | a `grpcs://` flume verified against this PEM CA (default the webPKI roots), or not at all |
| `--oidc-issuer <url>` | the OpenID Provider users log in through (`cf login --sso`); its discovery document is at `<url>/.well-known/openid-configuration` |
| `--oidc-client-id <id>` | the client registered with the provider, whose redirect URI is `<API URL>/uaa/callback` (required with `--oidc-issuer`) |
| `--oidc-client-secret <secret>` / `--oidc-client-secret-file <path>` | its secret; none = a public client (PKCE only) |
| `--oidc-claim <claim>=<pattern>` | who may log in: the ID token's claim must hold (a list claim: contain) the pattern, `*` matching any run of characters; repeatable (or comma-separated). One claim's patterns are alternatives, distinct claims must all hold. At least one rule is required with `--oidc-issuer` (`email=*` lets anyone the provider knows in) |
| `--oidc-scopes <list>` | the scopes asked for, comma-separated (default `openid,email,profile`; Dex sends the `groups` claim for the scope `groups`) |
| `--oidc-ca-cert <path>` | a PEM CA the provider's certificate is verified against (default the webPKI roots) |
| `--client-credentials <id>:<secret>` / `--client-credentials-file <path>` | a client of `cf auth <id> <secret> --client-credentials` (CI); repeatable, the file one per line. With `--oidc-issuer` only |
| `--tls-cert <path>` / `--tls-key <path>` | serve TLS: the certificate chain and its private key, PEM. Without them, behind a TLS proxy, `X-Forwarded-Proto: https` makes the links `https` |
| `--exit-on-stdin-close` | exit when stdin reaches its end (a parent's child process) |

Each option also reads `SLUICE_WICKET_CF_API_<NAME>` from the environment. The image
`ghcr.io/making/sluice/sluice-wicket-cf-api` (`latest` / `<sha>`, built from
`Dockerfile` at the repository root) keeps `/data` as the working directory; without
`--oidc-issuer` it listens on the loopback, so it is reached from the wicket's network
namespace (`--network host`, a sidecar, `kubectl port-forward`).

## Mapping

What holds on both wickets; a component's and a VM's are in
[Mapping to components](#mapping-to-components) and [Mapping to VMs](#mapping-to-vms).

| CF | wicket |
|---|---|
| app, started, with a droplet and a route | an app named by its guid, owner `cf`: its routes' hosts, the droplet's component, the sandbox below |
| app stopped, or without a route | no app in the wicket |
| route `<host>.<domain>` | a host: labels of `a-z`, `0-9` and `-`. No path, port or wildcard (422). A route serves one app: a second destination is refused; zero-downtime updates are `--strategy rolling` |
| shared domain | `--domain` |
| instance state | `RUNNING` once the wicket took the app; `CRASHED` with the wicket's refusal as `details` (`cf push` prints it: `Error starting instances: '...'`); `STARTING` while the wicket is unreachable |
| rolling deployment | the app put with the new droplet: the wicket swaps it in place; finalized at once |
| org / space | one of each |
| health check, log rate, stack, buildpacks | accepted and shown, no effect |

## Behavior

- as on CF, routes and `instances` apply at once; the droplet and the rest of the sandbox
  (memory, annotations, the service bindings, the environment variables) at a start, restart,
  restage or deployment: the running app is the snapshot taken then. For `scale -m` the CLI
  stops and starts the app itself
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
- an edit the wicket answers at once (map-route, unmap-route, scale -i, a manifest's routes)
  is refused with 422 and the wicket's reason, and changes nothing; a start, restart or
  deployment is accepted, and its outcome shows in the process stats and the log (`cf logs`)
- the desired state is the cf-api's: at startup and whenever the wicket's revision moves
  (`GET /api/apps?after=`), it lists the wicket's apps of its owner and puts / deletes the
  difference (the same app again changes nothing: the wicket's generation stays). The wicket
  serves on while the cf-api is down; with the wicket down, the apps show `STARTING` and are
  put when it is back, and an app deleted meanwhile (or whose removal it refused) is removed then
- an app of its owner the state does not know is left where it is: a new or lost `--data-dir`
  removes nothing. It is logged (`CELL: left in the wicket`), still serves, and keeps its hosts
  (a push of one is refused, naming it): `DELETE /api/apps/<guid>?owner=cf` of the wicket's
  console removes it. The login's key goes with the directory too: the CLI logs in again
- `cf logs` (log-cache `read`, polled by the CLI): an in-memory ring of 1000 lines per app of
  the cf-api's own lines: staging (`STG`), lifecycle (`API`), the wicket's outcome (`CELL`).
  With `--flume`, the app's own too, from the live access log the wicket feeds: its output
  (`APP/PROC/WEB` OUT / ERR), on a wasi wicket its requests (`RTR`, in the gorouter's access log
  format), each naming its instance, and the lines lost on the way (`LGR`). The cf-api
  watches its apps' hosts and resumes past the last line it took (kept in `state.json`); see
  [Live access log](../.kb/live-access-log.md#wickets-and-the-cf-api)
- auth: see [Login](#login). Every request but the root document, `/v3`, `/v3/info` and the
  UAA endpoints needs a valid token (log-cache's included), and whoever holds one may do
  everything
- errors are in CC's shape (`{"errors": [{"code", "title", "detail"}]}`); `/v3/info` names
  this API

## Login

The cf-api is its own UAA: `POST /uaa/oauth/token` answers a JWT (HS256) signed with the key in
`--data-dir` (`jwt.key`), good for 12 hours, with a refresh token good for 30 days and once: a
refresh answers a new pair and the old refresh token is spent (`sessions.json` keeps the logins,
so they outlive a restart). Who gets a token:

- without `--oidc-issuer`, anyone: the password, client_credentials and refresh_token grants
  take any credentials (`cf login -u me -p x`). That is why the listener then binds a loopback
  address only
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

## Limits

Refused with why (422): route paths, ports and wildcards, private and internal domains, tasks,
ssh, sidecars, process types but `web`, route services, environment variable groups,
user-provided services, service keys. One owner (`cf`) per wicket: two cf-apis must not share one.
No roles: whoever may log in may do everything.

## Tests

The integration test (`tests/cli.rs`) runs the OSS CLI -- `SLUICE_CF_CLI`, else `cf8` on the
PATH, never a `cf` that may be the Tanzu one -- against the cf-api and a wicket in local mode
on a flume, building the wicket and its example guests from `../sluice-wicket-runtime-wasi` and the
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
