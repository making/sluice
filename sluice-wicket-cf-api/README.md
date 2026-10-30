# sluice-wicket-cf-api

A Cloud Foundry API facade for one [sluice-wicket-wasi](../sluice-wicket-wasi/README.md): the
subset of the Cloud Controller v3 API, the UAA token endpoints and log-cache's read API the
stock `cf` CLI uses, on one origin. It keeps the desired state of its apps and has the wicket
serve them through the wicket's [app API](../sluice-wicket-wasi/README.md#apps), so a wasm
component is deployed with `cf push`, and routes, scaling, rolling updates, stop and start,
services (the wicket's keyvalue stores and capabilities) and `cf logs` work as the CLI expects.
Users log in through an OpenID Provider (`cf login --sso`; see [Login](#login)), or, on the
loopback, with any credentials.

```text
cf CLI --CC v3 / UAA / log-cache--> sluice-wicket-cf-api --/api/apps--> sluice-wicket-wasi <--tunnel-- sluice server
                                             ^                                  |
                                             +--Watch-- sluice-flume <--Feed----+  (cf logs, optional)
```

The `cf` here is the OSS CLI ([cloudfoundry/cli](https://github.com/cloudfoundry/cli) v8, whose
archive holds it as `cf` and `cf8`); the Tanzu distribution (`cf` 10.x) reads `/v3/info` and
refuses any other platform.

[![cf login --sso through Dex, cf push, routes, scale, environment variables, a keyvalue service and cf logs against a wicket in local mode](demo/sluice-cf-demo.gif)](demo/sluice-cf-demo.mp4)

The GIF links to the [MP4](demo/sluice-cf-demo.mp4); both are recorded by `demo/record.sh`
([demo/README.md](demo/README.md)).

## Run

```text
cargo build --release                                # needs protoc, and git (the build embeds the commit)

# optional, for the apps' output and requests in cf logs: a flume (see sluice-flume)
sluice-flume --token FLUME_SECRET                     # grpc://0.0.0.0:8002

# the wicket, serving on 8080 with its console on 9090 (see its README)
sluice-wicket-wasi --listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

# the cf-api, on http://127.0.0.1:9091
target/release/sluice-wicket-cf-api --wicket http://127.0.0.1:9090 --wicket-token SECRET \
  --flume grpc://127.0.0.1:8002 --flume-token FLUME_SECRET

cf login -a http://127.0.0.1:9091 -u me -p x   # any credentials: no --oidc-issuer (see Login)
cf push hello -p hello/                        # hello/ holds hello.wasm (a wasi:http 0.3 component)
curl http://hello.localhost:8080/              # the wicket's --listen; *.localhost is the loopback
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

### Docker Compose

The images on one host, with Redis holding the apps' keyvalue data and [Dex](https://dexidp.io/)
as the OpenID Provider of the cf-api and of the wicket's console alike, its users in its
configuration file. The apps are published on 28000 (8080 and its usual stand-ins are often taken), the
cf-api (9091), the console (9090) and Dex (5556) on the host's loopback: `cf` and the browser run on that host (Docker Engine on
Linux, Docker Desktop or OrbStack on macOS) or through an SSH tunnel
(`ssh -L 9091:127.0.0.1:9091 -L 9090:127.0.0.1:9090 -L 5556:127.0.0.1:5556 <host>`). Dex is
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
    image: ghcr.io/making/sluice/sluice-wicket-wasi
    pull_policy: always
    command: [--config, /etc/sluice/wicket.yaml]
    environment:                    # what wicket.yaml's ${...} take
      CONSOLE_TOKEN: ${CONSOLE_TOKEN:?CONSOLE_TOKEN}
      FLUME_TOKEN: ${FLUME_TOKEN:?FLUME_TOKEN}
      REDIS_PASSWORD: ${REDIS_PASSWORD:?REDIS_PASSWORD}
      OIDC_CLIENT_SECRET: ${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET}
    ports:
      - "28000:8080"                # the apps
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
listen: 0.0.0.0:8080            # the apps; with server and token also through a sluice server
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
open http://127.0.0.1:9090/                # the wicket's console: signed in through Dex too
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

## Mapping

| CF | wicket |
|---|---|
| app, started, with a droplet and a route | an app named by its guid, owner `cf`: its routes' hosts, the droplet's component, the sandbox below |
| app stopped, or without a route | no app in the wicket |
| route `<host>.<domain>` | a host: labels of `a-z`, `0-9` and `-`. No path, port or wildcard (422). A route serves one app: a second destination is refused; zero-downtime updates are `--strategy rolling` |
| shared domain | `--domain` |
| package (bits) | the uploaded zip: `cf push` takes a directory or a zip (`-p hello.wasm` fails in the CLI: `not a valid zip file`) |
| build | picks the component: the one the web process command names (`-c hello.wasm`, `command:`), else the package's only `.wasm`; it must be a component (`\0asm`, layer 1). Anything else fails the build, naming what it found |
| droplet | that component, by sha256 (`process_types.web` its path; stack `wasip3`, buildpack `wasm`) |
| web process `instances` | `pool`: warm instances. A request beyond them gets an instance of its own, as on every route: 0 still serves |
| web process `memory` | `memory-mib` |
| annotations `wicket.sluice.ik.am/budget-ms`, `wicket.sluice.ik.am/instances` | `budget-ms`, `instances` (the route's cap; below `pool` the wicket refuses) |
| environment variables (`cf set-env`, a manifest's `env:`), `VCAP_APPLICATION`, `VCAP_SERVICES` | the sandbox's `env`: what the guest's `wasi:cli/environment` answers |
| disk, health check, log rate, stack, buildpacks | accepted and shown, no effect |
| instance state | `RUNNING` once the wicket took the app; `CRASHED` with the wicket's refusal as `details` (`cf push` prints it: `Error starting instances: '...'`); `STARTING` while the wicket is unreachable |
| rolling deployment | the app put with the new droplet: the wicket swaps it in place; finalized at once |
| org / space | one of each |
| service offering `keyvalue`, plan `<store>` | a keyvalue store of the wicket (`keyvalue.stores`, else `local`) |
| service offering `wasi`, plan `<capability>` | a capability of the sandbox's `wasi`: `cli`, `inherit-network`, `allow-ip-name-lookup`, `tcp`, `udp` |
| keyvalue service instance | the namespace `cf/<instance guid>` of its plan's store |
| binding to a keyvalue instance | the sandbox's `keyvalue` map: its identifier (parameter `identifier`, default `""`) -> `<store>/cf/<instance guid>`, and the capability `keyvalue` |
| binding to a wasi instance | the plan's capability; a network one `cli` too |

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
  (0600): `state.json` holds the values
- an edit the wicket answers at once (map-route, unmap-route, scale -i, a manifest's routes)
  is refused with 422 and the wicket's reason, and changes nothing; a start, restart or
  deployment is accepted, and its outcome shows in the process stats and the log (`cf logs`)
- the desired state is the cf-api's: at startup and whenever the wicket's revision moves
  (`GET /api/apps?after=`), it lists the wicket's apps of its owner and puts / deletes the
  difference (the same app again changes nothing: the wicket's generation stays). The wicket
  serves on while the cf-api is down; with the wicket down, the apps show `STARTING` and are
  put when it is back
- `cf logs` (log-cache `read`, polled by the CLI): an in-memory ring of 1000 lines per app of
  the cf-api's own lines: staging (`STG`), lifecycle (`API`), the wicket's outcome (`CELL`).
  With `--flume`, the app's own too, from the live access log the wicket feeds: its output
  (`APP/PROC/WEB` OUT / ERR), its requests (`RTR`, in the gorouter's access log format) and
  the lines lost on the way (`LGR`). The cf-api watches its apps' hosts and resumes past the
  last line it took (kept in `state.json`); see
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

## Services

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

## Limits

Refused with why (422): route paths, ports and wildcards, private and internal domains, docker
images, tasks, ssh, sidecars, process types but `web`, route services, environment variable
groups, user-provided services, service keys. One owner (`cf`) per wicket: two cf-apis must not
share one. No roles: whoever may log in may do everything.

The integration test (`tests/cli.rs`) runs the OSS CLI -- `SLUICE_CF_CLI`, else `cf8` on the
PATH, never a `cf` that may be the Tanzu one -- against the cf-api and a wicket in local mode
on a flume, building the wicket and its example guests from `../sluice-wicket-wasi` and the
flume from `../sluice-flume` (or taking `SLUICE_WICKET_WASI`, `SLUICE_WICKET_GUESTS` and
`SLUICE_FLUME`); the login through a provider runs against a mock of one in the test, with TLS
of the cf-api's own:

```text
gh release download v8.19.0 -R cloudfoundry/cli -p 'cf8-cli_8.19.0_macosarm.tgz'
tar xzf cf8-cli_8.19.0_macosarm.tgz
SLUICE_CF_CLI=$PWD/cf cargo test
```
