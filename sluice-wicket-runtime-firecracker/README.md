# sluice-wicket-runtime-firecracker

> **Experimental.** The API, layout and behavior may change; what is not there yet is listed
> under [Not yet](#not-yet).

A tunnel client serving each route with **Firecracker microVMs**, as `sluice-wicket-runtime-wasi`
serves each with a wasi:http component:

```
sluice server --tunnel--> wicket --vsock, framed--> /init (PID 1) --TCP 127.0.0.1--> process in the microVM
```

The wicket connects to a sluice server like the Java client (the tunnel client is
`../sluice-wicket-tunnel`'s: the node list, a stream per node, flow control, the connection limit,
the drain) and relays each tunnel connection verbatim (no HTTP termination) to a vsock stream
of a guest. The guest's `/init` (`guest/`, static musl) relays it on to the process. A
route's VMs are:

- `mode=shared` (default): one VM booted at start serves every connection
- `mode=per-connection`: each connection gets a VM of its own, cloned from a snapshot of
  a template VM taken once its process accepted (`prime=N` sends N `GET /` first, so the
  clones start warmed up; each must be answered with a 2xx within 10s, or the route's
  start fails); the VM is killed when the connection ends. `warm=N` keeps N
  clones restored ahead
- `mode=reuse`: as per connection, but a clone whose connection ended serves the next one
  (as Lambda keeps a warm execution environment), until it was kept unused for `keep-ms`
  (default 60000), served `uses=N` connections or lived `lifetime-ms`, then it is killed:
  the route scales to zero when idle. It gives up the isolation: a connection sees what the
  previous ones left in the guest (its memory, its files, a session cached). A connection
  failing (the guest's stream cut, `idle-ms`) kills its clone
- `mode=pool`: `vms=N` VMs at most, cloned from the snapshot as per connection, each serving
  many connections as a shared VM; `warm=N` of them (default `vms`: a pool of a fixed size)
  are kept. A connection goes to the VM holding the fewest connections (not requests: a
  keep-alive connection stays on its VM) while it holds fewer than `conns=N` (16); past
  that it waits for a VM being started (one started for every `conns` connections waiting,
  up to `vms`), or goes to the VM holding the fewest when none is. A VM beyond `warm`
  serving none is killed after `keep-ms` (`warm=0` scales to zero); a VM that does not
  answer is killed, the connection served by another, and one started in its place below
  `warm`

The cloned modes (per-connection, reuse, pool) take `prime`, `warm`, `vms` and `wait-ms`.

| mode | VMs | a connection gets | between connections | idle | a VM failing | memory | new connections/s |
|---|---|---|---|---|---|---|---|
| `shared` | 1, booted at start | the VM | one guest for all | the VM stays | every connection fails until a reload | the pages its guest touched | 3,060 (1 vCPU), 7,072 (4) |
| `per-connection` | 1 per open connection, `warm` ahead | a clone of its own, killed with it | a clone each | `warm` clones stay | that connection | the snapshot (`mem`) + a few MiB a clone | 1,600 (`warm=16`) |
| `reuse` | 1 per open connection, kept ones, `warm` ahead | the clone used last, else a fresh one | a clone's connections in turn share its guest | kept clones killed after `keep-ms`: none left | that connection | as per connection | 8,918 |
| `pool` | `warm` to `vms`, many connections each | the VM holding the fewest | a VM's connections share its guest | down to `warm` after `keep-ms` (`warm=0`: none) | its connections; a VM started in its place | as per connection | 7,493 (4 VMs of 1 vCPU) |

The rates are the sample guest's through the server, `oha -c 16 --disable-keepalive` (see
[Findings](#findings)).

The rootfs is either the built-in sample (`scripts/build-guest.sh`: `/init` serves HTTP
itself) or an OCI image converted by `scripts/cnb-rootfs.sh`, e.g. a Cloud Native
Buildpacks image: `/init` starts the image's `Entrypoint` + `Cmd` as its `User` in its
`WorkingDir` with its `Env` (and `PORT`), and opens the vsock port once the process
accepts on `127.0.0.1:$PORT`. The process's output reaches the wicket's stdout / stderr,
and with `--flume` the live access log (see [Output](#output-and-the-live-access-log)). A
guest has no network device: it connects to the destinations its route allows through
the wicket (see [Egress](#egress)).

In [jail mode](#jail-mode) (`--jail-ids`, started as root) every VM runs in a jail of its
own: an id, namespaces, a root and a cgroup of its own, Firecracker's `jailer`'s isolation.

## Layout

Two Cargo workspaces:

- `.` -- the wicket: `src/lib.rs` wires the modules, `src/main.rs` reads the command line
  (`config.rs`); `routes.rs` the route table, its reload and the relay of a connection,
  `route.rs` a route's VMs, `capacity.rs` their budget, `vm.rs` one Firecracker VM,
  `output.rs` the VMs' output, `egress.rs` their egress, `wire.rs` the vsock framing, the
  restore notice and the egress rules,
  `listener.rs` `--listen` (`head.rs` the request head routing a connection, `hpack.rs`
  an h2c one's), `console.rs` the console (its page in `src/console/`), `deployed.rs` its
  routes and apps kept, `disk.rs` an app's disk, `image.rs` an OCI image pulled, `rootfs.rs`
  its layers as an ext4 image, `state.rs` the state dir, `seccomp.rs` the seccomp filters;
  jail mode's `keeper.rs` (the privileged half, and the wicket's client of it) and
  `jail.rs` (one VM's jail, the VMs' cgroups)
- `guest/` -- the VMs' `/init`, built for `x86_64-unknown-linux-musl` or
  `aarch64-unknown-linux-musl` (its `rust-toolchain.toml` has rustup add both, rust-lld
  links the other arch's): `log.rs` the processes' output, `egress.rs` their egress,
  `reaper.rs` PID 1's reaping; `guest/src/wire.rs` is a copy of `src/wire.rs`: keep them
  alike; `src/bin/sample-process.rs` what the tests' images run
- `scripts/` -- `build-guest.sh` (the sample rootfs), `cnb-rootfs.sh` (an OCI image's),
  `guest.sh` (the guest, which both build), `try.sh` (the debug build on `--listen`),
  `lima.sh` (a command in a VM of a Mac, see [On a Mac](#on-a-mac))
- `tests/vm.rs` -- the VM test (see [Tests](#tests))

## Setup

x86_64 or aarch64 Linux with KVM (bare metal, or a VM with nested virtualization):
`/dev/kvm` must exist. Ubuntu 24.04 on x86_64 was used; the versions below are the ones
tested (aarch64: see [aarch64](#aarch64); a Mac: [On a Mac](#on-a-mac)). The
[Docker image](#docker-image) holds steps 2 and 3. A host serving others: also
[Host setup](#host-setup).

1. Access to `/dev/kvm` through the `kvm` group (an ACL set by hand is rewritten by udev):

   ```text
   sudo usermod -aG kvm $USER     # effective at the next login; until then: sg kvm -c '...'
   ```

2. Firecracker (v1.17.0) from its release, checksum verified:

   ```text
   FC=v1.17.0 ARCH=$(uname -m)
   curl -fsSLO https://github.com/firecracker-microvm/firecracker/releases/download/$FC/firecracker-$FC-$ARCH.tgz
   curl -fsSL https://github.com/firecracker-microvm/firecracker/releases/download/$FC/firecracker-$FC-$ARCH.tgz.sha256.txt | sha256sum -c
   tar xzf firecracker-$FC-$ARCH.tgz
   install -D release-$FC-$ARCH/firecracker-$FC-$ARCH ~/.local/bin/firecracker
   ~/.local/bin/firecracker --version
   ```

3. A guest kernel, the latest of Firecracker's CI builds (6.18.56 was used; it has
   `CONFIG_VIRTIO_VSOCKETS`, `CONFIG_VMGENID` and, on x86_64, kvm-clock):

   ```text
   S3=https://s3.amazonaws.com/spec.ccfc.min ARCH=$(uname -m)
   CI=$(curl -fsSL "$S3?list-type=2&prefix=firecracker-ci/&delimiter=/" \
       | grep -oP "(?<=<Prefix>)firecracker-ci/[0-9]{8}-[^/]+/(?=</Prefix>)" | sort | tail -1)
   KERNEL=$(curl -fsSL "$S3?list-type=2&prefix=${CI}$ARCH/vmlinux-" \
       | grep -oP "(?<=<Key>)${CI}$ARCH/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3}(?=</Key>)" | sort -V | tail -1)
   mkdir -p ~/.local/firecracker && curl -fsSL -o ~/.local/firecracker/vmlinux "$S3/$KERNEL"
   ```

4. Build tools: `protoc` (the tunnel proto), `mkfs.ext4` 1.43+ (`-d`) and python3:

   ```text
   sudo apt-get install -y protobuf-compiler e2fsprogs python3
   ```

5. For CNB images through the script only (`PUT /api/images` needs neither, see
   [Apps](#apps)): docker, sudo (`cnb-rootfs.sh` keeps the image's file owners), and
   `pack` (v0.40.10):

   ```text
   PACK=v0.40.10
   curl -fsSL https://github.com/buildpacks/pack/releases/download/$PACK/pack-$PACK-linux.tgz | tar xz
   install pack ~/.local/bin/pack
   ```

6. A sluice server to connect to (root README, "Run"), of this version or later (it passes a
   `firecracker:` target through as the dial address): `./mvnw -DskipTests package` from the
   repository root, then
   `java -jar sluice-server/target/sluice-server-0.0.1-SNAPSHOT-exec.jar --sluice.token=SECRET`.

## Run

```text
cargo build --release
scripts/build-guest.sh                                   # -> target/guest.ext4 (sample guest)

# a CNB image as a rootfs (here the repository's sample upstream)
(cd .. && ./mvnw -q -DskipTests -pl sluice-example-upstream -am package)
pack build demo/upstream --builder paketobuildpacks/builder-noble-java-tiny \
    --path ../sluice-example-upstream/target/sluice-example-upstream-0.0.1-SNAPSHOT-exec.jar \
    --env BP_JVM_VERSION=25
scripts/cnb-rootfs.sh demo/upstream target/upstream.ext4 [port]   # port: the process's, default 8080

sg kvm -c "target/release/sluice-wicket-runtime-firecracker \
    --firecracker ~/.local/bin/firecracker --kernel ~/.local/firecracker/vmlinux \
    --server grpc://127.0.0.1:8001 --token SECRET \
    --vm hello.local=target/guest.ext4 \
    --vm 'app.local=target/upstream.ext4?mode=per-connection&mem=512&vcpus=2&prime=200&warm=4'"

curl -H 'Host: hello.local' http://127.0.0.1:8000/
```

The scripts build the guest for the host's arch, a VM's; `SLUICE_GUEST_ARCH=aarch64` (or
`x86_64`) builds a rootfs for the other's (`cnb-rootfs.sh` then takes the image of that
platform, `linux/arm64` or `linux/amd64`).

`--vm host=<rootfs>[?<options>]` adds a route: `mode=shared|per-connection|reuse|pool`,
`vcpus=N` (1), `mem=MiB` (128), `port=N` (the guest's vsock port, 8080), `per-client=N`,
`idle-ms=N`, `cpu=N` (jail mode), for the cloned modes `warm=N`, `prime=N`, `vms=N` and
`wait-ms=N` (see [Limits](#limits)), `keep-ms=N` (reuse, pool), `uses=N` and `lifetime-ms=N`
(reuse), `conns=N` (pool), `egress=<host>:<port>,...` (see [Egress](#egress)). The advertised
target is `firecracker:<rootfs>` plus the options set to other than their defaults; the server
dials it verbatim, and the hosts of one target share its VMs.

`--listen host:port` serves the routes over plain TCP too (without `--server`, all the wicket
does: a local loop for trying a rootfs; `scripts/try.sh <rootfs> '<options>' [port]` runs the
debug build so, on `a.local`, the arguments after the port the wicket's: `--vm
b.local=<rootfs>` a second route). A connection goes to the route of its first request's host
(the `Host` header, or the h2c `:authority`; the port left out, the case ignored), or, with a
single route, to that route whatever its host, nothing read first (its guest may speak first).
An unknown host is answered 404; a connection that is not HTTP, or whose head runs past 64KiB
or takes over 10s, is closed. The head is relayed verbatim with the rest: unlike the server,
the wicket routes a connection once -- a later request on it (an h2 stream, a keep-alive
request) for another host stays on the first one's route -- and adds no `X-Forwarded-*`
header (the guest sees what the client sent: the sample's `x-forwarded-for -`), since only
the first request of a connection would get them. `--max-connections` bounds the listener's
connections too, apart from the tunnel's (one beyond waits in the backlog).
`--firecracker` names the binary (default `firecracker` on the PATH), `--state-dir` the VMs'
files (their Unix sockets need paths below 108 bytes; the default is a directory of the
process's own, 0700, under `$XDG_RUNTIME_DIR` (a tmpfs where systemd sets it), else the
system's temp dir, removed at the end; a `--state-dir` given gets a `route-<n>` directory
(0700) per route started, removed with it). A cloned route's snapshot there is its
template's whole memory, unencrypted: keep the state dir on a tmpfs (the wicket warns
otherwise), or it is on disk until the route ends, and after a crash. The tmpfs must hold
every cloned route's `mem` (systemd sizes `$XDG_RUNTIME_DIR` to 10% of the host's
memory: `RuntimeDirectorySize` in `logind.conf`).

The tunnel's settings are `sluice-wicket-runtime-wasi`'s: `--token` (or `--token-file`; on the
command line every user of the host sees it), `--client-id`, `--ca-cert`,
`--insecure` (`grpcs://`), `--max-connections` (256), `--tunnel-stall-timeout` (60s) and
`--drain-grace` (20s: SIGTERM / SIGINT withdraw the routes and end the streams once their
connections ended; a second signal ends at once). Each also reads
`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>` from the environment, e.g. `SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN`.

`SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE=1` gives the VMs a serial console, the kernel's and
`/init`'s own output into each VM's `firecracker.log` (the processes' reaches the wicket
regardless, see [Output](#output-and-the-live-access-log));
`SLUICE_WICKET_RUNTIME_FIRECRACKER_TRACE=1` logs each clone's restore time.

### Configuration file

`--config <file>` reads the settings and routes from YAML, `${NAME}` (or `${NAME:-default}`)
taken from the environment; a flag wins over the environment, the environment over the file.
Paths are relative to the file; its routes come before the `--vm` flags', and a host is
routed once.

```yaml
server: grpc://127.0.0.1:8001
token: ${SLUICE_TOKEN}           # or token-file: <path>
kernel: /opt/firecracker/vmlinux
firecracker: firecracker        # on the PATH
max-vms: 64
max-memory: 16384               # MiB of guest memory
workers: 4                      # threads relaying the connections (see Findings)
watch-interval: 1s              # 0 = SIGHUP only
drain-grace: 20s
# flume: grpc://127.0.0.1:8002  # see Output and the live access log
# flume-token: ${FLUME_TOKEN}
# jail-ids: 900000-900127       # see Jail mode
# jail-user: sluice
# jail-cgroup: sluice
routes:
  - {host: hello.local, rootfs: guest.ext4}
  - {host: app.local, rootfs: upstream.ext4, mode: per-connection, mem: 512, vcpus: 2,
     prime: 200, warm: 4, vms: 16, wait-ms: 10000, per-client: 4, idle-ms: 30000,
     egress: [db.internal:5432, 10.0.0.0/24:6379, '[2001:db8::/32]:443']}
  - {host: api.local, rootfs: go.ext4, mode: pool, vms: 8, warm: 1, conns: 16, keep-ms: 60000}
  - {host: fn.local, rootfs: go.ext4, mode: reuse, keep-ms: 60000, uses: 1000,
     lifetime-ms: 3600000}
```

### Reloading routes

The routes are reloaded without a restart: their files are polled every `--watch-interval`
(default 1s) -- the config file, every rootfs, the kernel and the firecracker binary -- and
SIGHUP reloads at once. A change is reloaded once a poll found it unchanged since the previous
one, so a file still being written is not read half way. A reload reads the routes again and
starts the VMs of every target that is new or whose spec or files changed (a shared VM booted,
a cloned route's template booted and snapshotted again) before it changes anything; one
failure keeps the routes as they were (`routes reload (file change) failed; keeping
generation N: ...`). Otherwise (`routes reloaded (file change), generation N: added ...;
removed ...; restarted ...`):

- a target unchanged keeps its VMs
- a target restarted serves its next connection from the new VMs; a connection open stays
  on the VMs it got, which end with the last of them. Replace a rootfs by a rename: a VM has
  its file open
- a host added or removed is advertised on every node stream; a removed target is served
  until every stream acknowledged its removal (the server may have routed a connection to it
  before)
- an app's VM on a [disk](#disks) is stopped before its next one starts, last of all

Only the routes, the kernel and the firecracker binary are reloaded; the other settings apply
at startup. The console's edits go through the same reload (see [Console](#console)).

### Console

`--console <host:port>` (`console`) serves a page and a JSON API deploying rootfs images as
routes at once, without a restart: drop an ext4 image on the page (or choose one), give it a
host and the route's options, and it is served -- on the tunnel and on `--listen` alike.
Dropped on a route of the console, an image replaces its own; renamed, a route moves to
another host with its image and options. The routes of the config file and the `--vm` flags
are listed too, read-only. The page shows what the wasm wicket's has no counterpart for:
each route's VMs (shared, warm, kept, pooled, starting, all alive), its connections, a cloned
route's snapshot (its memory file, the template's boot and snapshot times) and the budget
(`--max-vms`, `--max-memory`) taken.

```text
target/debug/sluice-wicket-runtime-firecracker --kernel vmlinux --listen 127.0.0.1:8080 \
    --console 127.0.0.1:9090
# open http://127.0.0.1:9090/ and drop target/guest.ext4 on it: served as guest.localhost
curl http://guest.localhost:8080/   # *.localhost is the loopback, in browsers too
```

A deploy is a reload (see [Reloading routes](#reloading-routes)) of the routes as they would be
with it, kept only when its VMs started: options a route refuses, an image that does not boot,
a host the config file or a flag routes, routes past the budget refuse it with the reason, and
the routes stay as they were. The image is received to the console's directory as it comes
(4 GiB at most), hashed on the way, and refused unless it carries an ext2/3/4 superblock.

The console keeps its routes in `--console-dir` (`console-dir`, default `deployed` next to the
config file, else in the working directory: `/data/deployed` in the image) and serves them again
after a restart:

```text
deployed/routes.yaml                    the routes and apps, in the config file's route shape (and their time)
deployed/rootfs/<sha256>/<name>.ext4    the images they name, never rewritten
deployed/uploads/<sha256>.ext4          the images uploaded by digest, or made, for ten minutes
deployed/builds/                        what the images being made take, removed at startup
deployed/disks/<owner>/<app>.ext4       the apps' disks (see Disks), its owner's only (0600)
```

`routes.yaml` is written once a change started (0600), and an image no route names is
removed. An image's path names its digest, so other content is another target: the route's
VMs start anew, and its old target drains as a removed one does. Without `--console` the
directory is not read. One wicket per directory. In jail mode the console is the wicket's,
run as `--jail-user`: the directory is its user's, and an image is opened by it and passed to
the keeper as the config file's are.

The credentials are the wasm wicket's console's (its README, [Console](../sluice-wicket-runtime-wasi/README.md#console)):
`--console-token <token>` (`console-token`, `SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE_TOKEN`) as
the HTTP Basic password or a Bearer token, or a browser's sign-in through an OpenID Provider
(`--oidc-issuer`, `--oidc-client-id`, `--oidc-client-secret`, `--oidc-claim`, `--oidc-scopes`,
`--oidc-ca-cert`); without either the console must listen on a loopback address and answers
only requests for an IP address or `localhost` (no DNS rebinding). An edit from another origin
is refused.

The API the page uses; `<options>` are the `--vm` route options, all of them (one left out takes
its default):

| request | |
|---|---|
| `GET /api/routes[?after=<revision>]` | every route with its VMs, the budget and the last failed reload of the files, as JSON; with `after` the current `revision`, held until the routes change (25s at most; the VMs' counts move without a revision) |
| `PUT /api/routes/<host>?name=<file>&<options>` | deploys the body, an ext4 image, to the host (replacing the console's route of the host) |
| `PATCH /api/routes/<host>?<options>` | gives the console's route of the host other options |
| `PATCH /api/routes/<host>?host=<to>` | moves the console's route of the host to `<to>` as it is |
| `DELETE /api/routes/<host>` | removes the console's route of the host |

```text
curl -T guest.ext4 -H 'Authorization: Bearer SECRET' \
    'http://127.0.0.1:9090/api/routes/hello.localhost?name=guest.ext4&mode=per-connection&warm=2'
{"generation":2,"message":"added hello.localhost"}
```

An edit is answered 400 (a bad host name, option or image), 404 (no route of the console), 409
(a host the config file or a flag routes; for a move, any routed host), 413, 422 (the routes
did not start with it; the reason follows) or 503 (draining). The options are checked before
the image is read: a client sending `Expect: 100-continue` (curl does for a large file) is
refused before it uploads. The listing names each route's `origin` (`config`, `flag`,
`console`, `api`), its advertised `target`, its `rootfs` (a console route's path in the directory
and its digest), its `options` set to other than their defaults, its `vms` and `snapshot`, an
app's name and owner, and the names of its env.

#### Apps

An app is hosts served by one rootfs in one set of options with an env, put and removed by its
owner: the API of a controller keeping the desired state of its apps (as
`sluice-wicket-cf-api` does), the wasm wicket's
([Apps](../sluice-wicket-runtime-wasi/README.md#apps)) but its sandbox. Its image is uploaded by
digest, or made of an OCI image, first; the app names it:

| request | |
|---|---|
| `PUT /api/components/<sha256>` | keeps the body, an ext4 image of 4 GiB at most, under its digest (400 when the body has another): `{"sha256": .., "bytes": ..}` |
| `PUT /api/images` | makes an ext4 image of the OCI image the body names (below), kept under its digest as an upload: `{"image": <the reference pinned to its digest>, "name": .., "sha256": .., "bytes": ..}` |
| `GET /api/apps[?after=<revision>]` | the apps, as `/api/routes` (held with `after`): `{"revision", "generation", "runtime": "firecracker", "capabilities": [], "keyvalue_stores": [], "apps": [..]}` |
| `PUT /api/apps/<name>` | serves the app of the body (JSON, below) |
| `DELETE /api/apps/<name>?owner=<owner>` | removes the app of `owner` |

```json
{
  "owner": "cf",
  "hosts": ["shop.localhost", "store.localhost"],
  "component": {"sha256": "0954dc11…e8", "name": "shop.ext4"},
  "sandbox": {"mode": "pool", "vms": 2, "mem": 256, "egress": ["db.internal:5432"],
              "env": {"GREETING": "hello"}},
  "tag": "3f1c…9a"
}
```

- the sandbox is the route options of `--vm` in JSON (a number, or a string; `egress` a list),
  each left out taking its default, and its `env`, `name -> value`, over the image's. The
  listings name the variables only (`"env": ["GREETING"]`), never a value
- `tag` (optional) is opaque to the wicket, kept and listed as given: a controller tells by it
  what it put, an env it cannot read back included. A tag or hosts alone start no VM
- a PUT is one reload: every host of the app or none, kept only when its VMs started; the same
  app again changes nothing. An app's target names it (`?app=<name>`), so its VMs are its own,
  whoever else names the image; the env is in no target: another env starts its VMs anew
- 409 and `{"missing": [<sha256>]}` when no image of the digest is kept: upload it and put
  again. An upload is kept for ten minutes, a file an app named as long as an app does
- a host the config file, a flag, a console route or another app routes is refused (409), and so
  is a change of another owner's app; the page lists an app's routes, read-only

**An image of a registry**, `PUT /api/images` with `--guest <path>`
(`SLUICE_WICKET_RUNTIME_FIRECRACKER_GUEST`; the image sets it) and e2fsprogs' `mkfs.ext4` and
`debugfs` (in the image too), no root and no docker. Without `--guest` it is refused (422):

```json
{"image": "ghcr.io/traefik/whoami:v1.11.0", "port": 80}
```

Only `image` is required; `port` is the process's, `PORT` in its environment (default the
lowest the image exposes, else 8080), `username` and `password` a private image's credentials,
`restore` the restore hook's argv (see [Clones](#clones)).
The manifest of the wicket's platform is pulled (of an index, its `linux/<arch>` entry; other
arches are refused), then its layers (tar or gzip; zstd is refused), checked against their
digests: a registry on the loopback over plain HTTP, any other over HTTPS against the webPKI
roots. They unpack over each other as a runtime's do, whiteouts and all, a link never leading
out of the root, within 4 GiB. The image holds what they say of every file, the guest as `/init`
and `/sluice/init.json` from the image's config, as `scripts/cnb-rootfs.sh` writes it. One build
at a time; the request is held until it is done, ten minutes at most (a refusal is a 422 with
the reason). xattrs (file capabilities) are dropped.

The same manifest, guest, port and hook make the same bytes: a build again has the digest it
had, so its target and VMs stay. Another wicket's guest makes another digest.

#### Disks

An app's shared VM may have a disk: a writable drive that outlives it, kept by the app's owner
and name in the console's directory, whatever the image, options or env. The sandbox's `disk`
is where `/init` mounts it before the process starts, `disk-mib` its size (default 1024):

```json
{"owner": "ops", "hosts": ["db.localhost"], "component": {"sha256": "0954dc11…e8", "name": "db.ext4"},
 "sandbox": {"mem": 256, "disk": "/data", "disk-mib": 4096}}
```

- **made** at the app's first start, by `mkfs.ext4` (the wicket needs it, e2fsprogs: in the
  image), its room taken on the host then (`fallocate`): a disk the host has no room for is a
  refused PUT, not an I/O error in the guest later. ext4 with a journal: no fsck runs in a VM,
  and nothing on the host reads what a guest wrote
- **`disk`** is a directory of the image (the rootfs is read-only: `mkdir` works on `/tmp` alone).
  Its root goes to the image's user at each mount; what is below stays as it was, whoever wrote
  it. Refused with the guest's reason (`the guest's disk: ...`, 422) when it cannot mount, and an
  image whose `/init` predates disks is refused too: it would serve without one
- **one VM writes it**: `mode=shared` alone (a clone's snapshot holds its guest's page cache of
  the drive; refused in the other modes), and the file is locked by its `firecracker`
  (`flock`, the kernel's: gone with the process, whatever became of the wicket), so a second
  wicket on the directory, or a VM not yet gone, is waited for and then refused. A change of
  the VM (its image, options, env) is the exception to [Reloading routes](#reloading-routes):
  once every other target started, the old VM is stopped, then the new one started, on the
  same disk. Its connections fail meanwhile, a boot long (measured with the cf-api: 0.7-0.8s).
  A start failing starts the old VM again, the PUT refused as any other (with a process that
  never listens: 11.5s, the 10s of its boot's bound); failing as well, the route serves nothing
  until the next reload (SIGHUP, or the same PUT again), and the refusal says so
- **a stop** tells `/init`: SIGTERM to every process, 10s for them to end (then SIGKILL), what
  is dirty written out, the disk unmounted, the VM killed, 20s at most. So does the wicket's own
  end, after its drain (`--drain-grace` and these 20s: `--stop-timeout`, `TimeoutStopSec`). A VM
  simply killed,
  the wicket's or the host's crash, is a crash to the file system: the journal replays at the
  next mount, and what the process had not fsynced may be gone, up to the commit interval's
  5s. A file written and renamed stays the old one or the new one, not NUL bytes
  (`nodioread_nolock`, see [Findings](#findings)). Guest flushes reach the host's disk
  (`cache_type` `Writeback`)
- **grows** with a PUT of a larger `disk-mib` (the file, then `/init` at its next mount: online,
  0.2s to 64 GiB). A smaller one is refused (422), its VM untouched; one below 8 MiB too (400:
  `mkfs.ext4` makes such a file system without a journal). A start that failed may leave it
  grown, which the old VM then has: the next PUT asks for that much at least
- **stays** as its app goes (`DELETE /api/apps/<name>`), for the next app of that name and
  owner. `GET /api/disks` lists them, `{"disks": [{"owner", "name", "mib", "app": <whether it has
  its app>}]}`; `DELETE /api/disks/<name>?owner=<owner>` removes one, refused (409) while its app
  is there or a VM of it still holds it
- **a backup** is the file, while no VM holds it: `flock -n <file> cp --sparse=always <file> <copy>`
  fails while one does (stop it by removing its app: the disk stays). Copying under a running VM
  gives a file system nothing checks. A file of another wicket's restored under
  `deployed/disks/<owner>/<app>.ext4` is what the next start of that app finds
- **a wicket's start** fails on a disk that does not mount, as on any route that does not start:
  the first failure ends them all
- **jail mode**: the file is the wicket's user's to open, bound writable at the jail's `/disk`
  (see [Jail mode](#jail-mode)). A disk is an app's: the config file's, a flag's and the page's
  routes take none (they have no name to keep it by)

### Limits

`--max-vms N` (default 64) caps the VMs alive at once over every route -- shared ones,
templates, clones warm, in use, kept and pooled -- and `--max-memory MiB` (default none:
`--max-vms` x the largest `mem` bounds it) their guest memory (`mem`). A cloned route's
`vms=N` caps its own (at least `warm`). A VM takes its share before its `firecracker` starts
and gives it back once killed; a startup whose shared VMs, pools' `warm` VMs and largest
template do not fit fails, and so does a reload (a console's edit included).

A connection whose clone finds no room waits for one in turn (one held back by its own
route's `vms` alone keeps no other route waiting), at most its route's `wait-ms` (default
10000), then fails with an ERROR (`no room for a VM within ...`), which
the server answers with `502 Bad Gateway` (HTTP/1.1) or by closing the connection; a pool's
connection waiting for a VM being started takes one of the VMs up once its `wait-ms` ran out.
Warm clones take room nobody waits for, and give it up: the first connection in line short of
room has a spare VM of another route killed -- a warm clone, a reuse route's clone serving
no connection, a pool's VM beyond its `warm` serving none.

Without more, one client holds as many of a route's VMs as it opens connections, until the
server's `sluice.limits.idle-timeout` closes them. What bounds one client's share:

- **`per-client=N`**: a client address's connections to the route open at once (a
  per-connection or reuse route's VMs, those waiting for room included); one past them fails
  at once with an ERROR (`client <address> holds N connection(s) of the route already`). The
  address is the server's data plane peer, a PROXY protocol header's source where one
  applies (`Connect.client_address`; `--listen`: the TCP peer): behind a front end without
  the PROXY protocol every connection carries the front end's, and a server predating the
  field sends none, which the bound does not apply to. A browser opens up to 6 connections
  per host
- **`idle-ms=N`**: a connection relaying no byte, either way, for that long fails with an
  ERROR, and its clone (per connection, reuse) is killed: a bound below the server's idle
  timeout, of the route's own. A long poll or a stream quiet for longer is cut as well
- **`cpu=N`** (jail mode): a VM's host CPU time, in percent of one CPU (`cpu=50`, half a
  CPU; `cpu=200`, two), its cgroup's `cpu.max` (per 100ms), its VMM and API threads
  counted. Without the jail a VM takes at most its `vcpus` host CPUs (and its VMM thread's
  share), which a guest spinning them takes from the others: the route's `cpu` needs jail
  mode (`--jail-ids`)

### Output and the live access log

What the processes in the VMs write to their stdout and stderr -- an image's process, its
restore hook, the sample guest's own lines -- reaches the wicket's stdout and stderr (so
`docker logs` shows it), each line prefixed with its route's hosts and its VM:

```text
stdout [app.local template] :: Started Application in 1.9 seconds
stderr [app.local vm-3] :: WARN payment declined
stdout [hello.local, hi.local vm-0] :: GET /log, request 7 of boot id ...
```

- the VM is `vm-0` for a shared VM, `template` for a cloned route's template and
  `vm-<n>` for its clones, numbered in the order they were restored. A line is the VM's
  whose process wrote it: the template's lines are taken before its snapshot, and what a
  clone holds of them at its restore notice is discarded, with what its process wrote
  before the notice
- `/init` reads the pipes as lines (one longer than 16KiB in pieces) and sends them on a
  vsock stream (port 1025) the wicket dials before the process starts: a process failing
  to start shows why. A process never waits for the wicket: up to 4096 lines (1 MiB) wait
  in the VM, past them lines are dropped and counted (`route app.local vm-3: 12 line(s) of
  its output dropped, its buffer of them full`). The wicket writes the lines from a thread
  of its own: a slow reader of its stdout holds the VMs' streams back, not the connections
- a connection's own VM is killed once its output came (within 1s): what the process
  writes after its answer is not lost
- a rootfs whose `/init` predates the stream boots all the same, with no output (`the
  guest sends no output (vsock port 1025)`)

`--flume <grpc://host:port | grpcs://host:port>` with `--flume-token <token>` feeds the
lines to a sluice-flume as `sluice-wicket-runtime-wasi` feeds its own (its README, "Live
access log": the node `wicket:<client-id>`, the ring of 512, the queue of 4096, the gaps,
the redial; `--flume-ca-cert`, `--flume-insecure`, and `flume`, `flume-token`,
`flume-ca-cert`, `flume-insecure` in the config file), so the consoles watching the flume
show them:

| kind | fields |
|---|---|
| `stdout` / `stderr` | `vm` (the prefix's), `line` |

A line's route key is the first host of its route (the hosts of one target share its VMs;
a reload keeping the VMs names their lines after its hosts from then on), and the VMs'
drops reach the flume as gaps. The wicket relays a connection's bytes without reading
them, so it logs no `response` lines: the server's access log has the requests.

### Egress

A guest has no network device: its connections leave the VM through the wicket, to the
destinations of its route's `egress` alone (`egress=db.internal:5432,10.0.0.0/24:6379` in
`--vm`, a list in the config file, an IPv6 rule quoted). A rule is TCP to a port of:

- a name: `db.internal:5432`, which the wicket connects to as the host resolves it (a name
  is trusted to resolve where it should). `localhost:6379` is the host's
- an address or a range: `10.0.0.5:5432`, `10.0.0.0/24:5432`, `[2001:db8::5]:443`,
  `[2001:db8::/32]:443`. `0.0.0.0/0:443` is every address, the host's own and link-local
  ones (a cloud's metadata service) among them

The process connects to them as named. Before it starts, `/init`:

- gives each name an address of its own, 127.1.0.1 on, in `/etc/hosts` (the rootfs's, the
  names first, bound over it: the rootfs needs one, the scripts write it); `localhost` stays
  the guest's 127.0.0.1 and ::1
- routes each address and range to the guest itself (a local route on `lo`)
- listens at each name's address and each address, at the rule's port, and at every address
  at a range's port: the process cannot listen at every address (`0.0.0.0`, Go's `:8080`)
  on a rule's port, nor at all on a range's (on its own, `PORT`, `/init` says so)

and relays each connection over vsock (port 1026) to the wicket. The wicket checks the
destination against the route's rules (a guest's root may ask for any), connects within 10s
and relays the connection, or refuses it: the process's connection is reset. A VM's egress
connections open at once are 256 at most. Each is recorded once it ended or was refused, on
the wicket's stdout and as an `egress` line of the route's first host in the live access
log (`vm`, `to`, `peer`, `ms`, `out`, `in`, `error`):

```text
egress [app.local vm-3] db.internal:5432 (10.0.0.5:5432): 1532ms, 1234 bytes out, 15000 in
egress [app.local vm-3] db.internal:5433: denied: not in the route's egress
egress [app.local vm-3] cache.internal:6379: failed: Connection refused (os error 111)
```

The VM's output says what `/init` set up (`guest: egress to db.internal:5432 at 127.1.0.1,
...`) and what it could not; a rootfs whose `/init` predates egress boots without it. A
cloned route's template has its egress (a process connecting at its start, or
primed): a connection it left open is every clone's, and reset in each (a restore resets
every vsock stream): a pool opens another. The sample guest's
`POST /egress?to=<host>:<port>` connects as a process does: it sends the body, half-closes
and answers with the answer (`&rounds=N&reply=K`: N round trips of K bytes, timed).

### Clones

A clone's memory is its snapshot's. What the wicket and `/init` do about it:

- **the clock**: on x86_64 the VMs boot with `clocksource=kvm-clock` and are restored with
  `clock_realtime`: Firecracker advances kvmclock by the time since the snapshot, so a clone
  resumes at the time of the moment (the TSC, the default clocksource, it does not advance).
  On aarch64 Firecracker advances no clock (`clock_realtime` is x86_64's; its VMClock device
  marks the restore but carries no time): the restore notice carries the host's time, and
  `/init` sets the clock to it first
- **random state**: right after the restore the wicket sends the clone a restore notice on
  vsock port 1024 with 32 random bytes of the host's (and on aarch64 its time); `/init`
  puts them into the kernel's random pool, reseeds it (ahead of the reseed on the VM
  generation change, VMGenID, which comes a moment after the restore), runs the image's
  restore hook, and answers. A process's own generator seeded before the snapshot is its
  business: the hook
  (`SLUICE_RESTORE='["<program>", "<arg>"]' scripts/cnb-rootfs.sh ...`, `restore` in
  `/sluice/init.json`) runs as the image's user with `SLUICE_RESTORE_GENERATION` set, before
  the clone serves; a hook failing fails the clone. A rootfs whose `/init` predates the notice
  fails a cloned route's start
- **secrets**: what the process generated before the snapshot (a key pair, a session
  ticket key, a token) is every clone's alike, the reseed changes the generators, not what
  they produced: generate such secrets in the restore hook, or after it
- **a start not done**: the guest is ready once its port accepts, which a process may do
  before its start ended (Jetty binds first, then prints that it listens): each clone
  finishes the start, its last lines each clone's. `prime=N` has the template finish it

The kernel's boot id remains the snapshot's.

## Jail mode

`--jail-ids <first>-<last>` with `--jail-user <name|uid:gid>` (`jail-ids`, `jail-user` in the
config file) runs every VM in a jail of its own: Firecracker's
[`jailer`](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md)'s
isolation, done by the wicket (see [Findings](#findings) for why not the jailer). The wicket
starts as root; that process, the keeper, stays root to spawn, kill and reap the VMs, and
runs the wicket proper -- the tunnel, the routes, everything reading what the guests send --
as `--jail-user`, which asks the keeper for its VMs over a socket pair and drives them:

```text
keeper (root) --runs--> wicket (jail-user) --requests--> keeper --clone3--> firecracker (jailed)
```

Each VM's `firecracker`:

- runs as an id (uid and gid) of `--jail-ids` of its own, without supplementary group,
  capability (the bounding set dropped) or new privileges; the wicket's seccomp filter,
  Firecracker's stacked on it (see [Host setup](#host-setup))
- is PID 1 of a PID namespace of its own, in mount, IPC and UTS namespaces of its own and a
  network namespace without interfaces (the VMs' alike: one per VM cost 0.9ms of each spawn)
- has a read-only tmpfs as its root holding `/dev/kvm` (a node of its own), its rootfs and
  kernel (bound read-only, from the files the wicket opened), `/run` (a directory of the
  keeper's, the wicket's group's: its API and vsock sockets) and, for a cloned route,
  `/snapshot`: the template's to write, made read-only (root's) before the first clone; an
  app's [disk](#disks) at `/disk`, bound writable from the file the wicket opened, owned by the
  VM's id while it boots (the wicket's group keeps its way to it: 0660); on aarch64
  `/sys/devices/system/cpu/cpu0`, the host CPU's cache and `MIDR_EL1` files Firecracker reads,
  copied by the keeper at its start (the jailer copies them into every jail)
- runs its route's copy of `firecracker` (a sealed memfd): the jailer copies the binary into
  every jail so that VMs share none of its pages; the clones of a route share their
  snapshot's pages anyway, so they share their route's copy, and routes share nothing
- is in a cgroup (v2) of its own: `memory.max` its `mem` plus 32 MiB plus `mem`/128 (its
  page tables) plus 16 MiB (its log, bound by `RLIMIT_FSIZE`, a disk's size with a disk: the
  limit holds for an offset in any file), its `mem` twice for a template (it writes its
  snapshot), `pids.max` its vCPUs plus 16. Past its limit the kernel's OOM
  killer ends the VM, nothing else. A cloned route's snapshot stays charged to the
  route's cgroup (`route-<n>`) as long as the route lives; a clone's cgroup (`vm-<n>`, one
  per id) is reused. With its route's `cpu` (see [Limits](#limits)) it is in `capped/`, a
  group of the cpu controller of its own, `cpu.max` the route's `cpu`; else in
  `uncapped/`, without (the class one group of it): the vCPUs bound it. A group per VM cost
  10% of the clone rate (see [Findings](#findings))
- ends with the keeper: its parent death signal is set after the uid change (which clears
  it: the jailer's `firecracker` outlives the jailer's parent). The keeper forwards SIGTERM,
  SIGINT and SIGHUP to the wicket and ends with it, its VMs killed, their files and cgroups
  removed; the wicket ends with the keeper as well

What it needs:

- root, cgroup v2 (`/sys/fs/cgroup`). The keeper's directory is
  `/run/sluice-wicket-runtime-firecracker-<pid>` (a tmpfs), or such a directory in
  `--state-dir`
- `--jail-ids`: a range no user of the host has, nor a subordinate range of `/etc/subuid`, of
  `--max-vms` ids at least (one per VM alive)
- `--jail-user` reads what the wicket reads: the config file, the token, the CA certificates,
  the rootfs images and the kernel. The VMs read the images and the kernel as ids of their
  own: the files need others' read permission (`o+r`); their directory may stay private (a
  jail binds the file, not its directory)
- `--jail-cgroup <path>`: the VMs' parent cgroup under `/sys/fs/cgroup`, made, the memory and
  pids controllers enabled down to it, and cpu down to `capped/` where the hierarchy has it
  (a route's `cpu` fails its VMs' spawn without it). By default the wicket's own cgroup,
  which the keeper leaves for `keeper/` (cgroup v2 enables no controller for the children of
  a cgroup holding processes): a systemd service's with `Delegate=yes`, a container's; run
  by hand, give a path
- the firecracker binary is the keeper's, the one named at startup: a reload may replace the
  file (a rename), not name another

```text
sudo target/release/sluice-wicket-runtime-firecracker --jail-ids 900000-900127 --jail-user sluice \
    --jail-cgroup sluice --config /etc/sluice/wicket.yaml
```

As a systemd service: `ExecStart=` the wicket with its `--config`, `Delegate=yes` (the
default `--jail-cgroup`), `TimeoutStopSec=` above `--drain-grace`.

## Host setup

Firecracker's
[production host setup](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/docs/prod-host-setup.md)
asks of the host what the wicket cannot do itself. Firecracker isolates one process from
another, and a VM is a `firecracker` process of its own; the tenants to keep apart on a
wicket host are:

- **routes'**: a wicket serving routes of several owners, or several wickets on a host
- **clients' of a per-connection route**: a clone is per connection to the sluice server,
  which opens an upstream connection per client connection and route (h2 too: a
  connection's streams to a route share one). A proxy in front of the server pooling its
  clients' connections (a load balancer or CDN reusing its upstream connections) puts
  several clients in one clone; a reuse route's clone serves one client after another, a
  pool's VM many at once, as a shared VM. The clones of a route share their snapshot's pages
  (mapped private): what the template held is every clone's (see [Clones](#clones)), and the
  shared pages are the side channel KSM opens (Flush+Reload), whatever the host's KSM

What applies, in both modes unless said:

- **SMT off** where tenants share the host (`echo off > /sys/devices/system/cpu/smt/control`,
  or `nosmt` on the kernel command line): the wicket pins no VM to a CPU, any two vCPUs may
  be siblings. A host of one tenant may keep it
- **KSM off** (`/sys/kernel/mm/ksm/run` 0, the default)
- **swap off, or encrypted**: it writes the guests' memory, the snapshots (shmem on the
  state dir's tmpfs) and the VMs' logs to disk (`swapon --show` prints nothing). The
  memory bound of the [Docker image](#docker-image) assumes none
- **the host's mitigations and microcode**: the kernel's defaults (`mitigations=auto`),
  checked in `/sys/devices/system/cpu/vulnerabilities/` or with Firecracker's
  `spectre-meltdown-checker`; memory with ECC and TRR (Rowhammer). aarch64: a host kernel
  6.4+ (Firecracker resets a VM's physical counter, `CNTPCT`, with `KVM_CAP_COUNTER_OFFSET`;
  older ones show the host's)
- **seccomp**: Firecracker's default filters, as the wicket never passes
  `--seccomp-filter` or `--no-seccomp`, stacked on the wicket's own: the calls Docker's
  default seccomp profile refuses a process without capabilities (`src/seccomp.rs`: mounts,
  namespaces, `bpf`, `keyctl`, `io_uring`, `userfaultfd`, `clone3` (`ENOSYS`, as there),
  old socket families...), and `ptrace`. The wicket installs it on itself before it starts,
  and in jail mode on each jail before its `firecracker`; the keeper runs under the same but
  for the jails' namespace and mount calls
- **the serial console and the logs**: the VMs boot with `8250.nr_uarts=0` (the console
  only with `SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE`), which a guest's root may undo. A
  VM's log (`firecracker.log` in the state dir, on a tmpfs: memory) is bounded at 16 MiB
  (`RLIMIT_FSIZE`, a template's at its `mem` more for its snapshot; past it Firecracker
  ends the VM), and a VM dumps no core (of its memory)
- **network and disk**: the VMs have no network interface (vsock only; jailed, a network
  namespace without one either): their [egress](#egress) is the wicket's connections, which
  the host's firewall sees as the wicket's. Their rootfs is read-only, and the snapshots
  stay on the state dir's tmpfs. An app's [disk](#disks) is a file of the console's directory,
  a guest's data at rest as it wrote it: encrypt the host's disk where that matters
- **the jailer's isolation**: [jail mode](#jail-mode). Unjailed, every VM runs as the
  wicket's user: a guest escaping its `firecracker` (past KVM and seccomp) holds every VM's
  sockets and snapshots and the wicket's token, and the VMs have no cgroup of their own
  (the wicket's, a container's or a unit's `MemoryMax=`, holds them all). Tenants on one
  wicket call for jail mode
- **`/dev/kvm`**: unjailed, the wicket's user in the `kvm` group (see [Setup](#setup)), and
  nobody else: a member boots VMs past `--max-vms` and `--max-memory`. Jailed, nobody: the
  keeper (root) makes each jail a node of its own, its VM's id's alone, so the host's may
  stay root's (`--jail-user` needs no group)
- **x86_64's `kvm-pit/<pid>`**: a VM's PIT kernel thread stays in the root cgroup (the
  wicket does not move it): a guest driving its PIT takes host CPU past its route's `cpu`,
  at most what `kvm.min_timer_period_us` (200us by default) lets it

Not needed at the wicket's rates: Linux 6.1's boot time mitigations (`favordynmods`,
`kvm.nx_huge_pages=never`): `KVM_CREATE_VM` takes 0.26ms on the 6.8 kernel of
[Findings](#findings) (0.34-0.51ms, 16 at once), of a clone's 12ms restore.

## Docker image

`Dockerfile` (built from the repository root:
`docker build -f sluice-wicket-runtime-firecracker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .`;
published as `ghcr.io/making/sluice/sluice-wicket-runtime-firecracker`, amd64 and arm64) holds the
wicket, Firecracker v1.17.0 and the 6.18.56 kernel above (both of its arch, checksum
verified), the guest of the wicket's own version and the sample rootfs, under
`/usr/local/share/sluice-wicket-runtime-firecracker/`.
The host needs KVM (bare metal, or a VM with nested virtualization); the container needs
`/dev/kvm` and its group alone (no `--privileged`, no capability: vsock only), and runs under
Docker's default seccomp profile, the wicket's and Firecracker's own filters stacked on it:

```text
docker run --rm --device /dev/kvm --group-add $(stat -c %g /dev/kvm) \
    --tmpfs /run/sluice --memory 1g --stop-timeout 30 \
    --add-host host.docker.internal:host-gateway -e SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN=SECRET \
    ghcr.io/making/sluice/sluice-wicket-runtime-firecracker \
    --server grpc://host.docker.internal:8001 \
    --vm hello.local=/usr/local/share/sluice-wicket-runtime-firecracker/guest.ext4
```

- **its environment** sets `KERNEL`, `FIRECRACKER`, `GUEST` and `STATE_DIR`
  (`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>`), which win over a `--config` file's keys; `-e`
  overrides them
- **rootfs images** come in through a volume (`-v $PWD/rootfs:/rootfs:ro`), readable by the
  image's user (65532). Their `/init` must be the image's guest (a rootfs without the restore
  notice fails a cloned route): build them with it, `docker create --name guest
  ghcr.io/making/sluice/sluice-wicket-runtime-firecracker`,
  `docker cp guest:/usr/local/share/sluice-wicket-runtime-firecracker/init init`,
  `docker rm guest`, then `SLUICE_GUEST=init scripts/cnb-rootfs.sh ...` (or
  `scripts/build-guest.sh`); the scripts stay out of the image (they need docker and sudo)
- **`/run/sluice`**, the state dir, on a tmpfs (`--tmpfs`): the snapshots stay off the disk;
  the wicket warns otherwise
- **`--stop-timeout`** above `--drain-grace` (20s; Docker's default stop timeout is 10s), and
  20s more with an app's [disk](#disks). The wicket is the container's PID 1: it drains on
  SIGTERM and reaps its VMs, no init needed
- **`/data`** on a volume with `--console`: `deployed/`, the console's directory, holds the
  routes, the images and the apps' [disks](#disks). The volume is the image's user's (65532),
  as a new named volume is
- **open files**: the wicket raises its soft limit to the hard one (Docker's soft one is
  1024) and gives each `firecracker` `--max-connections` + 384 (256 of them its egress, see
  [Findings](#findings))
- **`docker logs`** shows the VMs' output, each line named after its route and VM (see
  [Output](#output-and-the-live-access-log))

[Jail mode](#jail-mode) runs as root in the container (the keeper; the wicket as `nonroot`)
with `CAP_SYS_ADMIN` (namespaces, mounts, `pivot_root`), unconfined: Docker's AppArmor
profile denies mounts, and its seccomp profile `pivot_root` (with the capability too). The
container's seccomp profile is replaced by the wicket's filters (see
[Host setup](#host-setup)): the profile's denials, the keeper's allowing the jails' calls.
Its default `--jail-cgroup` is the container's own (the keeper remounts the read-only
cgroup filesystem writable), and the VMs need no group:

```text
docker run --rm --device /dev/kvm -u 0 --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined --security-opt seccomp=unconfined \
    --tmpfs /run/sluice --memory 1g --stop-timeout 30 ... \
    ghcr.io/making/sluice/sluice-wicket-runtime-firecracker \
    --jail-ids 900000-900127 --jail-user nonroot --server ... --vm ...
```

The VMs' memory counts against the container's memory cgroup: a VM's guest memory as the
guest touches it, the `firecracker` process's few MiB, and a cloned route's snapshot
on the tmpfs, its `mem` whole (shmem: never reclaimed without swap). Measured:

| | container memory |
|---|---|
| shared VM, sample guest | 60 MiB |
| shared VM, CNB Java (2 vCPUs, 512 MiB), after a load | 212 MiB |
| VM per connection, sample guest, warm=16 | 175 MiB (128 snapshot) |
| VM per connection, CNB Java, prime=200, warm=16 | 602 MiB (512 snapshot); 694 MiB at start, the template beside its snapshot |
| the same, 48 keep-alive connections at 17k req/s | 3,277 MiB peak: about 56 MiB per busy JVM clone |

An idle clone costs its dirty pages (1-4 MiB here), a busy one up to its `mem`. The bound
to set (`--memory`, a Pod's limit): `--max-memory` (every VM at its `mem`; without it,
`--max-vms` x the largest `mem`) plus each cloned route's `mem` (its snapshot; twice
while a reload restarts it and the old clones live) plus 64 MiB for the wicket. Past it,
the kernel's OOM killer ends a `firecracker` or the wicket.

Through the image (`--network host` or a bridge, the same host) the [Findings](#findings)
cases are within the spread of runs outside it: two runs each, e.g. a shared VM on 2 vCPUs
13.5-13.8k keep-alive req/s (outside 13.8-13.9k, bridged 13.6-13.9k), a VM per connection
1,603-1,614 new connections/s (outside 1,598-1,615, bridged 1,614-1,652), CNB Java per
connection 647-737 (outside 623-672).

Kubernetes: `/dev/kvm` reaches a Pod through a device plugin (KubeVirt's
`devices.kubevirt.io/kvm`, or a generic one), or a privileged container;
`securityContext.supplementalGroups` holds the node's `kvm` gid, `/run/sluice` is an
`emptyDir` of `medium: Memory` (counted against the Pod's memory limit), and
`terminationGracePeriodSeconds` exceeds `--drain-grace`. Jail mode: `runAsUser: 0`,
`capabilities.add: [SYS_ADMIN]`, `seccompProfile` (the wicket's filters replace it) and
`appArmorProfile` `Unconfined` (or a privileged container).

## aarch64

The wicket, its guest and its image build for aarch64 as for x86_64. What differs on an
aarch64 host:

- **the clones' clock**: Firecracker restores none (see [Clones](#clones)); the restore
  notice carries the host's time and `/init` sets the clock to it. The VMs boot without
  x86_64's `pci=off` and `clocksource=kvm-clock`
- **jail mode**: a jail holds the keeper's copy of the host CPU's cache and `MIDR_EL1`
  files (see [Jail mode](#jail-mode))
- **VMGenID and vsock** are as on x86_64 (Firecracker gives an aarch64 VM its VMGenID
  through the device tree)

### On a Mac

macOS has no KVM, but an Apple silicon Mac (M3 or later, macOS 15 or later) runs Linux with
nested virtualization, whose `/dev/kvm` boots Firecracker. OrbStack's containers and machines
have no `/dev/kvm`; Lima's `vz` VMs with `--nested-virt` do. `scripts/lima.sh` makes such a
VM (`brew install lima`; Ubuntu 24.04, the CI's tools, the release and the kernel above) at
its first run, copies the work tree into it at each, and runs a command there:

```text
scripts/lima.sh                     # the VM test
scripts/lima.sh env SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1 cargo test --locked --test vm
scripts/lima.sh bash -c 'cargo build --locked && scripts/build-guest.sh'
scripts/lima.sh bash -c 'cd ../sluice-wicket-cf-api && cargo test --locked --test cli the_cf_cli_deploys_to_a_firecracker_wicket'
limactl shell sluice-firecracker    # a shell; the copy is ~/sluice
limactl delete -f sluice-firecracker
```

From the VM a sluice server on the Mac is `grpc://host.lima.internal:8001`; a port the wicket
listens at on the VM's loopback (`--listen`, `--console`, or Docker's `-p 127.0.0.1:...`) is
forwarded to the Mac's. `lima.sh` starts the VM with `LIMA_SSH_PORT_FORWARDER=true`: Lima's
default forwarder (2.1.1) leaves the VM's end of a connection open when the Mac's closes, and at
`--max-connections` of them the listener stops answering. Starting the VM by hand, set it too.

A Mac's timings say nothing of a real host (see [Findings](#findings)). The wicket listens once
every VM it restores has booted: 9.6s here with a Node.js image, 3.7s with a Go one.

## Tests

```text
sg kvm -c 'cargo test'
```

`tests/vm.rs` boots VMs where `/dev/kvm` is usable (it is skipped elsewhere): through a tunnel
server double, an app's disk through a restart of its VM, a failed start, a growth, the wicket's
end and its restart, and for a shared VM serving two hosts, a VM per connection, reused clones, a pool
and a route of two VMs at most, one per client address, it checks http/1.1 with keep-alive,
h2c, a 5MB POST under the tunnel's flow control and a client half-closing after its request,
the clones' clock and random values, a client past its share, the connection finding no room,
idle connections failed, a reused clone serving the next connection until its `uses` or
`keep-ms` ran out, a pool growing with the connections held, shrinking idle and replacing its
VMs killed (unjailed), the VMs' output (a shared VM's, a template's, a clone's) on the wicket's
stdout / stderr and on a flume (`sluice-wicket-tunnel`'s double), the guests' egress (a shared
VM's and a clone's, to a destination on the host by name, address, range and IPv6, and its
records), and reloads of the config file. It needs Firecracker
(`SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER`, else `firecracker` on the PATH) and a guest
kernel (`SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL`, else `~/.local/firecracker/vmlinux`), the
variables of the wicket's own `--firecracker` and `--kernel`, and builds the sample rootfs.
`SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS` adds routes (`--vm` values, whitespace-separated, e.g.
CNB images), checked by the answers' status:

```text
T=$PWD/target
sg kvm -c "SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS='java.local=$T/upstream.ext4?mem=512&vcpus=2 \
    jclone.local=$T/upstream.ext4?mode=per-connection&mem=512&vcpus=2&prime=200&warm=4' cargo test"
```

`SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1` runs it in [jail mode](#jail-mode), the wicket
started as root (`sudo -n`; skipped without it) or, with `..._IMAGE`, as the image runs it
jailed, and checks the jails of the VMs alive: an id of its own each, PID 1 of its
namespace, no capability, no new privileges, the jail's seccomp filter and Firecracker's
on the keeper's, a cgroup limiting its memory (and a route's clones' CPU, theirs alone with
the cpu controller), on aarch64 the host CPU's files, the wicket as `--jail-user` under its
filter; the keeper killed (SIGKILL), the VMs and the wicket end. Unjailed, it checks the
wicket's filter.

The CI runs it on an x86_64 and an aarch64 runner, where the runner exposes `/dev/kvm`,
with the Firecracker release and the kernel above pinned, and again jailed, on the runner
and in the image; a runner without `/dev/kvm` still builds both arches' guests and its
arch's image. `ubuntu-24.04-arm` has none: the test passes there without a VM, so run it
on a Mac ([On a Mac](#on-a-mac)) when changing what differs on aarch64.

## Findings

Measured on a 64-core x86_64 host, through a sluice server on the same host, with `oha
-c 16` for 5s (`demo.local` is the Java client relaying to the same Jetty on TCP).

| | boot to accepting | keep-alive req/s | new connection req/s (p50) |
|---|---|---|---|
| Java client -> Jetty (TCP) | - | 22,695 | 12,418 (1.3ms) |
| shared VM, sample guest | 0.70s | 10,762 | 3,060 (5.1ms) |
| shared VM, sample guest, 2 vCPUs | 0.69s | 13,131 | 5,429 (2.9ms) |
| shared VM, CNB Java (Jetty, 2 vCPUs, 512 MiB) | 2.0s | 8,228 | - |
| shared VM, CNB Go | 0.74s | 8,382 | - |
| shared VM, CNB Go, 4 vCPUs | 0.77s | 14,991 | - |
| VM per connection, sample guest, warm=16 | - | - | 1,600 (13.3ms); c=1: 650 (1.5ms) |
| VM per connection, CNB Java (2 vCPUs, 512 MiB), prime=200, warm=16 | - | - | 689 (18.1ms); c=1: 130 (7.2ms) |
| clones reused, sample guest | - | 15,979 | 8,918 (1.8ms) |
| clones reused, CNB Go | - | 12,587 | 8,649 (1.8ms) |
| pool of 4 VMs of 1 vCPU, sample guest | - | 16,552 | 7,493 (2.1ms) |
| pool grown from 1 VM to 4 (`warm=1&conns=4`), sample guest | - | 17,648 | 7,880 (2.0ms) |
| pool of 4 VMs, CNB Go | - | 16,729 | 7,497 (2.1ms) |

- **A reused clone serves new connections at 5.7 times a fresh one's rate**: 8,918 new
  connections/s against 1,569 per connection (warm=16, the same runs): a connection takes
  the clone used last, a vsock dial, no restore. At c=16 the route keeps 17-20 clones (a
  connection coming before the previous one's clone is back gets a fresh one); the clone
  is back for the next connection before the server is told the connection ended.

- **vsock carries no half-close** (Firecracker v1.17.0 `csm/connection.rs`): the host
  side's EOF reaches the guest as a full shutdown (its unsent answer is lost), and a
  guest's `shutdown(SHUT_WR)` never reaches the host. The CLOSE therefore travels in band
  (`src/wire.rs`, `[type][len][payload]`), and the host must not drop a write half early:
  tokio's owned write half shuts the socket down on drop.
- **No SDK is needed**: the API is REST over a Unix socket; `src/vm.rs` drives it with
  hyper. The Go SDK (firecracker-go-sdk) adds networking (CNI) and the jailer, neither of
  which a vsock-only wicket needs.
- **No root, no TAP**: vsock only (jail mode takes root for the keeper alone). The VMs share
  one read-only rootfs; `vsock_override` on snapshot load gives every clone its own socket.
- **CNB images fit the launcher model**: their config is one `Entrypoint`
  (`/cnb/process/web`), a few `Env` entries, a `User` and a `WorkingDir`; everything else
  is in `/layers`. The Java buildpack's exec.d helpers (memory calculator, CA certificates)
  work on the read-only rootfs with `/tmp` on tmpfs; the calculator sizes the heap from the
  VM's memory.
- **Snapshots turn the JVM's startup into a restore**: a clone is restored in about 10ms
  (`File` backend, the memory file mapped private), and accepts at once. Without priming,
  a JVM clone answers its first request in about 260ms (cold JIT, pages faulted in);
  `prime=200` brings that to 10-20ms. A clone costs 2 MiB (sample) / 8 MiB (JVM) of PSS:
  the rest are the snapshot's shared pages.
- **A VM's spawn must not fork the wicket**: a fork copies the page tables while it holds
  the process's memory map lock, 1-2ms (strace) in which no thread of the wicket takes a
  page fault or grows its heap -- for every clone restored. Setting the parent death signal
  between fork and exec forces a fork, so the wicket `posix_spawn`s itself as a launcher
  (`--launch-vm`, no page table copy) that sets the signal, checks its parent and execs
  `firecracker`: the clone rate at c=16 rose from 549 to 773 req/s, the c=1 latency fell
  from 3.5 to 2.5ms. A VM still never outlives the wicket (`kill -9` checked).
- **Firecracker's start grows with the open file limit**: it grows its file table to the
  soft `RLIMIT_NOFILE` at its start (`dup2` to the last descriptor), 2.6ms from spawn to
  API socket at 1024, 5.4ms at 1048576 (a login shell's limit here). The launcher sets
  each `firecracker`'s to `--max-connections` + 384 (its vsock streams, 256 of them its
  egress's, and its own); at + 128 the
  sample guest's clone rate at c=16 rose from 1,230 to 1,600 req/s (c=1: 545 to 650), the
  JVM's (bound by its guest) stayed. A container's default soft limit (1024) hid the cost.
- **The clone rate is bound by the restores in flight**: one refill worker capped it at
  about 230 VMs/s (1 / 4ms), 8 workers lift it further; the restore notice's round trip at
  refill time, before a clone is handed out, also takes the clone's first page faults out
  of its first request's way (773 to 1,170 req/s).
- **A shared VM is bound by its vCPUs, not Firecracker's VMM thread**: at the rates above
  the VMM thread is at most 57% busy while the vCPUs are at 85-97%: the guest's share of a
  request (a vsock and a loopback TCP leg, `/init`'s relay, the process) is the cost. A
  frame per write and buffered frame reads (`src/wire.rs`) cut the VMM thread's share
  (48% to 36% at 1 vCPU) and lift 2-4 vCPU rates by 6-13%; `/init` running a worker per
  vCPU took the sample on 2 vCPUs from 9.3k to 14k req/s. The gap to the Java client is the
  guest's two extra legs.
- **The guest's allocator gives memory back at once** (musl): a 32KiB frame buffer per
  connection cost about 10% of a shared VM's new connection rate in page faults; a page is
  enough (a larger payload is read past it).
- **A clone's clock resumes from the snapshot** unless the VM runs on kvm-clock and is
  restored with `clock_realtime` (it advances kvmclock, not the TSC, the guest's default
  clocksource): a clone restored 6s after its snapshot was 6s behind, now within
  milliseconds.
- **PID 1 reaps in one place**: `/init` reaps any child (orphans come to it), so its own
  wait for the restore hook raced that reaping and now and then lost the hook's status
  (`restore notice: ERR No child process`, a clone failed). The reaper hands the hook its
  status instead: 44 hooked clones of the CNB Java image, none failed.
- **Egress goes through the wicket, not a TAP device**: a TAP device per VM needs
  `CAP_NET_ADMIN` (the [Docker image](#docker-image) runs without a capability) and an
  address per VM, and would go into jail mode's network namespace, which the VMs share;
  its filtering would be the host's firewall's. Over vsock, egress needs neither, works
  alike in every mode, and the wicket checks, bounds and records each connection.
- **The relay costs a database round trip about 0.2ms**: a Redis PING on one connection
  (the sample guest's `POST /egress`, through `--listen`), p50 29-68us from the host (its
  spread over runs), 277-295us from a guest of 1 vCPU through the wicket (p99 410-700us),
  217-273us of 2 -- two loopback legs, `/init`'s and the wicket's relays and the vsock device
  both ways. Over 16 connections the guest's vCPUs bound it: 13-15k round trips/s on 1 vCPU,
  31-33k on 2 (the host's: 110-131k). A connection's first round trip, its setup with it:
  1.3-1.9ms to an address, 2.0-2.3ms to `localhost` (the host's resolver); a connection per
  request (`oha -c 16`): 2.3-2.7k req/s on 1 vCPU, 4.3k on 2 (11-12k and 20-23k for `GET /`).
- **The wicket relays on a few single-threaded workers, not tokio's work-stealing
  runtime**: that runtime (a worker per core, 64 here) wakes its idle workers to steal for
  each frame relayed. `--workers` (default 4) runtimes of one thread each relay instead: a
  node's tunnel stream and its connections on one (no frame crosses threads), a
  `--listen` or egress connection on the one with the fewest tasks; the main thread's
  boots, restores and reloads. Two runs each, `oha` 5s, the sample guest; rate, and the
  wicket's CPU per request (per round trip for the egress):

  | | 64 workers | 1 worker | `--workers 4` |
  |---|---|---|---|
  | server, shared VM (2 vCPUs), keep-alive c=16 | 13.5-13.6k, 116-117us | 15.3-15.8k, 46us | 15.3-16.1k, 44-45us |
  | server, 48 clones, keep-alive c=48 | 30.2-30.4k, 73-74us | 33.0-33.8k, 29-30us | 31.7-32.7k, 30-31us |
  | server, a clone per connection, c=16 | 1,505-1,555, 1,310us | 1,123-1,132, 733-739us | 1,472-1,488, 1,020-1,030us |
  | `--listen`, shared VM, keep-alive c=16 | 17.4k, 74-76us | 14.7-15.8k, 37-39us | 18.8-20.0k, 61-62us |
  | `--listen`, 48 clones, keep-alive c=48 | 82.4-82.9k, 47-49us | 47.5-49.6k, 20-21us | 87.7-87.9k, 40us |
  | egress, Redis PING over 16 connections (2 vCPUs) | 25.6-30.5k, 62-65us | 15.3-15.4k, 38us | 30.2-31.0k, 52us |

  The cost lies in the threads a request wakes more than in the stealing: one busy thread
  takes its events in batches (32 per `epoll_wait` at 49k req/s, 1.1 for four threads at
  88k); 16 workers took 62us a request on `--listen`, 101-108us an egress round trip. One
  worker (`TOKIO_WORKER_THREADS=1` on the work-stealing runtime, the VMs spawned on it)
  costs least, but halves the rate of what is not one stream: the `--listen` clones, the
  egress. A node's stream is about one core either way (its HTTP/2 connection): 30k req/s
  on 2.2 cores with 64 workers, 33k on one. The wicket's seccomp filter took 8% of a relay
  worker's time (clones through the server, `perf`).
- **A VM is spawned from a thread living as long as the wicket**: `posix_spawn` holds its
  thread until the launcher's exec (about 1ms), on a runtime's thread its relays with it
  (one worker: 1,130 clones/s against 1,500); and a child's parent death signal fires as
  the thread that spawned it ends, not its process: spawned from tokio's blocking pool
  (an idle thread ends after 10s), the VMs died with their threads. Four launching
  threads spawn them.

N VMs of 1 vCPU (`mode=pool&vms=N`) against one VM of N, the same session, keep-alive
req/s, through the server (`oha -c 16`, and `--disable-keepalive`) and through `--listen`
(no server, c=16 and c=64); the VMs' memory after a load is their private pages, a pool's
plus its snapshot on the tmpfs (128 MiB, `mem`):

| | server, keep-alive | server, new connection | `--listen` c=16 | `--listen` c=64 | memory |
|---|---|---|---|---|---|
| sample guest, one VM of 2 vCPUs | 16,969 | 5,588 | 17,734 | 25,598 | - |
| sample guest, pool of 2 | 14,578 | 4,698 | 18,689 | 25,387 | - |
| sample guest, one VM of 4 vCPUs | 18,407 | 7,072 | 19.8-32.1k | 43,480 | 56 MiB |
| sample guest, pool of 4 | 16,552 | 7,493 | 25.5-26.7k | 42,410 | 12 + 128 MiB |
| sample guest, one VM of 8 vCPUs | 14,372 | 8,654 | 14.6-15.2k | 36,310 | 59 MiB |
| sample guest, pool of 8 | 11,362 | 9,029 | 39.5-40.5k | 69,729 | 24 + 128 MiB |
| CNB Go, one VM of 4 vCPUs | 16,557 | 7,590 | 26,168 | 40,343 | 77 MiB |
| CNB Go, pool of 4 | 16,729 | 7,497 | 25,914 | 42,563 | 36 + 128 MiB |
| CNB Go, one VM of 8 vCPUs | 19,218 | 9,627 | 34,323 | 50,959 | 82 MiB |
| CNB Go, pool of 8 | 14,799 | 8,631 | 35,513 | 69,059 | 77 + 128 MiB |

- **Past 4 vCPUs a pool scales, one VM does not, and the VMM thread is not why**: one VM of
  8 (or 16) vCPUs keeps its VMM thread at 49-58% while its rate falls below 4 vCPUs' (the
  sample at c=16: 14.6-15.2k on 8 against 19.8-32.1k on 4, its vCPUs 396-407% busy for
  fewer requests; one vCPU, taking the device's interrupts, at 92-99% where the others idle):
  the cost is the guest's, vsock's single VMM thread shows nowhere up to 16 vCPUs. 8 VMs of
  1 vCPU, a VMM thread each (13-35%), reach 69-70k at c=64, against 36k (sample) / 51k (Go)
  for one VM of 8. Up to 4 the two are alike.
- **Through one server neither gains past 4 vCPUs**: every case ends at about 26k keep-alive
  req/s at c=64 (one node's tunnel stream, the server at 12 cores), and at c=16 a pool of
  many small VMs is slower than one VM (11.4k for 8 VMs against 14.4k): each VM's vCPU
  idles between requests and is woken for each, a request's latency the sum. New
  connections end at the wicket's worker relaying the node's stream (100% at 9k/s).
- **A pool costs its snapshot**: its clones share the snapshot's pages, 3 MiB each of their
  own (sample) or 9-10 (Go), but the snapshot is the whole guest memory (`mem`) on the
  tmpfs, where one VM holds the pages its guest touched: 140 MiB for a pool of 4 against
  56 for one VM of 4 vCPUs (sample).
- **A VM's failure takes its connections alone in a pool**: one VM killed (SIGKILL) 3s into
  10s of keep-alive load at c=16, of a pool of 4: its 4 connections cut, no 502, the next
  connections on the others, a VM started in its place (new connections: 2 of 76,896 in
  flight on it answered 502). One VM of 4 vCPUs killed so: every request after it 502
  (134,863 of 189,509), and the route stays so until a reload: a shared VM is not replaced.
- **A pool's connections spread when they come, not after**: a keep-alive connection stays
  on its VM, so connections waiting for a VM being started take it as it comes, rather than
  all taking the VM first up: 8 keep-alive connections at once on an empty pool
  (`warm=0&conns=1&vms=4`, `--listen`) were served at 9.0k req/s by the first VM, the
  other three idling out; spread, 19.8-20.9k.

[Jail mode](#jail-mode), on the same host through `--listen` (no server), `oha` for 5s:

| VM per connection, sample guest, warm=16 | new connection req/s, c=1 | c=16 |
|---|---|---|
| without the jail, 3 runs | 534-572 | 1,562-1,616 |
| jailed, the same runs alternating | 547-569 | 1,534-1,584 |
| jailed by `pivot_root`, a cpu group per VM (the same runs) | 555-592 | 1,231-1,239 |

- **Firecracker's jailer costs a VM's spawn 2.5-4.2ms**: from spawn to API socket 2.1ms
  without it, 4.6ms with it (`--new-pid-ns`, a chroot on a tmpfs), 6.3ms with a cgroup's
  `memory.max` and `pids.max`. It copies the binary (3.6 MB) into every jail, so every clone
  holds a copy of its own; it needs its chroot base executable and with devices (Ubuntu's
  `/run` is `noexec,nodev`, Docker's `--tmpfs` `noexec`) and the files hard-linked into the
  jails (one filesystem); its `setuid` clears the parent death signal, and `--new-pid-ns`
  forks it away. Hence the wicket's jail of its own.
- **A jailed restore is faster than the launcher's**: a fresh clone restored in 11.9ms
  (p50, c=1, warm=0) against 13.6ms: one fork of the small keeper and one `execveat` instead
  of the launcher's two execs (the wicket's own exec costs 1.2ms here).
- **`pivot_root` stalls every fork of the host**: it walks every thread of the host
  (`chroot_fs_refs`, 2,300 threads here) under the kernel's task list lock, which a fork
  or an exit takes, 0.25-1ms per VM. The keeper's `clone3` waited on it for 37% of its
  thread's time (82% busy at 1,232 VMs/s, 0.66ms per VM), Firecracker's threads as they
  started and ended alike. The jail's tmpfs is moved onto the template's root and chrooted
  into instead: 0.40ms of the keeper per VM, the jail's setup in the child 1-2ms instead
  of 2-4. Unmounting the keeper's directory in each jail (an expedited RCU grace period,
  0.3ms) costs nothing measurable.
- **A group of the cpu controller per VM costs 10% of the clone rate** (its scheduling
  entities on every CPU, their load tracked): 1,390-1,460 VMs/s at c=16 against
  1,530-1,630 without the controller, 1,543 with one group for every clone. Hence a group
  of its own only for a VM whose route caps its CPU (`capped/`).
- **The keeper's thread does not bound the jailed clone rate**: 63% busy at the unjailed
  rate, 0.4ms per VM (its fork 0.2ms), so about 2,500 VMs/s at most. Spawner processes
  forking in parallel (built and measured; the keeper's round trip p90 from 5.0ms to
  2.0ms) gained nothing at 8 refill workers, 2-3% at 16 (where the jailed rate is 93% of
  the unjailed, the keeper 58% busy). A vfork child would hold the keeper for the jail's
  setup.
- **A jail's mount namespace is copied from a template**: `clone3`'s `CLONE_NEWNS` copied
  the host's 41 mounts per VM in the keeper's thread (900 clones/s at c=16); a template of
  2 mounts copied in the child, 1,200; each id's directory and cgroup reused, 1,300. A
  route's files are bound from mounts of the template made once per route (`open_tree` of
  the wicket's fds: a mount of another namespace cannot be bound in the jail's).
- **A jailed VM's charge beyond its guest memory is 2.5-3 MiB** (its heap, KVM's
  structures, page tables of 0.25% of what the guest touched): a shared CNB Java VM (512
  MiB, 2 vCPUs) peaked at 248 MiB under load, its limit 564; its clones idle at 5-6 MiB and
  peak at 50-60 MiB under keep-alive load, the route's cgroup holding the 512 MiB snapshot.
  Jailed, 13.0k keep-alive req/s for the shared JVM, 41.5k over 16 JVM clones, 596 new
  connections/s (689 through the server above).
- **Docker's seccomp profile refuses `pivot_root`**, `CAP_SYS_ADMIN` or not (`unshare`,
  `mount`, `umount2` pass), and its AppArmor profile mounts: jail mode runs unconfined in a
  container.

[aarch64](#aarch64) first ran on an emulated machine -- QEMU 8.2 TCG (`-M
virt,virtualization=on,gic-version=3 -cpu max,pauth-impdef=on`), Ubuntu 24.04 arm64, whose KVM
ran Firecracker v1.17.0 and the 6.18.56 kernel -- with the wicket's boot and restore timeouts
raised to its pace (a guest boots in about 6s), and passed plain and jailed: the clones' clock
and random state, the half-close framing, the output, the reloads, the jails. Its timings say
nothing of a real host.

Then on an M4 Max, macOS 26.3 ([On a Mac](#on-a-mac): Lima 2.1, `vz`, 8 vCPUs, Ubuntu 24.04's
6.8 kernel at EL2, KVM's vGICv3), at the wicket's own timeouts: the VM test plain, jailed and
through the image, and a sluice server on the Mac serving a shared VM and clones over its tunnel
(http/1.1, h2c, a 5MB POST).

- **Boots contend under nested virtualization**: a guest alone is listening after about 1s, and
  four started at once after about 4.5s. The kernel's clock at one point of its boot read
  0.43s alone, 0.74s for 2 at once, 1.83s for 4, 4.74s for 8, the VM's vCPUs not all busy.

- **A clone's clock is its snapshot's without the notice's time**: 46.6s behind there (the
  snapshot taken at startup), within the test's second with it.
- **Firecracker boots without the host CPU's files** (v1.17.0, a warning: `Could not read
  ... for cache level 1`): the guest then lacks the host's cache topology (its device
  tree's cache nodes, `CLIDR_EL1`), and a restore its CPU manufacturer check (`MIDR_EL1`,
  a warning on a mismatch). The jail's copy keeps a jailed VM's CPU an unjailed one's.

Caveats:

- `/dev/kvm` ACLs set by hand do not last: udev rewrote one within minutes. Use the
  `kvm` group.
- An h2c-unaware process answers prior-knowledge h2 with 502, as behind the Java client;
  the Go sample enables h2c (`http.Protocols`).

- **The env goes over vsock, not a drive**: the kernel's command line (2048 bytes, and any
  process of the guest reads it) cannot hold it. A second drive would mean a file per target
  holding secrets on the disk, bound into each jail through the keeper. The wicket instead dials
  `/init` on a port of its own (`wire::ENV_PORT`) before the process starts: nothing but
  `routes.yaml` (0600) holds it on the host, a jail needs nothing more, and a clone has it in
  its snapshot's memory.
- **A rootfs is made on the wicket, not in the cf-api**: `/init` must be of the wicket's
  version and arch, which it alone knows; a pull straight to the wicket saves sending the
  image over again (a CNB image is hundreds of MiB); and the API serves a controller or a user
  alike. The cf-api keeps the pinned reference as its
  droplet and has the rootfs made again when the wicket lost it.
- **`mkfs.ext4 -d` alone cannot make an image's file system as a user** (e2fsprogs 1.47.0,
  uid 1000): every file gets the runner's uid (`-E root_owner` is the root directory's alone), a
  mode 000 file fails the build (`Permission denied`), and `-d` takes no tar before 1.47.1
  (Ubuntu 24.04 has 1.47.0). So the tree is unpacked owner-readable and `debugfs -w -f` sets each
  inode's owner, mode and times: 40000 `sif` commands in 0.48s against 1.0s of `mkfs` for 20000
  files. It exits 0 whatever a command did (the errors are its stderr); a name is quoted with
  `""` for a quote and a bare backslash; `mknod` takes an entry of the current directory, not
  a path (`/app/null` is one illegal name in `/`).
- **An ext4 image can be reproducible**: `E2FSPROGS_FAKE_TIME`, `-U` and `-E hash_seed` fix the
  file system's own bytes, and the directory's order does not matter; what `-d` copies of each
  file (atime, ctime, their nanoseconds) must be set too, or two builds differ in every inode.
  A Paketo noble run image with a binary (5029 files; 216 MiB, 118 MiB on the disk, sparse) was
  pulled from a local registry and made in 24.5s by the debug build, a build again of the same
  digest; served with the image's user (`cnb`, 1002:1001) and `/usr/bin/passwd` still setuid.

- **A disk is a drive of ext4 with a journal, not an API of the wicket's** (the cf-api's state,
  its first use): an API needs one writer as much (two VMs during a reload), so a fencing token,
  and in the cf-api an asynchronous store and its blobs through the egress; a drive needs a
  stop before the start and costs an app nothing: it writes files. Measured on the 64-core host
  (NVMe; v1.17.0, the 6.18.56 kernel):
  - `mkfs.ext4` of a sparse file: 6ms and 33 MiB on the disk at 1 GiB, 23ms and 1 GiB at 256 GiB
    (`mke2fs` punches an allocated file's holes back: allocate after it)
  - the guest's mount: 6ms; after a SIGKILL, its journal replayed, 55-65ms
  - a durable save of 100 KiB (write, `fsync`, rename, `fsync` of the directory): `cache_type`
    `Unsafe` (the default: a guest's flush is dropped) p50 0.7ms, `Writeback` 3.6ms; a SATA SSD
    12ms. `Unsafe` breaks the journal's ordering across a crash of the host
  - **a VM's SIGKILL is a crash of its file system, and ext4's default makes the worst of it**: a
    small file written and renamed every 20ms without an `fsync`, the VM killed 6-8s in, reads
    back as NUL bytes of the size it had in 10 kills of 10, and at 12s and 32s too (11 of 12 in
    all: `dioread_nolock`, the size committed, its extent still unwritten). With
    `nodioread_nolock` (ordered mode as it was) a complete file in 10 of 10, up to a commit
    interval's saves gone (5s), at the same save's cost; `data=journal` alike. With an `fsync`
    before the rename, none gone either way: why `/init` mounts so, and why the cf-api's writes
    `fsync` (its push right before a kill was lost without)
  - one drive under two VMs: nothing refuses, both mount, `e2fsck` finds damage in seconds.
    A `flock` taken by the launcher, on a file open across its exec, holds as long as
    `firecracker` lives and is gone 43ms after its SIGKILL (a 128 MiB VM's teardown): a start
    right after a kill must wait for it
  - `RLIMIT_FSIZE` of 16 MiB under a 256 MiB drive: Firecracker ends at the first write past
    16 MiB (signal 25, exit 151), the file's size whatever: the limit is the larger of the log's
    and the drive's
  - `EXT4_IOC_RESIZE_FS` from `/init`: 19ms to 1 GiB, 185ms to 64 GiB, `e2fsck` clean after
  - a stop: `sync` of 64 MiB dirty 60ms, the unmount 9ms

## Not yet

A shared VM replaced when it fails, its process restarted (a pool's VMs are: `mode=pool&vms=1`
is one; but a [disk](#disks) needs a shared VM: `guest: <program> exited` on the wicket's stderr is
all that says so), a disk on a route of the config file, a flag or the page, a backup of a
running disk, a pool
spreading by load rather than by connections, CPU limits without the jail, an image of the
page rather than the API (`PUT /api/images`), a docker-archive upload, the requests on the
flume (the VMs' output alone: `cf logs` shows no `RTR` line here), and egress over UDP or to
names a route does not list (`*.example.com`).
