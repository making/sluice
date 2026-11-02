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

| mode | VMs | a connection gets | between connections | idle | a VM failing | memory |
|---|---|---|---|---|---|---|
| `shared` | 1, booted at start | the VM | one guest for all | the VM stays | every connection, until another one started in its place ([A shared VM going down](#a-shared-vm-going-down)) | the pages its guest touched |
| `per-connection` | 1 per open connection, `warm` ahead | a clone of its own, killed with it | a clone each | `warm` clones stay | that connection | the snapshot (`mem`) + a few MiB a clone |
| `reuse` | 1 per open connection, kept ones, `warm` ahead | the clone used last, else a fresh one | a clone's connections in turn share its guest | kept clones killed after `keep-ms`: none left | that connection | as per connection |
| `pool` | `warm` to `vms`, many connections each | the VM holding the fewest | a VM's connections share its guest | down to `warm` after `keep-ms` (`warm=0`: none) | its connections; a VM started in its place | as per connection |

A fresh clone takes a restore per connection, a reused clone or a pool's VM a vsock dial: several
times the rate of new connections ([by mode](../.kb/wicket-firecracker-findings.md#rates-by-mode)).

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
`vcpus=N` (1), `mem=MiB` (128), `port=N` (the guest's vsock port, 8080), `boot-ms=N` (how long
a cold-booted guest may take to listen, 10000; its process exiting first fails it at once: a
JVM on one vCPU can take more), `per-client=N`,
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
or takes over 10s, is closed. A process unaware of h2c answers prior-knowledge h2 with 502, as
behind the Java client. The head is relayed verbatim with the rest: unlike the server,
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
`--workers` (4, at most the CPUs) sets the threads relaying the connections (see
[findings](../.kb/wicket-firecracker-findings.md#relay-workers)).

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
workers: 4                      # threads relaying the connections
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

### A shared VM going down

A shared VM serves no more when, and the wicket then starts another one in its place, from the
same spec:

- `/init` says the image's process ended (`guest: <program> exited`), whatever its status, and
  the port refuses: a route's process is there to serve. A launcher that left a daemon listening
  has not ended; nor has a stop of the wicket's, which empties the route first
- the VM's log stream ends: its `firecracker` ended, or the guest's kernel did
- a connection fails, and the port goes on refusing for the route's `boot-ms`: the daemon's end,
  a process that closed its port, a guest that no longer answers

A process that lives, keeps its port and answers nothing is not seen.

As a reload does it on a [disk](#disks), with or without one: the old VM stopped first (SIGTERM
to what is left, all written out, the disk free), the new one on the room in the budget the old
one held (no other route takes it meanwhile), its env and disk as they were. The first start is
at once; a VM that goes down within 60s of its start, or a start that fails, waits 1s, then 2s,
4s, up to 60s between starts, and a VM up for 60s starts that count anew. Its connections fail
meanwhile (`guest unreachable: its VM was stopped: the route is restarting`; the open ones end
with the VM), a boot long when all goes well.

The starts run beside the reloads: a route failing holds no edit, and fails no reload, of another
one. A reload keeps a route down as it is, unless its spec or files changed: that start then
takes over at once, the wait forgotten. So does `PUT` of another env; the same `PUT` is `no
change`, the VM coming back by itself. The wicket's stderr says each step (`route <hosts>: its
process ended; its VM stopped`, `its VM starts again in 2000ms`, `its VM's start failed: ...`),
`GET /api/routes` a route's `vms.restarts`, reset by a change of its spec, and `vms.down`, why it
is down (the page shows both).

### Console

`--console <host:port>` (`console`) serves a page and a JSON API deploying rootfs images as
routes at once, without a restart: drop an ext4 image on the page (or choose one), give it a
host and the route's options, and it is served -- on the tunnel and on `--listen` alike.
Dropped on a route of the console, an image replaces its own; renamed, a route moves to
another host with its image and options. The routes of the config file and the `--vm` flags
are listed too, read-only. The page shows what the wasm wicket's has no counterpart for:
each route's VMs (shared and its restarts, warm, kept, pooled, starting, all alive), its connections, a cloned
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
out of the root, within 4 GiB. The image holds what they say of every file, the guest as `/init`,
`/sluice/init.json` from the image's config, as `scripts/cnb-rootfs.sh` writes it, and what a
container runtime provides: `/etc/hosts`, and an empty `/etc/resolv.conf` (no name server; the
JVM buildpack's helper fails the launch without the file). One build
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
  same disk. Its connections fail meanwhile, a boot long.
  A start failing starts the old VM again, the PUT refused as any other (with a process that
  never listens: once its boot's bound ran out); failing as well, the refusal says so, and the
  route serves nothing until [its next start](#a-shared-vm-going-down) succeeds
- **a stop** tells `/init`: SIGTERM to every process, 10s for them to end (then SIGKILL), what
  is dirty written out, the disk unmounted, the VM killed, 20s at most. So does the wicket's own
  end, after its drain (`--drain-grace` and these 20s: `--stop-timeout`, `TimeoutStopSec`), a
  [process's end](#a-shared-vm-going-down), and a process exiting before it listens. A VM
  simply killed,
  the wicket's or the host's crash, is a crash to the file system: the journal replays at the
  next mount, and what the process had not fsynced may be gone, up to the commit interval's
  5s. A file written and renamed stays the old one or the new one, not NUL bytes
  (`nodioread_nolock`, see [findings](../.kb/wicket-firecracker-findings.md#disks)).
  Guest flushes reach the host's disk
  (`cache_type` `Writeback`)
- **grows** with a PUT of a larger `disk-mib` (the file, then `/init` at its next mount: online).
  A smaller one is refused (422), its VM untouched; one below 8 MiB too (400:
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
  on a rule's port, nor at all on a range's. On its own, `PORT`, `/init` says so, and where
  its listener would answer for the process (a range's, `localhost`'s, `127.0.0.1`'s) starts
  no process: the VM is refused

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
isolation, done by the wicket (see [findings](../.kb/wicket-firecracker-findings.md#jail-mode) for why
not the jailer). The wicket
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
  network namespace without interfaces (the VMs' alike: one per VM slows each spawn)
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
  `uncapped/`, without (the class one group of it): the vCPUs bound it. A group per VM slows
  the clones (see [findings](../.kb/wicket-firecracker-findings.md#jail-mode))
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
`kvm.nx_huge_pages=never`): `KVM_CREATE_VM` is a small part of a clone's restore
([measured](../.kb/wicket-firecracker-findings.md#boot-time-mitigations)).

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
  [findings](../.kb/wicket-firecracker-findings.md#clones-and-spawning-them))
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
on the tmpfs, its `mem` whole (shmem: never reclaimed without swap). An idle clone costs its dirty
pages, a busy one up to its `mem` ([measured](../.kb/wicket-firecracker-findings.md#docker-image-memory)).
The bound
to set (`--memory`, a Pod's limit): `--max-memory` (every VM at its `mem`; without it,
`--max-vms` x the largest `mem`) plus each cloned route's `mem` (its snapshot; twice
while a reload restarts it and the old clones live) plus 64 MiB for the wicket. Past it,
the kernel's OOM killer ends a `firecracker` or the wicket.

Through the image (`--network host` or a bridge) the rates are those outside it, within the spread
of runs ([findings](../.kb/wicket-firecracker-findings.md#through-the-docker-image)).

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

A Mac's timings say nothing of a real host (see [its findings](../.kb/wicket-firecracker-findings.md#aarch64)).
The wicket
listens once every VM it restores has booted, seconds there.

## Tests

```text
sg kvm -c 'cargo test'
```

`tests/vm.rs` boots VMs where `/dev/kvm` is usable (it is skipped elsewhere): through a tunnel
server double, an app's disk through a restart of its VM, its process's end, its VM killed, a failed
start, a growth, the wicket's end and its restart, a shared VM started again as its process keeps
ending, and for a shared VM serving two hosts, a VM per connection, reused clones, a pool
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

Benchmarks and design findings: [.kb/wicket-firecracker-findings.md](../.kb/wicket-firecracker-findings.md).

## Not yet

A health check of a process that lives and does not answer (`mode=shared` knows [what ends or
refuses](#a-shared-vm-going-down); a pool's VM goes on a failed dial), a disk on a route of the
config file, a flag or the page, a backup of a running disk, a pool
spreading by load rather than by connections, CPU limits without the jail, an image of the
page rather than the API (`PUT /api/images`), a docker-archive upload, the requests on the
flume (the VMs' output alone: `cf logs` shows no `RTR` line here), and egress over UDP or to
names a route does not list (`*.example.com`).
