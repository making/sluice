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

- `mode=boot` (default): `instances=N` VMs (default 1), each booted and none restored from a snapshot,
  for a process that does not survive a restore (what it opened before its snapshot is gone in a
  clone). A connection goes to the VM holding the fewest connections; none is grown past `instances` nor
  waited for, a boot being too slow to ask for on demand, so `warm`, `keep-ms`, `concurrency`, `prime` and
  `wait-ms` are refused
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
- `mode=pool`: `instances=N` VMs at most, cloned from the snapshot as per connection, each serving
  many connections; `warm=N` of them (default `instances`: a pool of a fixed size)
  are kept. A connection goes to the VM holding the fewest connections (not requests: a
  keep-alive connection stays on its VM) while it holds fewer than `concurrency=N` (16); past
  that it waits for a VM being started (one started for every `concurrency` connections waiting,
  up to `instances`), or goes to the VM holding the fewest when none is. A VM beyond `warm`
  serving none is killed after `keep-ms` (`warm=0` scales to zero); a VM going down
  ([A VM going down](#a-vm-going-down)) or that does not answer is killed, the
  connection served by another, and one started in its place below `warm`

The cloned modes (per-connection, reuse, pool) take `prime`, `warm` and `wait-ms`, every mode `instances`.
`sluice-wicket-cf-api` sets `instances` by `cf scale -i`, the mode, `warm`, `keep-ms`, `uses`,
`lifetime-ms` and `concurrency` by [annotations](../sluice-wicket-cf-api/README.md#how-long-a-vm-lives).

| mode | VMs | a connection gets | between connections | idle | a VM failing | memory |
|---|---|---|---|---|---|---|
| `boot` | `instances`, 1 by default, booted at start | the VM holding the fewest | a VM's connections share its guest | they stay | its connections; a VM started in its place ([A VM going down](#a-vm-going-down)); with none up, every connection | `instances` x the pages a guest touched |
| `per-connection` | 1 per open connection, `warm` ahead | a clone of its own, killed with it | a clone each | `warm` clones stay | that connection | the snapshot + a few MiB a clone |
| `reuse` | 1 per open connection, kept ones, `warm` ahead | the clone used last, else a fresh one | a clone's connections in turn share its guest | kept clones killed after `keep-ms`: none left | that connection | as per connection |
| `pool` | `warm` to `instances`, many connections each | the VM holding the fewest | a VM's connections share its guest | down to `warm` after `keep-ms` (`warm=0`: none) | its connections; a VM started in its place | as per connection |

A fresh clone takes a restore per connection, a reused clone or a pool's VM a vsock dial: several
times the rate of new connections ([by mode](../.kb/wicket-firecracker-findings.md#rates-by-mode)).
A snapshot holds what the template's guest wrote, the rest of `memory-mib` holes of a sparse file; clones
touching the rest add to it, up to `memory-mib`
([measured](../.kb/wicket-firecracker-findings.md#a-snapshots-memory)).

The rootfs is either the built-in sample (`scripts/build-guest.sh`: `/init` serves HTTP
itself) or an OCI image converted by `scripts/cnb-rootfs.sh`, e.g. a Cloud Native
Buildpacks image: `/init` starts the image's `Entrypoint` + `Cmd` as its `User` in its
`WorkingDir` with its `Env` (and `PORT`), and opens the vsock port once the process
accepts on `127.0.0.1:$PORT`. `/dev/stdin`, `/dev/stdout` and `/dev/stderr` exist and its
output is its user's, so an image's log files linked to them open as they do under runc.
The process's output reaches the wicket's stdout / stderr,
and with `--flume` the live access log (see [Output](#output-and-the-live-access-log)). A
guest has no network device: it connects to the destinations its route allows through
the wicket (see [Egress](#egress)).

In [jail mode](#jail-mode) (`--jail-ids`, started as root) every VM runs in a jail of its
own: an id, namespaces, a root and a cgroup of its own, Firecracker's `jailer`'s isolation.
Without it a VM has a [cgroup](#cgroups) of its own where the wicket's is delegated to it.

## Layout

Two Cargo workspaces:

- `.` -- the wicket: `src/lib.rs` wires the modules, `src/main.rs` reads the command line
  (`config.rs`); `routes.rs` the route table, its reload and the relay of a connection,
  `route.rs` a route's VMs, `capacity.rs` their budget, `vm.rs` one Firecracker VM,
  `forker.rs` the process forking them (without the jail), `cgroup.rs` their cgroups,
  `output.rs` the VMs' output, `egress.rs` their egress, `wire.rs` the vsock framing, the
  restore notice and the egress rules,
  `listener.rs` `--listen` (`head.rs` the request head routing a connection, `hpack.rs`
  an h2c one's, `exchange.rs` whether a request is in progress), `console.rs` the console (its page in `src/console/`), `deployed.rs` its
  routes and apps kept, `disk.rs` an app's disk, `image.rs` an OCI image pulled, `rootfs.rs`
  its layers as an ext4 image, `stage.rs` source staged by buildpacks into a droplet, `state.rs` the state dir, `seccomp.rs` the seccomp filters;
  jail mode's `keeper.rs` (the privileged half, and the wicket's client of it) and
  `jail.rs` (one VM's jail)
- `guest/` -- the VMs' `/init`, built for `x86_64-unknown-linux-musl` or
  `aarch64-unknown-linux-musl` (its `rust-toolchain.toml` has rustup add both, rust-lld
  links the other arch's): `log.rs` the processes' output, `egress.rs` their egress,
  `reaper.rs` PID 1's reaping, `stage.rs` a build's VM; `guest/src/wire.rs` is a copy of `src/wire.rs`: keep them
  alike; `src/bin/sample-process.rs` what the tests' images run,
  `src/bin/sample-buildpack.rs` what their builder builds with
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
    --vm 'app.local=target/upstream.ext4?mode=per-connection&memory-mib=512&vcpus=2&prime=200&warm=4'"

curl -H 'Host: hello.local' http://127.0.0.1:8000/
```

The scripts build the guest for the host's arch, a VM's; `SLUICE_GUEST_ARCH=aarch64` (or
`x86_64`) builds a rootfs for the other's (`cnb-rootfs.sh` then takes the image of that
platform, `linux/arm64` or `linux/amd64`).

`--vm host=<rootfs>[?<options>]` adds a route: `mode=boot|per-connection|reuse|pool`,
`vcpus=N` (1), `memory-mib=N` (128), `port=N` (the guest's vsock port, 8080), `boot-ms=N` (how long
a cold-booted guest may take to listen, 10000; its process exiting first fails it at once: a
JVM on one vCPU can take more), `per-client=N`,
`idle-ms=N`, `cpu=N` (in [cgroups](#cgroups)), `instances=N`, for the cloned modes `warm=N`, `prime=N` and
`wait-ms=N` (see [Limits](#limits)), `keep-ms=N` (reuse, pool), `uses=N` and `lifetime-ms=N`
(reuse), `concurrency=N` (pool), `readiness=process|port|http`, `readiness-endpoint=<path>`,
`readiness-timeout-ms=N` and `readiness-interval-ms=N` (boot, pool: see [Readiness](#readiness)),
`liveness=port|http`, `liveness-endpoint=<path>`, `liveness-timeout-ms=N` and `liveness-interval-ms=N`
(see [Liveness](#liveness)), `egress=<host>:<port>,...` (see [Egress](#egress)). The advertised
target is `firecracker:<rootfs>` plus the options set to other than their defaults; the server
dials it verbatim, and the hosts of one target share its VMs.

`--listen host:port` serves the routes over plain TCP too (without `--server`, all the wicket
does: a local loop for trying a rootfs; `scripts/try.sh <rootfs> '<options>' [port]` runs the
debug build so, on `a.local`, the arguments after the port the wicket's: `--vm
b.local=<rootfs>` a second route). A connection goes to the route of its first request's host
(the `Host` header, or the h2c `:authority`; the port left out, the case ignored). A single
route given by `--vm`, on a wicket without `--console`, takes any host, nothing read first (its
guest may speak first): not a route of the config file or the console, whose routes come and go.
An unknown host is answered 404, a request finding no room for its VM 502 (see [Limits](#limits));
a connection that is not HTTP, or whose head runs past 64KiB or takes over 10s, is closed. A
process unaware of h2c answers prior-knowledge h2 with 502, as
behind the Java client. The head is relayed verbatim with the rest: unlike the server,
the wicket routes a connection once -- a later request on it (an h2 stream, a keep-alive
request) for another host stays on the first one's route -- and adds no `X-Forwarded-*`
header (the guest sees what the client sent: the sample's `x-forwarded-for -`), since only
the first request of a connection would get them. `--max-connections` bounds the listener's
connections too, apart from the tunnel's; TCP keepalive (60s
idle, then 3 probes 10s apart) ends one whose client is gone without a FIN or reset. An HTTP
connection with no request in progress for 60s is closed, as for its client closing it (a reused
clone is kept, an h2c client reads a GOAWAY), and so is a connection sending nothing, or whose client
reads nothing of an answer for 60s. A request waits on its guest for as long as it takes; what is
not HTTP, a guest speaking first, an upgrade are relayed verbatim, only `idle-ms` ending them. All
taken, one with no request in progress, or whose request's body is not all sent, gives way to the next,
the source holding the most first, closed the same way; with none, the next waits in the backlog
([details](../.kb/connection-limits.md#sluice-wicket-runtime-firecrackers---listen)).
`--firecracker` names the binary (default `firecracker` on the PATH), `--state-dir` the VMs'
files (their Unix sockets need paths below 108 bytes; the default is a directory of the
process's own, 0700, under `$XDG_RUNTIME_DIR` (a tmpfs where systemd sets it), else the
system's temp dir, removed at the end; a `--state-dir` given gets a `route-<n>` directory
(0700) per route started, removed with it). A cloned route's snapshot there is what its
template wrote of its memory, unencrypted: keep the state dir on a tmpfs (the wicket warns
otherwise), or it is on disk until the route ends, and after a crash. The tmpfs must hold
every cloned route's snapshot, its `memory-mib` at most (systemd sizes `$XDG_RUNTIME_DIR` to 10% of
the host's memory: `RuntimeDirectorySize` in `logind.conf`).

The VMs are the wicket's children, forked for it by a process of its own (`sluice-forker`,
the wicket again, started first: a fork of the wicket itself would stall its threads; see
[findings](../.kb/wicket-firecracker-findings.md#the-forker)); in [jail mode](#jail-mode), the
keeper's.

The tunnel's settings are `sluice-wicket-runtime-wasi`'s: `--token` (or `--token-file`; on the
command line every user of the host sees it), `--client-id`, `--ca-cert`,
`--insecure` (`grpcs://`), `--max-connections` (256), `--tunnel-stall-timeout` (60s),
`--drain-grace` (20s: SIGTERM / SIGINT withdraw the routes and end the streams once their
connections ended; a second signal ends at once) and `--evacuation-grace` (0: before the drain,
the wicket serves on while the controllers move the apps of the console's API elsewhere, as
sluice-wicket-runtime-wasi's [Shutdown](../sluice-wicket-runtime-wasi/README.md#shutdown-drain);
an app with a [disk](#disks) is not waited for: no other wicket has its data). Each also reads
`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>` from the environment, e.g. `SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN`;
another variable of that prefix but the two below, a misspelled one, is refused at the start.
`--workers` (4, at most the CPUs) sets the threads relaying the connections (see
[findings](../.kb/wicket-firecracker-findings.md#relay-workers)). `--health <host:port>` serves the
wicket's readiness probe, `GET /ready`, as sluice-wicket-runtime-wasi's ([Readiness
probe](../sluice-wicket-runtime-wasi/README.md#readiness-probe)): ready once the routes started,
each with a VM [ready](#readiness) since or `--ready-grace` (60s) passed, and with `--server`
every node took them; not from the drain on. `--probe <host:port>` asks it (exit 0 when ready, 1
when not).

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
max-instances: 64
max-memory-mib: 16384           # of guest memory
max-cpus: 4x                    # the VMs' CPUs: 4 times the wicket's; or so many, 256
workers: 4                      # threads relaying the connections
watch-interval: 1s              # 0 = SIGHUP only
drain-grace: 20s
evacuation-grace: 0             # 60s: on SIGTERM, serves on while the apps move elsewhere
ready-grace: 60s                # the readiness probe waits this long for each route's first VM
# flume: grpc://127.0.0.1:8002  # see Output and the live access log
# flume-token: ${FLUME_TOKEN}
# jail-ids: 900000-900127       # see Jail mode
# jail-user: sluice
# jail-cgroup: sluice
routes:
  - {host: hello.local, rootfs: guest.ext4}
  - {host: app.local, rootfs: upstream.ext4, mode: per-connection, memory-mib: 512, vcpus: 2,
     prime: 200, warm: 4, instances: 16, wait-ms: 10000, per-client: 4, idle-ms: 30000,
     egress: [db.internal:5432, 10.0.0.0/24:6379, '[2001:db8::/32]:443']}
  - {host: api.local, rootfs: go.ext4, mode: pool, instances: 8, warm: 1, concurrency: 16,
     keep-ms: 60000, readiness: http, readiness-endpoint: /healthz}
  - {host: fn.local, rootfs: go.ext4, mode: reuse, keep-ms: 60000, uses: 1000,
     lifetime-ms: 3600000}
```

### Reloading routes

The routes are reloaded without a restart: their files are polled every `--watch-interval`
(default 1s) -- the config file, every rootfs, the kernel and the firecracker binary -- and
SIGHUP reloads at once. A change is reloaded once a poll found it unchanged since the previous
one, so a file still being written is not read half way. A reload reads the routes again and
starts the VMs of every target that is new or whose VMs or files changed (its VMs booted,
a cloned route's template booted and snapshotted again) before it changes anything; one
failure keeps the routes as they were (`routes reload (file change) failed; keeping
generation N: ...`). Otherwise (`routes reloaded (file change), generation N: added ...;
removed ...; restarted ...; adjusted ...`):

- a target unchanged keeps its VMs, and so does one [adjusted](#how-many-vms-and-how-long): only
  its numbers changed
- a target restarted serves its next connection from the new VMs; a connection open stays
  on the VM it got. A booted or pooled VM's process is told to stop then (SIGTERM, 10s to end
  its connections and itself, as [a stop](#disks) on a disk), the VM gone with it; a clone of a
  connection's own ends with its last connection. Replace a rootfs by a rename: a VM has its
  file open
- a host added or removed is advertised on every node stream; a removed target is served
  until every stream acknowledged its removal (the server may have routed a connection to it
  before), its VMs stopped so then
- an app's VM on a [disk](#disks) is stopped before its next one starts, last of all

The VMs start beside the rest (`routes reload (file change): its VMs starting`): no edit waits for
them. The next change of the files takes over from a start under way, ending its VMs; SIGHUP with
none leaves it.

Only the routes, the kernel and the firecracker binary are reloaded; the other settings apply
at startup. The console's edits go through the same reload (see [Console](#console)).

### How many VMs and how long

`instances`, `warm`, `concurrency`, `wait-ms`, `keep-ms`, `uses`, `lifetime-ms`, `per-client`, `idle-ms`,
`boot-ms`, the [readiness check](#readiness) and the [liveness check](#liveness) say how many VMs, how
long and when one serves, not what
a VM is: a running target takes a change of them as it is, by a reload or a console's edit, its VMs and
their snapshot kept (`adjusted <host>`):

- `instances` or `warm` up: VMs cloned as there is room, in a restore's time, or at `mode=boot` booted, a
  connection reaching one once its guest accepts, none other touched; a connection waiting for
  room finds it
- `instances` down: of a pool, the VMs past it (the highest indices) take no connection any more, and
  are killed once their own ended and `keep-ms` passed (at `mode=boot`, none: with their last
  connection); up again before that, they serve on. Per connection and reused, what serves none past it is killed at once, and the connections open
  finish
- `warm` down: warm clones are killed at once, a pool's VMs serving none after `keep-ms`
- the readiness check: each VM probed at once as it asks
- the liveness check: each VM probed an interval of it later, none started again for it
- the others bound the next connections, and the next boot (`boot-ms`)

Anything else is what a VM is and [restarts the target](#reloading-routes): `mode` too, and `prime`, what
its snapshot holds.

An app's target names none of the numbers: only its weight is advertised, its `instances`, by
which a sluice server shares its host among the wickets serving it (the fewest requests in flight
for their weights), and a server's keep-alive
connection stays on its VM. A route's does, as routes on one image with other numbers have VMs of
their own: its VMs go on under the target of its new numbers, advertised, the old one served by
them until every stream acknowledged it. A host going to other numbers away from a host staying
gets VMs of its own.

### A VM going down

A VM of `mode=boot` or `mode=pool` serves no more when, and the wicket then starts another one in
its place, from the same spec, under its instance index:

- `/init` says the image's process ended (`guest: <program> exited`), whatever its status, and
  the port refuses: a route's process is there to serve. A launcher that left a daemon listening
  has not ended; nor has a stop of the wicket's, which empties the route first
- the VM's log stream ends: its `firecracker` ended, or the guest's kernel did
- a connection fails, and the port goes on refusing for the route's `boot-ms`: the daemon's end,
  a process that closed its port, a guest that no longer answers
- a probe of its [liveness check](#liveness) fails (`liveness=http`): a process that lives, keeps its
  port and answers nothing, or answers its health endpoint other than 200

Without a liveness check, a process that lives, keeps its port and answers nothing is not seen.

Why is told with how it ended: `its process ended (exit status: 1)`, its wait status as `/init`
saw it; `its VM ended (signal 9)`, its `firecracker`'s. An OOM kill is named so: `its process
ended (signal: 9 (SIGKILL)): out of memory, the guest kernel's OOM killer took a process`, when
the guest's count of them (`/proc/vmstat`) rose while the process ran (a process it started taken,
it ending for that, says so too); `its VM ended (signal 9): out of memory, the host kernel's OOM
killer took it`, in a [cgroup](#cgroups) of its own when its `memory.events` counted it, without
one when the host's count rose since the sampler read it a moment before (another process's
`kill -9` of the VM as the OOM killer took one elsewhere reads as one). An `/init` predating it says `its process
ended`.

The others serve on, its connections alone cut. The old VM is killed (on a [disk](#disks), stopped
first: SIGTERM to what is left, all written out, the disk free), the new one on the room in the
budget the old one held (no other route takes it meanwhile), its env and disk as they were. Per
VM: the first start is at once; a VM that goes down within 60s of its start, or a start that fails,
waits 1s, then 2s, 4s, up to 60s between starts, and a VM up for 60s starts that count anew.
A change of its bounds (`warm`, `instances`, `cf scale -i`, ...) starts it at once, that count going on.
`instances.down` says why (the lowest index's), an app's listing `crashed` for each index. With none up,
a booted route's connections fail at once (`guest unreachable: no VM up: the route is restarting`;
the open ones end with their VM), a boot long when all goes well; a clone pool's wait for its
`wait-ms`, one asking for a VM ahead of its wait. The route leaves the advertisement meanwhile
([Readiness](#readiness)).

The starts run beside the reloads: a route failing holds no edit, and fails no reload, of another
one. A reload keeps a route down as it is, unless its spec or files changed: that start then
takes over at once, the wait forgotten. So does `PUT` of another env; the same `PUT` is `no
change`, the VM coming back by itself. The wicket's stderr says each step (`route <hosts>: VM 1: its
process ended; killed`, `VM 1 starts again in 2000ms`, `pool VM: ... (instance 1)`),
`GET /api/routes` a route's `instances.restarts`, the VMs up in the place of one, reset by a change of its
spec, and `instances.down`, why it is short (the page shows both).

A clone pool's VM comes back in a restore's time, from the snapshot, the VMs it keeps being those
below `warm`. A start failing for room within `wait-ms` is tried again every second, one failing
otherwise (its snapshot gone) after the same waits. So are a warm clone's restores: one at a time
while they fail, 1s, then 2s, 4s, up to 60s apart, the count starting anew once one succeeds, so
a route can read as down for up to 60s after the cause ended. A connection waits for none of this
(room within `wait-ms`, then its own restore, whose failure counts as a try and whose success
restores the warm clones missing at once); nor does a change of the route's bounds, which tries
at once, the count going on. A reload that changes nothing leaves the wait as it is.
`instances.down` says why until `warm` are up. What fails beyond `warm` is the connection's
to be told, and waiting for room within `wait-ms`, or for a warm clone's, is no failure, unless no VM
is alive and the restore failed for anything but room: then `warm=0` would read as idle at none, so
`instances.down` says why too and one VM is tried again, as above, until one is up
(gone again as `warm=0` has it). `instances.down` is `{"why": ..., "since": <seconds since the epoch>}`,
`since` the start of what goes on, each failure after it changing only `why`.

### Readiness

A VM serves once its guest accepts on the route's port. `readiness=port|http` (`mode=boot`, `mode=pool`)
has it serve once its readiness check passed as well, as a CF app's readiness health check;
`readiness=process`, the default, asks nothing more:

- **the check**: `port`, a connection to the process's port, which it accepts; `http`, a `GET` of
  `readiness-endpoint` (`/`; a path, its query with it) naming the route's first host, answered 200.
  The wicket makes it through the guest's relay, as a connection reaches the process: any rootfs
  takes it ([findings](../.kb/wicket-firecracker-findings.md#readiness))
- **when**: from a VM's start, once its guest accepts: at once, then every 500ms (`readiness-interval-ms`,
  if shorter) until it passes, then every `readiness-interval-ms` (30000), each within
  `readiness-timeout-ms` (1000). Failing, the VM takes no new connection, its open ones going on, and
  is probed every 500ms again until it passes. It is not restarted: what ends is ([A VM going
  down](#a-vm-going-down))
- **a connection** goes to the VM ready holding the fewest; one finding none is as one finding no
  VM up: a booted route's fails at once, a pool's waits for a VM started for it, up to `wait-ms`. A
  pool's VM started for connections waiting (as it grows, or at `warm=0`) takes them as it comes,
  unprobed, and is probed from then on
- **the advertisement**: a route whose next connection finds no VM -- none up, every one failing its
  check, or none alive and its starts failing -- leaves it on every node stream until one does, a
  route starting with the wicket included: the server sends its host to another client serving it,
  else as for a host no client advertises (a host pattern route, the catch-all, else its `503`).
  Its target stays bound: a connection the server routed before fails. `--listen` routes its host
  as ever, its connections failing alike
- **per connection, reused**: refused, a connection's VM being its own, restored as it comes
- **the wicket's probe** (`--health`) waits for each route's first VM ready since the wicket's
  start (`not ready: no instance ready yet for app.local`), up to `--ready-grace` (60s, 0: not at
  all) from the routes' start, when it logs those still without one. A route's readiness after
  that is its app's: one with none ready leaves the wicket ready, named
  (`ready; no instance ready for app.local`), so an app failing its check holds no update of the
  wickets (`docker compose up --wait`, a StatefulSet's rolling update)
- **told**: the log says each change (`route app.local: VM 1 not ready: GET /ready: HTTP/1.1 503
  Service Unavailable`, `route app.local: VM 1 ready`); the console's listing a route's `ready`
  (advertised) and `instances.ready` (its VMs taking connections), an app's `alive` each VM's `ready`
- **a change** of the check is taken as the route runs ([How many VMs and how
  long](#how-many-vms-and-how-long)), its VMs probed at once

```yaml
routes:
  - {host: app.local, rootfs: upstream.ext4, mode: pool, instances: 4, warm: 2, readiness: http,
     readiness-endpoint: /actuator/health/readiness, readiness-interval-ms: 10000}
```

### Liveness

A VM is up once its guest accepts on the route's port, and down as [A VM going down](#a-vm-going-down)
says. `liveness=http` asks more, as a CF app's health check (`health-check-type: http`); `liveness=port`,
the default, nothing:

- **at its start**: a booted VM is up, and a cloned route's template snapshotted, once a `GET` of
  `liveness-endpoint` (`/`) naming the route's first host is answered 200, asked every 100ms from its
  guest's accepting, within `boot-ms` from its boot; its process exiting first fails it at once. A
  clone starts from the snapshot of a process that passed it
- **after it**: a VM `mode=boot` or `mode=pool` keeps is probed every `liveness-interval-ms` (30000),
  each probe within `liveness-timeout-ms` (1000), through the guest's relay as the readiness check is.
  One probe failing has it gone down (`route app.local: VM 1: its liveness check failed: GET /livez:
  HTTP/1.1 500 Internal Server Error; killed`), another one started in its place, as Diego restarts an
  instance at its first failure; the readiness check takes a VM slow for a while out of the rotation
  instead. A clone of a connection's own is not probed past its template: `liveness-interval-ms` needs
  `mode=boot` or `mode=pool`
- **`port`** is the watch's, with no probe of its own (no timeout nor interval): a periodic connection
  to the port adds nothing to it, the guest's kernel accepting on a listening socket whatever the
  process does. `liveness=process` is refused: a route relays its connections to the port
- **a change** of the check is taken as the route runs ([How many VMs and how
  long](#how-many-vms-and-how-long)): the VMs up are probed an interval of it later, never started
  again for it ([findings](../.kb/wicket-firecracker-findings.md#liveness))

```yaml
routes:
  - {host: app.local, rootfs: upstream.ext4, instances: 2, liveness: http,
     liveness-endpoint: /actuator/health/liveness, liveness-interval-ms: 10000}
```

### Console

`--console <host:port>` (`console`) serves a page and a JSON API deploying rootfs images as
routes at once, without a restart: drop an ext4 image on the page (or choose one), give it a
host and the route's options, and it is served -- on the tunnel and on `--listen` alike.
Dropped on a route of the console, an image replaces its own; renamed, a route moves to
another host with its image and options. The routes of the config file and the `--vm` flags
are listed too, read-only. The page shows what the wasm wicket's has no counterpart for:
each route's VMs (up or pooled and their restarts, ready, warm, kept, starting, all alive, [what they
use](#what-a-vm-uses)) and whether it is
advertised ([Readiness](#readiness)), its connections, a cloned
route's snapshot (its memory file, the template's boot and snapshot times) and the budget
(`--max-instances`, `--max-memory-mib`, `--max-cpus`) taken. It takes 64 connections at once; TCP keepalive ends one whose
client is gone, and a client that leaves one waiting for 60s, not sending a request or not reading
its answer, loses it. All taken, one without a request the gate let through gives way to the next,
the source holding the most first
([details](../.kb/connection-limits.md#the-wickets-consoles-and-sluice-wicket-cf-api)); with none,
the next waits in the backlog.

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

An edit's VMs start beside everything else: none waits for a boot, which a process never listening
takes a `boot-ms` long, whether a [disk](#disks)'s VM is replaced or the files are reloaded. A later
edit of the same route or app takes over from it, ending its VMs (its request a 422; on a disk
[stopped](#disks), the edit's own VM waiting for that, 20s at most). An edit goes
by the config file as the routes last took it and leaves a changed file to its reload, whose failure
refuses no edit; an edit needing a disk that reload starts a VM on takes over from it, the reload
made again after it.

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
| `GET /api/routes[?after=<revision>]` | every route with its VMs (what each uses among it), the budget and the last failed reload of the files, as JSON; with `after` the current `revision`, held until the routes change (25s at most; the VMs' counts and use move without a revision) |
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
and its digest), its `options` set to other than their defaults, its `instances` and `snapshot`, an
app's name and owner, the names of its env, and whether it is `ready`: its next connection finds a
VM, every stream advertising it ([Readiness](#readiness)).

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
| `PUT /api/layers/<sha256>`, `GET /api/layers/<sha256>?at=<n>`, `POST /api/builds`, `GET` / `DELETE /api/builds/<id>`, `DELETE /api/caches/<name>` | source staged by buildpacks into a droplet ([below](#source-staged-by-buildpacks)) |
| `GET /api/apps[?after=<revision>]` | the apps, as `/api/routes` (held with `after`): `{"revision", "generation", "wicket", "budget", "runtime": "firecracker", "capabilities": [], "keyvalue_stores": [], "apps": [..]}`; `wicket` which wicket this is: `id`, made at its first start and kept in the console directory (`wicket-id`), `node`, its flume node (`wicket:<client-id>`, null without `--flume`), `build`, `since` (its start, in seconds since the epoch); `budget` `max_instances`, `max_memory_mib` (with `--max-memory-mib`), `max_cpus` (with `--max-cpus`, in CPUs) and what the routes keep of them for good (their booted and warm VMs): `instances`, `memory_mib`, `cpus`. An app needing more is refused 422 with `"no_room": true` |
| `PUT /api/apps/<name>` | serves the app of the body (JSON, below) |
| `DELETE /api/apps/<name>?owner=<owner>` | removes the app of `owner` |
| `PUT /api/evacuated/<owner>` | a wicket leaving (`"leaving"` in both listings, `--evacuation-grace`): `owner` moved what of its apps it could, those left staying until the drain; 409 unless leaving |
| `PUT /api/confirmed/<owner>` | the apps of `owner` the body names (`{"apps": ["<name>", ..]}`) are as it wants them: those held are served from now on (`held-ms` below). `{"released": [..], "held": [..]}` |

```json
{
  "owner": "cf",
  "hosts": ["shop.localhost", "store.localhost"],
  "component": {"sha256": "0954dc11…e8", "name": "shop.ext4"},
  "sandbox": {"mode": "pool", "instances": 2, "memory-mib": 256, "egress": ["db.internal:5432"],
              "env": {"GREETING": "hello"}},
  "tag": "3f1c…9a",
  "held-ms": 15000
}
```

- the sandbox is the route options of `--vm` in JSON (a number, or a string; `egress` a list),
  each left out taking its default, and its `env`, `name -> value`, over the image's. The
  listings name the variables only (`"env": ["GREETING"]`), never a value
- `tag` (optional) is opaque to the wicket, kept and listed as given: a controller tells by it
  what it put, an env it cannot read back included. A tag or hosts alone start no VM
- `held-ms` (optional; 1 to 3600000) holds the app at each start of the wicket, as the wasm
  wicket's ([Apps](../sluice-wicket-runtime-wasi/README.md#apps)): its VMs start, but it is
  neither advertised nor served on `--listen` (503), listed `"held": true`, the wicket not ready,
  until its owner confirms it (`PUT /api/confirmed/<owner>`), puts it again or removes it, or
  `held-ms` passed. It alone starts no VM
- `alive` in a listing: its VMs alive, lowest first, `{"index": 0, "since": 1793000000, "ready":
  true, "usage": {..}}`: the `instance` index, its start in seconds since the epoch, its guest
  accepting, whether it takes connections ([Readiness](#readiness)) and what it uses ([What a VM
  uses](#what-a-vm-uses)); `crashed`, those it
  keeps that are down, `{"index": 1, "why": "its process ended (exit status: 1)", "since": 1793000000}`, each until
  another one is up there; `down`, when told: why it is short of VMs, and since when (`instances.down`),
  until they are up; `advertised`, whether the server of every tunnel stream took its routes as
  they are, a VM of them ready (true in local mode): a controller moving an app here removes it
  from the wicket it leaves once they are
- a PUT is one reload: every host of the app or none, kept only when its VMs started; the same
  app again changes nothing. An app's target names it (`?app=<name>`), so its VMs are its own,
  whoever else names the image; the env is in no target: another env starts its VMs anew. Its
  numbers are in none either: [its VMs stay](#how-many-vms-and-how-long), and the PUT does not wait
  for the VMs a higher one clones
- 422 with `"did_not_start": true` when its VMs did not start, of what they are: a controller need
  not try again until something it asks changes; a 422 without it may pass as the routes change
  (no room for them)
- 409 and `{"missing": [<sha256>]}` when no image of the digest is kept: upload it and put
  again. An upload is kept for ten minutes, a file an app named as long as an app does
- a host the config file, a flag, a console route or another app routes is refused (409), and so
  is a change of another owner's app; the page lists an app's routes, read-only
- a write of an owner may carry its controller's epoch (`sluice-epoch: <n>`), one below the
  highest it sent refused (409, `"fenced": true`), as the wasm wicket's
  ([Apps](../sluice-wicket-runtime-wasi/README.md#apps))

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

##### Source staged by buildpacks

With `--guest`, the wicket is the platform of a [Cloud Native Buildpacks](https://buildpacks.io)
build: a builder's `detector` and `builder` run on the source in a VM of the builder's image,
and what they made comes back as a droplet, a tar of what goes over the run image's root file
system (the launch layers, the app, the launcher). No analyzer, restorer or exporter runs: the
build has no registry and no daemon in reach, and the wicket keeps the cache itself (below).

| request | |
|---|---|
| `PUT /api/layers/<sha256>` | keeps the body, a tar of 4 GiB at most, under its digest for ten minutes: a build's source (the app's files, no leading directory), a droplet built elsewhere. `{"sha256": .., "bytes": ..}` |
| `GET /api/layers/<sha256>?at=<n>` | the kept tar from byte `n` on, 8 MiB at most a request; `sluice-layer-bytes` its size |
| `POST /api/builds` | starts the build of the body (below): 202 `{"id": ..}`; 409 with `"missing": [<sha256>]` when the source is not kept |
| `GET /api/builds/<id>?after=<n>` | its output from line `n` on, held up to 20s until there is more: `{"state": "building" \| "built" \| "failed", "next": <the next request's after>, "lines": [{"stream": "stdout" \| "stderr", "line": ..}]}` and, once every line was read, `"error"`, or `"droplet": {"sha256", "bytes"}`, `"run-image"` (pinned to its digest) and `"launch"` |
| `DELETE /api/builds/<id>` | ends and forgets it; an ended build is forgotten after ten minutes |
| `DELETE /api/caches/<name>` | forgets the cache of that name (an app deleted); 404 when none is kept |

```json
{"builder": "paketobuildpacks/ubuntu-resolute-builder", "source": "<sha256>",
 "env": {"BP_JVM_VERSION": "21"}, "egress": ["repo.maven.apache.org:443", "github.com:443"],
 "buildpacks": ["paketo-buildpacks/java"], "cache": "<an app's guid>"}
```

`builder` and `source` are required; `username` and `password` are the builder's registry's
(the run image is asked with them when it is there too); `run-image` is the run image in place
of the one the builder's label names (Paketo's tiny, which `ubuntu-resolute-builder` names
none of), as `pack build --run-image`: of the builder's target, its OS and architecture, and its
distribution where both name one (`io.buildpacks.base.distro.*`, else
`io.buildpacks.stack.distro.*`), else the build fails before a VM is; the detector checks the
buildpacks' targets against it, as an analyzer's. `env` is the app's environment as the
buildpacks see it (`/platform/env`), `egress` the destinations the build may connect to, as a
route's (none: no network; a buildpack downloading its runtime needs each host it is
redirected to), `memory-mib` (2048), `vcpus` (2), `disk-mib` (8192: the build's scratch, which
holds the layers, the app, the droplet and the cache) and `timeout-ms` (15 minutes, an hour at
most) the build VM's. The VM counts in the budget as any other; a build finding no room within
30s fails. `buildpacks` (`<id>[@<version>]`, of the builder's: a version left out is the latest
it has; or `docker://<image>`, a buildpack image) are the only ones detecting, in one group, the
order the detector is given; none, the builder's order. A buildpack image is pulled as `pack
build --buildpack docker://<image>` takes one: its buildpack is its
`io.buildpacks.buildpackage.metadata`'s, its layers those `io.buildpacks.buildpack.layers` names
(the buildpack and those of its order, a layer of the builder's buildpacks alone left out),
checked against their diff IDs and laid over the builder's `/cnb/buildpacks` in the VM, each
writing its buildpacks' directories alone; the detector checks their targets as the builder's.
It is asked with the builder's credentials when it is on the builder's registry. The layers are
kept by their diff IDs (`buildpacks/` in the console's directory; one not used for a week goes
when another is kept): the next build pulls none of them again. `command` is what the droplet runs, a shell's command line: the launcher runs
it with bash (the run image's, as CF runs a command), in place of the default process. A run
image without `/bin/bash` (Paketo's tiny) takes none: its rootfs is refused.

`cache` names the cache the build restores and leaves (an app's guid, say): the `cache = true`
layers of the last build of that name, a tar kept in `caches/` of the console's directory,
are laid over `/layers` once the detector ran, those of the buildpacks detected, each
`<layer>.toml` holding its `metadata` alone, as the lifecycle's restorer leaves them (the
buildpack sets the types again), with its SBOM files; a built build leaves its own in their
place, a failed one the last. A cache is this wicket's: a build on another starts cold. One not
used for a week goes when another is kept.

The builder's rootfs is made as an image's and kept by its manifest's digest and the guest
(`builders/` in the console's directory; one not used for a week goes when another is made):
the first build with a builder waits for it, minutes for a large one. The builder's label
names its run image and the platform APIs its lifecycle speaks (0.12 to 0.15 here). In the VM
the builder's root is read-only, so the phases run in an overlay of it on the scratch disk, as
the builder's user: the guest kernel needs `CONFIG_OVERLAY_FS` (Firecracker's CI kernels have
it).

The droplet runs on its run image: `PUT /api/images` with the run image as `image`, the
droplet's digest in `layers` (kept layers, laid over the image's in order) and the build's
`launch` (what runs over the image's config: `entrypoint`, `env`, `path`, `workdir`, and
`needs`: the files it runs, each with why, which the image must hold, its user allowed to run
them, else 422) makes the rootfs, here or on any other wicket that was given the layer. 409
with `"missing"` names a layer not kept.

#### Disks

An app's booted VM may have a disk: a writable drive that outlives it, kept by the app's owner
and name in the console's directory, whatever the image, options or env. The sandbox's `disk`
is where `/init` mounts it before the process starts, `disk-mib` its size (default 1024):

```json
{"owner": "ops", "hosts": ["db.localhost"], "component": {"sha256": "0954dc11…e8", "name": "db.ext4"},
 "sandbox": {"memory-mib": 256, "disk": "/data", "disk-mib": 4096}}
```

- **made** at the app's first start, by `mkfs.ext4` (the wicket needs it, e2fsprogs: in the
  image), its room taken on the host then (`fallocate`): a disk the host has no room for is a
  refused PUT, not an I/O error in the guest later. ext4 with a journal: no fsck runs in a VM,
  and nothing on the host reads what a guest wrote
- **`disk`** is a directory of the image (the rootfs is read-only: `mkdir` works on `/tmp` alone).
  Its root goes to the image's user at each mount; what is below stays as it was, whoever wrote
  it. Refused with the guest's reason (`the guest's disk: ...`, 422) when it cannot mount, and an
  image whose `/init` predates disks is refused too: it would serve without one
- **one VM writes it**: `mode=boot` at `instances=1` alone (a clone's snapshot holds its guest's page
  cache of the drive; refused otherwise), and the file is locked by its `firecracker`
  (`flock`, the kernel's: gone with the process, whatever became of the wicket), so a second
  wicket on the directory, or a VM not yet gone, is waited for and then refused. A change of
  the VM (its image, options, env) is the exception to [Reloading routes](#reloading-routes):
  once every other target started, the old VM is stopped, then the new one started, on the
  same disk. Its connections fail meanwhile, a boot long.
  A start failing starts the old VM again, the PUT refused as any other (with a process that
  never listens: once its boot's bound ran out); failing as well, the refusal says so, and the
  route serves nothing until [its next start](#a-vm-going-down) succeeds
- **a stop** tells `/init`: SIGTERM to every process, 10s for them to end (then SIGKILL), what
  is dirty written out, the disk unmounted, the VM killed, 20s at most. So does the wicket's own
  end, after its evacuation and its drain (`--evacuation-grace`, `--drain-grace` and these 20s:
  `--stop-timeout`, `TimeoutStopSec`; an app on a disk moves to no other wicket, its wicket's
  stop is its downtime), a
  [process's end](#a-vm-going-down), a process exiting before it listens, and a start
  taken over or given up once `/init` mounted the disk: the next start on the disk waits for it.
  A VM simply killed,
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
  owner. `GET /api/disks` lists them, `{"disks": [{"owner", "name", "mib", "allocated_bytes",
  "used_bytes", "app": <whether it has its app>}]}`: what its file takes of the host's disk, and
  what its file system uses as the guest of its VM told last (left out without one);
  `DELETE /api/disks/<name>?owner=<owner>` removes one, refused (409) while its app
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

`--max-instances N` (default 64) caps the VMs alive at once over every route -- booted ones,
templates, clones warm, in use, kept and pooled --, `--max-memory-mib N` (default none:
`--max-instances` x the largest `memory-mib` bounds it) their guest memory (`memory-mib`) and
`--max-cpus` (default none: `--max-instances` x the largest `vcpus`) their CPUs. A route's
`instances=N` caps its own (at least `warm`). A VM takes its share before its `firecracker` starts
and gives it back once killed, and one going down holds it for the VM in its place; a startup whose
booted VMs (`instances` x `memory-mib`, x their CPUs) and pools' `warm` VMs do not fit, with
a template at a time beside the other routes', fails, and so does a reload (a console's edit
included).

A VM's CPUs are its `vcpus`, or its route's `cpu` where that is less (`vcpus=2`: 2; `cpu=50`: 0.5),
whatever it runs. `--max-cpus` is so many (`96`, `6.5`) or, with `x`, so many times the CPUs the
wicket runs on (`4x`: four vCPUs to each; its affinity and its cgroup's quota count, a container's
`--cpus`): the overcommit, which the wicket says at its start (`CPU limit 256 (on 64 CPU(s):
4.00x)`). Past the host's CPUs the VMs contend for them. In [cgroups](#cgroups) of their own,
each has a share by the CPUs it holds, and the wicket's own threads run before theirs (jailed:
with the default `--jail-cgroup`;
[measured](../.kb/wicket-firecracker-findings.md#the-cpu-budget-and-the-shares)); without them,
their threads and the wicket's contend as equals: keep `--max-cpus` below what the host has
there.

A connection whose clone finds no room waits for one in turn (one held back by its own
route's `instances` alone keeps no other route waiting), at most its route's `wait-ms` (default
10000), then its request is answered `502 Bad Gateway`, on the tunnel and on `--listen`, http/1.1
and h2c, saying so (`no room for a VM within 10000ms: a VM per connection, 2 at most`; the
budget's numbers stay in the wicket's log). A connection that is no HTTP request fails with an
ERROR, which the server answers by closing the connection (`--listen`: closed); a pool's
connection waiting for a VM being started takes one of the VMs up once its `wait-ms` ran out.
Warm clones take room nobody waits for, and give it up: the first connection in line short of
room has a spare VM of another route killed -- a warm clone, a reuse route's clone serving
no connection, a pool's VM beyond its `warm` serving none.

Without more, one client holds as many of a route's VMs as it opens connections, until the
server's `sluice.limits.idle-timeout` closes them (`--listen`: 60s with no request in progress). What
bounds one client's share:

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
- **`cpu=N`**: a VM's host CPU time, in percent of one CPU (`cpu=50`, half a CPU; `cpu=200`,
  two), its [cgroup](#cgroups)'s `cpu.max` (per 100ms), its VMM and API threads counted.
  Without a cgroup of its own a VM takes at most its `vcpus` host CPUs (and its VMM thread's
  share), and a route's `cpu` is refused

### Cgroups

A VM in a cgroup (v2) of its own is held to what it was given, whatever its guest does:

- **`memory.max`**: its `memory-mib` plus 32 MiB plus `memory-mib`/128 (its page tables) plus
  16 MiB (its log, bound by `RLIMIT_FSIZE`, a disk's size with a disk: the limit holds for an
  offset in any file), its `memory-mib` twice for a template (it writes its snapshot). Past
  it the kernel's OOM killer ends the VM, nothing else
- **`pids.max`**: its vCPUs plus 16
- **`cpu.weight`**: 10 per CPU the VM holds of the budget (see [Limits](#limits)), so VMs short
  of CPUs share them by what each was given, and **`cpu.max`** its route's `cpu`. A clone of
  a connection's own (`mode=per-connection`) without `cpu` is in `shared/` instead, one group
  of the controller for all of them, weighted by what they hold together (within 100ms): a
  group per VM costs 8% of their rate (see
  [findings](../.kb/wicket-firecracker-findings.md#the-cpu-budget-and-the-shares))

A cloned route's snapshot stays charged to the route's cgroup (`route-<n>`) as long as the
route lives; a serving VM's (`vm-<n>`, one per VM alive) is reused by the next.

Who makes them:

- **[jail mode](#jail-mode)**: the keeper, under `--jail-cgroup`
- **without the jail**: the wicket, beside its own cgroup, where that is delegated to it: its
  to write (its directory, `cgroup.procs`, `cgroup.subtree_control`), the memory and pids
  controllers in it, no other process. A systemd service's with `Delegate=yes` and `User=`
  (`KillMode=mixed` and `TimeoutStopSec=` as [in jail mode](#jail-mode)), or a scope of the
  user's manager:

  ```text
  systemd-run --user --scope -p Delegate=yes target/release/sluice-wicket-runtime-firecracker ...
  ```

  The wicket leaves its cgroup for `wicket/` in it (cgroup v2 enables no controller for the
  children of a cgroup holding processes), of the most `cpu.weight` (10000): its threads run
  before the VMs', whose cgroups are beside it. Without the cpu controller there, no VM has a
  share, and a route's `cpu` is refused

The wicket says which at its start (`the VMs' cgroups in ...`, or `no cgroup of the VMs' own:
...` and why). With none -- a shell's session, the [Docker image](#docker-image)'s container
(its cgroup is read-only to the image's user: jail mode there) -- the wicket's own cgroup bounds
the VMs all together (a unit's or a container's `MemoryMax=`), they contend for the CPUs with
its threads, and a route's `cpu` is refused. The cgroups without the jail are the wicket's
user's to write, which the VMs run as: they hold a guest, not one that escaped its
`firecracker` (see [Host setup](#host-setup)).

### What a VM uses

Both listings give each VM alive what it uses of the host, read every 5s: an app's `alive` entries
and a route's `alive` in `GET /api/routes` (the page sums them per route, each VM's in its title),
as `"usage"`:

```json
{"cpu_ns": 1840000000, "cpu": 0.0312, "memory_bytes": 73400320, "memory_peak_bytes": 79691776,
 "disk_bytes": 21708800}
```

- **`cpu_ns`**: the CPU time of its `firecracker` since its start, every thread (its vCPUs, its VMM
  and API threads); `cpu` the CPU of the last 5s in CPUs (1.0: one all along, what `cf app` shows
  as 100%)
- **`memory_bytes`, `memory_peak_bytes`**: its process's resident memory and the most of it so far
  (`VmRSS`, `VmHWM`): the guest memory it touched and the VMM's own. A clone's counts the pages of
  its snapshot it maps, shared with the route's other clones (the snapshot is the route's,
  `snapshot.memory_bytes`)
- **`disk_bytes`**: what its disk's file system uses, as its `/init` answers (none without a disk;
  an `/init` predating it answers none)

They are read of the process alike with the jail or without it, as the wicket's user; an entry
has none until the first read. The values move no listing's `revision` (a long poll does not wake
for them). How a VM ended, an OOM kill named so, is [A VM going down](#a-vm-going-down)'s
([findings](../.kb/wicket-firecracker-findings.md#what-a-vm-uses)).

### Output and the live access log

What the processes in the VMs write to their stdout and stderr -- an image's process, its
restore hook, the sample guest's own lines -- reaches the wicket's stdout and stderr (so
`docker logs` shows it), each line prefixed with its route's hosts and its VM:

```text
stdout [app.local template] :: Started Application in 1.9 seconds
stderr [app.local vm-3] :: WARN payment declined
stdout [hello.local, hi.local vm-0] :: GET /log, request 7 of boot id ...
```

- the VM is `vm-<index>` for a booted VM, `template` for a cloned route's template and
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
| `stdout` / `stderr` | `vm` (the prefix's), `instance`, `line` |

`instance` is the VM's index among its route's VMs alive, as CF numbers an app's instances
(`cf logs` shows it): 0 for a template; a VM holds the lowest index free while it lives, warm,
serving or booting, so a route of `instances` VMs names `0` to `instances - 1` and one in the place of a VM that
failed takes its index. `vm` is no such index: it counts the route's clones.

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
connections open at once are 256 at most; TCP keepalive (as on `--listen`) ends one whose
destination is gone without a FIN or reset. Each is recorded once it ended or was refused, on
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
- is in a [cgroup](#cgroups) of its own, the keeper's to write alone. With the default
  `--jail-cgroup` the wicket's own cgroup (`keeper/`) is beside the VMs', of the most weight
  (10000): its threads run before theirs
- ends with the keeper: its parent death signal is set after the uid change (which clears
  it: the jailer's `firecracker` outlives the jailer's parent). The keeper forwards SIGTERM,
  SIGINT and SIGHUP to the wicket and ends with it, its VMs killed, their files and cgroups
  removed; the wicket ends with the keeper as well

What it needs:

- root, cgroup v2 (`/sys/fs/cgroup`). The keeper's directory is
  `/run/sluice-wicket-runtime-firecracker-<pid>` (a tmpfs), or such a directory in
  `--state-dir`
- `--jail-ids`: a range no user of the host has, nor a subordinate range of `/etc/subuid`, of
  `--max-instances` ids at least (one per VM alive)
- `--jail-user` reads what the wicket reads: the config file, the token, the CA certificates,
  the rootfs images and the kernel. The VMs read the images and the kernel as ids of their
  own: the files need others' read permission (`o+r`); their directory may stay private (a
  jail binds the file, not its directory)
- `--jail-cgroup <path>`: the VMs' parent cgroup under `/sys/fs/cgroup`, made, the memory and
  pids controllers enabled down to it, and cpu where the hierarchy has it (without it the
  keeper warns: no VM has a share of its own, and a route's `cpu` fails its VMs' spawn). It
  is one group beside the host's others, its own `cpu.weight` (100) the operator's to set.
  By default the wicket's own cgroup, which the keeper leaves for `keeper/` (cgroup v2
  enables no controller for the children of a cgroup holding processes): a systemd
  service's with `Delegate=yes`, a container's; run by hand, give a path
- the firecracker binary is the keeper's, the one named at startup: a reload may replace the
  file (a rename), not name another

```text
sudo target/release/sluice-wicket-runtime-firecracker --jail-ids 900000-900127 --jail-user sluice \
    --jail-cgroup sluice --config /etc/sluice/wicket.yaml
```

As a systemd service: `ExecStart=` the wicket with its `--config`, `Delegate=yes` (the
default `--jail-cgroup`), `KillMode=mixed` (the stop's SIGTERM the wicket's alone: by default
every process of the unit takes it, and a `firecracker` ends on it, before any drain),
`TimeoutStopSec=` above `--evacuation-grace` and `--drain-grace` together (and 20s more with a
[disk](#disks)).

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
  pool's VM many at once. The clones of a route share their snapshot's pages
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
  and each `firecracker` gets it before its exec (in its jail, or from the forker); the keeper
  runs under the same but for the jails' namespace and mount calls, the forker but for
  `clone3`, by which it forks a VM into its cgroup
- **the serial console and the logs**: the VMs boot with `8250.nr_uarts=0` (the console
  only with `SLUICE_WICKET_RUNTIME_FIRECRACKER_SERIAL_CONSOLE`), which a guest's root may undo. A
  VM's log (`firecracker.log` in the state dir, on a tmpfs: memory) is bounded at 16 MiB
  (`RLIMIT_FSIZE`, a template's at its `memory-mib` more for its snapshot; past it Firecracker
  ends the VM), and a VM dumps no core (of its memory)
- **network and disk**: the VMs have no network interface (vsock only; jailed, a network
  namespace without one either): their [egress](#egress) is the wicket's connections, which
  the host's firewall sees as the wicket's. Their rootfs is read-only, and the snapshots
  stay on the state dir's tmpfs. An app's [disk](#disks) is a file of the console's directory,
  a guest's data at rest as it wrote it: encrypt the host's disk where that matters
- **the jailer's isolation**: [jail mode](#jail-mode). Unjailed, every VM runs as the
  wicket's user: a guest escaping its `firecracker` (past KVM and seccomp) holds every VM's
  sockets and snapshots, the wicket's token and, where the wicket made them, the VMs'
  cgroups (their limits its to lift). Tenants on one wicket call for jail mode
- **a VM's memory, threads and CPUs**: a [cgroup](#cgroups) of its own, in jail mode or
  where the wicket's cgroup is delegated to it (a service: `Delegate=yes`). Without one the
  wicket's cgroup (a container's or a unit's `MemoryMax=`) holds the VMs all together, and
  they have no share of the CPUs
- **`/dev/kvm`**: unjailed, the wicket's user in the `kvm` group (see [Setup](#setup)), and
  nobody else: a member boots VMs past the budget, in no cgroup of the wicket's. Jailed, nobody: the
  keeper (root) makes each jail a node of its own, its VM's id's alone, so the host's may
  stay root's (`--jail-user` needs no group)
- **x86_64's `kvm-pit/<pid>`**: a VM's PIT kernel thread stays in the root cgroup (the
  wicket does not move it): a guest driving its PIT takes host CPU past its route's `cpu`
  and its share, at most what `kvm.min_timer_period_us` (200us by default) lets it

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
- **`--stop-timeout`** above `--evacuation-grace` and `--drain-grace` together (20s by default;
  Docker's default stop timeout is 10s), and 20s more with an app's [disk](#disks). The wicket is
  the container's PID 1: it evacuates and drains on SIGTERM and reaps its VMs, no init needed
- **a healthcheck** in its exec form (the image has no shell, which `docker run --health-cmd`
  runs): with `--health 127.0.0.1:8081`, Compose's `healthcheck: {test: [CMD,
  /usr/local/bin/sluice-wicket-runtime-firecracker, --probe, 127.0.0.1:8081]}`
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
on the tmpfs, what its template and its clones touched (shmem: never reclaimed without swap). An idle clone costs its dirty
pages, a busy one up to its `memory-mib` ([measured](../.kb/wicket-firecracker-findings.md#docker-image-memory)).
The bound
to set (`--memory`, a Pod's limit): `--max-memory-mib` (every VM at its `memory-mib`; without it,
`--max-instances` x the largest `memory-mib`) plus each cloned route's `memory-mib` (its snapshot; twice
while a reload restarts it and the old clones live) plus 64 MiB for the wicket. Past it,
the kernel's OOM killer ends a `firecracker` or the wicket.

Through the image (`--network host` or a bridge) the rates are those outside it, within the spread
of runs ([findings](../.kb/wicket-firecracker-findings.md#through-the-docker-image)).

Kubernetes: `/dev/kvm` reaches a Pod through a device plugin (KubeVirt's
`devices.kubevirt.io/kvm`, or a generic one), or a privileged container;
`securityContext.supplementalGroups` holds the node's `kvm` gid, `/run/sluice` is an
`emptyDir` of `medium: Memory` (counted against the Pod's memory limit),
`terminationGracePeriodSeconds` exceeds `--evacuation-grace` and `--drain-grace` together, and a
`readinessProbe`'s `httpGet` asks
`/ready` of `--health` (on `0.0.0.0`). Jail mode: `runAsUser: 0`,
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
`lima.sh` also sets the VM's transparent huge pages to `always` at each run: on the 4K pages of
Ubuntu's `madvise` default (Firecracker madvises none of a guest's memory), a guest of more than
1 GiB took 10-20s to reach `/init` (1.5s at 1 GiB), past the wicket's waits; a build's VM of
2048 MiB failed so. On huge pages: about 1s at 2 and 3 GiB.

A Mac's timings say nothing of a real host (see [its findings](../.kb/wicket-firecracker-findings.md#aarch64)).
The wicket
listens once every VM it restores has booted, seconds there.

## Tests

```text
sg kvm -c 'cargo test'
```

A build (`stage`) is tested by sluice-wicket-cf-api's CLI test, which has the registry double
a builder comes from; here its parts are (`stage.rs`, `image.rs`, `rootfs.rs`, the guest's
`stage.rs`).

`tests/vm.rs` boots VMs where `/dev/kvm` is usable (it is skipped elsewhere): through a tunnel
server double, an app's disk through a restart of its VM, its process's end, its VM killed (told
so), a failed start, a growth, what its VM uses (of its disk too), the wicket's end and its restart, a VM started again as its process keeps
ending, several VMs booted (spread over, one going down alone, scaled up and down), and for a booted
VM serving two hosts, a VM per connection, reused clones, a pool
and a route of two VMs at most, one per client address, it checks http/1.1 with keep-alive,
h2c, a 5MB POST under the tunnel's flow control and a client half-closing after its request,
the clones' clock and random values, a client past its share, the connection finding no room,
a pool's VM past the CPU budget not started,
idle connections failed, clients left quiet on `--listen` closed and giving way, a reused clone serving the next connection until its `uses` or
`keep-ms` ran out, a pool growing with the connections held, shrinking idle and replacing its
VMs killed (unjailed) or their processes ended, a route's and an app's VMs kept as their numbers
change, the VMs' output (a booted VM's, a template's, a clone's) on the wicket's
stdout / stderr and on a flume (`sluice-wicket-tunnel`'s double), the guests' egress (a booted
VM's and a clone's, to a destination on the host by name, address, range and IPv6, and its
records), reloads of the config file, an edit taking over from a reload on a disk, a route
given by `--vm` alone handing through a guest speaking first, its process taken by the guest
kernel's OOM killer told so, a booted route checking its VMs'
readiness: one failing its check taking no connection, none ready leaving the advertisement until
one answers again, a process ending likewise, and their liveness: one whose endpoint fails
restarted, the other serving meanwhile, a start whose endpoint never answers 200 failing at
`boot-ms`. It needs Firecracker
(`SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER`, else `firecracker` on the PATH) and a guest
kernel (`SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL`, else `~/.local/firecracker/vmlinux`), the
variables of the wicket's own `--firecracker` and `--kernel`, and builds the sample rootfs.
`SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS` adds routes (`--vm` values, whitespace-separated, e.g.
CNB images), checked by the answers' status:

```text
T=$PWD/target
sg kvm -c "SLUICE_WICKET_RUNTIME_FIRECRACKER_VMS='java.local=$T/upstream.ext4?memory-mib=512&vcpus=2 \
    jclone.local=$T/upstream.ext4?mode=per-connection&memory-mib=512&vcpus=2&prime=200&warm=4' cargo test"
```

`SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1` runs it in [jail mode](#jail-mode), the wicket
started as root (`sudo -n`; skipped without it) or, with `..._IMAGE`, as the image runs it
jailed, and checks the jails of the VMs alive: an id of its own each, PID 1 of its
namespace, no capability, no new privileges, the jail's seccomp filter and Firecracker's
on the keeper's, a cgroup limiting its memory (a disk's VM taken by the OOM killer as its
cgroup's memory is squeezed, told so) and weighted by its CPUs (a route's `cpu` capping it, a
connection's own clones in the group they share, the keeper's of the most weight), on aarch64
the host CPU's files, the wicket as `--jail-user` under its
filter; the keeper killed (SIGKILL), the VMs and the wicket end. Unjailed, it checks the
wicket's filter, and runs the wicket in a scope of the user's manager where that gives one
with the controllers (`systemd-run --user --scope -p Delegate=yes`; not with `..._IMAGE`): its
VMs in [cgroups](#cgroups) of the wicket's own, checked as the keeper's are, the wicket's of the
most weight, the disk's VM taken by the OOM killer told so there too. `tests/vm.rs` also has
the forker fork a process with its limits.

The CI runs it on an x86_64 and an aarch64 runner, where the runner exposes `/dev/kvm`,
with the Firecracker release and the kernel above pinned (the wicket in a scope of the
runner's user's manager, started by lingering), and again jailed, on the runner
and in the image; a runner without `/dev/kvm` still builds both arches' guests and its
arch's image. `ubuntu-24.04-arm` has none: the test passes there without a VM, so run it
on a Mac ([On a Mac](#on-a-mac)) when changing what differs on aarch64.

## Findings

Benchmarks and design findings: [.kb/wicket-firecracker-findings.md](../.kb/wicket-firecracker-findings.md).

## Not yet

A liveness check of a VM per connection past its template (a clone goes on a failed dial), a disk on
a route of the config file, a flag or the page, a backup of a running disk, a pool
spreading by load rather than by connections, an image of the
page rather than the API (`PUT /api/images`), a docker-archive upload, a build's cache between
builds and its buildpacks chosen by the request, the requests on the
flume (the VMs' output alone: `cf logs` shows no `RTR` line here), and egress over UDP or to
names a route does not list (`*.example.com`).
