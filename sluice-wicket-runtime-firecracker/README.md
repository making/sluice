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
  `listener.rs` `--listen`, `state.rs` the state dir, `seccomp.rs` the seccomp filters;
  jail mode's `keeper.rs` (the privileged half, and the wicket's client of it) and
  `jail.rs` (one VM's jail, the VMs' cgroups)
- `guest/` -- the VMs' `/init`, built for `x86_64-unknown-linux-musl` or
  `aarch64-unknown-linux-musl` (its `rust-toolchain.toml` has rustup add both, rust-lld
  links the other arch's): `log.rs` the processes' output, `egress.rs` their egress,
  `reaper.rs` PID 1's reaping; `guest/src/wire.rs` is a copy of `src/wire.rs`: keep them
  alike
- `scripts/` -- `build-guest.sh` (the sample rootfs), `cnb-rootfs.sh` (an OCI image's),
  `guest.sh` (the guest, which both build), `try.sh` (the debug build on `--listen`)
- `tests/vm.rs` -- the VM test (see [Tests](#tests))

## Setup

x86_64 or aarch64 Linux with KVM (bare metal, or a VM with nested virtualization):
`/dev/kvm` must exist. Ubuntu 24.04 on x86_64 was used; the versions below are the ones
tested (aarch64: see [aarch64](#aarch64)). The [Docker image](#docker-image) holds steps 2
and 3. A host serving others: also [Host setup](#host-setup).

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

5. For CNB images only: docker, sudo (`cnb-rootfs.sh` keeps the image's file owners), and
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

`--vm host=<rootfs>[?<options>]` adds a route: `mode=shared|per-connection`, `vcpus=N` (1),
`mem=MiB` (128), `port=N` (the guest's vsock port, 8080), `per-client=N`, `idle-ms=N`, `cpu=N`
(jail mode) and, per connection, `warm=N`, `prime=N`, `vms=N` and `wait-ms=N` (see
[Limits](#limits)), `egress=<host>:<port>,...` (see [Egress](#egress)). The advertised target is
`firecracker:<rootfs>` plus the options set to other than their defaults; the server dials
it verbatim, and the hosts of one target share its VMs. `--listen host:port` serves the sole
route over plain TCP too (without `--server`, all the wicket does: a local loop for trying a
rootfs; `scripts/try.sh <rootfs> '<options>' [port]` runs the debug build so).
`--firecracker` names the binary (default `firecracker` on the PATH), `--state-dir` the VMs'
files (their Unix sockets need paths below 108 bytes; the default is a directory of the
process's own, 0700, under `$XDG_RUNTIME_DIR` (a tmpfs where systemd sets it), else the
system's temp dir, removed at the end; a `--state-dir` given gets a `route-<n>` directory
(0700) per route started, removed with it). A per-connection route's snapshot there is its
template's whole memory, unencrypted: keep the state dir on a tmpfs (the wicket warns
otherwise), or it is on disk until the route ends, and after a crash. The tmpfs must hold
every per-connection route's `mem` (systemd sizes `$XDG_RUNTIME_DIR` to 10% of the host's
memory: `RuntimeDirectorySize` in `logind.conf`).

The tunnel's settings are `sluice-wicket-runtime-wasi`'s: `--token` (or `--token-file`; on the
command line every user of the host sees it), `--client-id`, `--ca-cert`,
`--insecure` (`grpcs://`), `--max-connections` (256), `--tunnel-stall-timeout` (60s) and
`--drain-grace` (20s: SIGTERM / SIGINT withdraw the routes and end the streams once their
connections ended; a second signal ends at once). Each also reads
`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>` from the environment, e.g. `SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN`.

`SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE=1` gives the VMs a serial console, the kernel's and
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
```

### Reloading routes

The routes are reloaded without a restart: their files are polled every `--watch-interval`
(default 1s) -- the config file, every rootfs, the kernel and the firecracker binary -- and
SIGHUP reloads at once. A change is reloaded once a poll found it unchanged since the previous
one, so a file still being written is not read half way. A reload reads the routes again and
starts the VMs of every target that is new or whose spec or files changed (a shared VM booted,
a per-connection template booted and snapshotted again) before it changes anything; one
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

Only the routes, the kernel and the firecracker binary are reloaded; the other settings apply
at startup.

### Limits

`--max-vms N` (default 64) caps the VMs alive at once over every route -- shared ones,
templates, warm and in-use clones -- and `--max-memory MiB` (default none: `--max-vms` x the
largest `mem` bounds it) their guest memory (`mem`). A per-connection route's `vms=N` caps
its own (at least `warm`). A VM takes its share before its `firecracker` starts and gives
it back once killed; a startup whose shared VMs and largest template do not fit fails.

A connection whose clone finds no room waits for one in turn (one held back by its own
route's `vms` alone keeps no other route waiting), at most its route's `wait-ms` (default
10000), then fails with an ERROR (`no room for a VM within ...`), which
the server answers with `502 Bad Gateway` (HTTP/1.1) or by closing the connection. Warm
clones take room nobody waits for, and give it up: the first connection in line short of
room has a warm clone of another route killed.

Without more, one client holds as many of a route's VMs as it opens connections, until the
server's `sluice.limits.idle-timeout` closes them. What bounds one client's share:

- **`per-client=N`**: a client address's connections to the route open at once (a
  per-connection route's VMs, those waiting for room included); one past them fails at
  once with an ERROR (`client <address> holds N connection(s) of the route already`). The
  address is the server's data plane peer, a PROXY protocol header's source where one
  applies (`Connect.client_address`; `--listen`: the TCP peer): behind a front end without
  the PROXY protocol every connection carries the front end's, and a server predating the
  field sends none, which the bound does not apply to. A browser opens up to 6 connections
  per host
- **`idle-ms=N`**: a connection relaying no byte, either way, for that long fails with an
  ERROR, and its clone is killed: a bound below the server's idle timeout, of the route's
  own. A long poll or a stream quiet for longer is cut as well
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

- the VM is `vm-0` for a shared VM, `template` for a per-connection route's template and
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
per-connection route's template has its egress (a process connecting at its start, or
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
  fails a per-connection route's start
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
  keeper's, the wicket's group's: its API and vsock sockets) and, for a per-connection route,
  `/snapshot`: the template's to write, made read-only (root's) before the first clone; on
  aarch64 `/sys/devices/system/cpu/cpu0`, the host CPU's cache and `MIDR_EL1` files
  Firecracker reads, copied by the keeper at its start (the jailer copies them into every
  jail)
- runs its route's copy of `firecracker` (a sealed memfd): the jailer copies the binary into
  every jail so that VMs share none of its pages; the clones of a route share their
  snapshot's pages anyway, so they share their route's copy, and routes share nothing
- is in a cgroup (v2) of its own: `memory.max` its `mem` plus 32 MiB plus `mem`/128 (its
  page tables) plus 16 MiB (its log, bound by `RLIMIT_FSIZE`), its `mem` twice for a template
  (it writes its snapshot), `pids.max` its vCPUs plus 16. Past its limit the kernel's OOM
  killer ends the VM, nothing else. A per-connection route's snapshot stays charged to the
  route's cgroup (`route-<n>`) as long as the route lives; a clone's cgroup (`vm-<n>`, one
  per id) is reused. `cpu.max` its route's `cpu` (see [Limits](#limits)), else unlimited:
  the vCPUs bound it
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
  pids controllers enabled down to it, and cpu where the hierarchy has it (a route's `cpu`
  fails its VMs' spawn without it). By default the wicket's own cgroup, which the keeper
  leaves for `keeper/` (cgroup v2 enables no controller for the children of a cgroup holding
  processes): a systemd service's with `Delegate=yes`, a container's; run by hand, give a path
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
  several clients in one clone. The clones of a route share their snapshot's pages (mapped
  private): what the template held is every clone's (see [Clones](#clones)), and the
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
  only with `SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE`), which a guest's root may undo. A
  VM's log (`firecracker.log` in the state dir, on a tmpfs: memory) is bounded at 16 MiB
  (`RLIMIT_FSIZE`, a template's at its `mem` more for its snapshot; past it Firecracker
  ends the VM), and a VM dumps no core (of its memory)
- **network and disk**: the VMs have no network interface (vsock only; jailed, a network
  namespace without one either): their [egress](#egress) is the wicket's connections, which
  the host's firewall sees as the wicket's. Their rootfs is read-only, and the snapshots
  stay on the state dir's tmpfs
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

- **its environment** sets `KERNEL`, `FIRECRACKER` and `STATE_DIR`
  (`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>`), which win over a `--config` file's keys; `-e`
  overrides them
- **rootfs images** come in through a volume (`-v $PWD/rootfs:/rootfs:ro`), readable by the
  image's user (65532). Their `/init` must be the image's guest (a rootfs without the restore
  notice fails a per-connection route): build them with it, `docker create --name guest
  <image>`, `docker cp guest:/usr/local/share/sluice-wicket-runtime-firecracker/init init`,
  `docker rm guest`, then `SLUICE_GUEST=init scripts/cnb-rootfs.sh ...` (or
  `scripts/build-guest.sh`); the scripts stay out of the image (they need docker and sudo)
- **`/run/sluice`**, the state dir, on a tmpfs (`--tmpfs`): the snapshots stay off the disk;
  the wicket warns otherwise
- **`--stop-timeout`** above `--drain-grace` (20s; Docker's default stop timeout is 10s).
  The wicket is the container's PID 1: it drains on SIGTERM and reaps its VMs, no init
  needed
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
guest touches it, the `firecracker` process's few MiB, and a per-connection route's snapshot
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
`--max-vms` x the largest `mem`) plus each per-connection route's `mem` (its snapshot; twice
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

## Tests

```text
sg kvm -c 'cargo test'
```

`tests/vm.rs` boots VMs where `/dev/kvm` is usable (it is skipped elsewhere): through a
tunnel server double, for a shared VM serving two hosts, a VM per connection and a route of
two VMs at most, one per client address, it checks http/1.1 with keep-alive, h2c, a 5MB POST
under the tunnel's flow control and a client half-closing after its request, the clones'
clock and random values, a client past its share, the connection finding no room, idle
connections failed, the VMs' output (a shared VM's, a template's, a clone's)
on the wicket's stdout / stderr and on a flume (`sluice-wicket-tunnel`'s double), the
guests' egress (a shared VM's and a clone's, to a destination on the host by name, address,
range and IPv6, and its records), and reloads of the config file. It needs Firecracker
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
on the keeper's, a cgroup limiting its memory (and a route's clones' CPU), on aarch64 the
host CPU's files, the wicket as `--jail-user` under its filter; the keeper killed
(SIGKILL), the VMs and the wicket end. Unjailed, it checks the wicket's filter.

The CI runs it on an x86_64 and an aarch64 runner, where the runner exposes `/dev/kvm`,
with the Firecracker release and the kernel above pinned, and again jailed, on the runner
and in the image; a runner without `/dev/kvm` still builds both arches' guests and its
arch's image.

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
- **A runtime worker per core doubles the wicket's CPU per relayed request**: an egress round
  trip took 55-69us of the wicket's CPU with the default workers (64 here), 25-30us with one
  (`TOKIO_WORKER_THREADS=1`), at the same rate; a keep-alive request on `--listen` 75us
  against 27us, at 17k req/s against 25k.

[Jail mode](#jail-mode), on the same host through `--listen` (no server), `oha` for 5s:

| VM per connection, sample guest, warm=16 | new connection req/s, c=1 | c=16 |
|---|---|---|
| without the jail, 3 runs | 504-650 | 1,455-1,544 |
| jailed, the same runs alternating | 508-658 | 1,295-1,320 |

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
- **The keeper's thread bounds the jailed clone rate** (85% of the unjailed at c=16, as
  above): it forks every VM, and a fork of any process costs 130us on this 64-core 6.8
  kernel (its new memory map's per-CPU counters), with the copy-on-write faults after it
  about 0.6ms of the keeper's thread per VM. A vfork child would hold the keeper for the
  jail's setup, 1-4ms per VM (mostly the kernel's mount namespace lock: `pivot_root`,
  `mount`, `umount2`, `unshare`; in parallel in the children).
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

[aarch64](#aarch64) had no machine of its own: the VM test ran on an emulated one -- QEMU
8.2 TCG (`-M virt,virtualization=on,gic-version=3 -cpu max,pauth-impdef=on`), Ubuntu 24.04
arm64, whose KVM ran Firecracker v1.17.0 and the 6.18.56 kernel -- with the wicket's boot and
restore timeouts raised to its pace (a guest boots in about 6s), and passed plain and jailed:
the clones' clock and random state, the half-close framing, the output, the reloads, the
jails. Its timings say nothing of a real host.

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

## Not yet

Clone reuse and pools of shared VMs, CPU limits without the jail, the wasm wicket's
console, and egress over UDP or to names a route does not list (`*.example.com`).
