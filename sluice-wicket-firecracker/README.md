# sluice-wicket-firecracker

> **Experimental.** The API, layout and behavior may change; what is not there yet is listed
> under [Not yet](#not-yet).

A tunnel client serving each route with **Firecracker microVMs**, as `sluice-wicket-wasi`
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
  clones start warmed up); the VM is killed when the connection ends. `warm=N` keeps N
  clones restored ahead

The rootfs is either the built-in sample (`scripts/build-guest.sh`: `/init` serves HTTP
itself) or an OCI image converted by `scripts/cnb-rootfs.sh`, e.g. a Cloud Native
Buildpacks image: `/init` starts the image's `Entrypoint` + `Cmd` as its `User` in its
`WorkingDir` with its `Env` (and `PORT`), and opens the vsock port once the process
accepts on `127.0.0.1:$PORT`.

## Layout

Two Cargo workspaces:

- `.` -- the wicket: `src/lib.rs` wires the modules, `src/main.rs` reads the command line
  (`config.rs`); `routes.rs` the route table, its reload and the relay of a connection,
  `route.rs` a route's VMs, `capacity.rs` their budget, `vm.rs` one Firecracker VM,
  `wire.rs` the vsock framing and the restore notice, `listener.rs` `--listen`
- `guest/` -- the VMs' `/init`, built for `x86_64-unknown-linux-musl` (its
  `rust-toolchain.toml` has rustup add the target); `guest/src/wire.rs` is a copy of
  `src/wire.rs`: keep them alike
- `scripts/` -- `build-guest.sh` (the sample rootfs), `cnb-rootfs.sh` (an OCI image's),
  `try.sh` (the debug build on `--listen`)
- `tests/vm.rs` -- the VM test (see [Tests](#tests))

## Setup

x86_64 Linux with KVM (bare metal, or a VM with nested virtualization): `/dev/kvm` must
exist. Ubuntu 24.04 was used; the versions below are the ones tested. The
[Docker image](#docker-image) holds steps 2 and 3.

1. Access to `/dev/kvm` through the `kvm` group (an ACL set by hand is rewritten by udev):

   ```text
   sudo usermod -aG kvm $USER     # effective at the next login; until then: sg kvm -c '...'
   ```

2. Firecracker (v1.17.0) from its release, checksum verified:

   ```text
   FC=v1.17.0 ARCH=x86_64
   curl -fsSLO https://github.com/firecracker-microvm/firecracker/releases/download/$FC/firecracker-$FC-$ARCH.tgz
   curl -fsSL https://github.com/firecracker-microvm/firecracker/releases/download/$FC/firecracker-$FC-$ARCH.tgz.sha256.txt | sha256sum -c
   tar xzf firecracker-$FC-$ARCH.tgz
   install -D release-$FC-$ARCH/firecracker-$FC-$ARCH ~/.local/bin/firecracker
   ~/.local/bin/firecracker --version
   ```

3. A guest kernel, the latest of Firecracker's CI builds (6.18.56 was used; it has
   `CONFIG_VIRTIO_VSOCKETS`, `CONFIG_VMGENID` and kvm-clock):

   ```text
   S3=https://s3.amazonaws.com/spec.ccfc.min
   CI=$(curl -fsSL "$S3?list-type=2&prefix=firecracker-ci/&delimiter=/" \
       | grep -oP "(?<=<Prefix>)firecracker-ci/[0-9]{8}-[^/]+/(?=</Prefix>)" | sort | tail -1)
   KERNEL=$(curl -fsSL "$S3?list-type=2&prefix=${CI}x86_64/vmlinux-" \
       | grep -oP "(?<=<Key>)${CI}x86_64/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3}(?=</Key>)" | sort -V | tail -1)
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

sg kvm -c "target/release/sluice-wicket-firecracker \
    --firecracker ~/.local/bin/firecracker --kernel ~/.local/firecracker/vmlinux \
    --server grpc://127.0.0.1:8001 --token SECRET \
    --vm hello.local=target/guest.ext4 \
    --vm 'app.local=target/upstream.ext4?mode=per-connection&mem=512&vcpus=2&prime=200&warm=4'"

curl -H 'Host: hello.local' http://127.0.0.1:8000/
```

`--vm host=<rootfs>[?<options>]` adds a route: `mode=shared|per-connection`, `vcpus=N` (1),
`mem=MiB` (128), `port=N` (the guest's vsock port, 8080) and, per connection, `warm=N`,
`prime=N`, `vms=N` and `wait-ms=N` (see [Limits](#limits)). The advertised target is
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

The tunnel's settings are `sluice-wicket-wasi`'s: `--token` (or `--token-file`; on the
command line every user of the host sees it), `--client-id`, `--ca-cert`,
`--insecure` (`grpcs://`), `--max-connections` (256), `--tunnel-stall-timeout` (60s) and
`--drain-grace` (20s: SIGTERM / SIGINT withdraw the routes and end the streams once their
connections ended; a second signal ends at once). Each also reads
`SLUICE_WICKET_FIRECRACKER_<NAME>` from the environment, e.g. `SLUICE_WICKET_FIRECRACKER_TOKEN`.

`SLUICE_FIRECRACKER_CONSOLE=1` gives the VMs a serial console (into each VM's
`firecracker.log`), `SLUICE_FIRECRACKER_TRACE=1` logs each clone's restore time.

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
routes:
  - {host: hello.local, rootfs: guest.ext4}
  - {host: app.local, rootfs: upstream.ext4, mode: per-connection, mem: 512, vcpus: 2,
     prime: 200, warm: 4, vms: 16, wait-ms: 10000}
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

### Clones

A clone's memory is its snapshot's. What the wicket and `/init` do about it:

- **the clock**: the VMs boot with `clocksource=kvm-clock` and are restored with
  `clock_realtime`: Firecracker advances kvmclock by the time since the snapshot, so a clone
  resumes at the time of the moment (the TSC, the default clocksource, it does not advance)
- **random state**: right after the restore the wicket sends the clone a restore notice on
  vsock port 1024 with 32 random bytes of the host's; `/init` puts them into the kernel's
  random pool, reseeds it (ahead of the reseed on the VM generation change, VMGenID, which
  comes a moment after the restore), runs the image's restore hook, and answers. A process's
  own generator seeded before the snapshot is its business: the hook
  (`SLUICE_RESTORE='["<program>", "<arg>"]' scripts/cnb-rootfs.sh ...`, `restore` in
  `/sluice/init.json`) runs as the image's user with `SLUICE_RESTORE_GENERATION` set, before
  the clone serves; a hook failing fails the clone. A rootfs whose `/init` predates the notice
  fails a per-connection route's start
- **secrets**: what the process generated before the snapshot (a key pair, a session
  ticket key, a token) is every clone's alike, the reseed changes the generators, not what
  they produced: generate such secrets in the restore hook, or after it

The kernel's boot id remains the snapshot's.

## Docker image

`Dockerfile` (built from the repository root:
`docker build -f sluice-wicket-firecracker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .`;
published as `ghcr.io/making/sluice/sluice-wicket-firecracker`, amd64 only) holds the wicket,
Firecracker v1.17.0 and the 6.18.56 kernel above (both checksum verified), the guest of the
wicket's own version and the sample rootfs, under `/usr/local/share/sluice-wicket-firecracker/`.
The host needs KVM (bare metal, or a VM with nested virtualization); the container needs
`/dev/kvm` and its group alone (no `--privileged`, no capability: vsock only), and runs under
Docker's default seccomp profile, Firecracker's own filters stacked on it:

```text
docker run --rm --device /dev/kvm --group-add $(stat -c %g /dev/kvm) \
    --tmpfs /run/sluice --memory 1g --stop-timeout 30 \
    --add-host host.docker.internal:host-gateway -e SLUICE_WICKET_FIRECRACKER_TOKEN=SECRET \
    ghcr.io/making/sluice/sluice-wicket-firecracker \
    --server grpc://host.docker.internal:8001 \
    --vm hello.local=/usr/local/share/sluice-wicket-firecracker/guest.ext4
```

- **its environment** sets `KERNEL`, `FIRECRACKER` and `STATE_DIR`
  (`SLUICE_WICKET_FIRECRACKER_<NAME>`), which win over a `--config` file's keys; `-e`
  overrides them
- **rootfs images** come in through a volume (`-v $PWD/rootfs:/rootfs:ro`), readable by the
  image's user (65532). Their `/init` must be the image's guest (a rootfs without the restore
  notice fails a per-connection route): build them with it, `docker create --name guest
  <image>`, `docker cp guest:/usr/local/share/sluice-wicket-firecracker/init init`,
  `docker rm guest`, then `SLUICE_GUEST=init scripts/cnb-rootfs.sh ...` (or
  `scripts/build-guest.sh`); the scripts stay out of the image (they need docker and sudo)
- **`/run/sluice`**, the state dir, on a tmpfs (`--tmpfs`): the snapshots stay off the disk;
  the wicket warns otherwise
- **`--stop-timeout`** above `--drain-grace` (20s; Docker's default stop timeout is 10s).
  The wicket is the container's PID 1: it drains on SIGTERM and reaps its VMs, no init
  needed
- **open files**: the wicket raises its soft limit to the hard one (Docker's soft one is
  1024) and gives each `firecracker` `--max-connections` + 128 (see [Findings](#findings))

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
`terminationGracePeriodSeconds` exceeds `--drain-grace`.

## Tests

```text
sg kvm -c 'cargo test'
```

`tests/vm.rs` boots VMs where `/dev/kvm` is usable (it is skipped elsewhere): through a
tunnel server double, for a shared VM serving two hosts, a VM per connection and a route of
one VM at most, it checks http/1.1 with keep-alive, h2c, a 5MB POST under the tunnel's flow
control and a client half-closing after its request, the clones' clock and random values,
the connection finding no room, and reloads of the config file. It needs Firecracker
(`SLUICE_FIRECRACKER`, else `firecracker` on the PATH) and a guest kernel
(`SLUICE_FIRECRACKER_KERNEL`, else `~/.local/firecracker/vmlinux`), and builds the sample
rootfs. `SLUICE_FIRECRACKER_VMS` adds routes (`--vm` values, whitespace-separated, e.g. CNB
images), checked by the answers' status:

```text
T=$PWD/target
sg kvm -c "SLUICE_FIRECRACKER_VMS='java.local=$T/upstream.ext4?mem=512&vcpus=2 \
    jclone.local=$T/upstream.ext4?mode=per-connection&mem=512&vcpus=2&prime=200&warm=4' cargo test"
```

The CI runs it on a runner exposing `/dev/kvm`, with the Firecracker release and the kernel
above pinned.

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
- **No root, no TAP**: vsock only. The VMs share one read-only rootfs; `vsock_override` on
  snapshot load gives every clone its own socket.
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
  each `firecracker`'s to `--max-connections` + 128 (its vsock streams and its own); the
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

Caveats:

- `/dev/kvm` ACLs set by hand do not last: udev rewrote one within minutes. Use the
  `kvm` group.
- An h2c-unaware process answers prior-knowledge h2 with 502, as behind the Java client;
  the Go sample enables h2c (`http.Protocols`).

## Not yet

The jailer (chroot, cgroups, seccomp) and aarch64 (`.todo` items of their own), clone reuse
and pools of shared VMs (`.todo` 181), and the wasm wicket's console and live access log.
