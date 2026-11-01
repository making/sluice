# sluice-wicket-firecracker (spike)

> **Spike.** Shows that a wicket can serve its routes with Firecracker microVMs the way
> `sluice-wicket-wasi` serves them with wasi:http components. Not a product: the scope
> left out is listed under [Not in the spike](#not-in-the-spike).

```
sluice server --tunnel--> wicket --vsock, framed--> /init (PID 1) --TCP 127.0.0.1--> process in the microVM
```

The wicket connects to a sluice server like the Java client and relays each tunnel
connection verbatim (no HTTP termination) to a vsock stream of a guest. The guest's
`/init` (`guest/`, static musl) relays it on to the process. A route's VMs are:

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

## Setup

x86_64 Linux with KVM (bare metal, or a VM with nested virtualization): `/dev/kvm` must
exist. Ubuntu 24.04 was used; the versions below are the ones the spike ran.

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
   `CONFIG_VIRTIO_VSOCKETS` and `CONFIG_VMGENID`):

   ```text
   S3=https://s3.amazonaws.com/spec.ccfc.min
   CI=$(curl -fsSL "$S3?list-type=2&prefix=firecracker-ci/&delimiter=/" \
       | grep -oP "(?<=<Prefix>)firecracker-ci/[0-9]{8}-[^/]+/(?=</Prefix>)" | sort | tail -1)
   KERNEL=$(curl -fsSL "$S3?list-type=2&prefix=${CI}x86_64/vmlinux-" \
       | grep -oP "(?<=<Key>)${CI}x86_64/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3}(?=</Key>)" | sort -V | tail -1)
   mkdir -p ~/.local/firecracker && curl -fsSL -o ~/.local/firecracker/vmlinux "$S3/$KERNEL"
   ```

4. Build tools: `protoc` (the tunnel proto), `mkfs.ext4` 1.43+ (`-d`), python3, and the musl
   target for the guest (pure Rust, no C toolchain needed):

   ```text
   sudo apt-get install -y protobuf-compiler e2fsprogs python3
   rustup target add x86_64-unknown-linux-musl
   ```

5. For CNB images only: docker, sudo (`cnb-rootfs.sh` keeps the image's file owners), and
   `pack` (v0.40.10):

   ```text
   PACK=v0.40.10
   curl -fsSL https://github.com/buildpacks/pack/releases/download/$PACK/pack-$PACK-linux.tgz | tar xz
   install pack ~/.local/bin/pack
   ```

6. A sluice server to connect to (root README, "Run"): `./mvnw -DskipTests package` from the
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
scripts/check.sh hello.local app.local                   # h1, h2c, keep-alive, POST, half-close
```

Without a server, `scripts/try.sh <rootfs> '<query>' [port]` runs the debug build (`cargo build`) on
`--listen 127.0.0.1:<port>` (default 18080) with the paths of steps 2 and 3.

`--vm host=<rootfs>[?mode=shared|per-connection&vcpus=N&mem=MiB&port=N&warm=N&prime=N]`
(defaults: 1 vCPU, 128 MiB, vsock port 8080). `--listen host:port` serves a single route
over plain TCP without a server. `--firecracker` names the binary (default `firecracker`
on the PATH), `--state-dir` the VMs' files (their Unix sockets need paths below 108
bytes). `SLUICE_FIRECRACKER_CONSOLE=1` gives the VMs a serial console (into each VM's
`firecracker.log`), `SLUICE_FIRECRACKER_TRACE=1` logs each clone's restore time.

## Findings

Measured on a 64-core x86_64 host, through a sluice server on the same host, with `oha
-c 16` for 5s (`demo.local` is the Java client relaying to the same Jetty on TCP).

| | boot to accepting | keep-alive req/s | new connection req/s (p50) |
|---|---|---|---|
| Java client -> Jetty (TCP) | - | 21,568 | 11,806 (1.3ms) |
| shared VM, sample guest | 0.65s | 8,195 | 2,903 (5.5ms) |
| shared VM, CNB Java (Jetty, 512 MiB) | 2.0s | 7,486 | - |
| shared VM, CNB Go | 0.76s | 8,488 | - |
| VM per connection, sample guest, warm=16 | - | - | 884 (17ms); c=1: 376 (2.6ms) |
| VM per connection, CNB Java, prime=200, warm=16 | - | - | 747 (21ms); c=1: 91 (10.5ms) |

- **The wasm wicket's shape carries over**: the tunnel, routes advertised by host, the
  server unchanged. h1, h2c, keep-alive, a 5MB POST under flow control, `X-Forwarded-*` and
  a client half-closing after its request all pass (`scripts/check.sh`).
- **vsock carries no half-close** (Firecracker v1.17.0 `csm/connection.rs`): the host
  side's EOF reaches the guest as a full shutdown (its unsent answer is lost), and a
  guest's `shutdown(SHUT_WR)` never reaches the host. The CLOSE therefore travels in band
  (`src/wire.rs`, `[type][len][payload]`), and the host must not drop a write half early:
  tokio's owned write half shuts the socket down on drop.
- **No SDK is needed**: the API is REST over a Unix socket; `src/vm.rs` drives it with
  hyper in about 300 lines. The Go SDK (firecracker-go-sdk) adds networking (CNI) and the
  jailer, neither of which a vsock-only wicket needs.
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
- **The clone rate is bound by the restores in flight**: one refill worker capped it at
  about 230 VMs/s (1 / 4ms), 8 workers lift it to 750-880/s.
- **A shared VM costs throughput**: about 40% of the Java client's keep-alive rate, and a
  new connection is dearer (2.9k/s vs 11.8k/s): two more hops (vsock, the guest's TCP)
  and Firecracker's single VMM thread handling every vsock stream of a VM.

Caveats found along the way:

- Clones share their memory, user-space RNG state included. Firecracker's VMGenID makes
  the guest kernel reseed on restore (with a short race window), not a JVM `SecureRandom`
  seeded before the snapshot; the kernel's boot id and the clock (kvmclock resumes) are
  the snapshot's too.
- `/dev/kvm` ACLs set by hand do not last: udev rewrote one within minutes. Use the
  `kvm` group.
- An h2c-unaware process answers prior-knowledge h2 with 502, as behind the Java client;
  the Go sample enables h2c (`http.Protocols`).

## Not in the spike

A shared tunnel crate with `sluice-wicket-wasi` (session, supervisor, flow control are
re-written here in a minimal form: one server, no ListNodes fan-out, no drain, reclaims
ignored), a config file / reload / console, limits (VMs, memory), the jailer, aarch64,
snapshot invalidation on a rootfs change, and automated tests beyond `src/wire.rs`.
