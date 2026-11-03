# sluice-wicket-runtime-firecracker: findings

Measurements and design findings of the wicket; paths are relative to its directory. What a user
needs of them is in [its README](../sluice-wicket-runtime-firecracker/README.md).

## Rates by mode

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

New connections per mode. The rates are the sample guest's through the server,
`oha -c 16 --disable-keepalive`:

| mode | new connections/s |
|---|---|
| `shared` | 3,060 (1 vCPU), 7,072 (4) |
| `per-connection` | 1,600 (`warm=16`) |
| `reuse` | 8,918 |
| `pool` | 7,493 (4 VMs of 1 vCPU) |

## vsock and the VMs' API

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

## CNB images

- **CNB images fit the launcher model**: their config is one `Entrypoint`
  (`/cnb/process/web`), a few `Env` entries, a `User` and a `WorkingDir`; everything else
  is in `/layers`. The Java buildpack's exec.d helpers (memory calculator, CA certificates)
  work on the read-only rootfs with `/tmp` on tmpfs; the calculator sizes the heap from the
  VM's memory.

## Clones and spawning them

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

## A shared VM's rate

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

## A clone's clock

- **A clone's clock resumes from the snapshot** unless the VM runs on kvm-clock and is
  restored with `clock_realtime` (it advances kvmclock, not the TSC, the guest's default
  clocksource): a clone restored 6s after its snapshot was 6s behind, now within
  milliseconds.

## PID 1

- **PID 1 reaps in one place**: `/init` reaps any child (orphans come to it), so its own
  wait for the restore hook raced that reaping and now and then lost the hook's status
  (`restore notice: ERR No child process`, a clone failed). The reaper hands the hook its
  status instead: 44 hooked clones of the CNB Java image, none failed.

## Egress

- **Egress goes through the wicket, not a TAP device**: a TAP device per VM needs
  `CAP_NET_ADMIN` (the [Docker image](../sluice-wicket-runtime-firecracker/README.md#docker-image) runs without a capability) and an
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

## Relay workers

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

## The threads spawning VMs

- **A VM is spawned from a thread living as long as the wicket**: `posix_spawn` holds its
  thread until the launcher's exec (about 1ms), on a runtime's thread its relays with it
  (one worker: 1,130 clones/s against 1,500); and a child's parent death signal fires as
  the thread that spawned it ends, not its process: spawned from tokio's blocking pool
  (an idle thread ends after 10s), the VMs died with their threads. Four launching
  threads spawn them.

## Pools against one VM

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

## Jail mode

[Jail mode](../sluice-wicket-runtime-firecracker/README.md#jail-mode), on the same host through `--listen` (no server), `oha` for 5s:

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

A network namespace per VM cost 0.9ms of each spawn: the VMs share one.

## aarch64

[aarch64](../sluice-wicket-runtime-firecracker/README.md#aarch64) first ran on an emulated machine -- QEMU 8.2 TCG (`-M
virt,virtualization=on,gic-version=3 -cpu max,pauth-impdef=on`), Ubuntu 24.04 arm64, whose KVM
ran Firecracker v1.17.0 and the 6.18.56 kernel -- with the wicket's boot and restore timeouts
raised to its pace (a guest boots in about 6s), and passed plain and jailed: the clones' clock
and random state, the half-close framing, the output, the reloads, the jails. Its timings say
nothing of a real host.

Then on an M4 Max, macOS 26.3 ([On a Mac](../sluice-wicket-runtime-firecracker/README.md#on-a-mac): Lima 2.1, `vz`, 8 vCPUs, Ubuntu 24.04's
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

On a Mac the wicket listens once every VM it restores has booted: 9.6s here with a Node.js image,
3.7s with a Go one.

## Caveats

- `/dev/kvm` ACLs set by hand do not last: udev rewrote one within minutes. Use the
  `kvm` group.
- An h2c-unaware process answers prior-knowledge h2 with 502, as behind the Java client;
  the Go sample enables h2c (`http.Protocols`).

## The env and a rootfs

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

## Disks

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

Through the wicket:

- a change of an app's VM: its connections fail meanwhile, a boot long (measured with the cf-api:
  0.7-0.8s). A start failing starts the old VM again, the PUT refused (with a process that never
  listens: 11.5s, the 10s of its boot's bound)
- a growth, by `/init` at its next mount: online, 0.2s to 64 GiB
- a shared VM going down, the sample guest on a disk, from its end to the route's listing saying
  it is up (polled every 20ms, on the 64-core host; three runs): its process ended 0.79s (64 MiB),
  its `firecracker` killed 0.71-0.74s (128 MiB), the journal replayed. A dial alone says nothing of the process:
  `/init`'s vsock port stays open once the process listened, and `/init` drops the stream of a
  connection refused, so a look reads the stream for a drop (`Vm::accepting`)
- the wait before it, `firecracker` killed, from the kill to the route answering (the sample guest,
  64 MiB): killed again at once 0.76s, 1.74s, 2.75s, 4.75s; after 30s up 8.75s; after 65s up 0.76s,
  and 1.76s the next
- an edit during a disk's VM replaced by a reload takes over: one VM throughout, the PUT a boot long,
  what the VM before it wrote there, the reload made again finding no change
- a start given up (taken over, or ended with its reload) dropped its `Vm` in the aborted task: a
  kill, and what it wrote and did not sync did not stay (a counter: 1, then 2 of three starts; a file
  written at the start gone from the next VM, 1-2s later). So a `Vm` on a disk stops as it is dropped
  (`vm.rs`), in a task the wicket's end waits for, however it was dropped; before `/init` answered of
  the disk it is still killed: no process ran

## Docker image memory

Container memory, measured:

| | container memory |
|---|---|
| shared VM, sample guest | 60 MiB |
| shared VM, CNB Java (2 vCPUs, 512 MiB), after a load | 212 MiB |
| VM per connection, sample guest, warm=16 | 175 MiB (128 snapshot) |
| VM per connection, CNB Java, prime=200, warm=16 | 602 MiB (512 snapshot); 694 MiB at start, the template beside its snapshot |
| the same, 48 keep-alive connections at 17k req/s | 3,277 MiB peak: about 56 MiB per busy JVM clone |

An idle clone costs its dirty pages (1-4 MiB here), a busy one up to its `mem`.

## Through the Docker image

Through the image (`--network host` or a bridge, the same host) the cases above are within the
spread of runs outside it: two runs each, e.g. a shared VM on 2 vCPUs
13.5-13.8k keep-alive req/s (outside 13.8-13.9k, bridged 13.6-13.9k), a VM per connection
1,603-1,614 new connections/s (outside 1,598-1,615, bridged 1,614-1,652), CNB Java per
connection 647-737 (outside 623-672).

## Boot time mitigations

Linux 6.1's boot time mitigations (`favordynmods`, `kvm.nx_huge_pages=never`) are not needed at the
wicket's rates: `KVM_CREATE_VM` takes 0.26ms on the 6.8 kernel of these findings (0.34-0.51ms, 16
at once), of a clone's 12ms restore.
