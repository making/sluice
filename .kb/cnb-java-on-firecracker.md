# A CNB Java image on a Firecracker wicket through the cf-api

Measured 2026-11-02: amd64, Linux 6.8, KVM, the cf-api's Docker Compose with locally built binaries,
`ghcr.io/categolj/blog-frontend-htmx:jvm` (`sha256:ae0ab71e...`, Spring Boot 4.1, Tomcat), one vCPU.

## Memory

The Paketo memory calculator's fixed regions: 606135K (`-XX:MaxDirectMemorySize=10M`,
`-XX:MaxMetaspaceSize=94135K`, `-XX:ReservedCodeCacheSize=240M`, `-Xss1M` * 250 threads).

| VM `memory-mib` | available to the calculator | result |
|---|---|---|
| 256M | 202940K | fails the launch: `fixed memory regions require 606135K` |
| 1G | 890340K | `-Xmx284204K` |

## Time

| what | time |
|---|---|
| staging (the wicket makes the rootfs, 512753664 bytes) | 36-38s |
| process start to `Started BlogFrontendHtmxApplication` | about 12s (`in 10.096 seconds`, `process running for 11.2`): past the wicket's default `boot-ms` of 10s |

## A pool's cost at the default memory

The sample guest at 1G, a `Full` snapshot: template booted in 689ms, snapshot taken after 1374ms, a
1024 MiB memory file, the wicket container's memory from 359 MiB to 1.36 GiB, the snapshot on
`--tmpfs /run/sluice`. Now it holds what its guest wrote: 681ms, 767ms, 69 MiB
([findings](wicket-firecracker-findings.md#a-snapshots-memory)).

## Source staged by buildpacks on the wicket

Measured 2026-11-10: amd64, Linux 6.8, KVM, guest kernel 6.18.56, a release build of the wicket,
a build VM of 2 vCPUs, 2048 MiB and an 8 GiB scratch disk, no cache. The apps: Spring Boot 3.5
(Maven, one starter), Express, a Go module with one dependency. Two builders, both of lifecycle
0.21.28 (platform API 0.13 used):

- `paketobuildpacks/ubuntu-resolute-builder@sha256:b17a8dce...` (Ubuntu 26.04) on
  `ubuntu-resolute-run@sha256:09a7c9ed...`
- `paketobuildpacks/builder-jammy-base@sha256:71927953...` (Ubuntu 22.04) on
  `run-jammy-base@sha256:dc8dd702...`

| what | resolute | jammy |
|---|---|---|
| the builder's rootfs, once per builder and guest | 152s, 3648 MiB | 165s (about 120s of it the pull), 4138 MiB; a debug build of the wicket: 501-517s |
| Java: build, droplet, rootfs, start | 63s, 235 MiB, 14s to make 580 MiB, `Started` after 4.9s at 1024 MiB | 65s, 235 MiB, 13s to make 538 MiB, after 5.4s |
| Node: build, droplet, rootfs | 20s, 278 MiB, 20s to make 671 MiB | 22s, 278 MiB, 18s to make 629 MiB |
| Go: build, droplet, rootfs | 35s, 17 MiB, 12s to make 293 MiB | 34s, 17 MiB, 10s to make 251 MiB |

A cold build is well within `CF_STAGING_TIMEOUT` (15 minutes), the first one with a builder
included. These are cold builds, before the wicket kept a cache. The jammy builds with
`detector` and `builder` alone in Docker on every core of the host: Java 61s (peak 1.7 GiB
with the page cache, `/layers` 786 MiB), Node 12s (484 MiB, 293 MiB), Go 24s (827 MiB,
389 MiB).

On arm64 (2026-11-10, an M4 Max in Lima, [On a Mac](../sluice-wicket-runtime-firecracker/README.md#on-a-mac),
the release wicket, lifecycle 0.21.28 linux.arm64), the same apps with `ubuntu-resolute-builder`'s
arm64 image, the same egress: the builder's rootfs made in 186s (3465 MiB); built in 11s (Node,
270 MiB), 13s (Go, 16 MiB, serving `linux/arm64`), 26s (Java, 230 MiB), each served. The Java
app took 164s to start in its template (`Started HelloApplication in 148.481 seconds`): the
JVM's start on a Mac, not the build's. With the cache (the same day, `35560cf3`): Node 13s cold,
11s warm (201 MiB restored, the engine reused), Java 157s cold and 51s warm on a loaded Mac
(554 MiB restored, the JDK, Maven and the app reused), 30s cold again on a quiet one (Maven's
7.9s, 50s before), Go 36s cold; `-b
paketo-buildpacks/nodejs` built, an id the builder lacks failed naming its order, `-c 'node
custom.js'` served.

The hosts a build connected to (each on 443, the same with both builders), what `egress` has
to name:

| app | hosts |
|---|---|
| Java (Maven) | `github.com`, `release-assets.githubusercontent.com` (the JDK, Maven, the helpers), `repo.maven.apache.org`, `repo1.maven.org` |
| Node (npm) | `nodejs.org`, `registry.npmjs.org` |
| Go | `go.dev`, `dl.google.com`, `proxy.golang.org` |

### With the cache

Measured 2026-11-10 on the same host, a release build, the resolute builder, the cache streamed
over vsock as it is unpacked and packed (written to the build's disk first, the restore took 18s
of a warm Java build of 67s):

| app | cache | cold build | warm build | what the warm one did again |
|---|---|---|---|---|
| Java | 566 MiB (the JDK, Maven, its repository, the compiled app, syft) | 69s | 38-41s | the JRE (a launch layer: downloaded) |
| Node | 203 MiB (Node.js, the npm cache) | 20s | 12-17s | `npm install` from its cache |

The cache restored in 2.5s (566 MiB), kept in 1-14s after the droplet. A build in jail mode
restores and keeps it the same (Node, 15s).

### A tiny builder

Measured 2026-11-10 on the same host, a release build:
`paketobuildpacks/builder-noble-java-tiny@sha256:8da97829...` (lifecycle 0.21.28) on
`ubuntu-noble-run-tiny:0.0.157@sha256:7af66d69...` (no shell), the Java app, the same egress.

| what | time, size |
|---|---|
| the builder's rootfs | 57s, 1189 MiB |
| build, droplet | 65s cold, 28s warm (566 MiB cached), 235 MiB |
| rootfs | 7s to make 386 MiB |
| start at 1024 MiB | `Started` after 5.8s (`-Xmx415922K`) |

With no command it serves: the `web` process is direct, the exec.d helpers (the memory
calculator, the CA certificates) are binaries. `cf push -c` builds, then the rootfs is refused:
the launcher runs a command with `/bin/bash` (`launch/bash.go`), which the run image lacks, so the
build's `launch` names it in `needs` and the wicket checks it (the app keeps its droplet).
The launcher's direct form (`launcher -- <argv>`) is no way to a command there: no shell syntax,
no `$VAR` expansion, and the variables the layers set (`$JAVA_HOME`) unknown before the
launcher runs, unlike CF's `-c`.

### A run image of the platform's

Measured 2026-11-10 on the same host, a release build, `--cnb-run-image`:
`ubuntu-resolute-builder@sha256:b17a8dce...` (whose metadata names `ubuntu-resolute-run`
alone) on `ubuntu-resolute-run-tiny@sha256:675c19ba...` (no shell), against its own
`ubuntu-resolute-run@sha256:09a7c9ed...` the same session. The rootfs time includes the run
image's pull; the builds on `ubuntu-resolute-run` were warm (the cache of the tiny ones).

| app | run image | build | droplet | rootfs | start |
|---|---|---|---|---|---|
| Go | tiny | 34s cold | 17 MiB | 3s to make 89 MiB | |
| Go | resolute-run | 18s warm | 17 MiB | 14s to make 292 MiB | |
| Java | tiny | 66s cold | 235 MiB | 7s to make 377 MiB | `Started` in 11.7s at 1024 MiB |
| Java | resolute-run | 27s warm | 235 MiB | 20s to make 580 MiB | in 12.0s |

Both serve with no command: Go's binary and Java's `web` process are direct. `-c` on tiny fails
staging (`needs`). The run image's stack id (`io.buildpacks.stacks.resolute.tiny`) is not the
builder's (`io.buildpacks.stacks.resolute`), which nothing checks in a build. The lifecycle checks
a run image only through the detector (each buildpack's `[[targets]]` against the run image's
os, arch, variant and distribution, `TargetSatisfiedForBuild`); the stack id only in the
rebaser. The wicket refuses one of another target than the builder's before the build:
`ubuntu-noble-run-tiny` (ubuntu 24.04) under the resolute builder (26.04) fails staging at once.
Paketo's images name their distribution in `io.buildpacks.stack.distro.*`, not the
`io.buildpacks.base.distro.*` the lifecycle reads; with neither, the detector reads the build
image's `/etc/os-release`.

### A buildpack by its image

Measured 2026-11-10 on the same host, a release build, the same builder and run images as
above: `cf push -b docker://paketocommunity/rust:latest` (`paketo-community/rust` 0.92.2
`@sha256:02e7da45...`, in no Paketo builder) of a Cargo app with one dependency (`itoa`), a
plain `TcpListener` on `$PORT`. The image is flattened: one layer of 35 MiB (pulled in 2s, kept
for the next build) holds the composite and the six buildpacks of its order, of which the
builder has `paketo-buildpacks/procfile` and `syft` too. The builder's rootfs was made the same
session (148s).

| run image | build | droplet | rootfs | listening after the rootfs |
|---|---|---|---|---|
| tiny | 38s cold, 8s warm (722 MiB cached: rustup, the toolchain, Cargo's) | 3 MiB | 2s to make 72 MiB | 5.2s |
| resolute-run | 8s warm | 3 MiB | 12s to make 275 MiB | 5.8s |

The binary, linked against glibc, runs on tiny (no shell) with no command. The hosts a cold build
connected to: `static.rust-lang.org` (rustup and the toolchain), `index.crates.io`,
`static.crates.io`, `github.com` and `release-assets.githubusercontent.com` (syft, and tini,
which the cargo buildpack downloads warm too).

### An egress by proxy

`--cnb-egress` naming a forward proxy alone (a CONNECT proxy on the host), `HTTPS_PROXY` and
`HTTP_PROXY` the app's env: Node and Go build, every download through it (the buildpacks' and
npm's, Go's); a Java build's buildpacks download the JDK and Maven through it, Maven itself does
not read `HTTPS_PROXY`: a `settings.xml` naming the proxy in the app, `BP_MAVEN_SETTINGS_PATH`
`/workspace/settings.xml`, and it builds. The hosts the proxy was asked for:

| app | hosts |
|---|---|
| Java (Maven) | `github.com`, `release-assets.githubusercontent.com`, `repo1.maven.org`, `repo.maven.apache.org`, `central.sonatype.com`, `oss.sonatype.org`, `repository.apache.org`, `repository.ow2.org` |
| Node (npm) | `nodejs.org`, `registry.npmjs.org` |
| Go (no dependency) | `go.dev`, `dl.google.com` |

The app's env is its process's too: the running app has `HTTPS_PROXY` as well.

## `docker compose down` and `up`

With the images in place, two apps: `up` returned after 2.2s, both apps served after 3.1s, `cf apps`
showed them running after 3.3s (`web:0/1` and `The wicket is unreachable` until then). On an Apple
silicon Mac in a Lima VM, 15s was the wait checked, not a measured minimum.
