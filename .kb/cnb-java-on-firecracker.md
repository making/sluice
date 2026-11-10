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
included. The jammy builds with `detector` and `builder` alone in Docker on every core of the
host: Java 61s (peak 1.7 GiB with the page cache, `/layers` 786 MiB), Node 12s (484 MiB, 293 MiB),
Go 24s (827 MiB, 389 MiB).

On arm64 (2026-11-10, an M4 Max in Lima, [On a Mac](../sluice-wicket-runtime-firecracker/README.md#on-a-mac),
the release wicket, lifecycle 0.21.28 linux.arm64), the same apps with `ubuntu-resolute-builder`'s
arm64 image, the same egress: the builder's rootfs made in 186s (3465 MiB); built in 11s (Node,
270 MiB), 13s (Go, 16 MiB, serving `linux/arm64`), 26s (Java, 230 MiB), each served. The Java
app took 164s to start in its template (`Started HelloApplication in 148.481 seconds`): the
JVM's start on a Mac, not the build's.

The hosts a build connected to (each on 443, the same with both builders), what `egress` has
to name:

| app | hosts |
|---|---|
| Java (Maven) | `github.com`, `release-assets.githubusercontent.com` (the JDK, Maven, the helpers), `repo.maven.apache.org`, `repo1.maven.org` |
| Node (npm) | `nodejs.org`, `registry.npmjs.org` |
| Go | `go.dev`, `dl.google.com`, `proxy.golang.org` |

## `docker compose down` and `up`

With the images in place, two apps: `up` returned after 2.2s, both apps served after 3.1s, `cf apps`
showed them running after 3.3s (`web:0/1` and `The wicket is unreachable` until then). On an Apple
silicon Mac in a Lima VM, 15s was the wait checked, not a measured minimum.
