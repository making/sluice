# A CNB Java image on a Firecracker wicket through the cf-api

Measured 2026-11-02: amd64, Linux 6.8, KVM, the cf-api's Docker Compose with locally built binaries,
`ghcr.io/categolj/blog-frontend-htmx:jvm` (`sha256:ae0ab71e...`, Spring Boot 4.1, Tomcat), one vCPU.

## Memory

The Paketo memory calculator's fixed regions: 606135K (`-XX:MaxDirectMemorySize=10M`,
`-XX:MaxMetaspaceSize=94135K`, `-XX:ReservedCodeCacheSize=240M`, `-Xss1M` * 250 threads).

| VM `mem` | available to the calculator | result |
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

## `docker compose down` and `up`

With the images in place, two apps: `up` returned after 2.2s, both apps served after 3.1s, `cf apps`
showed them running after 3.3s (`web:0/1` and `The wicket is unreachable` until then). On an Apple
silicon Mac in a Lima VM, 15s was the wait checked, not a measured minimum.
