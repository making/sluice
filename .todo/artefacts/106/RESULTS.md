# netty h2 demux measured (todo 106, 2026-10-13)

Commit measured: `0a62925` (main), the netty `NettyHttp2DemuxRelay` as the only h2 demux.

## Method

- Four JVMs on one machine: `sluice-server` and `sluice-client` exec jars, the harness
  upstream (`harness/src/Upstream.java`: netty, h1 on one port, h2c prior knowledge on another,
  `/small` 2-byte body with browser-like response headers, `/bytes?n=N`, `/sink`, `/hold`), and the
  load client (`Load.java`: netty h2 prior knowledge with S concurrent streams per connection, or h1
  with one request in flight per connection, browser-like request headers, every body checked).
  Optional `DelayProxy.java` adds a fixed one-way delay: between sluice-client and the server's
  gRPC port (tunnel RTT) or between the load client and the data port (client RTT).
- `harness/bench.py` runs a suite on a fresh stack: 3 s warmup, 10 s measured window (some 8, 12
  or 15 s). CPU per process from `proc_pid_rusage` (user + sys of the whole task; macOS `ps`
  drops exited threads), allocation from `jvm_gc_memory_allocated_bytes_total`, both read at
  the window's start and end. Byte tallies per frame type on the load client's and the
  upstream's sockets (`FrameTally.java`). `run-all.sh` is the recorded series; raw rows are in
  `results.jsonl`, the tables below come from `summarize.py`.
- Machine: MacBook Pro, 16 cores, 128 GB, macOS 25.3, Oracle GraalVM 25.0.3 (HotSpot with
  the Graal JIT), G1, `-Xmx1g` for server and client. Load average 3.8 to 6.5 from other
  processes during the series. Small-request rates moved by up to 30 % between series (h1 c10:
  34.9k req/s in an exploratory run, 24.7k in the recorded one), so compare rows of one series.
  Production runs a native image (Serial GC, aarch64), so absolute numbers differ there.
- Server limits: `max-connections=4000` (the derived limit would have refused the bigger
  runs); everything else default, access log on.
- Loop lock contention: JFR does not record a virtual thread parking on a `ReentrantLock`
  (checked: 8 virtual threads contending a lock give no `jdk.ThreadPark` with a lock frame,
  8 platform threads give 621k). Thread-dump sampling (`jcmd Thread.dump_to_file`) crashed
  the server JVM under load (SIGSEGV in `GetThreadSnapshotClosure`, a JVM bug), so the loop
  lock was measured with an instrumented `Loop` (`harness/patches/loop-lock-counters.patch`:
  `tryLock` first, counts contended acquisitions, wait and hold time). JFR execution samples
  were too sparse to attribute CPU (44 samples in 8 s at 1.8 cores; the time is spent parking
  and unparking virtual threads and in the kernel), so CPU is reported per process only.

## Findings the user should weigh

1. **The relay's windows cap each h2 connection at about 64 KiB per round trip** (todo 127).
   The relay announces netty's defaults on both faces: 65535 stream window and 65535
   connection window, refilled as DATA arrives. Downloads are bound by the tunnel RTT, uploads
   by the client RTT (and by the upstream's own window over the tunnel RTT). With a 20 ms
   tunnel RTT, one h2 connection downloads at 1.7 MiB/s with 1 stream and 1.4 MiB/s with 8
   streams, while h1 gets 41.7 MiB/s per connection. With the client 20 ms away, an h2 upload
   moves 1.7 MiB/s (1.4 MiB/s with 8 streams); h1 moves 477 MiB/s. A patched build with a 1 MiB
   stream window and a 16 MiB connection window downloads at 22.3 MiB/s (1 stream) and
   31.8 MiB/s (8 streams) at 20 ms, and at 526 MiB/s instead of 258 MiB/s on loopback. Small
   requests do not change. The cost is memory: the derived connection limit (3340 KiB per slot,
   54 connections in production) follows these windows. Also, `PendingPayload` grows by
   copying, so the server's allocation rose from 6.9 to 18.4 bytes per relayed byte.
2. **CPU per byte is 4 to 6 times the h1 path's.** Loopback, one connection: h2 263 MiB/s at
   1.36 server cores (5.2 ms CPU per MiB) against h1 1662 MiB/s at 2.12 cores (1.3 ms per MiB).
   The server allocates 6.8 to 8.2 bytes per relayed byte on h2 downloads, against 3.1 on h1.
   About 3.9 of these bytes are the demux's copies (JFR allocation samples, h2 download): the
   reader's copy into the channel (`Hop.feedLocked`, `Loop.call`), the bridge copy
   (`Flow.data`), the writer queue's heap copy (`Hop.enqueueLocked`) and backlog growth
   (`PendingPayload.append`). The rest is the tunnel (`TunnelSession.handle`,
   `VirtualConnection.appendCoalesced`, protobuf), which costs more per byte here than on h1
   because the window cuts the stream into smaller bursts. The production traffic is far
   below this, so no todo was filed beyond 127.
3. **Loop lock contention is visible only with many streams on one connection.** With 100
   streams on one connection (147k req/s), 39.6 % of the lock acquisitions were contended,
   the lock was held 70 % of the time and the threads waited 598 ms per second (60 µs per wait).
   With 10 streams per connection it is 6 to 21 %, and with 50 connections of 1 to 10 streams
   2 to 4 %. That single connection still did 147k req/s at 1.5 server cores. With many
   connections, the tunnel (one gRPC stream, about 4.5 server cores, 21k to 32k req/s for h1
   and h2 alike) is the limit, not the lock. No misuse was seen: no failure reached the
   loop's sink, and no WARN or ERROR came from sluice in any run. The hardening step (a custom
   `Channel` on a netty event loop) is **not filed**. One event loop per relay serializes the
   same work; it would remove the lock handoffs, not the serialization, and the measured
   ceiling is far above the tunnel's.
4. **A client vanishing mid-request is counted as a protocol error** (todo 128). Every h2
   upload that the load client ended by closing its connection added one
   `sluice_h2demux_streams_reset_total{reason="protocol"}` per stream in flight. With a
   delayed tunnel or 10 streams it also added `reason="upstream"` resets and one
   `sluice_h2demux_legs_failed_total`. netty logs `Sending GOAWAY failed: ... errorCode '2'` on
   the client hop as the connection closes. Downloads ended the same way count nothing. The
   counters on the dashboard would read a client going away as protocol errors.
5. **Memory and threads per connection are small next to the derived bound.** A held h2
   connection (one leg, one open stream) is 163 to 228 KiB of live server heap, plus 3 to
   19 KiB per further open stream. A held h1 connection is 143 to 150 KiB. The client side is
   138 to 146 KiB per connection either way. The derived 3340 KiB per slot is the worst case
   with every window and queue full: 15 to 20 times the held footprint. An h2 connection with
   one leg runs 7 virtual threads on the server (client and leg read, write and done, plus the
   ticker), and each further leg adds 3; h1 runs 2. The client runs 2 per connection.
6. **HPACK never-indexed costs bytes, not CPU.** A browser-like request head is 20 bytes on
   the client side (indexed) and 455 bytes on the tunnel; a response head is 57 bytes from the
   upstream and 205 bytes to the client. Encoding never-indexed is cheaper: 166 ns per request
   block against 407 ns indexed (`HpackBench.java`). At 30k req/s the request heads take about
   13 MB/s of a tunnel that carries 1.6 GiB/s, so no todo was filed.

## Key numbers

Loopback (tunnel on loopback), per connection unless noted:

| | h2 | h1 |
|---|---|---|
| download, 1 conn, 1 stream | 263 MiB/s, 1.36 server cores | 1662 MiB/s, 2.12 cores |
| download, 10 conns | 785 MiB/s, 4.96 cores | 1680 MiB/s (tunnel ceiling) |
| download, 1 conn, 10 streams | 197 MiB/s | - |
| upload, 1 conn | 286 MiB/s | 1684 MiB/s |
| small, 1 conn, 1 / 10 / 100 streams | 6.7k / 40k / 141k req/s, p50 0.15 / 0.25 / 0.67 ms, p99 1.13 ms at 100 | 6.6k req/s, p50 0.15 ms |
| small, 10 conns, 1 stream | 27.9k req/s, p50 0.35 ms, p99 0.49 ms | 24.7k req/s, p50 0.40 ms |
| small, 50 conns, 1 / 100 streams | 21.3k / 72.4k req/s, p50 2.3 / 67 ms | 25.8k req/s, p50 1.9 ms |
| server CPU per small request | 10.6 µs (1 conn, 100 streams), 156 µs (10 conns) | 185 µs (10 conns) |

The harness without sluice: 1548 MiB/s for one h2 stream, 889k req/s of small h2 requests,
so the load client and upstream do not limit these rows.

Tunnel RTT 20 ms: h2 download 1.7 MiB/s (1 stream; the same with browser-sized client
windows), 1.4 MiB/s (8 streams); h1 41.7 MiB/s (1 conn), 49.0 MiB/s (8 conns). Upload
h2 1.7 MiB/s, h1 41.6 MiB/s. A small request: h2 p50 25.4 ms, h1 24.0 ms.

Client RTT 20 ms: h2 download 1.7 MiB/s with the load client's default 64 KiB window (its own
choice) and 140 MiB/s with browser-sized windows (6 MiB stream, 15 MiB connection); h1 992 MiB/s.
Upload h2 1.7 MiB/s (1 stream), 1.4 MiB/s (8 streams); h1 477 MiB/s.

## Production watch (read-only)

- Deployed `0a62925` (the measured commit) at 12:41 UTC, native image on aarch64, 512 Mi
  limit. Startup log: `data plane connection limit 54 (derived: (half of max heap 409MiB - h2
  queue budget) / 3340KiB per connection)`, h2 queue budget 26208 KiB.
- From the deploy to 13:30 UTC the only data plane traffic was the 17 probe requests below
  (the access log shows their `type=conn` closes, rate-limited at 10 lines). There was no
  WARN or ERROR from sluice; one Micrometer WARN at startup says the native image gives no GC
  notifications. Before the probes, every `sluice_h2demux_*` and
  `sluice_data_connections_*_closed` counter read 0. Two tunnel clients were connected: a
  sluice-client and a sluice-wasmlet.
- Probes (`harness/prod-probe.sh`: 12 requests to the httpbin route, plus 3 by hand to the
  wasm demo and synology routes): the httpbin and synology routes negotiate http/1.1 even
  when h2 is offered, and the wasm demo route gets h2 (200, 68 to 138 ms).
- The earlier deploys' logs went with their pods; nothing was soaked across deploys.

## Reproduce

```
./mvnw -DskipTests package
mkdir -p lib classes && unzip -j sluice-server/target/sluice-server-*-exec.jar 'BOOT-INF/lib/netty-*' -d lib
javac -d classes -cp 'lib/*' .todo/artefacts/106/harness/src/*.java
# bench.py expects classes/ and lib/ next to it, and out/ for results
python3 bench.py <repo> direct,throughput,small --tag base     # see run-all.sh for the series
jfr configure --input profile jdk.ThreadPark#threshold=0ms jdk.JavaMonitorEnter#threshold=0ms \
  jdk.ObjectAllocationSample#throttle=1000/s --output contention.jfc   # for suite_profile
```

The lock and window suites (`--patched`, `--window-patch`) run the server unpacked
(`patched/unpacked`) with patched classes compiled from `harness/patches/*.patch` into
`patched/classes` (Loop) and `patched/classes2` (Hop, NettyHttp2DemuxRelay) ahead of
`BOOT-INF/classes`; the window patch reads `-Drelay.streamWindow` and `-Dhop.connWindow`.

## Tables (generated by `summarize.py results.jsonl`)

Columns: `server ms/MiB` CPU per relayed MiB, `server us/req` CPU per request,
`alloc B/B` bytes allocated per relayed byte, `h2demux counters` the counter deltas of the run
(the end of each run closes the load client's connections with requests in flight).

### Direct (harness ceiling, no sluice)

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base direct download h2 c1 s1 | 6.1 | 1547.7 | 164.484 | 176.994 | 176.994 | 0 | 0.006 | 0.031 | 0.14 | 0.004 | 919.3 | 0.0 | 705.05 |  |
| base direct download h1 c1 s1 | 48.6 | 12445.0 | 19.237 | 33.785 | 36.131 | 0 | 0.006 | 0.007 | 0.17 | 0.0 | 115.7 | 0.0 | 0.0 |  |
| base direct upload h2 c1 s1 | 24.1 | 1543.5 | 41.372 | 44.803 | 49.056 | 0 | 0.004 | 0.005 | 0.13 | 0.003 | 182.6 | 0.0 | 186.95 |  |
| base direct small h2 c10 s100 | 888854.7 | 1.7 | 0.96 | 2.208 | 3.878 | 0 | 0.006 | 0.006 | 0.15 | 3.608 | 0.0 | 0.0 | 0.0 |  |
| base direct small h2 c1 s10 | 129301.9 | 0.2 | 0.076 | 0.1 | 2.223 | 0 | 0.003 | 0.004 | 0.18 | 12.625 | 0.0 | 0.0 | 0.0 |  |
| base direct small h1 c10 | 187782.7 | 0.4 | 0.051 | 0.082 | 1.527 | 0 | 0.004 | 0.004 | 0.16 | 8.873 | 0.0 | 11.0 | 0.02 |  |

### Throughput, loopback

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base download h1 c1 s1 | 6.5 | 1662.0 | 151.558 | 184.297 | 184.297 | 0 | 2.121 | 2.064 | 0.47 | 1.276 | 326466.5 | 3.07 | 804580.43 |  |
| base download h1 c10 s1 | 7.0 | 1680.2 | 1500.106 | 1719.844 | 1719.844 | 0 | 2.034 | 2.058 | 0.5 | 1.211 | 290692.1 | 3.08 | 756940.8 |  |
| base download h2 c1 s1 | 1.0 | 263.0 | 966.384 | 995.704 | 995.704 | 0 | 1.357 | 0.894 | 0.46 | 5.159 | 1357258.1 | 6.9 | 1857945.6 |  |
| base download h2 c10 s1 | 3.0 | 785.0 | 3230.688 | 3283.561 | 3283.561 | 0 | 4.959 | 3.342 | 0.59 | 6.317 | 1653794.4 | 6.76 | 1812070.4 |  |
| base download h2 c1 s10 | 0.0 | 196.7 | 0.0 | 0.0 | 0.0 | 0 | 1.476 | 0.874 | 0.45 | 7.504 | 14763149.6 | 8.2 | 16515071.99 |  |
| base download h2 c1 s1 bigwin | 1.0 | 273.7 | 937.138 | 948.935 | 948.935 | 0 | 0.913 | 0.649 | 0.5 | 3.335 | 913348.2 | 6.75 | 1892352.0 |  |
| base upload h1 c1 s1 | 26.4 | 1684.0 | 37.59 | 43.318 | 43.355 | 0 | 2.031 | 1.721 | 0.54 | 1.206 | 76954.0 | 2.15 | 140753.45 |  |
| base upload h1 c10 s1 | 27.0 | 1706.4 | 373.621 | 415.071 | 415.33 | 0 | 2.074 | 1.775 | 0.56 | 1.215 | 76838.0 | 2.18 | 140834.13 |  |
| base upload h2 c1 s1 | 4.4 | 286.2 | 221.426 | 273.669 | 273.669 | 0 | 1.235 | 0.719 | 0.53 | 4.314 | 280747.3 | 5.75 | 383278.55 | sluice_h2demux_streams_reset_total{,reason="protocol"}=1 |
| base upload h2 c10 s1 | 9.9 | 586.4 | 1044.744 | 1265.291 | 1265.291 | 0 | 4.385 | 2.767 | 0.59 | 7.478 | 443180.3 | 5.96 | 361440.97 | sluice_h2demux_streams_reset_total{,reason="protocol"}=10 |
| base upload h2 c1 s10 | 4.0 | 205.5 | 3115.994 | 3185.77 | 3185.77 | 0 | 1.214 | 0.663 | 0.44 | 5.907 | 303500.6 | 11.44 | 602112.0 | sluice_h2demux_legs_failed_total{}=1 sluice_h2demux_streams_reset_total{,reason="protocol"}=10 sluice_h2demux_streams_reset_total{,reason="upstream"}=10 |

### Small requests, loopback

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base small h2 c1 s1 | 6737.6 | 0.0 | 0.146 | 0.217 | 1.555 | 0 | 0.662 | 0.397 | 0.52 |  | 98.2 |  | 15.34 |  |
| base small h2 c1 s10 | 40074.3 | 0.1 | 0.25 | 0.333 | 3.595 | 0 | 0.672 | 0.352 | 0.39 | 6724.614 | 16.8 | 3366.99 | 8.6 |  |
| base small h2 c1 s100 | 141374.0 | 0.3 | 0.667 | 1.128 | 2.401 | 0 | 1.496 | 0.668 | 0.29 | 4985.834 | 10.6 | 3479.59 | 7.56 |  |
| base small h2 c10 s1 | 27881.6 | 0.1 | 0.354 | 0.489 | 2.051 | 0 | 4.36 | 3.236 | 0.61 | 43596.165 | 156.4 | 3691.15 | 13.56 |  |
| base small h2 c10 s10 | 30508.6 | 0.1 | 3.266 | 4.137 | 5.828 | 0 | 4.466 | 3.205 | 0.59 | 44660.912 | 146.4 | 3690.15 | 12.39 |  |
| base small h2 c10 s100 | 32342.2 | 0.1 | 30.619 | 34.283 | 35.507 | 0 | 4.665 | 3.138 | 0.57 | 46654.396 | 144.3 | 4341.83 | 13.75 |  |
| base small h2 c50 s1 | 21273.8 | 0.0 | 2.349 | 2.774 | 5.24 | 0 | 4.525 | 3.248 | 0.59 |  | 212.7 |  | 13.57 |  |
| base small h2 c50 s10 | 24708.4 | 0.0 | 20.383 | 22.065 | 22.742 | 0 | 4.461 | 3.241 | 0.6 |  | 180.5 |  | 12.84 |  |
| base small h2 c50 s100 | 72389.7 | 0.1 | 67.45 | 88.982 | 113.396 | 0 | 5.053 | 3.057 | 0.49 | 50534.475 | 69.8 | 7059.47 | 9.99 |  |
| base small h1 c1 | 6625.8 | 0.0 | 0.148 | 0.24 | 3.232 | 0 | 0.705 | 0.399 | 0.42 |  | 106.3 |  | 40.16 |  |
| base small h1 c10 | 24673.4 | 0.0 | 0.397 | 0.598 | 3.619 | 0 | 4.575 | 3.265 | 0.5 |  | 185.4 |  | 40.55 |  |
| base small h1 c50 | 25784.2 | 0.0 | 1.9 | 2.688 | 3.998 | 0 | 4.851 | 3.543 | 0.51 |  | 188.1 |  | 40.44 |  |
| base small h1 c100 | 26344.9 | 0.1 | 3.737 | 4.689 | 5.754 | 0 | 4.887 | 3.548 | 0.51 | 48874.8 | 185.5 | 10385.81 | 40.37 |  |

### Tunnel RTT 20ms (10ms one-way between sluice-client and sluice-server)

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| wan10 download h1 c1 s1 | 0.6 | 41.7 | 1487.361 | 1505.157 | 1505.157 | 0 | 0.249 | 0.289 | 0.15 | 5.968 | 414883.3 | 3.08 | 219249.78 |  |
| wan10 download h2 c1 s1 | 0.0 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.108 | 0.04 | 0.09 | 63.681 | 1624183.4 | 8.63 | 225280.0 |  |
| wan10 download h2 c1 s1 bigwin | 0.0 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.102 | 0.041 | 0.08 | 60.037 | 1531442.8 | 8.62 | 225280.0 |  |
| wan10 download h2 c1 s8 | 0.0 | 1.4 | 0.0 | 0.0 | 0.0 | 0 | 0.086 | 0.053 | 0.09 | 61.749 | 1297074.8 | 8.57 | 184320.0 |  |
| wan10 download h1 c8 | 0.5 | 49.0 | 0.0 | 0.0 | 0.0 | 0 | 0.208 | 0.219 | 0.23 | 4.247 | 390258.7 | 3.06 | 288256.0 |  |
| wan10 upload h1 c1 s1 | 2.6 | 41.6 | 385.689 | 392.494 | 392.494 | 0 | 0.194 | 0.213 | 0.23 | 4.665 | 74640.8 | 2.21 | 36181.33 |  |
| wan10 upload h2 c1 s1 | 0.1 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.078 | 0.043 | 0.14 | 45.782 | 1167686.6 | 7.21 | 188416.0 | sluice_h2demux_streams_reset_total{,reason="protocol"}=1 |
| wan10 small h2 c1 s1 | 37.9 | 0.0 | 25.4 | 37.465 | 39.92 | 0 | 0.094 | 0.024 | 0.12 |  | 2490.2 |  | 54.04 |  |
| wan10 small h1 c1 | 41.0 | 0.0 | 24.004 | 29.396 | 33.945 | 0 | 0.082 | 0.022 | 0.1 |  | 2004.5 |  | 0.0 |  |

### Client RTT 20ms (10ms one-way between load client and data port)

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cwan10 cwan download h1 c1 s1 | 15.5 | 991.7 | 64.34 | 69.194 | 74.902 | 0 | 1.43 | 1.445 | 0.48 | 1.442 | 92461.9 | 3.07 | 201401.38 |  |
| cwan10 cwan download h2 c1 s1 | 0.0 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.178 | 0.059 | 0.07 | 104.986 | 2677687.9 | 13.25 | 346112.0 |  |
| cwan10 cwan download h2 c1 s1 bigwin | 2.2 | 140.3 | 456.773 | 459.395 | 459.395 | 0 | 0.674 | 0.456 | 0.41 | 4.803 | 306384.4 | 6.74 | 440506.18 |  |
| cwan10 cwan upload h1 c1 s1 | 29.8 | 476.7 | 33.471 | 35.9 | 36.258 | 0 | 0.604 | 0.579 | 0.54 | 1.266 | 20259.3 | 2.17 | 35617.79 |  |
| cwan10 cwan upload h2 c1 s1 | 0.1 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.055 | 0.024 | 0.23 | 32.61 | 831765.5 | 13.25 | 346111.99 | sluice_h2demux_streams_reset_total{,reason="protocol"}=1 |
| cwan10 cwan upload h2 c1 s8 | 0.0 | 1.4 | 0.0 | 0.0 | 0.0 | 0 | 0.062 | 0.046 | 0.2 | 44.422 | 932914.1 | 0.0 | 0.0 | sluice_h2demux_streams_reset_total{,reason="protocol"}=8 |

### Window experiment

| scenario | req/s | MiB/s | p50 ms | p99 ms | max ms | err | server cores | client cores | server sys | server ms/MiB | server us/req | server alloc B/B | server alloc KiB/req | h2demux counters |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| winbase-d0 winexp download h2 c1 s1 | 4.0 | 257.9 | 247.643 | 283.413 | 283.413 | 0 | 1.54 | 1.185 | 0.43 | 5.97 | 385018.1 | 6.91 | 456213.33 |  |
| winbase-d0 winexp download h2 c1 s8 | 2.7 | 203.8 | 2515.409 | 2519.604 | 2519.604 | 0 | 1.282 | 0.887 | 0.47 | 6.291 | 481024.0 | 7.93 | 621152.0 |  |
| winbase-d0 winexp download h1 c1 | 26.1 | 1669.6 | 37.865 | 43.818 | 47.103 | 0 | 1.862 | 1.963 | 0.54 | 1.115 | 71431.1 | 3.09 | 202352.87 |  |
| winbase-d0 winexp upload h2 c1 s1 | 17.9 | 287.4 | 55.542 | 61.242 | 69.281 | 0 | 1.21 | 0.753 | 0.52 | 4.211 | 67582.7 | 5.72 | 94055.59 | sluice_h2demux_streams_reset_total{,reason="protocol"}=1 |
| winbase-d0 winexp small h2 c1 s100 | 144396.9 | 0.3 | 0.651 | 1.17 | 2.891 | 0 | 1.451 | 0.729 | 0.32 | 4836.567 | 10.0 | 3569.85 | 7.6 |  |
| win1m-d0 winexp download h2 c1 s1 | 8.2 | 525.7 | 122.483 | 144.335 | 144.335 | 0 | 2.101 | 0.955 | 0.33 | 3.997 | 254782.3 | 18.35 | 1197759.35 |  |
| win1m-d0 winexp download h2 c1 s8 | 6.0 | 390.3 | 1303.094 | 1358.658 | 1358.658 | 0 | 1.688 | 0.585 | 0.33 | 4.325 | 281427.9 | 19.47 | 1297223.11 |  |
| win1m-d0 winexp download h1 c1 | 26.1 | 1668.1 | 37.924 | 43.475 | 48.687 | 0 | 2.004 | 1.834 | 0.5 | 1.201 | 76849.6 | 3.1 | 203046.44 |  |
| win1m-d0 winexp upload h2 c1 s1 | 13.8 | 221.8 | 70.79 | 91.869 | 99.249 | 0 | 0.993 | 0.605 | 0.38 | 4.479 | 71821.2 | 14.92 | 245038.27 | sluice_h2demux_streams_reset_total{,reason="protocol"}=1 |
| win1m-d0 winexp small h2 c1 s100 | 144745.0 | 0.3 | 0.653 | 1.154 | 3.833 | 0 | 1.576 | 0.744 | 0.31 | 5253.688 | 10.9 | 3416.61 | 7.25 |  |
| winbase-d10 winexp download h2 c1 s1 | 0.0 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.14 | 0.126 | 0.09 | 82.313 | 1679194.2 | 6.91 | 144384.0 |  |
| winbase-d10 winexp download h2 c1 s8 | 0.0 | 1.3 | 0.0 | 0.0 | 0.0 | 0 | 0.168 | 0.07 | 0.06 | 128.867 | 2010495.3 | 9.04 | 144384.0 |  |
| winbase-d10 winexp download h1 c1 | 0.3 | 22.9 | 2877.308 | 2878.497 | 2878.497 | 0 | 0.172 | 0.153 | 0.12 | 7.507 | 515925.0 | 3.2 | 225536.0 |  |
| winbase-d10 winexp upload h2 c1 s1 | 0.1 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.124 | 0.045 | 0.09 | 72.863 | 1487020.5 | 7.2 | 150528.0 | sluice_h2demux_legs_failed_total{}=1 sluice_h2demux_streams_reset_total{,reason="protocol"}=1 sluice_h2demux_streams_reset_total{,reason="upstream"}=1 |
| winbase-d10 winexp small h2 c1 s100 | 4254.3 | 0.0 | 23.228 | 26.114 | 26.999 | 0 | 0.178 | 0.066 | 0.08 |  | 41.8 |  | 7.46 |  |
| win1m-d10 winexp download h2 c1 s1 | 0.3 | 22.3 | 2856.643 | 2875.704 | 2875.704 | 0 | 0.489 | 0.284 | 0.09 | 21.911 | 1466062.0 | 13.39 | 917759.99 |  |
| win1m-d10 winexp download h2 c1 s8 | 0.1 | 31.8 | 0.0 | 0.0 | 0.0 | 0 | 0.245 | 0.12 | 0.24 | 7.713 | 2944497.3 | 19.09 | 7460863.96 |  |
| win1m-d10 winexp download h1 c1 | 0.7 | 43.0 | 1482.933 | 1497.442 | 1497.442 | 0 | 0.119 | 0.1 | 0.33 | 2.758 | 177938.4 | 3.08 | 203264.0 |  |
| win1m-d10 winexp upload h2 c1 s1 | 0.1 | 1.7 | 0.0 | 0.0 | 0.0 | 0 | 0.062 | 0.039 | 0.13 | 36.48 | 744508.8 | 11.42 | 238592.0 | sluice_h2demux_legs_failed_total{}=1 sluice_h2demux_streams_reset_total{,reason="protocol"}=1 sluice_h2demux_streams_reset_total{,reason="upstream"}=1 |
| win1m-d10 winexp small h2 c1 s100 | 4204.9 | 0.0 | 23.443 | 25.15 | 25.7 | 0 | 0.165 | 0.062 | 0.1 |  | 39.2 |  | 12.08 |  |
| win1m-up1m-d10 winexp download h2 c1 s1 | 0.3 | 22.3 | 2862.143 | 2883.09 | 2883.09 | 0 | 0.503 | 0.27 | 0.09 | 22.554 | 1509263.9 | 13.21 | 904959.99 |  |
| win1m-up1m-d10 winexp download h2 c1 s8 | 0.0 | 23.0 | 0.0 | 0.0 | 0.0 | 0 | 0.463 | 0.135 | 0.12 | 20.125 | 5556742.6 | 15.45 | 4369407.98 |  |
| win1m-up1m-d10 winexp download h1 c1 | 0.6 | 39.1 | 1601.491 | 1805.54 | 1805.54 | 0 | 0.246 | 0.159 | 0.18 | 6.294 | 422018.4 | 3.09 | 212260.57 |  |
| win1m-up1m-d10 winexp upload h2 c1 s1 | 1.4 | 22.4 | 710.971 | 753.201 | 753.201 | 0 | 0.266 | 0.142 | 0.17 | 11.875 | 187838.6 | 13.66 | 221244.24 | sluice_h2demux_legs_failed_total{}=1 sluice_h2demux_streams_reset_total{,reason="protocol"}=1 sluice_h2demux_streams_reset_total{,reason="upstream"}=1 |
| win1m-up1m-d10 winexp small h2 c1 s100 | 3972.6 | 0.0 | 23.991 | 35.402 | 40.52 | 0 | 0.173 | 0.061 | 0.15 |  | 43.6 |  | 10.79 |  |

### Loop lock (instrumented build)

| scenario | req/s | MiB/s | acquired/s | contended | wait ms/s | held ms/s | avg wait us | avg hold us |
|---|---|---|---|---|---|---|---|---|
| lock lock download h2 c1 s1 | 1.0 | 256.2 | 52675 | 8.5% | 27.3 | 151.3 | 6.1 | 2.87 |
| lock lock download h2 c1 s10 | 1.0 | 205.3 | 48302 | 20.9% | 154.9 | 305.4 | 15.3 | 6.32 |
| lock lock upload h2 c1 s10 | 4.0 | 210.3 | 43325 | 19.2% | 105.0 | 260.6 | 12.6 | 6.01 |
| lock lock small h2 c1 s1 | 6969.0 | 0.0 | 27873 | 0.0% | 0.0 | 104.2 | 3.4 | 3.74 |
| lock lock small h2 c1 s10 | 41726.6 | 0.1 | 20868 | 6.2% | 11.9 | 221.9 | 9.3 | 10.63 |
| lock lock small h2 c1 s100 | 147415.2 | 0.3 | 25026 | 39.6% | 597.6 | 703.4 | 60.3 | 28.11 |
| lock lock small h2 c10 s100 | 43778.4 | 0.1 | 122756 | 10.9% | 126.4 | 937.9 | 9.5 | 7.64 |
| lock lock small h2 c50 s1 | 28257.4 | 0.1 | 125237 | 2.2% | 5.8 | 699.8 | 2.1 | 5.59 |
| lock lock small h2 c50 s10 | 33471.5 | 0.1 | 124819 | 3.8% | 32.0 | 740.4 | 6.7 | 5.93 |
| win1m-d0 winexp download h2 c1 s1 | 8.2 | 525.7 | 66012 | 11.8% | 580.5 | 473.8 | 74.9 | 7.18 |
| win1m-d0 winexp download h2 c1 s8 | 6.0 | 390.3 | 55247 | 12.8% | 567.5 | 545.0 | 80.4 | 9.87 |
| win1m-d0 winexp download h1 c1 | 26.1 | 1668.1 | 0 | 0.0% | 0.0 | 0.0 | 0.0 | 0.0 |
| win1m-d0 winexp upload h2 c1 s1 | 13.8 | 221.8 | 27277 | 6.7% | 222.0 | 255.3 | 122.0 | 9.36 |
| win1m-d0 winexp small h2 c1 s100 | 144745.0 | 0.3 | 26459 | 40.0% | 606.4 | 704.1 | 57.3 | 26.61 |
| win1m-d10 winexp download h2 c1 s1 | 0.3 | 22.3 | 2893 | 12.6% | 17.5 | 26.2 | 48.0 | 9.06 |
| win1m-d10 winexp download h2 c1 s8 | 0.1 | 31.8 | 4418 | 11.2% | 45.9 | 56.8 | 92.4 | 12.86 |
| win1m-d10 winexp download h1 c1 | 0.7 | 43.0 | 0 | 0.0% | 0.0 | 0.0 | 0.0 | 0.0 |
| win1m-d10 winexp upload h2 c1 s1 | 0.1 | 1.7 | 219 | 2.4% | 0.2 | 7.7 | 30.3 | 35.23 |
| win1m-d10 winexp small h2 c1 s100 | 4204.9 | 0.0 | 468 | 23.1% | 10.9 | 31.9 | 101.5 | 68.22 |
| win1m-up1m-d10 winexp download h2 c1 s1 | 0.3 | 22.3 | 2897 | 13.2% | 18.6 | 28.1 | 48.7 | 9.72 |
| win1m-up1m-d10 winexp download h2 c1 s8 | 0.0 | 23.0 | 3284 | 12.5% | 46.6 | 61.8 | 113.3 | 18.82 |
| win1m-up1m-d10 winexp download h1 c1 | 0.6 | 39.1 | 0 | 0.0% | 0.0 | 0.0 | 0.0 | 0.0 |
| win1m-up1m-d10 winexp upload h2 c1 s1 | 1.4 | 22.4 | 2027 | 10.9% | 8.4 | 51.5 | 38.2 | 25.39 |
| win1m-up1m-d10 winexp small h2 c1 s100 | 3972.6 | 0.0 | 576 | 21.7% | 23.6 | 70.9 | 188.1 | 123.01 |

### Held connections and streams (live heap after full GC, threads)

| scenario | server KiB/conn | server KiB/stream | server threads/conn | client KiB/conn | client threads/conn | server threads added |
|---|---|---|---|---|---|---|
| hold hold h2 c1 s1 | 228.0 | 228.0 | 7.0 | 146.0 | 2.0 | sluice-h#n-leg-#-write 1, sluice-h#n-leg-#-done 1, sluice-h#n-tick 1, sluice-h#n-client-read 1, sluice-h#n-client-write 1, sluice-h#n-client-done 1, sluice-h#n-leg-#-read 1 |
| hold hold h2 c50 s1 | 171.3 | 171.32 | 7.28 | 138.5 | 2.22 | sluice-h#n-client-write 50, sluice-h#n-leg-#-write 50, sluice-h#n-client-done 50, sluice-h#n-leg-#-done 50, sluice-h#n-client-read 50, sluice-h#n-tick 50, sluice-h#n-leg-#-read 50 |
| hold hold h2 c100 s1 | 162.8 | 162.83 | 7.14 | 140.1 | 2.13 | sluice-h#n-leg-#-write 100, sluice-h#n-leg-#-done 100, sluice-h#n-client-read 100, sluice-h#n-client-write 100, sluice-h#n-client-done 100, sluice-h#n-leg-#-read 100, sluice-h#n-tick 100 |
| hold hold h2 c1 s100 | 718.0 | 7.18 | 21.0 | 231.0 | 15.0 | sluice-h#n-tick 1, sluice-h#n-client-read 1, sluice-h#n-client-write 1, sluice-h#n-client-done 1, sluice-h#n-leg-#-read 1, sluice-h#n-leg-#-write 1, sluice-h#n-leg-#-done 1 |
| hold hold h2 c10 s100 | 338.3 | 3.38 | 8.4 | 145.8 | 3.3 | sluice-h#n-tick 10, sluice-h#n-client-read 10, sluice-h#n-client-write 10, sluice-h#n-client-done 10, sluice-h#n-leg-#-read 10, sluice-h#n-leg-#-write 10, sluice-h#n-leg-#-done 10 |
| hold hold h2 c50 s10 | 187.2 | 18.72 | 7.28 | 138.3 | 2.26 | sluice-h#n-leg-#-write 50, sluice-h#n-leg-#-done 50, sluice-h#n-leg-#-read 50, sluice-h#n-tick 50, sluice-h#n-client-read 50, sluice-h#n-client-write 50, sluice-h#n-client-done 50 |
| hold hold h1 c50 s1 | 150.3 | 150.26 | 2.28 | 137.9 | 2.26 | sluice-reroute-out 50, sluice-reroute-in-# 50 |
| hold hold h1 c100 s1 | 143.3 | 143.34 | 2.14 | 139.0 | 2.13 | sluice-reroute-out 100, sluice-reroute-in-# 100 |

### HPACK: header bytes per request (frame header included)

| scenario | request HEADERS client->relay | relay->upstream (tunnel) | response HEADERS upstream->relay (tunnel) | relay->client |
|---|---|---|---|---|
| base small h2 c1 s1 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c1 s10 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c1 s100 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c10 s1 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c10 s10 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c10 s100 | 20.0 | 455.0 | 56.8 | 205.0 |
| base small h2 c50 s1 | 20.1 | 455.0 | 56.8 | 205.0 |
| base small h2 c50 s10 | 20.1 | 455.0 | 56.8 | 205.0 |
| base small h2 c50 s100 | 20.0 | 455.0 | 56.8 | 205.0 |

## After todo 127: 1MiB windows on both faces

The relay announces a 1MiB stream window and grants a 1MiB connection window on both faces
(the tunnel's per-connection window), and `PendingPayload` holds its backlog in arrays
filled in order instead of growing one array by copying. Series `harness/series-127.sh`
(rows in `results-127.jsonl`), run back to back on one machine with no surefire JVM running
(load 3 to 5): `after` is the build of this change, `base2` the main it is based on
(`2f7d457`). Same harness and settings as above.

| | base2 | after |
|---|---|---|
| download h2, loopback, 1 stream | 260 MiB/s, 6.9 B alloc/B | 754 MiB/s, 6.3 B alloc/B |
| download h2, loopback, 8 streams | 205 MiB/s | 529 MiB/s |
| upload h2, loopback | 291 MiB/s | 291 MiB/s |
| small h2, loopback, 100 streams | 148k req/s | 148k req/s |
| download h2, tunnel RTT 20 ms, 1 stream | 1.5 MiB/s | 20.5 MiB/s |
| download h2, tunnel RTT 20 ms, 8 streams | 1.3 MiB/s | 20.6 MiB/s |
| download h1, tunnel RTT 20 ms (same runs) | 20.5 MiB/s | 25.0 MiB/s |
| upload h2, tunnel RTT 20 ms, upstream window 64KiB | 1.6 MiB/s | 1.6 MiB/s |
| upload h2, tunnel RTT 20 ms, upstream window 1MiB | 21.1 MiB/s | 20.9 MiB/s |
| upload h2, client RTT 20 ms, 1 stream | 1.6 MiB/s | 29.7 MiB/s |
| upload h2, client RTT 20 ms, 8 streams | 1.3 MiB/s | 20.7 MiB/s |
| download h2, client RTT 20 ms, client windows 6/15MiB | 136 MiB/s | 232 MiB/s |

- At a 20 ms tunnel RTT an h2 connection now moves what an h1 connection does over the
  same tunnel: 20.5 MiB/s for one stream and for eight (h1: 20.5 to 25.0 MiB/s in these
  runs; 40.8 to 43.0 MiB/s in other runs of the same suite above, so h1 at a delayed
  tunnel varies between runs). Eight streams add nothing over one, which fits the bound
  being the upstream connection's own: one virtual connection, whose 1MiB credit window
  (granted back at half) bounds an h1 connection the same way. So a relay window past
  1MiB would not raise a download through one upstream connection.
- Uploads with the upstream's window at 64KiB stay at 1.6 MiB/s: the upstream's own
  window over the tunnel RTT bounds them, which the relay cannot change.
- The server allocates 6.3 bytes per relayed byte on a loopback download instead of
  18.4 with the 1MiB windows of the experiment above (6.9 on main with 64KiB windows).
- The derived connection limit follows the windows: 5261KiB per slot instead of 3340KiB,
  34 connections from production's 409MiB heap instead of 54.
