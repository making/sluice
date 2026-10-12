Difficulty: High

# Feasibility: hand the h2 demux's framing and stream state to netty's HTTP/2 codec

A study, not a migration. Decide with the user whether to migrate before working the open
h2 demux todos (066, 073, 075, 078, 080, 085, 088, 089, 090, 091, 092, 093): a migration
would change or subsume most of them.

## Problem

`Http2DemuxRelay` (about 4,300 lines) hand-writes HTTP/2: frame parsing and writing, stream
state (RFC 9113 5.1), flow control, SETTINGS, GOAWAY, and the error scope. netty
(`netty-codec-http2`, already a dependency) is used only for HPACK. Each fix exposes the
next unvalidated path: 082, 088, 089 and 092 are validation that a mature codec does, and
090 and 093 are flow-control accounting. The hand-written state machine keeps producing todos.

## Questions to answer

- **Layer:** which netty layer fits?
  - `Http2FrameReader` / `Http2FrameWriter` only (validated framing; keep our state).
  - Plus `DefaultHttp2Connection` and the flow controllers (stream state and windows).
  - Or the full `Http2ConnectionHandler` / `Http2FrameCodec` / `Http2MultiplexHandler` in a
    channel pipeline.
- **I/O:** I/O model fit. The relay runs blocking I/O on virtual threads. The client side is
  a raw socket handed over by `DataProxyServer` after head inspection (TLS terminated or
  plain). Each upstream leg is a tunnel `VirtualConnection`, not a socket. netty pipelines
  need a channel: an `EmbeddedChannel` driven by our threads, or a custom `Channel` over
  `VirtualConnection`, or moving the data plane onto netty event loops. Cost and threading
  risk of each.
- **Behaviours to keep:** what in the current behaviour netty would have to support or let
  us plug in:
  - Stream-by-stream rerouting to different legs and routes, and leg retirement.
  - The shared queue budget, window withholding and `refuseWaiting` (083).
  - The stall timeout (071), the idle and keep-alive timeouts.
  - The upstream error scope (081): a 502 before the head, `RST_STREAM` after it,
    `REFUSED_STREAM`, GOAWAY handling.
  - Upstream `MAX_CONCURRENT_STREAMS` waiting (067), the client-face SETTINGS, header
    re-encoding, and the metrics.
- **Alternatives:** other libraries (e.g. Jetty's low-level http2), or keeping our state
  machine behind a validated netty frame layer. Native image impact.
- **Acceptance suite:** the `sluice-it` `H2Demux*E2ETest` classes and the other h2 E2E
  tests are the behavioural contract. List any test that encodes behaviour netty cannot
  give.

## Deliverable

- A short design note in this file, with a recommendation (migrate / partial / stay) and a
  migration plan in steps that keep `sluice-it` green.
- Per open h2 todo: whether the migration subsumes it, changes it, or leaves it.
- A spike under `.todo/artefacts/096/` that proves the riskiest point (likely: one client
  connection demuxed to two legs over `VirtualConnection` with netty doing the framing and
  flow control), with a test.
- The user decides; file the migration todos only after that.

## Findings (2026-10-12)

Netty 4.2.17.Final (`netty-codec-http2`, plus `netty-transport` already on the classpath via
grpc-netty). Sources read: `DefaultHttp2FrameReader`, `DefaultHttp2ConnectionDecoder`,
`DefaultHttp2Connection`, `DefaultHttp2LocalFlowController`,
`DefaultHttp2RemoteFlowController`, `Http2ConnectionHandler(Builder)`,
`StreamBufferingEncoder`, `EmbeddedChannel` / `EmbeddedEventLoop`, `ByteToMessageDecoder`,
`AdaptivePoolingAllocator`. Contract: the 173 h2 test methods in `sluice-it`, catalogued
by test (tags: wire ids, end-to-end windows, settings replay, exact codes, budget, hooks).

### Recommendation: migrate, staged behind an engine switch -- the user decides

- The spike shows that netty can carry the demux. Each hop is a full netty HTTP/2
  endpoint. The bridge forwards streams and holds flow control across the hops. Backpressure
  holds in both directions, a slow client socket stops the upstream, and resets and
  upstream protocol errors stay scoped to their stream or leg.
- Netty subsumes 7 of the 12 held todos (table below): framing and stream-state
  validation, closed-stream handling, push, flow-control accounting, and relay-answered
  pages through the flow controller. These are the classes of bug the hand-written machine
  keeps producing.
- The cost is a rewrite of about 4,300 lines (to about 2,000 to 2,500). About 60 to 70 of
  the 173 h2 test methods need rewriting, because the semantics move from end-to-end stream
  windows to per-hop windows (the contract section below). That figure is an estimate from
  the catalog, not a run.
- The main new risk is the I/O boundary (see "I/O model"). The spike hit, and fixed, a real
  payload-corruption bug there.
- "Stay" is cheaper per step but never ends; "partial" removes only part of the cause.

### Spike (`.todo/artefacts/096/`, standalone, not in the reactor)

Run: `./mvnw -q -DskipTests install -pl sluice-proto -am`, then
`./mvnw -f .todo/artefacts/096/pom.xml test`. Recorded output: `test-output.txt`.
Race probe: `./mvnw -f .todo/artefacts/096/pom.xml test -Dgroups=probe -Dspike.excludedGroups=`.

Setup: a real loopback TCP client connection, and one relay. Each leg runs over the real
`VirtualConnection` (`TunnelPair`: two windowed connections wired back to back, 64KiB
credit). Upstreams and the client are netty endpoints too. Each endpoint is an
`Http2ConnectionHandler` in an `EmbeddedChannel` (`Hop`), driven by virtual-thread
blocking I/O. One lock per relay serializes every channel operation (`Loop`). Netty's
paranoid leak detector runs with a listener; the suite fails on any leak (checked:
leaking one buffer on purpose fails it).

`NettyDemuxSpikeTest`, 12 tests, all pass, green in 20 of 20 full runs. Each flow-control
bound has a negative control that breaks it:

| Test | Shows |
|---|---|
| streamsOfOneConnection...WithTheirIdsKept | streams 1..7 alternate two authorities; each upstream sees only its own streams, with the client's ids (netty accepts a leg opening a subsequence of ids); one leg per route |
| aRequestBodyIsHeldToTheUpstreamWindow... | upstream holds its window: it gets exactly 65535 of 1MiB; the relay reads at most its own announced window on top (<= 2*65535); a 200KiB request on the other leg completes meanwhile; after release, 1MiB arrives intact (CRC) |
| consumingOnArrivalBuffersTheWholeRequestBody (negative) | consuming on read instead of on the other hop's write: the relay reads the whole 1MiB |
| withoutAutoRefill...StarvesTheOtherStreams (negative) | with netty's default connection window, one held stream starves the connection's other streams; auto refill of the connection window (stream windows bound the queue) is required for leg isolation |
| aResponseBodyIsHeldToTheClientWindow... | client holds its window: it gets 65535 of 1MiB; the relay reads <= 2*65535 from the upstream; after release the body is intact |
| consumingOnArrival...ResponseBody (negative) | the relay reads the whole 1MiB |
| aClientNotReadingItsSocketStopsTheUpstreamThroughWritability | client grants 64MiB, then stops reading the socket: the relay reads < 2MiB of a 16MiB response (writer queue drives a user-defined writability bit); after resuming, 16MiB arrive intact |
| withoutWritabilityFeedback... (negative) | the relay reads all 16MiB into its writer queue |
| aClientResetReachesTheUpstream... | client RST_STREAM(CANCEL) reaches the upstream on the same id; both hops' streams and the exchange are freed; 300KiB then pass the same leg intact |
| anUpstreamResetReachesTheClient... | upstream RST_STREAM(ENHANCE_YOUR_CALM) reaches the client; the other leg completes |
| streamsBeyondTheUpstreamConcurrencyLimitWaitInTheRelay | upstream MAX_CONCURRENT_STREAMS 1: three streams wait in `StreamBufferingEncoder`, at most one open on the upstream, ids [1,3,5], no refusal. Removing the relay's "1 until the upstream's SETTINGS" made it fail (the encoder assumes 100 until SETTINGS) |
| anUpstreamProtocolErrorEndsOnlyItsLeg | upstream SETTINGS on a stream: netty's reader makes it a connection error, the leg sends GOAWAY PROTOCOL_ERROR and closes, the stream gets 502, the other leg completes, the client's connection survives and the next stream opens a new leg |

Found on the way (each turned into code or a test):
- **ByteBufs must not cross the lock.** The first version gave the writer threads the
  ByteBufs the channels wrote, read outside the lock. Payloads were corrupted in 4 of 10
  full-suite runs, as a CRC mismatch with the byte count right: a 16KiB read landed at the
  wrong offset of the relay's cumulation buffer. Checks inside the relay showed the raw
  socket bytes correct and the payload wrong when parsed. In 40 runs of the two upload
  tests each:
  - zero-copy writer: 6 runs corrupted (11 of 30 with the final code and
    `-Dspike.writerReadsByteBufs=true`);
  - heap copy under the lock (the spike's design now): 0;
  - copying DATA at the bridge instead: 0.
  Mechanism, shown by `ByteBufCrossThreadProbeTest` (misplacement in 6 and 9 of 10 JVM
  runs, in two batches) and
  by source (`AdaptivePoolingAllocator.java:2309-2313, 2502-2510`): with netty 4.2's default
  adaptive allocator, `getBytes(int, OutputStream, int)` on another thread resets the
  buffer's cached internal NIO buffer, which the owning thread's in-place append uses.
  Forwarded DATA is a slice of the reading hop's cumulation. Single-threaded retention of
  such slices is fine (`RetainedSliceProbeTest`).
- `StreamBufferingEncoder` keeps its own concurrency limit (100 until SETTINGS) and reads
  the connection's only in `remoteSettings`. The "one stream until SETTINGS" rule has to go
  through `encoder.remoteSettings(...)`.
- Netty's client endpoint, too, rejects a peer whose first frame is not SETTINGS
  (`PrefaceFrameListener`).

Not proven by the spike:
- TLS, seed-head replay (`DataProxyServer` hands over bytes already read), and the
  first-route-resolved-once rule.
- The shared `H2DemuxBudget`, `refuseWaiting`, the stall / idle / keep-alive timeouts, the
  PING probe after the client's EOF, and leg slots (`ConnectionLimits`).
- 1xx, trailers, CONNECT, GOAWAY from an upstream mapped to REFUSED_STREAM, push
  rejection, and header validation choices (`validateHeaders`).
- Metrics and access logs.
- Performance: throughput, latency, the extra copy, contention on one lock per relay.
- Native image. The `sluice-it` suite.

### Layer

The full `Http2ConnectionHandler`, one per hop, with our own `Http2FrameListener`.
- **Frame reader/writer only:** validated framing, but it leaves the stream state, the
  windows and the error scope ours, where most todos live.
- **Connection plus flow controllers without the handler:** not separable. The flow
  controllers need the handler's `ChannelHandlerContext` and frame writer
  (`DefaultHttp2LocalFlowController#channelHandlerContext`; `consumeBytes` asserts
  `inEventLoop`).
- **`Http2FrameCodec` / `Http2MultiplexHandler`:** they assign upstream stream ids at the
  first write and add a child channel per stream. The bridge needs explicit ids,
  `consumeBytes` timing and `StreamBufferingEncoder`, which the plain handler exposes.

### I/O model

1. **`EmbeddedChannel` driven by our threads (the spike).** The smallest change: virtual
   threads and `DuplexPipe` / `VirtualConnection` stay as they are. Costs and risks:
   - `EmbeddedChannel` is a test utility. `EmbeddedEventLoop#inEventLoop` is always true and
     its task queue is a plain `ArrayDeque`, so netty's thread assertions never fire; one
     lock per relay is the only guard.
   - Scheduled tasks run only when called (a ticker).
   - Writability has to be fed back by hand.
   - Every outbound byte is copied once (the relay copies today too).
   - Recorded exceptions must be swallowed in the pipeline.
2. **A custom `Channel` over `DuplexPipe` / `VirtualConnection` on a netty `EventLoop`**
   (all channels of a relay on one loop). This is netty's supported threading. It needs an
   `AbstractChannel` with blocking-I/O shims (a reader thread submits to the loop; `doWrite`
   hands buffers to a writer). That means about one extra transport class, and a
   cross-thread hop per read.
3. **Data plane on netty event loops** (NIO channels, `SslHandler`, PROXY protocol, head
   parsing, the h1 relay and passthrough). The cleanest end state and by far the largest
   change: it rewrites `DataProxyServer`, `ReroutingRelay` and the TLS path, beyond h2.

Option 1 to migrate, with option 2 as the hardening step if production shows lock
contention or misuse. The ByteBuf confinement rule and a CRC stress test are mandatory in
either case.

### Behaviours: netty gives / we plug in / changes

- **Netty gives:**
  - RFC 9113 framing and stream-state validation for both faces, with correct GOAWAY /
    RST_STREAM codes.
  - Per-hop HPACK, which removes the table-less re-encode.
  - SETTINGS / PING acks, control-frame and RST flood limits (10000 queued control
    frames).
  - Both flow controllers.
  - Upstream MAX_CONCURRENT_STREAMS waiting (`StreamBufferingEncoder`).
  - Push rejection once ENABLE_PUSH 0 (`DefaultHttp2Connection:812`).
  - Required pseudo-header validation, CONNECT aware (`DefaultHttp2ConnectionDecoder`
    `validateRequiredPseudoHeaders`).
- **Plug in (application code in the listener or around the handler):**
  - Per-stream routing and leg choice, leg retirement (channel inactive), the 081 error
    scope (`Http2Connection.Listener#onStreamClosed`, promise failures; GOAWAY
    last-stream-id to REFUSED_STREAM via `onGoAwayRead`).
  - "One stream until SETTINGS".
  - The budget: withhold `consumeBytes`; queued bytes = unconsumed bytes plus the
    remote-controller backlog.
  - `refuseWaiting` and 091 (`writeRstStream` on a buffered stream removes it).
  - Stall / idle / keep-alive (our timers read netty state: active streams, windows,
    `isWritable`).
  - Header re-encoding and rewrite-host (edit `Http2Headers`), metrics, access log.
- **Changes, or netty cannot give:**
  - Stream windows and SETTINGS are per hop, not end-to-end: the upstream sees the relay's
    SETTINGS and window grants, not the client's; padding is not relayed; the relay holds
    up to its announced window per stream per hop. The memory bound derivations
    (`MAX_QUEUED_BYTES`, `UNBUDGETED_BYTES`, `RESPONSE_QUEUE_BYTES`,
    `ConnectionLimits.connectionBytes`) must be redone.
  - Netty closes right after an error GOAWAY (`Http2ConnectionHandler#onConnectionError`),
    with no half-close and grace.
  - Netty acks PING as it reads.
  - Its error choices differ from the decisions in 088/089:
    - a reused stream id or a frame on a forgotten stream is a stream error
      `STREAM_CLOSED`, not a connection error (`DefaultHttp2ConnectionDecoder:712-730`,
      "the lesser of the two errors");
    - HEADERS depending on itself is a connection error (`DefaultHttp2FrameReader:437-446`);
    - frames on a stream the relay reset are ignored only while netty still holds it;
      netty forgets it once the reset is written, and then answers each later frame with
      RST_STREAM `STREAM_CLOSED` (`Http2ConnectionHandler#onStreamError`).
  - WINDOW_UPDATE 0 on an unknown stream is a stream error.
  - Netty's default header list limit is 8KiB; the relay needs 64KiB set.
- **Tests that encode what netty does not give** (rewrite or drop, about 60 to 70 of 173):
  - Settings replay or end-to-end windows:
    - `H2DemuxFramingE2ETest`: clientSettingsReach..., requestHeaderBlocks...,
      responseHeaderBlocks...
    - `H2DemuxClientFrameValidationE2ETest`: initialWindowChange..., streamWindowPast...,
      streamClosedBothWays...
    - `H2DemuxLegWriterE2ETest#clientWindowUpdatesCoalesceWhileQueued`
    - `H2DemuxClientWindowE2ETest`: the four `awaitWaiting` tests, paddedFrames...
    - `H2DemuxStallTimeoutE2ETest#anUpstreamSlowerThanTheStallTimeoutIsNotCut[0]`
    - `H2DemuxUpstreamErrorE2ETest#aConnectionErrorStillDeliversTheResponsesTheUpstreamCompleted`
    - `HttpPostE2ETest#h2PriorKnowledgePostCarriesTheDataFrames`
  - Raw stubs that never send SETTINGS first: the `H2DemuxUpstreamFlowControlE2ETest`
    offence tests, the `H2DemuxFramingE2ETest` recording stubs.
  - Exact connection-window grants (4 tests).
  - Error codes as above (about 10 validation cases, 5 tests).
  - PING withheld or ordered (`trailersPastTheAllowance...`, `upstreamPingIsAckedAheadOfQueuedData`).
  - GOAWAY grace (`upstreamIgnoringTheGoAwayIsClosedAfterTheGrace`).
  - `WriteSeam` race tests: `H2DemuxResetRaceE2ETest` (7) and 2 others. Those races
    disappear under one lock; replace them with tests of the new boundary.
  - Budget and stall tests that use the derived constants (re-derive, keep the scenarios).
  - Stream ids can be kept (spike), so the about 100 wire-id tests need no change for ids.

### Native image

netty-transport and codec-http2 ship native-image metadata (`--initialize-at-build-time=io.netty`
with run-time exceptions for `Http2ConnectionHandler` and others; generated handler
reflect-config). They are already in the image via grpc-netty. Our own handlers subclass
`ChannelHandler`. `ChannelHandlerMask` reflects on their public methods
(`getMethod` for `@Skip`), so they need `queryAllPublicMethods` entries in
`reflect-config.json`, or a `MissingReflectionRegistrationError` is possible. Not built in
the spike; one `-Pnative` build and a smoke run are part of step 1.

### Alternatives

- **Jetty's low-level HTTP/2** (`jetty-http2-common`'s `Parser` / `Generator`,
  `HTTP2Session`). Jetty also needs its own I/O abstraction (`EndPoint`) and adds a second
  network stack beside netty, which is already on the classpath for gRPC. It brings no
  advantage over netty for this design. Not evaluated further.
- **Keep our state machine behind netty's `DefaultHttp2FrameReader`** (partial). It needs
  only `ctx.alloc()` and passes `ctx` through (`DefaultHttp2FrameReader:462-566`), so it
  can run on the current parse thread with a dummy context. It covers 092 and the PRIORITY
  half of 089 and padding and length checks. It does not cover 073, 088, 090, 091 or 093.
  If the migration follows, it is throwaway work.

### Migration plan (keeps `sluice-it` green throughout)

1. Production `Hop` / `Loop` in `sluice-server`: ByteBuf confinement, writability,
   ticker, exception sink. Add a CRC stress test (many uploads and downloads over two legs),
   the leak detector on in tests, and the native-image entries plus one `-Pnative` smoke
   build.
2. `sluice.h2demux.engine=legacy|netty` (default legacy). `DataProxyServer` constructs
   either relay; the netty one feeds the seed head first and keeps the first route
   resolved once.
3. Parameterize the h2 E2E classes by engine. Each step below makes its subset pass on
   netty while legacy stays green:
   - routing, rewrite-host, no-route / forbidden pages, TLS h2;
   - the error scope (081): 502 / INTERNAL_ERROR / REFUSED_STREAM, upstream GOAWAY;
   - concurrency waiting (067) and the 091 deadline;
   - flow-control isolation and bounds;
   - the budget and `refuseWaiting`;
   - stall / idle / keep-alive / probe;
   - leg limits and slots, metrics and access logs.
4. Rewrite the per-hop-incompatible tests listed above against the netty semantics. Record
   each intentional behaviour change in README.
5. Switch the default to netty and soak in the deployment (CI, then the auto-deploy).
6. Remove the legacy relay, its constants and seams, and close or cancel the subsumed
   todos.

Size: large, about 8 to 12 todo-sized steps, several `High`. The bridge alone is about 560
lines in the spike without budget, timeouts or metrics.

### Held h2 todos

| Todo | With the migration |
|---|---|
| 066 retired legs kept | changed: a leg is removed when its channel goes inactive (spike does it); keep the test |
| 073 batch accounting | subsumed: no `PendingPayload` accounting; queued bytes are netty's unconsumed bytes |
| 075 stream to a retiring leg | changed: routing and leg state sit under one lock, and a write to a closed or GOAWAY'd leg fails its promise, giving REFUSED_STREAM; the test is still useful |
| 078 gone-client release test | unaffected (tunnel / `StreamRelay` behaviour) |
| 080 server push | subsumed: ENABLE_PUSH 0 on legs; netty rejects PUSH_PROMISE and the client's even-stream RST / WINDOW_UPDATE |
| 085 CONNECT head verbatim | unaffected in `ConnectionHeadParser` / `DataProxyServer`; the demux side (CONNECT stream routing) follows netty's CONNECT-aware validation |
| 088 closed-stream frames | mostly subsumed, with netty's codes (STREAM_CLOSED stream errors, not connection errors); the user accepts or overrides the decision |
| 089 self-dependency | subsumed for PRIORITY (stream error); HEADERS is a connection error in netty, not the stream error the todo asks |
| 090 coalesced WINDOW_UPDATE cap | subsumed: no relayed window updates |
| 091 waiting-stream deadline | changed: a timer resets the buffered stream in `StreamBufferingEncoder`, then REFUSED_STREAM |
| 092 upstream frame validation | subsumed (spike: SETTINGS on a stream gives GOAWAY PROTOCOL_ERROR to the upstream and 502 to the client, the connection survives) |
| 093 relay answers bypass the client window | subsumed: error pages go through netty's remote flow controller; the 502-body decision stays |

### Risks

- The I/O boundary: an `EmbeddedChannel` outside netty's threading, ByteBuf confinement,
  manual writability and timers. The spike's corruption shows how quietly it fails.
  Mitigation: the step 1 stress test and leak detection, or option 2.
- The semantics change to per-hop windows: memory bounds and stall-timeout reasoning are
  re-derived, and some client-visible behaviour changes (settings and windows no longer
  relayed).
- Netty's protocol choices differ from earlier todo decisions (088 / 089, GOAWAY grace).
  Netty upgrades can change behaviour (validation was added within 4.2.x), so pin it and
  read the release notes.
- Effort and a long dual-engine period. Performance is unmeasured.

### Open decision for the user

1. Migrate, partial (frame reader only), or stay.
2. If migrating: accept per-hop semantics (the upstream no longer sees the client's
   SETTINGS or stream window updates; padding dropped), and netty's error codes where they
   differ from 088 / 089 and the GOAWAY grace.
3. The I/O model: `EmbeddedChannel` plus lock first (recommended), or a custom `Channel` on
   a netty event loop from the start.
4. Which held todos to cancel now: subsumed 073, 080, 088, 089, 090, 092, 093. Hold or
   re-target 066, 075, 091 to the new engine. Unaffected: 078, 085 (head parser part).

## Decision (2026-10-12, the user)

1. Migrate, in steps behind an engine switch: todos 098 to 107.
2. Per-hop semantics accepted: the upstream no longer sees the client's SETTINGS or stream
   WINDOW_UPDATEs, padding is dropped, netty's error codes where they differ from 088 /
   089, no half-close and grace after an error GOAWAY.
3. I/O model: `EmbeddedChannel` plus one lock per relay.
4. Subsumed todos cancelled now: 073, 080, 088, 089, 090, 092, 093. 066, 075 and 091 are
   retargeted to the netty engine (104, 101); 078 and 085 proceed as they are.
