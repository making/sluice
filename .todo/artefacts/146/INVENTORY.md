# sluice-it inventory (2026-10-17, before the consolidation)

One line per class: what it proves. Time = class time in CI run 39006432190 (6 forks).
Stack: `ctx` = one `@SpringBootTest` server per class; `app` = servers started per test.

## h2 demux (am.ik.sluice.server.proxy)

| Class | Time | What it proves |
|---|---|---|
| H2DemuxBudgetE2ETest | 23s | 3MiB budget: stalled connections stop at the budget (windows withheld, no reset); another route keeps flowing; new stream / trailers past the allowance refused / reset; withheld window granted once others free budget; gone clients release |
| H2DemuxConnectionShareE2ETest | 29s | 12MiB budget: one connection past its share is withheld while another keeps its window; a held relay is not re-reviewed on others' releases; a client not reading stops its upstream at the share; header blocks admitted up to twice the share; waiting streams kept within the allowance |
| NettyH2DemuxBudgetE2ETest | 39s | 3MiB budget: held request/response stays within the stream window while the leg's other stream flows; small frames coalesced; responses past the budget stop at the withheld window; every exit (reset, leg end, client gone) releases |
| H2DemuxWaitingStreamBudgetE2ETest | 11s | 256KiB budget: waiting streams exhausting the budget are refused past the allowance, the open stream completes |
| H2DemuxUpstreamHeaderBlockE2ETest | 26s | 256KiB budget: interim/final/trailer blocks at a client not reading stay within the budget; interim heads past the list limit lose their stream; table-referencing heads admitted as far as they fit |
| H2DemuxConnectionMemoryE2ETest | 6s | connection-memory: h2 connections hold at most budget + reservations; past it new h2 connections get GOAWAY / ALPN falls back to http/1.1 / 503 |
| H2DemuxWindowCreditE2ETest | 8s | peers' windows charged to the budget: held + credit within budget + reservations; leg window charged and released with its upstream / client |
| H2DemuxWindowFloorE2ETest | 12s | 1B budget: larger window declined, transfers complete at RFC 9113's window |
| H2DemuxWindowRoundTripE2ETest | 1s | delayed hops: stream windows announced, connection windows grow past the client's |
| H2DemuxClientFrameValidationE2ETest | 75s | 85 cases: each client frame violation ends the connection with the right GOAWAY; stream errors reset one stream; well-formed counterparts keep it |
| H2DemuxClientGoneE2ETest | 13s | a client going away mid-request/response is no protocol error; upstream streams cancelled |
| H2DemuxClientWindowE2ETest | 14s | client connection/stream windows never exceeded; window-update validation; waiting data on reset/close |
| H2DemuxConnectE2ETest | 8s | a connection opening with CONNECT is demuxed; idle / stall timeouts; per-stream routing and access checks; HPACK errors |
| H2DemuxLegOpenFailureE2ETest | 2s | a leg that cannot open refuses its stream and returns its slot |
| H2DemuxLegWriterE2ETest | 22s | upstream PING acked ahead of queued data; PING flood pauses the reader; finished streams forgotten; one queue gauge per route |
| H2DemuxLongLivedConnectionE2ETest | 42s | a stream in flight across the 30s hop-done timeout completes; the connection lives on |
| H2DemuxRoutingE2ETest | 12s | first stream takes the connection's route; ids kept per leg; header list limit; leg of a gone upstream takes no new stream; per-route bytes; trailers; interim heads |
| H2DemuxStallTimeoutE2ETest | 83s | h2 stall timeout: 7 cut cases, 10 not-cut cases (slow upstream, resting exchange, CONNECT, 100-continue, held uploads, slow grants/uploads, one write piece a second) |
| H2DemuxUpstreamErrorE2ETest | 13s | upstream resets / GOAWAY / connection errors end only their stream or leg |
| H2DemuxUpstreamFlowControlE2ETest | 18s | upstream windows never exceeded; window validation; half-closed clients; probes |
| H2DemuxUpstreamStreamLimitE2ETest | 15s | upstream MAX_CONCURRENT_STREAMS: streams wait, reset, are freed |
| NettyH2DemuxRelayE2ETest | 38s | 431 / 502 pages held to the stream window; wait deadline refusals; codec resets; malformed upstream frames end only their leg; close grace; no buffer leak |
| KeepAliveIdleTimeoutE2ETest | 73s | h1/h2/TLS keep-alive idle timeout and head timeout; activity keeps a connection open |
| MalformedRequestE2ETest | 35s | malformed h1 heads/bodies answered 400 and closed, unrelayed; UTF-8 values relayed |
| ProbeResponseRaceE2ETest | 13s | a response arriving while a half-close probe is held follows the probe |
| StallTimeoutE2ETest | 41s | h1 stall timeout: cut cases and not-cut cases |
| VerbatimIdleTimeoutE2ETest | 63s | per-route verbatim idle timeout on tcp / passthrough / upgraded / hostless connections |
| VerbatimIdleTimeoutServerDefaultE2ETest | 18s | server default verbatim idle timeout and a route's override |
| DrainAdmissionE2ETest (server.tunnel) | 1s | a stream admitted after the drain began is failed at once |

## Tunnel and client

| Class | Time | What it proves |
|---|---|---|
| ClientQueuedFramesHoldSlotsE2ETest | 2s | client: ended connections hold their slots while their frames are queued |
| ClientTunnelStallWatchdogE2ETest | 13s | client stall watchdog cancels a stalled stream, frees slots, reconnects; slow progress not cut |
| QueuedFramesHoldSlotsE2ETest | 11s | server: ended relays (h1/h2/passthrough) hold slots while frames are queued |
| TunnelStallWatchdogE2ETest | 24s | server stall watchdog fails a stalled session; slow progress / non-reading upstream not cut |
| TunnelAdvertiseDeadlineE2ETest | 3s | a stream that never advertises is failed at the bound |
| TunnelBackpressureE2ETest | 6s | both directions backpressured; dead streams release connections and threads |
| TunnelBudgetE2ETest | 6s | 2MiB tunnel budget: buffers within allowances and budget; every exit releases; one connection grows to the full window |
| TunnelPendingLimitE2ETest | 4s | pending-stream cap: refusals, places freed by advertise / deadline / end |
| TunnelPendingPerAddressLimitE2ETest | 2s | per-address pending cap; the Java client retries and connects |
| TunnelReconnectTests | 11s | client reconnects after a server restart; wrong token keeps retrying |
| TunnelSessionLimitE2ETest | 3s | session cap: refusals, replacement, bounded logging |
| TunnelTransportWindowE2ETest | 17s | the gRPC transport window does not bound the tunnel's connections |
| TunnelEndToEndTest | 1s | basic h1 round trip; per-route byte counters; keep-alive reuse |
| WebSocketTunnelE2ETest | 1s | WebSocket handshake and echo through the tunnel |
| WindowedThroughputE2ETest | 8s | 256MiB each way over one windowed connection completes; grants per window bounded |
| WindowlessClientRejectionE2ETest | 1s | a client without a window is rejected |
| WindowlessServerRejectionE2ETest | 0s | a windowless CONNECT is answered with an error |
| HeadOfLineBlockingE2ETest | 6s | a slow reader on one connection does not stall others |
| HalfCloseProbeE2ETest | 27s | half-close probe behaviour (interval 500ms) |
| HalfCloseProbeDisabledE2ETest | 3s | probe interval 0 disables it |
| HeadDeadlineE2ETest | 15s | head timeout: silent / dripping / TLS-stalled connections closed |
| ClientActuatorE2ETest | 2s | client health and prometheus follow the tunnel |
| ClientConnectionLimitE2ETest | 2s | client max-connections refuses without dialing |
| ClientStreamEndCleanupE2ETest | 2s | upstream sees EOF when the server vanishes |
| ClientStreamFailureBackoffE2ETest | 8s | stream failures logged; backoff grows; jitter halves waits |
| ClientTeardownFullQueueE2ETest | 1s | client teardown completes with a full sender queue |
| ServerShutdownE2ETest | 2s | context close does not wait out the gRPC grace |
| GrpcKeepAliveTest | 8s | client keepalive pings are permitted |

## Data plane, routing, limits

| Class | Time | What it proves |
|---|---|---|
| AccessControlDenyE2ETest / AccessControlE2ETest / AccessControlForwardedE2ETest | 3s | deny list, per-route allow list, trusted-proxy forwarded judgement |
| AccessLogE2ETest | 1s | access log lines per connection and request |
| ConnectionLimitE2ETest / ConnectionLimitOverflowE2ETest / ConnectionLimitOverflowProxyProtocolE2ETest / PerAddressConnectionLimitE2ETest / ConnectionReleaseE2ETest | 33s | data-plane limits, overflow 503s, per-address limit, slot release on every exit |
| ProxyProtocolE2ETest | 1s | PROXY v1/v2 headers |
| ForceHttp1MisdirectedE2ETest | 5s | force-http1 routes answer coalesced h2 streams 421 |
| H2cDataPlaneE2ETest | 3s | h2c prior knowledge routed by authority; settings first; upgrade passes through |
| H2DemuxDrainHeldLegE2ETest | 3s | a drain waiting on a held leg ends at the idle timeout |
| H2DemuxFlowControlE2ETest | 7s | 100 compliant streams not torn down; concurrency limit; slow leg does not block others; refused-stream DATA released |
| H2DemuxFrameLimitsE2ETest | 3s | client/upstream frame size and header block limits |
| H2DemuxFramingE2ETest | 10s | header blocks contiguous; per-hop settings; priority/unknown frames dropped |
| H2DemuxLegLimitE2ETest | 2s | legs beyond the first take connection-limit slots |
| H2DemuxResetOrderE2ETest | 11s | nothing follows a reset on either hop |
| H2DemuxStallE2ETest | 10s | a stalled leg keeps others flowing; releases; flood past the window ends the connection |
| H2ReroutingE2ETest / KeepAliveReroutingE2ETest | 6s | per-stream / per-request rerouting, trailers, resets, no-route answers |
| HostPatternE2ETest / RewriteHostE2ETest / RoundRobinE2ETest / SameHostUpstreamsE2ETest / TcpRouteHostIsolationE2ETest / NoRouteE2ETest | 12s | host patterns, host rewriting, round robin, http+tcp on one host, isolation, no-route page |
| HttpPostE2ETest / TlsDataPlaneE2ETest | 8s | POST bodies h1/h2/TLS; TLS termination with ALPN |
| SniRoutingE2ETest / SniRoutingRedisE2ETest / TlsPassthroughRedisE2ETest / RedisE2ETest / TcpEchoE2ETest | 26s | SNI passthrough routing, tcp routes (Redis via Testcontainers), listen-port re-advertise |
| ClusterFanoutE2ETests / ClusterGrpcTlsE2ETests / ClusterGrpcMutualTlsE2ETests | 46s | cluster membership, failover, drain, gRPC TLS and mTLS |
| ConsoleAuthE2ETest / ConsoleE2ETest / ConsolePeerBuildE2ETest / GrafanaDashboardE2ETest | 35s | console auth (form; the nested OIDC class never ran), console pages, peer builds, dashboard metrics |
| ServerStartupLogE2ETest / TestPortsTest / H2TestClientTest | 1s | no introspection failures at startup; port claims; the test client's header-block handling |

## Changed in this task

- TunnelTransportWindowE2ETest: two round-trip-counting methods (48x4MiB each) -> one method asserting the announced transport windows from the frames (H2WindowTap); RoundTripProxy removed.
- TunnelBudgetE2ETest, H2DemuxWindowRoundTripE2ETest, H2DemuxWindowFloorE2ETest, StallTimeoutE2ETest, H2DemuxStallTimeoutE2ETest, ClientStreamFailureBackoffE2ETest: wall-clock / rate / round-trip bounds replaced with window state, pipe counters and logged backoff waits; the jitter method folded into the parameterized backoff test.
- H2DemuxBudgetE2ETest: the new-stream case holds charged credit, so the admitLocked credit argument is pinned.
- WebSocketTunnelE2ETest merged into TunnelEndToEndTest (one stack, one client with both upstreams).
- ClientTeardownFullQueueE2ETest merged into ClientStreamEndCleanupE2ETest (same server setup).
- ConsoleAuthE2ETest$Oidc (never run: surefire skips nested classes) moved to ConsoleOidcE2ETest.

## Left as is

- Same-budget h2 classes (H2DemuxBudgetE2ETest / NettyH2DemuxBudgetE2ETest at 3MiB, H2DemuxUpstreamHeaderBlockE2ETest / H2DemuxWaitingStreamBudgetE2ETest at 256KiB) could share a stack; each pair has 1000+ lines of its own helpers and before/after checks, not merged in the time box.
- The long poles are timeout tests that wait out 2s timeouts (H2DemuxStallTimeoutE2ETest 83s, H2DemuxClientFrameValidationE2ETest 75s, KeepAliveIdleTimeoutE2ETest 73s, VerbatimIdleTimeoutE2ETest 63s, H2DemuxLongLivedConnectionE2ETest 31s in one sleep past the 30s hop-done timeout).
- "Closed at the deadline" checks (isBetween(timeout, timeout + slack)) remain: the lower bound is the behaviour, the upper bound a hang guard of 1-4s slack.
- Tight "within N ms" bounds telling two deadlines apart (ConnectionLimitsTest 290ms, TunnelSessionReceivePathTest 300..1000ms, ...) need clock seams: todo 149.
