Difficulty: High (parent; work happens in the children below)

# Head-of-line blocking across virtual connections of one tunnel stream

One stalled virtual connection stalls every other connection multiplexed on the same
gRPC stream, indefinitely, and the server -> client direction buffers without limit. Any
anonymous public peer can trigger both. The root cause is that the protocol has no
per-connection flow control: backpressure is expressed only by blocking the shared
stream.

Repro: `.todo/artefacts/041-tunnel-head-of-line-blocking/HeadOfLineBlockingE2ETest.java`.
Copy it into `sluice-it/src/test/java/am/ik/sluice/it/`; all 3 tests fail on aece0aa.
(`HOLDUMP` lines print the threads parked in `acceptData` / `enqueue`; drop them when
promoting the test.)

## Findings

F1. **Receive path blocks on one connection's queue** (confirmed, both directions).
`TunnelSession#handle` (server) / `NodeConnection#handle` (client) call
`VirtualConnection#acceptData` synchronously from the gRPC `onNext`. It loops on
`offer(100ms)` while that connection's 128 x 64KiB queue is full. gRPC serializes `onNext`
per call, so no frame of any other connection (DATA, CONNECT, CLOSE, ERROR) is delivered
until the slow consumer drains. There is no timeout: a peer holding its TCP window at zero
holds the stall forever (keepalive keeps the stream up). `remoteClosed` / `remoteFailed`
(`enqueueAfterDrained`) block the same way.
- Server side: a public peer requests a large response and never reads
  (`slowPublicReaderDoesNotStallOtherConnections`: echo on another route gets 0 bytes in
  5s; the thread dump shows `grpc-default-executor` parked in `acceptData` under
  `TunnelService$1.onNext`).
- Client side: the upstream does not read a large request body
  (`slowUpstreamReaderDoesNotStallOtherConnections`; parked in `acceptData` under
  `NodeConnection$1.onNext`).

F2. **Server -> client has no backpressure** (confirmed). `SessionSender#awaitReady`
only recognizes `ClientCallStreamObserver`. The server's observer is a
`ServerCallStreamObserver`, so the server never waits and `onNext` buffers in Netty without
limit (`serverToClientDirectionIsBackpressured`: 8952MiB accepted in 10s, Netty direct
memory +8490MiB). This is an OOM vector and gives unbounded latency to every frame queued
behind. README "the gRPC send path honors `isReady()`" is false on the server.

F3. **`TunnelSession#close` can hang forever** (thread dump plus reading). When the
stream dies, the sender worker exits on the `onNext` exception and the 256-entry queue
stays full. `close()` -> `VirtualConnection#close` -> `sendClose` -> `enqueue` then loops
while `running`, which only `sender.close()` (called last) clears. The gRPC executor
thread leaks and the remaining connections are never closed.

F4. **Send queue is one FIFO for all connections** (by reading, not measured).
`SessionSender` is 256 x 64KiB. An interactive connection's frame waits behind up to 16MiB
of a bulk transfer, and so do CLOSE / ERROR / control frames.

F5. **wasmlet receive loop has the same block** (by reading, not tested). `main.rs`
`stream()` calls `sender.send(Ctrl::Payload(..)).await` on the per-connection channel
(`SERVE_BUFFER = 32`); CLOSE / ERROR are delivered the same way; the no-route ERROR reply
and the teardown `Abort` loop (`attempt()`) await too. With HTTP/1.1, hyper reads the body
only when the guest polls it, so a guest that is slow to read a large POST stalls the
stream (usually bounded by the 10s budget).

F6. **Java client leaks virtual connections on stream end** (by reading, not tested).
`NodeConnection#runSession` never closes `connections` when the stream ends. Their
relay-in threads wait on `take()` forever and the upstream sockets stay open.

F7. **h2 demux couples its legs on the outbound side** (by reading). `Http2DemuxRelay`
`pumpOutbound` is one parse loop that writes every leg inline (`relayFrame`) and grants the
client's connection window (`grantClient`) only after the write. Once writes can block per
connection (credit wait, 046; per-connection send cap, 044), one stalled leg stops the parse loop: DATA for other
routes stops, the connection window is no longer granted, and the whole client h2
connection stalls. Today this is masked by F2.

Not issues: the accept loops dispatch per connection. h1 rerouting waits for the previous
response by design (h1 is sequential). The h2 demux passes stream-level WINDOW_UPDATE
through, so a paused h2 stream is held back by the upstream. TCP-level HOL of the single
transport connection is inherent. Out of scope: the wasmlet guest worker starvation found
in the same investigation is todo 042.

## Children (do in this order)

| todo | finding | difficulty | depends on |
|---|---|---|---|
| 043-tunnel-sender-backpressure-and-teardown | F2, F3, F6 (+ non-blocking markers) | Medium | - |
| 049-tunnel-teardown-under-full-queues | 043 review follow-up | Medium | 043 |
| 044-session-sender-fair-scheduling | F4 | Medium | 043 |
| 050-session-sender-scheduler-scaling | 044 review follow-up | Medium | 044; before or with 046 |
| 045-h2-demux-per-leg-writers | F7 | Medium | 043, 044; before or with 046 |
| 051-h2-demux-leg-queue-follow-up | 045 review follow-up | High | 045; before or with 046 |
| 052-sender-cap-wait-ignores-connection-close | 051 review follow-up (teardown hang) | Medium | before or with 046 |
| 046-per-connection-flow-control-java | F1 (protocol + Java) | High | 043, 044, 045 |
| 047-per-connection-flow-control-wasmlet | F5 | High | 046 |
| 056-wasmlet-outbound-fair-scheduling | F4 on the wasmlet (047 review follow-up); done | Medium | 047 |
| 048-data-plane-connection-limits | memory bound (see "Residual after the children"); done | Medium | - (defaults derive from 044-046) |
| 057-h2-demux-global-queue-budget | h2 memory outside the 048 bound; done | High | 048 |
| 058-receive-buffer-per-frame-overhead | per-frame overhead outside the 048 bound; done | Medium | 048 |
| 061-slot-released-before-queued-frames-left | Java send queues of ended connections outside the 048 bound (056 review) | Medium | 048 |
| 054-remove-legacy-flow-control | legacy removal (see "Follow-up"; not a child, the parent does not wait for it) | Medium | 047; no pre-046/047 peer deployed |

Each child is one commit series with its own tests and docs. This parent is done when all
children are, and the legacy-removal todo below is filed.

## Protocol contract (shared by 046 and 047)

Credit-based per-connection flow control, following HTTP/2 stream windows, yamux and SSH
channel windows. Each side announces its **receive window** per connection. The peer may
have at most that many DATA payload bytes in flight on that connection beyond what the
receiver has granted back.

```proto
message Frame { oneof body { ... WindowUpdate window_update = 10; } }

// Either direction: the sender consumed `increment` more payload bytes of `conn_id`;
// the receiver may send that many more.
message WindowUpdate {
  int64 conn_id = 1;
  uint32 increment = 2;
}

message Advertise {
  repeated Upstream upstreams = 1;
  // Client's receive window per connection, in bytes; 0 = the client has no
  // per-connection flow control (legacy).
  uint32 connection_window = 2;
}

message Connect {
  int64 conn_id = 1;
  string address = 2;
  // Server's receive window for this connection, in bytes; 0 = legacy mode
  // for this connection (no WindowUpdate either way).
  uint32 connection_window = 3;
}
```

- **Per-connection negotiation**: the mode is decided per connection, not per stream. A
  CONNECT can be queued before the AdvertiseAck (`router.register` runs before
  `sendAdvertiseAck`), so a per-stream switch would race.
  - **Server**: uses the latest `Advertise.connection_window` of the session. If > 0, the
    connection is windowed: CONNECT carries the server's receive window, and the server's
    send credit starts at the client's window. Otherwise CONNECT carries 0.
  - **Client**: a connection is windowed iff `Connect.connection_window > 0`. Its send
    credit starts at that value; its receive window is the one it advertised.
- **Legacy**: an old peer sends 0, or omits the field; protobuf ignores unknown fields
  and oneof cases. The legacy mode keeps today's behavior, including the residual HOL.
- **Window**: 1MiB on both sides, a shared constant. Because the value travels in the
  protocol, a later tuning needs no protocol change. Rationale: grpc-java's default HTTP/2
  stream window is 1MiB, so one connection can still fill the stream as today.
- **Bounds**:
  - Memory per side = window x active connections; capped by the connection limits
    of 048 (derived default on the server since 057: 2.16MiB per virtual connection
    plus an h2 relay's unbudgeted queue, and the shared h2 queue budget, in half the
    max heap). Since 058 the receive buffer coalesces DATA payload into 16KiB
    segments, so tiny frames add no per-frame overhead: the window plus 37KiB.
  - Throughput per connection <= window / RTT (1MiB at 50ms ~ 20MiB/s).
- **Grant rule**: the receiver sends WindowUpdate once its consumed-but-ungranted bytes
  reach half the window. Consumed means handed to the local consumer, not merely
  received.
- **Violation**: payload beyond the granted credit fails only that connection (ERROR
  "flow control window exceeded", close locally). The stream stays up.
- **The receive path never blocks** on a windowed connection. The sender waits for credit
  on the connection's own producer, never on a shared thread.
- **Local close while the peer may still send** (added by 046): a windowed connection
  closed before the peer's CLOSE or ERROR arrived sends ERROR ("connection closed")
  instead of CLOSE, like a TCP RST. Its later payload would be dropped and never granted,
  so a peer writer waiting for credit would wait forever. A writer waiting for credit
  gives up on the peer's ERROR. Grants are only sent for windowed connections; a
  WindowUpdate for an unknown or legacy connection is ignored.

## Residual after the children

What 043-048 still leave open, by design or elsewhere. Do not report the parent as
"HOL solved" without these caveats.
- Legacy peers keep the stream-wide stall on their connections until legacy removal
  (follow-up below). This includes the wasmlet behind a pre-046 server (047 confirmed
  the stall in a test: the receive loop waits on a connection whose guest stopped
  reading).
- wasmlet: a CPU-bound guest stalls every connection by holding a tokio worker (042, not
  a tunnel mechanism).
- Inside one public connection, h1 is sequential and h2 shares one TCP socket to the
  peer. That is protocol semantics, not tunnel HOL.
- TCP-level HOL of the single transport connection: only a QUIC transport removes it.
  Out of scope (no todo).
- Total memory (048, done): the connection limits bound it; since 057 (done) an h2
  connection's further legs take slots and its demux queue beyond a per-connection
  allowance is charged to a budget shared by the node; since 058 (done) tiny DATA
  frames add no per-frame overhead to the receive buffers. Not within the bound:
  wasmlet guest instances (059), keep-alive connections idle between requests hold
  their slots (060), and the Java
  send queues of connections whose slot was already released (061). (ADVERTISE
  off the receive path and the credit-lock pitfall are done in 046.)

## Follow-up

Filed as `.todo/054-remove-legacy-flow-control.md`: remove legacy mode (the blocking
`acceptData`, `connection_window == 0` paths in Java and wasmlet) once no pre-046 client
or server is deployed.
