Difficulty: High

# Per-connection credit flow control: protocol + Java (041 child 4/5)

Parent: `.todo/041-tunnel-head-of-line-blocking.md` (Finding F1, section "Protocol
contract": implement exactly that contract; 047 implements the same in Rust). Depends on
043, 044 and 045.

## Problem

`TunnelSession#handle` (server) and `NodeConnection#handle` (client) call
`VirtualConnection#acceptData` from the gRPC `onNext`. It blocks while that connection's
queue is full, and gRPC serializes `onNext` per call. So one slow consumer stalls every
connection on the stream, indefinitely. Repro:
`slowPublicReaderDoesNotStallOtherConnections` and
`slowUpstreamReaderDoesNotStallOtherConnections` in
`.todo/artefacts/041-tunnel-head-of-line-blocking/HeadOfLineBlockingE2ETest.java`.

## Design

### Proto

Add `WindowUpdate`, `Advertise.connection_window` and `Connect.connection_window` as
specified in the parent's contract. Check that old peers ignore them: the `default ->`
branches of both `handle` switches.

### Negotiation

- Server: `TunnelSession` stores the latest `Advertise.connection_window`.
  `TunnelSession#open` creates a windowed `VirtualConnection` when it is > 0:
  - send credit = the client's window;
  - receive window = the server's own (1MiB constant);
  - the CONNECT carries the receive window.
  Otherwise it creates a legacy connection and sends CONNECT with 0.
- Client: `NodeConnection#runSession` advertises 1MiB. On CONNECT with
  `connection_window > 0`, the connection is windowed: send credit = that value, receive
  window = 1MiB. Otherwise it is legacy.
- Put the constant in `sluice-proto` (e.g. `VirtualConnection.DEFAULT_WINDOW`).

### `VirtualConnection`

- **Construction**: `VirtualConnection(id, writer)` stays legacy. Add a windowed factory
  or constructor that takes the receive window and the initial send credit.
- **Receive, windowed**:
  - `acceptData` never blocks: `outstanding += len` (received minus granted).
  - If `outstanding > receiveWindow`, fail the connection: send ERROR "flow control
    window exceeded", deliver `Failure` to the reader, close.
- **Receive, legacy**: unchanged from 043 (the bounded DATA counter blocks).
- **Grant**: `Source#read` adds the bytes it hands out to `consumed`. At
  `consumed >= receiveWindow / 2` it calls `frameWriter.sendWindowUpdate(id, consumed)`
  and resets. This runs on the reader thread, never on the gRPC callback.
- **Send, windowed**: `sendCredit` is guarded by the existing lock plus a condition.
  - `grant(int)`, called from the receive path, adds and signals and never blocks.
  - `Sink#write` loops: wait while credit is 0, then send
    `min(credit, remaining, MAX_CHUNK)` and subtract it.
  - The wait wakes on grant, `close()`, remote failure, and sender death (043); the last
    three throw `IOException`.
  - No timeout: the wait blocks only this connection's producer.
- **Send, legacy**: unchanged.

### Writers and callers

- `FrameWriter#sendWindowUpdate(long, int)`; `SessionSender` puts it in the control queue
  (044). Update the fakes in `StreamRelayTest` / `VirtualConnectionTest`.
- `StreamRelay#doRelayToRemote` currently calls `frameWriter.sendData` / `sendClose`
  directly, bypassing the connection. Change it to `connection.sink().write` /
  `sink().close()` so credit applies; this also removes a possible double CLOSE (the
  direct `sendClose` does not set `writeClosed`). Keep `sendError` for the failure path.
- `TunnelSession#handle` and `NodeConnection#handle`: add
  `case WINDOW_UPDATE -> connection.grant(...)`. Look the connection up as for DATA and
  ignore unknown ids.
- Metric for test pinning and operations: counter `sluice.tunnel.window.updates` (sent),
  tagged `direction`, on both sides. The server uses its `MeterRegistry`; the client uses
  the one given to `TunnelClient`.

## Tests (watch each fail first)

- `VirtualConnectionTest`, windowed:
  - `acceptData` returns within 100ms with exactly `window` bytes buffered and no reader;
  - one more byte -> ERROR recorded by the fake writer, and the reader gets `IOException`;
  - reading half the window emits exactly one WindowUpdate with that increment;
  - `Sink#write` at 0 credit blocks (assert the thread state, not just "not done"),
    resumes on `grant`, and throws on `close()`;
  - a write larger than the credit is split at the credit.
- `sluice-it`:
  - promote `slowPublicReaderDoesNotStallOtherConnections` and
    `slowUpstreamReaderDoesNotStallOtherConnections` from the artefact. Pin windowed mode:
    `sluice.tunnel.window.updates` > 0 on the client registry after the echo;
  - legacy client against the new server: a raw `TunnelGrpc` stub client in the test
    advertises without `connection_window` and answers CONNECT by echoing DATA back as
    DATA. A data-plane connection to it echoes;
  - new client against a legacy server: a minimal `TunnelImplBase` fake in the test sends
    CONNECT without a window and relays DATA. The client relays to its upstream, and the
    window counter stays 0 (pins legacy mode);
  - throughput sanity: a 256MiB transfer over one windowed connection completes, and the
    window counter shows updates (guards against stuck credit).
- Everything in `sluice-it` stays green (WebSocket / keep-alive half-close relies on
  CLOSE ordering).

## Docs

- README overview (line 5), the sequence-diagram note (`bounded queue 128x64K`) and the
  "flow control" bullet: state the per-connection window (1MiB), the memory bound
  (window x connections per side), the throughput bound (window / RTT per connection),
  and that legacy peers keep the old blocking behavior.
- Proto comments as in the contract.
- When this lands, file the legacy-removal todo (see parent).
