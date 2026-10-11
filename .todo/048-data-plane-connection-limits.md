Difficulty: Medium

# Connection limits: bound the per-connection buffers (041 child 6/6)

Parent: `.todo/041-tunnel-head-of-line-blocking.md`. After 043-046, buffering is bounded
per connection (window, sender cap, h2 demux queue bound) but not in total: the total is
per-connection worst case x connections, and the number of connections is unlimited. An
anonymous peer can open many connections that never read and exhaust memory. Not HOL
itself; it is what makes the parent's memory bound real. 046 is done (receive window
1MiB per connection and direction). The defaults depend on the per-connection worst
case, so recompute them if 044-046 change their caps.

## Today

- `DataProxyServer#acceptLoop` and `TcpPortGateway#acceptLoop` accept without limit.
  Each connection gets a virtual thread, buffers, and a virtual connection on the tunnel.
- `DataProxyServer#transport` (PROXY header, TLS handshake, head parse) has no timeout:
  a peer that connects and sends nothing, or drips one byte at a time, holds the
  connection forever (slowloris).
- The client (`NodeConnection`, `CONNECT` case) dials every CONNECT; the server can make
  a client open unlimited upstream sockets.

## Design

Properties: keys under one prefix, e.g. `sluice.limits.*` on the server. Check the names
against todo 029 (k8s service-link env collisions), and keep `application.properties`
keys in alphabetical order (CLAUDE.md).

1. **Global concurrent limit** (`max-connections`), shared by the data port and every tcp
   route port: one counter, taken at accept and released exactly once when the
   connection ends (relay finished, rejected, failed).
   - At the limit: close the accepted socket immediately, without reading.
   - Count `sluice.data.connections.rejected` with tag `reason=limit`, and log through
     the access log rate limit (`AccessLog.RateLimit`).
   - The existing `activeConnections` counters stay for display; the limit gets its own
     counter so the release-once rule is easy to test.
2. **Per-source-address limit** (`max-connections-per-address`). The source is the
   PROXY protocol source when `proxy-protocol` is on (known only after the header is
   parsed, so the check runs there), the socket peer otherwise. Same rejection as 1.
3. **Head deadline** (`head-timeout`, default 10s): one deadline for the whole transport
   phase (PROXY header + TLS handshake + head), not per read. A peer dripping one byte
   every 9s must still be cut at 10s. Implementation hint: a scheduled close of the
   socket, cancelled when the head is complete, since `setSoTimeout` alone is per read.
   Reset to no timeout before the relay starts: established relays are long-lived by
   design (WebSocket, keep-alive).
4. **Client limit** (`sluice.client.max-connections`): a CONNECT beyond the limit is
   answered with ERROR "connection limit reached" without dialing or registering. The
   server side sees `remoteFailed` and closes the public connection.

Defaults: derive from the worst-case memory per connection after 044-046 (receive window
1MiB + sender cap 1MiB + socket buffers, per virtual connection). An h2 client connection
opens one virtual connection per route it reaches (a leg), and its demux queue adds at
most `Http2DemuxRelay.MAX_QUEUED_BYTES` per client connection (051: 100 streams x
(128KiB payload budget + 2 x 64KiB header blocks + framing), about 25MiB; the payload
part alone is 12.5MiB). Starting point:
`max-connections` 2048, `max-connections-per-address` 256, client `max-connections` 2048.
Check them against the memory limits of the deployment in `k8s.md` and write the formula
and the resulting worst case in the README.

Accepted residual: a peer that reads slowly but steadily within the per-address limit
keeps its slots. That is legitimate use; only the per-address limit bounds it.

## Tests (`sluice-it`; watch each fail first)

- Global: with `max-connections=2`, two idle connections are held open. A third is
  closed within 1s (read returns -1 or reset) and the rejected counter is 1. After one of
  the two closes, a new connection relays normally (pins slot release).
- Release on every path: no-route, forbidden, relay end, failure. After each, the limit
  counter returns to its previous value. Assert the counter itself, not only that later
  connections work.
- tcp route ports count against the same global limit.
- Per-address: macOS cannot bind `127.0.0.2` (see the project memory), so use
  `proxy-protocol=true` and PROXY headers with two different sources. Source A is
  rejected at its limit while source B is still accepted.
- Head deadline: with `head-timeout=2s`, a connection sending nothing is closed within
  3s; one dripping a byte every 500ms is closed within 3s too (pins whole-phase deadline
  vs per-read timeout); a WebSocket relay idle for 5s after its head stays open.
- Client limit: client `max-connections=1`; while one relayed connection is open, a
  second public connection fails with the client's ERROR, and the first is unaffected.
