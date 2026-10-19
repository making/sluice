# Metrics

Every meter `GET /actuator/prometheus` exports besides the JVM ones. The server's meters
carry a `node` common tag. User-facing summary: README "Health / metrics".

## Tunnel (server and client)

| Metric | Description |
|---|---|
| `sluice_tunnel_bytes_total` | bytes relayed on the tunnel, by route; for an h2 connection, what each of its upstream connections writes and reads on the tunnel, HTTP/2 framing included. The same bytes make the console's session traffic and the access log's `bytesIn` / `bytesOut` |
| `sluice_tunnel_sessions` | server: connected tunnel clients (the health detail `clients`); client: its tunnel streams established, one per server node |
| `sluice_tunnel_window_updates_total` | per-connection credit grants, `direction` = `sent` / `received` |
| `sluice_tunnel_credit_waits_total` | times a connection had payload for its peer and waited for its credit |
| `sluice_tunnel_budget_bytes` / `sluice_tunnel_budget_used_bytes` | the tunnel budget and what the windows past 64KiB and the send queues past one frame take of it |
| `sluice_tunnel_buffered_bytes` | payload the peer sent that the relays have not read yet |
| `sluice_tunnel_stall_seconds` | how long a tunnel stream has held frames queued for its peer, not ready, with none taken; on the server the most stalled session's, 0 when none |
| `sluice_tunnel_stall_failures_total` | tunnel streams failed by the stall watchdog |

## Tunnel (server)

| Metric | Description |
|---|---|
| `sluice_tunnel_advertise_timeouts_total` | tunnel streams failed for a missing `Advertise` |
| `sluice_tunnel_streams_refused_total` | tunnel streams refused, `reason` = `session-limit` / `session-address-limit` (past a bound on the registered sessions, at once or at the client's first `Advertise`), `pending-limit` / `pending-address-limit` (past a bound on the streams waiting for their `Advertise`), `draining` (opened while the node drains) |
| `sluice_tunnel_sessions_build_unknown` | connected clients without a build (see README "Console") |

## Client

| Metric | Description |
|---|---|
| `sluice_connections_active` | upstream connections open over every server node |
| `sluice_reconnect_total` | tunnel stream re-establishments after a drop, per node |
| `sluice_advertise_rejected_total` | advertised listen ports the server rejected |
| `sluice_connections_limit` / `sluice_connections_admitted` / `sluice_connections_rejected_total` | upstream connection limit, connections admitted, `CONNECT`s rejected past it |

## Data plane (server)

| Metric | Description |
|---|---|
| `sluice_data_connections_limit` / `sluice_data_connections_admitted` | connection limit and the connections (and further upstream connections of h2 connections) holding a slot of it |
| `sluice_data_connections_memory_bytes` / `sluice_data_connections_memory_reserved_bytes` | connection memory (absent when unbounded) and what is reserved of it |
| `sluice_data_connections_rejected_total` | `reason` = `limit` (past the connection limit, the overflow's connections included) / `address-limit` / `head-timeout` / `h2-limit` (an h2 connection whose relay's bytes did not fit) |
| `sluice_data_connections_overflow` | places of the overflow taken |
| `sluice_data_connections_overflow_answered_total` | connections past the limit answered `503`, `protocol` = `h1` / `h2` |
| `sluice_data_connections_overflow_expired_total` | overflow connections closed by the overflow timeout |
| `sluice_data_connections_h2_declined_total` | TLS connections offering h2 and HTTP/1.1 given HTTP/1.1, the h2 bytes not fitting |
| `sluice_data_connections_idle_closed_total` | connections closed for idling between requests |
| `sluice_data_connections_stall_closed_total` | connections closed for an exchange stalled on its client |
| `sluice_data_connections_verbatim_idle_closed_total` | connections relayed verbatim closed by the verbatim idle timeout |

## h2 demux (server)

| Metric | Description |
|---|---|
| `sluice_h2demux_queued_bytes` | bytes the h2 demux holds of a route's streams, both ways, plus the upstream connections' writer queues, one series per route |
| `sluice_h2demux_held_bytes` | bytes every h2 demux relay holds, both ways, the client and upstream connections' writer queues included, as of each relay's last review |
| `sluice_h2demux_credit_bytes` | the connection window the clients and upstreams of every h2 demux relay hold (what they may still send), as of each relay's last review |
| `sluice_h2demux_budget_bytes` / `sluice_h2demux_budget_used_bytes` | the shared h2 queue budget and the bytes charged to it (what the relays hold past their allowance and their peers' windows past 65535 bytes), which may pass the budget by what the relays cannot refuse |
| `sluice_h2demux_window_withheld_total` | times a connection's windows were withheld, `reason` = `budget` (past its 128KiB with the budget exhausted) / `share` (its share of the budget at 3MiB) |
| `sluice_h2demux_window_declined_total` | times a peer's connection window was kept from growing, or fell back to 65535 bytes, for want of the budget or the share its growth is charged to |
| `sluice_h2demux_streams_refused_total` | `reason` = `budget` (a head the budget had no room for) / `share` (one taking its connection's share past 6MiB) / `leg-limit` (a stream whose route needed a new upstream connection with no slot left) / `waiting` (a stream waiting for its upstream's concurrency limit refused to free a connection whose window is withheld) / `wait-timeout` (refused past `sluice.limits.h2-stream-wait-timeout`) |
| `sluice_h2demux_streams_reset_total` | streams the relay reset on its own account, `reason` = `budget` (a header block that did not fit the budget) / `share` (one taking its connection's share past 6MiB) / `interim` (an upstream's interim heads past 64KiB) / `protocol` (netty's codec reset a client stream on its stream error) / `upstream` (the same on an upstream's stream); a frame on a stream no longer known is answered with a reset that does not count |
| `sluice_h2demux_legs_failed_total` | upstream connections of an h2 connection ended by a connection error, with a GOAWAY of its code; a client or an upstream connection closing mid-stream counts in neither |
| `sluice_h2demux_streams_misdirected_total` | streams answered `421`, `reason` = `force-http1` |
