# Tunnel flow control

How the tunnel stream carries many virtual connections without one stalling the others.
User-facing summary: README "Architecture".

## Relay

All blocking I/O runs on virtual threads; outbound frames are scheduled, not merely queued: control frames go first, connection frames wait on a per-connection bounded queue drained round-robin, and gRPC flow control is honored.

- one bidi stream per client; each proxied TCP connection becomes a `conn_id` on that stream
- server: accept, route by `Host`, `VirtualConnection` + `CONNECT`, then raw relay (`SocketRelay`, one virtual thread per direction)
- client: `CONNECT`, dial the local upstream (strict forwarding), same raw relay; upstream URLs with `https://` are dialed with TLS (verified against the JDK trust store; any certificate with `sluice.insecure=true`), negotiating over ALPN the protocol the server speaks on the connection (`Connect.application_protocol`: `h2` for an h2 demux leg, `http/1.1` for an HTTP/1.1 one, none for a verbatim relay)
- flow control: bounded queues end to end; the gRPC send path honors `isReady()` on a dedicated sender thread, sends control frames before connection frames, and drains the per-connection queues round-robin so one busy connection cannot starve the others

## Per-connection credit windows

- per-connection flow control (credit windows, as in HTTP/2 streams): the server and sluice-client announce a 64KiB receive window per connection (`Advertise` / `Connect`; sluice-wicket-wasi a fixed 1MiB), and a writer sends only against the credit its peer granted with `WINDOW_UPDATE`. The receive path never blocks, so one stalled connection does not stall the others on the stream; a peer sending past its credit fails only that connection. A receiver grants back what its reader took at every half window, and once a writer that ran out of credit said so (`WINDOW_UPDATE` with `blocked`) and the reader emptied its buffer, if it took a quarter of the window since its latest grant: a writer spending its credit in bursts, one per round trip, otherwise finds up to half the window held back for a round trip (an h2 download through one virtual connection moved 512KiB per 20ms tunnel round trip so, half its 1MiB window); the quarter keeps a writer sending as fast as the reader takes from getting its credit back in many small grants. It decides its window at every half window its reader took: the window doubles, up to 1MiB, when that happened while the reader kept up; it halves, granting back less than was read, when the writer was blocked but the reader never found its buffer empty and is a quarter of the window behind, or when the writer was not blocked for two decisions in a row. The part of a window past 64KiB is charged to the node's tunnel budget (`sluice.limits.tunnel-budget` / `sluice.client.tunnel-budget`) before the grant raising it leaves, within a share of the budget, and released as the window shrinks; a reader idle for 1s asks its peer to give back the credit past 64KiB (`reclaim_above`, answered with `released`), and the remote's end releases all but what is buffered. Bounds: a receiver buffers at most 64KiB per connection plus the budget in total (the number of connections is capped, see [connection-limits.md](connection-limits.md)); and one connection has at most one window in flight, so its throughput is at most window / RTT: about 50MiB/s at a 20ms tunnel round trip at the full 1MiB, about 3MiB/s at 64KiB. A peer without the window is refused: the server fails the tunnel stream of a client announcing none with `FAILED_PRECONDITION` (sluice-client 046cd21 / sluice-wicket-wasi f76388d or later required), and the client and the wicket answer a CONNECT without one with an ERROR for that connection (sluice-server 046cd21 or later required); each rejection is logged once

## Transport (gRPC) window

- transport flow control: the server and sluice-client announce a fixed HTTP/2 window (connection and stream alike) for the gRPC connection carrying the tunnel, twice the node's tunnel budget and at least 1MiB, which turns gRPC's own window tuning off; both log it at startup (`gRPC flow control window`). The connections' windows bound what may be in flight, so the transport window only stays out of their way. It does not widen what a receiver may hold: what gRPC buffers before the connections take it was sent against their windows, which the budget charges from the grant on (measured mid-transfer: 32 downloads at about 1GB/s held about 9MiB more direct memory on the receiving server than idle, with 30MiB of windows charged). Left to gRPC, the window started at 1MiB and grew only when one round trip carried more than half of it; as gRPC grants a window back only at every half of it read, a transfer could settle with half the window in flight and half read but not granted back, and the tuning never grew it: in a frame log of such a stack, the whole tunnel moved 512KiB per round trip, about 20MiB/s at a 20ms tunnel round trip, however many connections shared it. Twice the budget leaves the budget's worth in flight even then. In `TunnelTransportWindowE2ETest` 48 downloads, and 48 uploads, through one tunnel move 48MiB per round trip (round trips counted by a proxy holding the receiver's answers back, not timed), where gRPC's tuning moved at most 1.9MiB. sluice-wicket-wasi announces the same (twice `--max-connections` x 1MiB, adaptive window off; 32MiB sent to it take at most 3 round trips against 17 at hyper's defaults of 2MiB per stream, in a unit test with the same round-counting proxy idea)

## Tunnel budget

What one relayed connection buffers on each side of the tunnel outside the tunnel budget
is its receive buffer at the initial 64KiB window + one frame of its send queue (64KiB) +
the two relay buffers (2 x 64KiB) = 293KiB. The receive buffer holds at most the window of
unread payload, coalesced into 16KiB segments whatever the size of the DATA frames, plus
37KiB: a partly read and a partly filled segment, and the overhead of at most 80 segments
(a 1MiB window's). Past that, a connection's window grown beyond 64KiB (up to 960KiB more)
and its send queue past one frame (up to 15 frames more, 960KiB) take from the tunnel
budget (`sluice.limits.tunnel-budget`, an eighth of half the max heap; the client's
`sluice.client.tunnel-budget` likewise), shared by every connection of the node: a
connection grows only within its share -- the budget over the connections holding part of
it plus one, so they leave room for one more -- and by what the budget has left, and a
send queue past its frame waits for room, its own frames leaving letting it go on. A
connection whose reader stops holds its window but grows no further; one whose budget
share is cut gives back what it can as it drains. So a node's connections buffer at most
293KiB each plus the budget: in `TunnelBudgetE2ETest` eight downloads whose clients stop
reading, and eight uploads to upstreams that stop reading, stay within 8 x 64KiB plus a
2MiB budget at every sample on the server and on the client (not 8 x 1MiB), and the public
client going away, the upstream's reset and the verbatim idle timeout each release what
the window took. The budget's size decides how many connections move at the full window
at once: at a 20ms tunnel round trip a connection at 1MiB moves up to about 50MiB/s, one
at 64KiB about 3MiB/s. The 25.6MiB budget of production's 409MiB heap holds the full
receive window of 26 connections at once (each share above 960KiB), and every other
connection keeps flowing at 64KiB; each MiB of budget costs 3.5 connections of the derived
limit. With room, one connection grows to the full window: an 8MiB download or upload
through a 20ms tunnel round trip takes about 18 waits for credit in
`TunnelBudgetE2ETest`, where 64KiB alone would take 256, and an h2 connection's upstream
connection grows the same way (`H2DemuxWindowRoundTripE2ETest`).
