# Connection limits

Detailed behavior behind README "Connection limits". The h2 demux's own buffering is in
[h2-demux.md](h2-demux.md), the tunnel budget in [tunnel-flow-control.md](tunnel-flow-control.md).

## Connection count and memory

The data port and every tcp route port share one limit on the connections open at once
(`sluice.limits.max-connections`) and on the bytes they reserve for their worst-case
buffering (`sluice.limits.connection-memory`), taken at accept: a connection beyond either
is closed unread, but for the overflow below. An h2 connection reserves more once its
protocol is chosen (see below).
`sluice.limits.max-connections-per-address` caps one source address; it is off by
default because behind an SNAT front end without the PROXY protocol every connection
carries the front end's address. `sluice.limits.head-timeout` is one deadline for the
whole head phase (PROXY header, TLS handshake, request head), not a timeout per read, so a
peer dripping its head is cut as well. Rejected connections are logged as rate-limited
`type=conn event=reject` access log lines. The client caps its upstream connections over
every node (`sluice.client.max-connections`): a `CONNECT` beyond it gets an ERROR without a
dial, and the server closes the public connection.


## Overflow (503 past the limit)

Past the limit, up to `sluice.limits.overflow-connections` (8) data port connections at
once are answered instead of closed unread, so that a client learns the server is busy
rather than seeing a failed TLS handshake or a reset. Each is served under one deadline
counted from accept, `sluice.limits.overflow-timeout` (5s): the TLS handshake of a route
the server terminates (ALPN: http/1.1 when offered, else h2) and the request head, then a
`503` busy page with `Retry-After` (`sluice.limits.overflow-retry-after`, 5s; h2: on the
request's stream, then a GOAWAY `NO_ERROR` naming that stream as the last processed); the
connection is closed once the client closed it, at the deadline at the latest. Closed
unanswered instead: a connection the deadline reaches first, one whose handshake fails or
whose client goes away (its place freed at once), a TLS passthrough connection (the
handshake is the upstream's, nothing the server could answer in) and a head that is no
HTTP request, an h2 head that cannot be decoded included. With `sluice.data-proxy-protocol=true`
the PROXY header (v1 or v2) comes first on an overflow connection too: the answer follows
it, its access log reject line carries the PROXY source address, and a malformed header
closes the connection unanswered. Past the overflow, a connection is closed at accept as before, and tcp
route ports have no overflow. An overflow connection holds no slot and no virtual
connection; the derived connection memory leaves 256KiB for each, its ClientHello and its
head each read into a buffer of up to 128KiB (the TLS engine's records are not counted).
A flood past the limit holds each place for the overflow timeout at most, and the
connections past the places are closed at accept, as with `overflow-connections=0`.
Overflow connections count as `rejected` with `reason=limit` like those closed at accept;
the answered ones also as `sluice.data.connections.overflow.answered` (`protocol` = `h1` /
`h2`), and their access log reject line carries `transport` and `status=503`; those the
overflow timeout closed as `sluice.data.connections.overflow.expired`.
`sluice.data.connections.overflow` gauges the places taken.

## Idle timeout

A plaintext or TLS-terminated HTTP connection idle between requests is closed after
`sluice.limits.idle-timeout`, releasing its slot: HTTP/1.1 once every response is relayed
and nothing of a next request has arrived (a next request head left incomplete is then cut
by `head-timeout`), h2 once no stream is open (with a GOAWAY `NO_ERROR` naming the highest
stream the client opened; PINGs do not count as activity, and a stream the relay answers
itself starts the timeout over). A response or h2 stream taking longer than the timeout is
not cut, nor is an upgraded connection (WebSocket; a connection whose upgrade the upstream
declines stays HTTP). An idle h2 connection whose client stopped reading (it floods PINGs,
say) is closed under the relay's blocked write after the one-second teardown grace.
Behind a front end that reuses its connections to the data plane, set the timeout above
the front end's idle timeout, so the front end closes an idle connection before sluice
does. Connections closed so count as
`sluice.data.connections.idle.closed`.

## Stall timeout

While a request or response is in flight on such a connection,
`sluice.limits.stall-timeout` bounds the time the relay waits on the client without
`sluice.limits.stall-min-bytes` moving to or from it (the counterpart of nginx
`send_timeout` / `client_body_timeout`, with a minimum rate): a write to the client not
completing (it does not read), h2 response DATA (a page the relay answers itself
included) waiting for a window the client does not grant (its connection window, or the
stream window of a response begun), or a request
body that is due and not sent -- an HTTP/1.1 body announced by `Content-Length` or
chunked, an h2 stream not ended by the client while it holds window to send it. A client
dripping bytes below the minimum is cut as if it sent none. The connection is closed (h2:
GOAWAY `ENHANCE_YOUR_CALM` first) and counted as `sluice.data.connections.stall.closed`.
Waiting for an upstream does not count: a slow or long-polling response, an upload the
upstream does not read yet (h2: the relay grants the client no stream window meanwhile),
h2 uploads whose connection window the relay withholds for want of budget once the client
spent it. Nor does a client awaiting a `100 Continue` it asked for (until a response head
or a byte of its body), an h2 request while its response is in progress (a streaming
exchange), or an h2 CONNECT stream (a tunnel). The time waited is checked eight times per
timeout, so a stall is cut up to a quarter of the timeout past it. Write progress is what
the client's TCP stack accepts: once the buffers towards a client that reads slowly have
filled up, a blocked write may return long after the client resumed reading (observed on
Linux loopback: only once the client drained about 900KB), so keep the timeout well above
such gaps. The relay writes in 16KiB pieces, each counted as progress once it completes;
on h2, DATA read from the client counts as progress too. A client streaming small pieces
(a gRPC client stream) slower than the minimum is cut too: set `stall-min-bytes=1B` to
count every byte, or raise the timeout. Connections relayed verbatim keep no stall
timeout; see the verbatim idle timeout below.

## Verbatim idle timeout

A connection relayed verbatim -- TLS passthrough, a tcp route, a hostless connection (a
RESP head, plaintext or TLS-terminated), an upgraded HTTP/1.1 connection (a WebSocket,
say) -- has no request boundaries, so neither `idle-timeout` nor `stall-timeout` applies
to it. It is bounded by the verbatim idle timeout instead: once no byte was read from the
client nor written to it for the timeout, the connection is closed, its slot released, and
counted as `sluice.data.connections.verbatim.idle.closed`. Bytes flowing in one direction
keep it open. A connection half-closed by its upstream stays under the timeout, as does
one whose client stopped reading: the relay's write to it blocks, nothing more is written,
and the timeout closes the connection under the blocked write (a TLS-terminated one after
the one-second teardown grace, should the relay's teardown wait on that write). A write
counts as each of its 16KiB pieces completes. What idles legitimately depends on the
protocol (a Redis or database pool keeps connections idle for minutes), so the timeout is
set per route by the client operator, `sluice.client.upstream[n].verbatim-idle-timeout`;
`sluice.limits.verbatim-idle-timeout` applies to the routes that set none, so a server
operator can bound every route. A route's own timeout replaces the server's, longer or
shorter; a route cannot turn the server's off. Both are off by default: a peer that goes
quiet on a connection relayed verbatim, or stops reading from it, then holds its slot of
`max-connections` (and of `max-connections-per-address`) until it or the upstream closes
the connection.

## TCP keepalive

A peer that is gone without its FIN or reset reaching the relay -- a pod or host removed,
a NAT or conntrack entry dropped while the connection idled -- never closes the
connection, and the kernel reclaims a half-closed socket only once the relay closed it. So
every accepted data plane socket (`sluice.limits.tcp-keep-alive`) and every upstream
socket of the client (`sluice.client.tcp-keep-alive`) carries TCP keepalive: after 60s
without a byte the kernel probes the peer every 10s, and fails the socket's reads and
writes once 3 probes went unanswered or one was answered with a reset, which ends the
relay and releases its slot. A peer that is there answers the probes; only the verbatim
idle timeout closes a live connection for idling.

## Half-close probes

A client's FIN does not tell whether it half-closed or left: HTTP/1.1 lets a half-closed
client still wait for its response (RFC 9112 9.6), and only a write fails on a client gone.
So once a plaintext or TLS-terminated HTTP/1.1 client ended its side while a response is
owed and none of it relayed, the relay writes `HTTP/1.1 100 Continue` after each
`sluice.limits.half-close-probe-interval` without a write to the client or a request
relayed, at most four per response. A client gone fails the second write: the relay ends,
fails its tunnel connection, and the client closes the upstream connection. A client that
only half-closed reads the 100s, then its response. A gone client is not noticed, and is
held until the upstream answers or closes (for a wicket route, its request budget), once
a response has begun (nothing can be added to it), towards an HTTP/1.0 request (which must
not get a 1xx), while a request asking for an upgrade awaits its answer (the relay reads
nothing from the client meanwhile), and after the four probes. `0` turns the probe off.

An h2 client ending its side (its FIN) while a request of its is unfinished -- a stream it
has not ended, which can never complete now -- ends the relay at once. Otherwise the relay
drains: the responses owed are relayed, each upstream connection is closed (a GOAWAY, then
its end) once no stream of it is left, open or waiting for the upstream's concurrency
limit, the request bytes it still held written first, and the client connection once all
its streams completed and every upstream connection closed. Meanwhile the relay writes a
PING to the client after each `half-close-probe-interval` without a write to it: a client
gone fails a write, and the relay ends, its upstream connections with it.

Why a `100` and at most four probes per response:

The clients measured (curl, wget, Go, the JDK `HttpClient` and `HttpURLConnection`,
Python's `http.client`, Node's `http` and `fetch`) do not half-close after a request, so a
live client that gets a probe half-closed on purpose (`nc`-style tools). No 1xx suits every
client: Node's `fetch` (undici 7.28) fails on an unsolicited 100, while Python's
`http.client` and `HttpURLConnection` before JDK 20 mishandle a 102 or 103 as the final
response; the others skip a 100 too. Go before 1.24 fails a response at its sixth 1xx and
the JDK `HttpClient` at its ninth, hence four.

## Reservations and the derived limit

Each connection reserves the worst case of what it buffers, from
`sluice.limits.connection-memory`, once its protocol tells what that is. At accept every
connection reserves its virtual connection's 293KiB (see "Tunnel budget" in [tunnel-flow-control.md](tunnel-flow-control.md)), all an HTTP/1.1, TLS
passthrough or tcp connection buffers outside the tunnel budget. An h2 connection reserves 711KiB more, its
relay's 452KiB and its first upstream connection's 260KiB, when its protocol is chosen:
in the ALPN of a TLS handshake that offers h2, otherwise once its connection head is read;
each further upstream connection of it reserves 552KiB (its virtual connection and its
260KiB) with its slot. All of it comes back when the slot does. A TLS client offering h2
and HTTP/1.1 whose h2 bytes do not fit is given HTTP/1.1 (counted as
`sluice.data.connections.h2.declined`); an h2 connection whose bytes do not fit otherwise
(plaintext prior knowledge, or TLS offering h2 alone) gets the server preface and a GOAWAY
`NO_ERROR` naming no stream processed (debug data `connection limit`), so its client may
retry the request on another connection, and is closed (`rejected` with `reason=h2-limit`).
So the reservations and the budget bound what every open connection buffers, whatever its
protocol, without an HTTP/1.1 or passthrough connection reserving an h2 relay's worst
case: in `H2DemuxConnectionMemoryE2ETest` four h2 connections fill the reservable bytes,
two downloading and two uploading eight stream windows each (32MiB in all) to a peer that
does not read, and what their relays hold settles past the 2MiB budget and within it plus
the 4 x 711KiB they reserve.

An unset (`0`) h2 queue budget and an unset tunnel budget are each an eighth of half the
max heap, and unset (`0`) connection memory is half the max heap less both budgets and the
overflow's 256KiB per place; with `max-connections` set and no `connection-memory`, the
bytes are not bounded and the configured count alone limits. An unset (`0`) connection
limit is what the memory holds of connections reserving 293KiB:
`max(1, (max heap / 2 - h2 queue budget - tunnel budget - overflow x 256KiB) / 293KiB)`.
For the 409MiB heap a 512Mi container gives the server in production that is 151.4MiB of
connection memory and a limit of 529 connections, of which 154 h2 connections with one
upstream connection each (1005KiB apiece); 517 for a 400MiB heap, 5361 for 4GiB. Before
the tunnel budget, every connection reserved its full windows (2213KiB) and the 409MiB heap
admitted 81 connections; before the reservation was split by protocol, every slot reserved
an h2 connection's, and it admitted 34; before its peers' connection windows were charged
to the h2 queue budget, an h2 connection reserved 3048KiB (1MiB of window per peer) and
it admitted 46 h2 connections; before a writer queue's DATA and header blocks were told
from its control frames, an h2 connection reserved 1127KiB (a writer queue's 320KiB per
hop) and it admitted 109. The client derives its limit the same way:
`max(1, (max heap / 2 - tunnel budget) / 293KiB)`.
The effective values are logged at startup (`data plane connection limit` / `upstream
connection limit`) and exported as gauges; with the 512Mi container of `k8s.md` they
follow the heap the server gets there.

## Slot release

A slot is released only once its connection ended and the frames it queued for the tunnel
stream left the sender, on the server as on the client (`sluice.client.max-connections`);
the end of the tunnel session drops the frames and releases their slots. An h2
connection keeps its own slot until it ended and the frames the upstream connection on
that slot queued left the same way, a further upstream connection its slot until its own
frames left, and an HTTP/1.1 connection switching routes sends nothing to
the new route until the frames queued for the previous one left. So the send queues of
ended connections stay within the bound; while a stream stays not ready, their slots stay
taken until it drains or the session ends.

## Tunnel stall watchdog

A peer that stops reading the tunnel stream while it keeps the connection alive (it answers
the keepalive pings) would hold those slots for good, so a progress watchdog bounds the
wait on both sides: once connection frames waited `sluice.limits.tunnel-stall-timeout`
(server) / `sluice.client.tunnel-stall-timeout` (client) with the stream not ready and none
of them handed to it, the sender drops its queued frames, which frees their slots, and the
stream is reset: the server fails the session (its routes are removed and its live
connections fail with it, the client reconnects), the client cancels the stream and
reconnects. A frame handed to the stream restarts the wait, so a peer that reads slowly but
keeps taking frames is not cut; control frames, which bypass flow control, do not count, and
with no connection frame queued nothing is stalled. The time stalled is exported as
`sluice.tunnel.stall`, the streams failed as `sluice.tunnel.stall.failures`. No share of the
server's slots is reserved per client: tunnel clients are authenticated, and the watchdog
bounds how long one can hold slots.

## Tunnel streams before `Advertise`

A tunnel stream becomes its client id's session only with the client's first `Advertise`,
which carries the client's window: the session the client id had keeps serving its routes
until then, and is replaced at that point. A stream whose `Advertise` does not arrive
within `sluice.limits.advertise-timeout` is failed with `DEADLINE_EXCEEDED` (counted as
`sluice.tunnel.advertise.timeouts`); the client logs the status and reconnects with its
backoff. The cluster membership is pushed to a client only once it advertised. A server
shutting down ends the streams still waiting for their `Advertise` at the drain start, and
from then on fails every new tunnel stream at once with `UNAVAILABLE` ("node is draining";
counted as `sluice.tunnel.streams.refused` with `reason` = `draining`), a stream already
past its call's start included, so none outlives the drain.

The streams waiting for their `Advertise` are bounded: at most
`sluice.limits.max-pending-tunnel-streams` (256) at once, and at most
`sluice.limits.max-pending-tunnel-streams-per-address` from one source address (off by
default, for the same reason as `max-connections-per-address`: behind a front end that hides
the client address every stream carries the front end's). A stream beyond a bound is failed
at once with `RESOURCE_EXHAUSTED`, counted as `sluice.tunnel.streams.refused` (`reason` =
`pending-limit` / `pending-address-limit`) and logged at most once per 10s, the line reporting
how many were refused since the previous one; the client logs the status and retries with its
backoff. A waiting stream frees its place when its `Advertise` registers it or is refused,
when the deadline fails it, or when it ends. Streams of one client id are not bounded on
their own: the id is a header any token holder picks.

## Tunnel sessions

The registered sessions (one per client id) are bounded as well: at most
`sluice.limits.max-tunnel-sessions` (1024) on the node, and at most
`sluice.limits.max-tunnel-sessions-per-address` from one source address (off by default, for
the same reason). A newer stream of a registered client id replaces that session and takes
no further place (none from its address when the replaced session has the same address). A
stream of a client id that could not register is failed with `RESOURCE_EXHAUSTED`: at once
when that is known as it opens, else at its first `Advertise`, where the registration
decides -- another stream may have taken the last place meanwhile, or the session it was to
replace may have ended. It is counted as `sluice.tunnel.streams.refused` (`reason` =
`session-limit` / `session-address-limit`) and logged through the same throttle; the
registered sessions go on, and the client retries with its backoff. A session frees its
place when it ends or is replaced.

## Log throttles

Each tunnel stream logs a few lines of its own (established, the client's build, its
routes, closing, and its warnings: no `Advertise` in time, no window). At most 32 streams
per 10s log them at info / warn, the first ones of the period; the others log them at debug,
each stream all or nothing, and the next stream logged reports how many were demoted. An `Advertise`
repeating the stream's routes and their outcome logs at debug; one changing them takes one
of the 32 as well.

The tcp route port lines (bound, unbound, taken over, rejected, failed to bind) are
bounded the same way, by a throttle of their own: 32 per 10s at info / warn, the others at
debug, and the next line logged reports how many were demoted.
