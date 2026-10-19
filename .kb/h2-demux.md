# h2 demux

How the data plane relays plaintext and TLS-terminated h2. User-facing summary: README
"Constraints and notes".

## Upstream connections

h2 (plaintext and TLS-terminated) is demultiplexed on netty's HTTP/2 codec: the relay
is the HTTP/2 endpoint of the client connection and a client of one upstream connection
-- a virtual connection -- per route the connection reaches: the first on the
connection's own slot of `sluice.limits.max-connections`, each further one open at once on
a slot of its own, reserving its bytes (see "Reservations and the derived limit" in [connection-limits.md](connection-limits.md)). A stream whose route needs a new
upstream connection while the limit has no slot or bytes left is refused with
`REFUSED_STREAM` (the client may retry it), the connection going on. An upstream connection that closed is let go by the relay and the
tunnel session, and its slot comes back once the frames it queued for the tunnel stream
left the sender. An upstream connection on a tunnel client that is shutting down
(withdrew its routes) is closed as soon as no stream is open on it, waiting for it or
parked for its route's credential check, so it does not hold the client's drain; the
route's next stream is resolved anew. A stream beyond the upstream's
`SETTINGS_MAX_CONCURRENT_STREAMS` (one until the upstream's first SETTINGS) waits in the
relay, for the same upstream connection, until one of the streams there closes; a stream
still waiting after `sluice.limits.h2-stream-wait-timeout` (30s; `0` = off) is refused
with `REFUSED_STREAM`, its upstream never having seen it, so an upstream that keeps its
streams open or announces a limit of 0 does not hold it for good.

## Windows

The windows are per hop. The relay announces a 1MiB stream window to the client and a
2MiB one to each upstream, grants a stream's window back only for what it wrote on the other hop, and
refills its connection windows as DATA arrives, not as it is written (a connection window
is refilled once half of it is spent): a stream held by its upstream or by its client
holds at most one stream window in the relay, and the connection's other streams go on,
on the same upstream connection too. A connection window starts at RFC 9113's 65535 bytes
and grows with what its peer sends per round trip, up to 1MiB for the client and 2MiB for
an upstream: the relay sends a PING with
the DATA a peer sends and counts the DATA read until the PING's ack (as gRPC's HTTP/2
transport sizes its window), and once a peer sent half its window within that round trip,
the window doubles (128KiB, 256KiB, 512KiB, 1MiB, 2MiB) at its next refill; a client sending
small request bodies, or an upstream a slow stream, keeps 65535 bytes. A peer that does
not answer PINGs keeps 65535 bytes too. So an upload moves at most 1MiB per round trip of
the client, all the connection's streams together, and at most the upstream's own windows
per tunnel round trip. A download moves at most the tunnel's per-connection window, 1MiB,
per tunnel round trip, the same bound as an HTTP/1.1 connection's: an upstream
connection, one virtual connection, has no more than that in flight per round trip. The
upstream's windows are twice that because both are granted back only at every half of
them (netty's update ratio for a stream, the refill for the connection): an upstream
sending in bursts, one per round trip, finds up to half of each held back for a round
trip, and at 1MiB they bound a download to 512KiB per tunnel round trip (20.5MiB/s at
20ms, where an HTTP/1.1 connection moved about 40MiB/s; with the windows at 2MiB one h2
stream moved 37 to 43MiB/s in the harness of `.todo/artefacts/106`). While its windows
grow from 65535 bytes an h2 download takes a few round trips more than an HTTP/1.1 one.

## Buffering and the h2 queue budget

DATA held for a stream
is copied once, into buffers holding it in order and rounded up to 1KiB in all, however
small the frames. What the relay holds -- that DATA and the header blocks not yet
written, both ways, plus the writer queue of the client connection and of each upstream
connection -- is bounded across connections, and so is the connection window its peers
hold, which they may still send: each connection may hold 128KiB, and the bytes beyond
it plus the window each of its peers holds past 65535 bytes, its share, are charged to
`sluice.limits.h2-queue-budget`, shared by every h2 connection. A window grows, and is
refilled, past 65535 bytes only once its part past them is charged, within the budget's
room and below the connection's 3MiB share: what a peer sends was charged before it could
arrive, but for 65535 bytes. A window whose growth or refill does not fit stays smaller,
at worst 65535 bytes, at which its streams still go on: through a 20ms round trip a
4MiB download and a 4MiB upload each took about 2.5s at 65535 bytes in
`H2DemuxWindowFloorE2ETest` (a 65535-byte window cannot move them faster than 1.3s),
against about 0.3s to 0.5s at the grown windows in `H2DemuxWindowRoundTripE2ETest`. A
window granted cannot be taken back (HTTP/2 has no negative WINDOW_UPDATE): it stays
charged until its peer sends it or its connection, or upstream connection, closes -- an
h2 connection with no stream open is closed after `sluice.limits.idle-timeout` --, so a
connection that moved a large body holds its grown window's part of the budget, up to
960KiB for its client and 1984KiB per upstream connection, while it stays open; with
production's 25.6MiB budget about 13 upstream windows grown to 2MiB fit at once (27 at
1MiB), and the windows past them stay smaller. Past its 128KiB with
the budget exhausted, or with its share at 3MiB (one connection window each way) while
the budget has room, the relay grants the client and every upstream connection no more
connection window (a peer stops once the window it holds is spent) until the connection
drains below 128KiB, or below its share with the budget's room back. A header block does
not fit when the budget has no room for it or when it takes the connection's share past
6MiB (twice the share: a connection granting no window passes its share by what its
peers still send uncharged, and its new streams are still relayed): a new stream
whose head does not fit is refused with `REFUSED_STREAM`, and a later header block that
does not fit resets its stream (`RST_STREAM` `ENHANCE_YOUR_CALM` to the
client, `CANCEL` to the upstream) while the connection's other streams go on: the
client's trailers, and an upstream's interim (1xx) head, final head or trailers once the
header blocks queued or waiting for the client connection hold 64KiB (so a response is not
held back for bytes queued towards the upstreams). A page the relay answers a stream with
(`503`, `403`, `421`, `502`) is admitted the same way, and held for the client's window as
the relay holds DATA: one that does not fit refuses its stream (`REFUSED_STREAM`), or
resets it (`ENHANCE_YOUR_CALM`) in place of the bad-gateway page. A header
block counts at its size as HPACK counts it (RFC 7541 4.1), not at the bytes it took on
the wire: one referring to the peer's dynamic table, a few bytes on the wire, counts
about what the relay writes for it, up to 64KiB. The interim heads of one response may
sum to 64KiB counted so (the header list limit); the one past it is not relayed: the
upstream's stream is reset with `ENHANCE_YOUR_CALM` and the client gets the bad-gateway
page (`502`). While the window is withheld, the streams waiting for an upstream's
concurrency limit hold at most 128KiB of the connection: past it the most recent of them
are refused with `REFUSED_STREAM` (their upstream never saw them; the client may retry
them), since their bytes drain only once a stream ahead of them closes, which may need
the very request body the withheld window holds back. So a connection whose peers read
keeps flowing however exhausted the budget is. And one connection takes no more of the
budget than its 3MiB share, what its peers still send uncharged once nothing more is
granted (180KiB each, see below) and header blocks up to 6MiB: a client that stops
reading its responses, or an upstream that stops reading its uploads, holds that much,
while the other connections keep their windows. With the 409MiB heap of production (a
25.6MiB budget), 9 connections held at their share exhaust the budget. A connection then
holds outside the budget its 128KiB, what its client still sends uncharged (the 16KiB
read under way, 65535 bytes of window, under 1KiB of rounding per stream: 180KiB), the
control frames of its client connection's writer queue (frame headers, acks, resets: a
connection is no longer read past 16KiB of them queued, plus at most 64KiB: 80KiB) and the
64KiB of response header blocks taken unadmitted, about 452KiB
(`NettyHttp2DemuxRelay.UNBUDGETED_BYTES`); each upstream connection adds its upstream's
180KiB and the 80KiB of control frames its writer queue holds, about 260KiB
(`NettyHttp2DemuxRelay.RESPONSE_QUEUE_BYTES`), covered by what the connection reserves.
The rest of a writer queue, the DATA and header blocks the relay writes into it, was
charged before it got there; DATA reaches a writer queue only while it is under its 64KiB
high water mark, and it does not stop the relay reading the peer it is queued for. In `H2DemuxWindowCreditE2ETest` sixteen h2 connections, eight downloading to
clients that grant no connection window and eight uploading to upstreams that grant no
stream window, hold with the window their peers hold no more than a 4MiB budget plus
their sixteen reservations at every sample, though their peers would hold 32MiB of window
alone at 1MiB windows granted outright.

## Settings, resets and flow control

h2 flow control (plaintext and TLS-terminated h2): the windows are per hop (see "Windows" above). Request DATA towards an upstream stays within the upstream's windows, response DATA towards the client within the client's; a stream without window holds back neither the other streams to its upstream nor the other streams of the client. DATA is relayed without its padding. The settings are per hop too: the client gets the relay's SETTINGS (100 concurrent streams, a 1MiB initial stream window, the 64KiB header list limit), each upstream the relay's own (push off, a 1MiB initial stream window, the 64KiB header list limit) once as its connection opens, whatever the client announces, and each a connection window growing from 65535 bytes to 1MiB with what it sends. A reset the relay writes -- one relayed from one side to the other, one netty's codec answers a frame with, or the relay's own (a header block past the budget, interim heads past 64KiB) -- is never followed by DATA of its stream, even when the read that caused it also opened the stream's window: what the relay held of it for the peer goes out before the `RST_STREAM` or is dropped. An upstream sending past the stream window the relay granted it gets its stream reset with `FLOW_CONTROL_ERROR`, its connection going on. A client that half-closes its TCP connection after its requests still gets the responses, within the windows it granted before (see "Half-close probes" in [connection-limits.md](connection-limits.md)). A client that never grants window is cut by the stall timeout.

## Request heads

h2 request heads (plaintext and TLS-terminated h2): every stream, the connection's first included, is routed by its `:authority`, access-checked and logged (`type=req`) on its own; a later stream the relay refuses is answered on its own stream (`403`, `503`, `421` for a `force-http1` route, see below), the connection going on. A CONNECT (RFC 9113 8.5: `:authority`, no `:path`) is a tunnel to the upstream of its `:authority`, its data, end and reset relayed either way, logged with the authority as its path; a connection opening with one is demultiplexed like any other. A head that is no request -- no `:method`, no `:path` (but for a CONNECT), a CONNECT without `:authority` -- is reset with `PROTOCOL_ERROR` and reaches no upstream, the connection going on; so is a head carrying the connection-specific `connection` field (RFC 9113 8.2.2). A header block that does not decode at all, the connection's first or a later one, ends the connection with GOAWAY `COMPRESSION_ERROR` (RFC 9113 4.3; a later one names stream 2^31-1 as the last processed) and reaches no upstream: the decoder state is unknown from there on, so no later head could be trusted to route by. A header list past 64KiB is answered `431` on its stream, the connection going on. A reset head logs a `type=req` line with `route=- path=-` (its method, if any), a compression error one with `route=- method=- path=-`.

## Credential checks

A stream of a route with credentials (README "Route authentication") whose credential is
checked against `bcrypt` / `pbkdf2` and not remembered is parked while the check runs on a
virtual thread of its own: the connection's other streams, of its route too, go on
meanwhile. Streams presenting the credential of a check under way wait for that check. A
parked stream holds its head and what the client sends on it, counted in the h2 queue
budget as a stream waiting for its upstream's concurrency limit: while the windows
are withheld the most recent parked and waiting streams past the connection's 128KiB are
refused with `REFUSED_STREAM`, and so is a parked stream whose check has not ended after
`sluice.limits.h2-stream-wait-timeout` (`0` = off). Once the check ended the stream goes on
as an unparked one does: answered `401` / `503`, or relayed with what the client sent
meanwhile; a check that throws resets the stream with `INTERNAL_ERROR` and logs a warning. A
stream the client resets, or whose connection ends, while parked is dropped; its check runs
to its end and resumes nothing. Streams of one route keep their stream ids on its upstream
connection, but a parked stream that a later stream of its route overtook takes the next id
the upstream connection may open.

## Framing limits

h2 framing limits (plaintext and TLS-terminated h2): the relay announces no larger frame size to either side and a 64KiB header list limit, so a client frame above 16KiB ends the connection with GOAWAY `FRAME_SIZE_ERROR`, and a client header block past 80KiB (netty's threshold over the list limit; a list past 64KiB within it is answered `431`, see "Request heads") with GOAWAY `PROTOCOL_ERROR`; an upstream's frame above 16KiB ends that upstream's connection with GOAWAY `FRAME_SIZE_ERROR`, and its header block past 80KiB or interleaved with another with GOAWAY `PROTOCOL_ERROR`.

## Client errors

h2 client errors (plaintext and TLS-terminated h2): netty's codec answers the client's protocol errors. A connection error -- a PING, SETTINGS, GOAWAY, RST_STREAM or HEADERS frame of a length its type does not allow, a frame on stream 0 that belongs on a stream or the reverse, a frame on an idle stream other than a HEADERS opening it or a PRIORITY (an even stream included), a CONTINUATION out of place or another frame inside a header block, padding past the payload, a PUSH_PROMISE, an invalid SETTINGS value or a `SETTINGS_INITIAL_WINDOW_SIZE` change taking a stream window past 2^31-1 (a stream whose response ended included), a client preface not followed by SETTINGS -- ends the connection at once with a GOAWAY of the RFC's code naming stream 2^31-1 as the last processed: the open streams are dropped and the upstream connection is closed, with no grace for the streams in flight. A stream error resets that stream alone, `CANCEL` to its upstream: a PRIORITY of a length other than 5 (on an idle stream too) or a padded DATA frame too short for its pad length with `FRAME_SIZE_ERROR`; a stream WINDOW_UPDATE of 0 with `PROTOCOL_ERROR`, on a closed stream too; after the client ended a stream, DATA on it with `STREAM_CLOSED`, a further header block with `PROTOCOL_ERROR` while netty holds the stream (RFC 9113 5.1 names `STREAM_CLOSED`), and a DATA or HEADERS frame with `STREAM_CLOSED` once netty forgot the stream -- but the frames the client had in flight on a stream the relay reset while the client was still sending on it, which are ignored (RFC 9113 5.1): up to the DATA the stream window held at the reset (an empty frame not ending the stream counting one byte) and one header list (64KiB) of header blocks, until one ends the stream, for the last 100 streams the relay reset; past that they are answered as before. A request the client ended whose response ended still reaches its upstream when netty resets the client's stream after that (a WINDOW_UPDATE of 0). A client sending past a stream window gets that stream reset with `FLOW_CONTROL_ERROR`. After the 201st stream error of the client within 30s, answered with its reset, the relay ends the connection with GOAWAY `ENHANCE_YOUR_CALM` (a client streaming DATA on a stream reset for its window, say); the resets the relay decides itself or relays from an upstream do not count (more than 200 within 30s end neither the client's connection nor the upstream's), nor do the frames the client had in flight on those streams (the rest of the request bodies and their trailers). A client resetting more than 200 streams within 30s gets GOAWAY `ENHANCE_YOUR_CALM` too (netty's limit against rapid resets). Settings netty does not know are ignored whatever their value (RFC 9218's `SETTINGS_NO_RFC7540_PRIORITIES` too).

## Upstream errors

h2 upstream errors (plaintext and TLS-terminated h2): an upstream's failure ends only what it covers, never the client's connection nor the streams of other upstreams. A stream lost to its upstream is answered by the relay with the bad-gateway page (`502`, held to the client's stream window like the `403` / `503` pages) while no response frame but interim (1xx) heads was relayed on it (then `RST_STREAM NO_ERROR` after the page if the client was still sending its request), and reset with `INTERNAL_ERROR` once its response began; a stream the upstream never saw (waiting for its `SETTINGS_MAX_CONCURRENT_STREAMS`) is refused with `REFUSED_STREAM`, which the client may retry. A stream error of an upstream (a stream WINDOW_UPDATE of 0 or past 2^31-1, DATA past the stream window) loses that stream alone, reset towards the upstream with the error's code. An upstream connection has no limit on the resets it takes or the stream errors the relay answers on it: more than 200 within 30s do not end it. The response frames an upstream had in flight on a stream the relay reset (the client cancelled it, say) are ignored within the same bounds as the client's. netty's codec validates the upstream's framing: a connection error (a frame above 16KiB, a header block too large or interleaved, a WINDOW_UPDATE of a wrong length or of 0 on stream 0, a window past 2^31-1, a frame on a stream the relay has not opened on it; SETTINGS, PING or GOAWAY on a stream, a frame of a length its type does not allow, DATA or HEADERS on stream 0, a CONTINUATION without a header block, an undecodable header block, padding past the payload, a PUSH_PROMISE) ends its connection alone with a GOAWAY of the RFC's code, naming stream 2^31-1 as the last processed; the upstream connection is half-closed right after the GOAWAY and closed after 1s if the upstream does not close it, and the connection closing or failing ends it the same way without the GOAWAY. Responses the upstream completed are still delivered; the route's next stream opens a new upstream connection. An upstream flooding PINGs without reading, the acks piling up towards it, is no longer read once the relay queued 16KiB of control frames for it, so the relay holds at most 80KiB of them for it; its connection is not ended for it. A client connection left with nothing open idles under `idle-timeout`. An upstream's GOAWAY stops new streams on its connection: the streams above its last stream id, and those waiting for its concurrency limit, are refused with `REFUSED_STREAM`; the relay sends it a GOAWAY `NO_ERROR` of its own and closes the connection once the other streams completed.

## force-http1

Upstream without HTTP/2 support: set `force-http1=true` on the upstream so the data plane's TLS termination offers http/1.1 first in ALPN and ingress falls back to HTTP/1.1. The preference applies to the route resolved from the ClientHello SNI (ingress by bare IP sends no SNI and resolves via the catch-all route). An h2 stream whose route is `force-http1` is never relayed: the relay answers it with a bodiless `421 Misdirected Request` (RFC 9110 15.5.20; a request still being sent is then reset with `NO_ERROR`) and opens no upstream connection, the h2 connection going on. This covers a browser coalescing the host onto an h2 connection it opened for another host of the same certificate (RFC 9113 9.1.1) -- Chrome and Firefox retry a 421 on a new connection, whose SNI makes ALPN pick http/1.1 -- as well as a TLS client offering only h2 (the handshake still settles on h2) and plaintext h2c with prior knowledge, which have no HTTP/1.1 to fall back to. A connection that offered http/1.1 but got h2 for the host itself -- its handshake resolved no `force-http1` route, say while the route was gated shut, or it sent no SNI -- also gets a GOAWAY `NO_ERROR` with the 421: Chromium retries a 421 on a new connection only past a coalesced one, so on this one it would be misdirected again. Each is logged as `type=req ... status=421 reason=force-http1`. Over an `https://` target the client offers h2 (with http/1.1) in ALPN for an h2 leg: an upstream negotiating http/1.1 or no protocol fails the leg before a byte of h2 reaches it, its streams get `502`, and the client logs a warning naming `force-http1` (a client from before `Connect.application_protocol` offers no ALPN, so every `https://` upstream takes the h2 preface for HTTP/1.1 and h2 streams get `502`).
