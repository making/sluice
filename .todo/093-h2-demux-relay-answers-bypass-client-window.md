Difficulty: Medium

# h2 demux: the responses the relay answers itself bypass the client's flow control

Found while working on 081 (by reading, not tested). The relay answers a stream it
cannot route with the 503 no-route or 403 forbidden page (`Http2DemuxRelay#refuse`,
`ErrorResponse#noRouteStream` / `#forbiddenStream`): HEADERS plus about 10KiB of DATA,
written straight to the client. That DATA is charged neither to the relay's send window
towards the client (`Http2DemuxRelay#clientSendWindow`) nor checked against the stream's
window (the client's SETTINGS_INITIAL_WINDOW_SIZE, which may be 0):

- A client whose initial stream window or remaining connection window is below the page
  receives DATA past it (RFC 9113 6.9.1), a flow-control error on its side.
- The client deducts the page from its connection window, the relay does not: from then
  on the relay believes it may send more than the client allows, and a later response at
  the window's edge overruns it.

081's `502` for a stream lost to its upstream (`ErrorResponse#badGatewayStream`) has no
body for that reason.

## Direction

Write a relay-answered body through the response path: charge `clientSendWindow` (queue
behind `responseQueue` like upstream DATA), and keep within the stream's window (the
client's initial window, `clientInitialWindow`, plus its WINDOW_UPDATEs of the stream,
which the relay does not track for a stream it never routed); send the page only when it
fits, else a bodiless response, or queue it.

**User decision needed** once that holds: whether the `502` of a lost stream gets an
error page like the 503/403 ones (a new template), or stays bodiless.

## Tests (`sluice-it`; watch each fail first)

- A client announcing SETTINGS_INITIAL_WINDOW_SIZE 0, then a stream to an unrouted host:
  the client receives no DATA past its window (`H2TestClient#maxReceiveExcess` style
  check per stream), and the response completes once it grants.
- A client withholding its connection window after an unrouted answer, then a response
  of exactly its remaining window from a routed upstream: no DATA past the window.
