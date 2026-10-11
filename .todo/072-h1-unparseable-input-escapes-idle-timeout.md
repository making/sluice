Difficulty: Medium

# Data plane h1: input the parser cannot follow escapes rerouting and the idle timeout

Found while working on 060. `Http1RequestParser` degrades the rest of a connection to
verbatim passthrough when it meets input it cannot follow: a request head with a byte
>= 0x80 (`ConnectionHeadRewriter.isAscii`), a head over 64KiB, an invalid chunk size.
`ReroutingRelay` then treats the connection like an upgraded one: no further rerouting,
and the keep-alive deadline disarmed for good. Any plaintext client can therefore hold a
slot of `sluice.limits.max-connections` forever by putting one non-ASCII byte in a header
and going quiet -- the bypass 060 closed for declined upgrades, open through this door.
The response side is similar: a response head `ResponseTracker` cannot parse stops the
tracking, so the requests after it stay owed and no deadline is armed.

Worse, when the first request head itself holds a non-ASCII byte, the parser emits it as
data before any leg exists, `ReroutingRelay#onRequestData` drops it ("unreachable: a head
always precedes the body"), and the connection hangs with no leg until the client goes
away. Confirmed with a throwaway E2E probe: `GET /` with `X-Name: é` (UTF-8) got no
response within 3s while holding its slot (admitted gauge 1).

## Direction

- Field values may carry obs-text (RFC 9110 5.5: treat it as opaque): parse and rewrite
  heads as ISO-8859-1 instead of refusing non-ASCII, so such heads are routed and tracked
  like any other.
- What stays unparseable (an oversized head, a broken chunk size) is a malformed request:
  answer 400 at a request boundary and close, as HTTP servers do, instead of relaying it
  verbatim -- which is also a request smuggling surface, as the upstream may frame the
  bytes differently. Check the README's claims on passthrough before changing it.

## Tests (`sluice-it`; watch each fail first)

- A first request head with a UTF-8 header value is relayed and answered.
- After a request with a UTF-8 header value, the connection idles out at the idle timeout
  and the next request is rerouted by its Host.
- A later request with an invalid chunk size is answered 400 and the connection closed.
