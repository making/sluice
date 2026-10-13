Difficulty: Medium

# Answer connections past the limit with an HTTP 503 instead of closing them at accept

Split from 132 (2026-10-14).

## Problem

A connection past `max-connections` / `connection-memory` is closed at accept, unread. A
TLS client sees the handshake fail (`SSL_ERROR_SYSCALL` in curl, a network error page in a
browser), a plaintext HTTP client a reset; neither learns that the server is busy, and
browsers retry at once. An h2 connection whose relay's bytes do not fit is already
answered after its head (GOAWAY, or HTTP/1.1 through ALPN), so only the accept-time
rejection is left.

## Direction (recommended)

- A rejection lane: a small fixed number of connections past the limit (e.g. 8, its own
  reservation: the TLS engine's buffers and the head limit, not a virtual connection) is
  served far enough to answer: TLS handshake on routes the server terminates, the head
  read under a short deadline, then a 503 page with `Retry-After` (h2: on the first stream,
  GOAWAY after it) and close. Past the lane, close at accept as now.
- TLS passthrough and tcp routes keep closing at accept (nothing to answer in).
- Count it (`rejected` reason `limit` stays, plus whether a 503 went out) and log it like
  the other rejections.

## Failure modes to cover

- A flood past the limit cannot hold the lane: the head deadline cuts slow peers, the lane
  is bounded, the lane's own bytes are part of the derivation.
- A handshake failing in the lane releases its place.

## Tests

- Stack test: limit full, a TLS client offering http/1.1 gets `503` with `Retry-After`; an
  h2 one gets the page on its stream; the lane full, the next one is closed at accept;
  every place released afterwards.
