Difficulty: Medium

# h2 demux: trailers past the budget end the whole connection

Found while working on 057. A request trailers block is admitted like a new stream's
head: within the connection's allowance or with room in the shared queue budget. One
that fits neither ends the connection with GOAWAY `ENHANCE_YOUR_CALM`
(`H2DemuxBudgetE2ETest.trailersPastTheAllowanceWithTheBudgetExhaustedEndTheConnection`),
taking down the connection's other streams too. Only a connection already past its
allowance while the budget is exhausted gets there, so the client is a heavy user, but
the other streams of a coalesced connection may belong to other routes.

A stream reset (RST_STREAM to the client, CANCEL to the upstream, the stream's queued
data purged) would confine it to the stream. What blocks it today: after the relay reset
a stream towards the client, it relays the header blocks and other non-DATA frames the
upstream still sends on it, which the client must treat as a stream error (RFC 9113 5.1,
closed state). Upstream DATA of a stream the relay reset or no longer tracks is dropped
and its connection window granted back since 062 (`Http2DemuxRelay#relayData`). The relay
would also have to drop such a stream's header blocks (still decoding them to keep the
leg's HPACK decoder in step, `relayHead`) until the upstream ends or resets it, without
keeping that state forever when the upstream never answers the CANCEL (for example, keep
the stream counted against the concurrency limit until then).

## Tests (`sluice-it`; watch each fail first)

- Past the allowance with the budget exhausted, trailers on one stream reset that stream
  only; another stream of the connection completes.
- An upstream response arriving on the reset stream does not reach the client.
