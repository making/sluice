Difficulty: Medium

# netty h2 demux: client frames in flight after the relay's reset count as stream errors

Found while separating the relay's resets from the client's stream errors (115).

## Problem

netty forgets a stream once its RST_STREAM is written. A frame the client sent before it
read the relay's reset -- DATA of a request body still uploading, trailers -- then arrives
on a stream netty no longer knows: the codec answers it with RST_STREAM `STREAM_CLOSED`
(`DefaultHttp2ConnectionDecoder#shouldIgnoreHeadersOrDataFrame`), and `HopHandler` counts
it as a stream error of the client. RFC 9113 5.1 says an endpoint that sent RST_STREAM
MUST ignore frames it receives on that stream afterwards (it MAY limit how long). So a
well-behaved client uploading while the relay refuses or relays upstream resets -- each
reset with up to a stream window of DATA in flight, in frames of any size -- answers for
the relay's resets after all: past 200 such frames within 30s its connection ends with
GOAWAY `ENHANCE_YOUR_CALM`. Read from the code, not shown by a test.

The client flood the limit is for (`H2DemuxStallE2ETest#streamOverItsWindowIsResetAndTheFloodPastItEndsTheConnection`)
looks the same on the wire, but there the first reset is the codec's (`FLOW_CONTROL_ERROR`),
not the relay's.

## Directions

- Remember the streams the relay reset (bounded: the last N ids, or for a while), and in
  `HopHandler#onStreamError` neither answer nor count a `STREAM_CLOSED` stream error of an
  inbound frame on one of them (netty already returned the frame's bytes to the
  connection window before throwing). Ignoring is safe: no reset, no work.
- Mind the README's "any frame with `STREAM_CLOSED` once netty forgot the stream".

## Tests

- First, failing: more than 200 streams the relay resets (upstream `INTERNAL_ERROR`, or
  refusals) while the client still sends each one's body, a few DATA frames in flight
  per reset: the connection goes on, and the client reads no RST_STREAM `STREAM_CLOSED`.
- The flood case above still ends the connection.
