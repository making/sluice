Difficulty: Medium

# h2 demux: client WINDOW_UPDATE frames that RFC 9113 makes errors are ignored

Found while working on 062 (by reading, not tested). `Http2DemuxRelay#onClientWindowUpdate`
returns on a WINDOW_UPDATE whose length is not 4 and on an increment of 0. RFC 9113 6.9:
a length other than 4 is a connection error `FRAME_SIZE_ERROR`; an increment of 0 is a
stream error `PROTOCOL_ERROR`, a connection error on stream 0. A stream-level overflow
(the client's stream windows are relayed, not tracked by the relay) is the upstream's to
detect. Harmless for the relay's own accounting -- such a frame credits nothing -- but a
client relying on the error (conformance suites such as h2spec) sees none.

## Direction

Connection errors: GOAWAY with the code, then end the relay (as the overflow does since
062). The stream error is `Http2DemuxRelay#resetStream(stream, ERROR_PROTOCOL)` (a new
0x1 constant; the method since 065, for trailers past the budget): RST_STREAM with the
code to the client, `CANCEL` to the upstream behind the frames queued for the stream, its
queued data and waiting response dropped; the upstream's later frames on the stream are
dropped and the stream stops being tracked once the reset is written to the upstream. It
returns `false` for a stream already reset by either side; count a meter only on `true`
(e.g. a `protocol` reason on `sluice.h2demux.streams.reset`). An increment of 0 on a
stream the relay no longer tracks has nothing to reset (RFC 9113 5.1 lets a closed
stream's frames be ignored).

## Tests (`sluice-it`; watch each fail first)

- A client WINDOW_UPDATE of length 3 or 5 gets GOAWAY `FRAME_SIZE_ERROR`; one with
  increment 0 on stream 0 gets GOAWAY `PROTOCOL_ERROR`.
- One with increment 0 on an open stream gets RST_STREAM `PROTOCOL_ERROR` on that stream
  only; another stream of the connection completes.
