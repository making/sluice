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
062). The stream error needs the stream reset both ways: RST_STREAM `PROTOCOL_ERROR` to
the client, `CANCEL` to the upstream, which runs into 065's problem of the upstream's
frames still arriving on the reset stream; decide whether to share 065's mechanism.

## Tests (`sluice-it`; watch each fail first)

- A client WINDOW_UPDATE of length 3 or 5 gets GOAWAY `FRAME_SIZE_ERROR`; one with
  increment 0 on stream 0 gets GOAWAY `PROTOCOL_ERROR`.
- One with increment 0 on an open stream gets RST_STREAM `PROTOCOL_ERROR` on that stream
  only; another stream of the connection completes.
