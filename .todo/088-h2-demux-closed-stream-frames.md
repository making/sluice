Difficulty: Medium

# h2 demux: client frames on closed streams, and reused stream ids, are ignored

Found while working on 082 (by reading, not tested). The relay forgets a stream once it
is closed for the client and drained (`Http2DemuxRelay#updateActiveLocked`), and it
validates the frames of idle streams (`Http2DemuxRelay#validate`), not those of closed
ones:

- A HEADERS on an odd stream at most the highest the client opened and not tracked is
  decoded and dropped (`Http2DemuxRelay#onHeadBlock`). It may be trailers on a stream the
  relay refused, answered or reset (to be ignored, RFC 9113 5.1), or a client opening a
  new stream with an id it already used (5.1.1: a connection error `PROTOCOL_ERROR`);
  the relay cannot tell them apart.
- DATA or HEADERS after the client's END_STREAM: on a tracked stream (half-closed
  remote) a stream error `STREAM_CLOSED` (5.1), on a stream closed both ways a connection
  error `STREAM_CLOSED`. Today DATA is dropped and its window granted back (`onData`),
  HEADERS dropped (`onTrailers`).
- Frames after the client's RST_STREAM other than PRIORITY: a stream error
  `STREAM_CLOSED` (5.1). Today dropped.

5.1 has frames on a stream the relay itself reset ignored for a while (the client may
have sent them before it read the reset), so the relay needs to remember how each
stream closed.

## Direction

Keep a bounded record of the streams closed recently and how (client END_STREAM, client
reset, reset by the relay, refused or answered by the relay); validate against it, and
keep ignoring frames of streams older than the record.

## Tests (`sluice-it`; watch each fail first)

- A HEADERS reusing the id of a stream that completed: GOAWAY `PROTOCOL_ERROR`.
- DATA after the client's END_STREAM, while the response goes on: RST_STREAM
  `STREAM_CLOSED`; after the response ended too: GOAWAY `STREAM_CLOSED`.
- Trailers or DATA on a stream the relay refused or reset: ignored, the connection goes
  on.
