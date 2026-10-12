Difficulty: Medium

# h2 demux: client frames RFC 9113 makes errors are ignored or relayed

Found while working on 076 (by reading, not tested). The relay validates the client's
WINDOW_UPDATE (076), frame size and header block size; other malformed frames pass:

- Frame sizes (6.3-6.7, connection error `FRAME_SIZE_ERROR`): a PING of a length other
  than 8 is acked with the same length; an RST_STREAM of a length other than 4 is
  relayed to the upstream as it is; a SETTINGS whose length is not a multiple of 6 has
  the remainder ignored, a SETTINGS ACK with a payload is taken; a PRIORITY of a length
  other than 5 is ignored.
- Stream 0 rules (connection error `PROTOCOL_ERROR`): DATA, HEADERS, CONTINUATION or
  RST_STREAM on stream 0 are ignored; SETTINGS or PING on another stream are handled as
  if on stream 0.
- Idle streams (5.1): DATA, RST_STREAM or WINDOW_UPDATE on a stream above the highest the
  client opened are ignored (a connection error `PROTOCOL_ERROR`); a HEADERS on an even
  stream id is taken as a request (5.1.1, `PROTOCOL_ERROR`).
- Inside a header block (6.10): a frame other than CONTINUATION of the same stream is a
  connection error `PROTOCOL_ERROR`; a HEADERS of another stream is ignored, other types
  are processed.
- SETTINGS values (6.5.2): `ENABLE_PUSH` above 1 is relayed; `INITIAL_WINDOW_SIZE` above
  2^31-1 (`FLOW_CONTROL_ERROR`) is relayed to the legs. Check against 080.
- Stream WINDOW_UPDATEs coalesced for a leg cap at 2^31-1
  (`Leg#queueWindowUpdate`): a client overflowing a stream window within one coalesced
  update reaches the upstream as 2^31-1, which detects the overflow only if its window
  was above 0.

## Direction

One validation step in `Http2DemuxRelay#onFrame` before dispatch (length per type,
stream 0 rules, idle stream, header block in progress), ending the connection with
GOAWAY and the code like the 076 checks.

## Tests (`sluice-it`; watch each fail first)

- One per item: the GOAWAY code, or the stream reset; a well-formed counterpart keeps
  the connection.
