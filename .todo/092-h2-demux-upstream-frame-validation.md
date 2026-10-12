Difficulty: Medium

# h2 demux: an upstream's malformed frames are relayed or ignored, not rejected

Found while working on 081 (by reading, not tested). 081 made an upstream's connection
error end only its leg (`Http2DemuxRelay.UpstreamError`, `Http2DemuxRelay#endLeg`), but
the leg's inbound side validates only what the todo listed (frame size, header block size
and interleaving, WINDOW_UPDATE length and increments, windows, DATA past the relay's
window, frames on streams not opened on the leg, ping floods, GOAWAY under 8 octets). The
rest of the upstream's framing is not checked, as 082 did for the client
(`Http2DemuxRelay#validate`):

- `relayStreamFrame`'s default branch writes any other frame type on a tracked stream to
  the client as it is: a SETTINGS, PING or GOAWAY on a non-zero stream (RFC 9113 6.5,
  6.7, 6.8: `PROTOCOL_ERROR`) reaches the client, which then ends its whole connection --
  the very scope 081 confines to the leg.
- `onUpstreamReset` relays an RST_STREAM of a length other than 4 as it is (6.4:
  `FRAME_SIZE_ERROR`), with the same effect on the client.
- SETTINGS of a length not a multiple of 6 or an ACK with a payload, PING of a length
  other than 8 (6.5, 6.7: `FRAME_SIZE_ERROR`) are ignored or half-applied
  (`Leg#applySettings` skips the trailing octets); DATA or HEADERS on stream 0 are dropped
  (6.1, 6.2: `PROTOCOL_ERROR`), the latter without decoding it.
- Inside a header block, a frame other than a CONTINUATION of the block's stream: only a
  second HEADERS is caught (`Leg#inboundHead`); a CONTINUATION of another stream, or
  without a block, is dropped (6.10: `PROTOCOL_ERROR`), desynchronizing the leg's HPACK
  decoder.
- A header block the leg's decoder cannot decode becomes an empty head
  (`Leg#inboundHead`) instead of a connection error `COMPRESSION_ERROR` (4.3).
- Padding past the payload of DATA or HEADERS (6.1, 6.2: `PROTOCOL_ERROR`).

## Direction

Mirror `Http2DemuxRelay#validate` for the leg's inbound side: throw
`UpstreamError(<code>, ...)` before the frame has any effect, and never write an
upstream's connection-level frame to the client. Even streams belong to 080.

## Tests (`sluice-it`; watch each fail first)

For each case, as in `H2DemuxUpstreamErrorE2ETest`: the upstream reads a GOAWAY with the
RFC's code, the client gets the leg's streams answered (`502`) or reset (`INTERNAL_ERROR`)
and no GOAWAY, and a stream of another route completes. The SETTINGS-on-a-stream and
RST_STREAM-of-5-octets cases must show the client's connection surviving (today it gets
the malformed frame).
