Difficulty: Low

# h2 demux: an upstream's server push is relayed half-way

Found while working on 065 (by reading, not tested). The relay replays the client's
`SETTINGS_ENABLE_PUSH` to every leg (0x2 is in `KNOWN_SETTINGS`); a client that never
sends it leaves the default 1, so an upstream may push. Its PUSH_PROMISE goes through the
default branch of `Http2DemuxRelay#relayStreamFrame` as it is: the header block fragment,
encoded against the upstream's dynamic table, is neither decoded by the leg's decoder nor
re-encoded for the client, so either side's HPACK state can fall out of step. The pushed
stream (an even id) is never tracked: its DATA has been dropped since 062, its header
blocks since 065, so the client holds a promised stream that never completes.

## Direction

Announce `SETTINGS_ENABLE_PUSH` 0 in every leg's preface and drop the client's value
from what is replayed; a PUSH_PROMISE from an upstream is then a connection error
`PROTOCOL_ERROR` on the leg (RFC 9113 8.4), ending the relay like other upstream
protocol errors.

Once no push can reach the client, a client RST_STREAM or WINDOW_UPDATE on an even stream
is a frame on an idle stream (RFC 9113 5.1): have `Http2DemuxRelay#requireOpened` treat
even ids as idle. 082 left them unchecked (the counterpart cases "RST_STREAM / WINDOW_UPDATE
on an even stream" of `H2DemuxClientFrameValidationE2ETest` pin that), since a relayed
PUSH_PROMISE may have reserved one. A client `SETTINGS_ENABLE_PUSH` above 1 is a
connection error since 082, before anything is replayed.

## Tests (`sluice-it`; watch each fail first)

- A client announcing `SETTINGS_ENABLE_PUSH` 1: the upstream reads 0 in the leg's
  settings.
- An upstream sending PUSH_PROMISE anyway: the client never receives it, and the relay
  ends as for other upstream protocol errors.
- A client RST_STREAM or WINDOW_UPDATE on an even stream: GOAWAY `PROTOCOL_ERROR` (turn
  the two counterpart cases above into violations).

## Decision (2026-10-12)

Follows 081's decision: a PUSH_PROMISE from an upstream is a connection error on that
leg only (GOAWAY `PROTOCOL_ERROR` to the upstream, its streams handled as 081 decided);
the relay and the other legs go on. Do this after 081.
