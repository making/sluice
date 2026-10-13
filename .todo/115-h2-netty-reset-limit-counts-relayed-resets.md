Difficulty: Medium

# netty h2 demux: netty's limit on the resets a server sends ends client connections on the relay's own resets

Found while rewriting the per-hop tests (105).

## Problem

netty's server endpoint wraps its encoder in `Http2MaxRstFrameLimitEncoder` by default
(`AbstractHttp2ConnectionHandlerBuilder`: `DEFAULT_MAX_RST_FRAMES_PER_CONNECTION_FOR_SERVER`,
200 per 30s): the 201st RST_STREAM it writes within the window, other than `CANCEL` and
`NO_ERROR`, is a connection error, GOAWAY `ENHANCE_YOUR_CALM`. It guards against a client
making the server reset streams ("MadeYouReset"). Shown for a client that keeps sending
DATA on a stream reset for its window: `H2DemuxStallE2ETest#streamOverItsWindowIsResetAndTheFloodPastItEndsTheConnection`.

The relay writes its own resets to the client through the same encoder
(`NettyHttp2DemuxRelay#reset` -> `hop.encoder().writeRstStream`), so, read from the source
and not tested, they count too: `REFUSED_STREAM` (budget, waiting, wait deadline, a leg
that did not open, streams above an upstream's GOAWAY), `INTERNAL_ERROR` (a stream lost
after its response began), `PROTOCOL_ERROR` (malformed heads), `ENHANCE_YOUR_CALM`
(trailers past the budget), and every upstream reset relayed with its code. A busy client
connection whose upstream refuses or resets 200 streams in 30s -- an upstream restarting,
the budget exhausted -- is then ended as a whole, its other streams with it.

## Directions

- Decide which resets should count: only those netty's codec answers to the client's own
  frames (the attack the limit is for), not the relay's or the upstream's.
- Options: turn the encoder limit off on the client endpoint
  (`encoderEnforceMaxRstFramesPerWindow(0, 0)`) and count the codec's stream errors in
  the client listener / `onStreamError` instead; or write the relay's resets past the
  limiting encoder (the frame writer), keeping netty's stream bookkeeping
  (`Http2ConnectionHandler#resetStream` closes the stream once written).

## Tests

- First, failing: 201 streams refused or reset by the relay within 30s (upstream resets
  with `INTERNAL_ERROR`, or wait-deadline refusals) end the client connection today; after
  the fix the connection goes on.
- The client flood case above still ends the connection.
