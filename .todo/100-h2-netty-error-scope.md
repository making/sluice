Difficulty: High

# netty h2 demux, step 3: upstream error scope

Step 3 of the netty h2 demux migration (context and user decisions: 098). Depends on 099.

## Scope

- The error scope of the legacy relay on the netty engine: a stream lost to its upstream
  before its head gets 502, after it RST_STREAM INTERNAL_ERROR; an upstream connection
  error ends only its leg (GOAWAY to it), the client connection survives; streams past an
  upstream GOAWAY's last-stream-id get REFUSED_STREAM (`onGoAwayRead`).
- Netty closes right after an error GOAWAY (no half-close and grace); accepted by the user.
- Decide whether the 502 of a lost stream gets an error page like the 503 / 403 ones (a new
  template) or stays bodiless; relay-answered bodies now go through netty's remote flow
  controller, so the reason it was bodiless is gone. Record the decision here.
- Upstream frame validation now comes from netty (was 092): cover it.

## Tests

- The `H2DemuxUpstreamError*` subsets pass on netty, rewritten where they encode the grace
  or end-to-end behaviour.
- A relay-answered page never exceeds the client's window (was 093): a client with
  SETTINGS_INITIAL_WINDOW_SIZE 0 receives no DATA past its window.
- An upstream protocol error (e.g. SETTINGS on a stream) gives 502 and the connection
  survives.
