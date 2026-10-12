Difficulty: High

# h2 demux: an upstream's protocol error ends the whole client connection

Found while working on 076. Every protocol error of an upstream ends the relay
(`Http2DemuxRelay#abort`): the client's connection closes without a GOAWAY, taking the
streams of every other route on it down too (coalesced connections), and the offending
upstream gets no GOAWAY either. That covers connection errors (frame above 16KiB, header
block past the limit or interleaved, window past 2^31-1, WINDOW_UPDATE of a length other
than 4 or with an increment of 0 on stream 0, DATA past the relay's window) and stream
errors the relay treats as connection errors (RFC 9113 5.4.2): a stream window past
2^31-1, a stream WINDOW_UPDATE with an increment of 0. The client did nothing wrong.

## Direction

- Stream errors: reset only the stream, the code to the upstream (`FLOW_CONTROL_ERROR`,
  `PROTOCOL_ERROR`), a code to the client, like `Http2DemuxRelay#resetStream` with the
  roles swapped (it is called by the outbound parse only today; 079 covers its races).
- Connection errors: GOAWAY with the code to the upstream and end only its leg; its
  streams are reset to the client, the other legs go on. Today a leg whose upstream
  fails with a response owed ends the relay (`pumpInbound`, `Leg#retire`); this changes
  that rule for the failing leg.

080 (an upstream's PUSH_PROMISE as a protocol error) follows whichever rule this sets.

Also unvalidated today: frames an upstream sends on a stream the relay has not opened on
it -- a stream waiting for the upstream's SETTINGS_MAX_CONCURRENT_STREAMS (idle on the
upstream, RFC 9113 5.1: a connection error `PROTOCOL_ERROR`) -- are handled as if the
stream were open (a response head is relayed, an END_STREAM forgets the stream as owed).
A leg retiring cleanly refuses its waiting streams with `REFUSED_STREAM` (the upstream
never saw them, RFC 9113 8.7); a connection error ending only the leg should refuse them
likewise rather than give them the code chosen for lost streams.

**User decision needed**: the code the client gets for a stream lost to its upstream's
error (`INTERNAL_ERROR`, `CANCEL`, ...), and whether a connection error should still end
the relay while the client has nothing else open.

## Tests (`sluice-it`; watch each fail first)

- An upstream WINDOW_UPDATE with an increment of 0 (and one past 2^31-1) on a stream:
  the client gets RST_STREAM on that stream only, the upstream RST_STREAM with the code;
  another stream on another route of the same connection completes.
- An upstream connection error (each kind above) on one route: the upstream gets GOAWAY
  with the code, the client gets the leg's streams reset and no GOAWAY, and a stream of
  another route completes. Update `H2DemuxUpstreamFlowControlE2ETest` /
  `H2DemuxFrameLimitsE2ETest`, which pin the current end-the-relay behavior.

## Decision (2026-10-12)

- Stream lost to its upstream's error, response head not yet relayed: the relay answers
  it with a 502 response (as it answers unrouted streams), so the client sees an HTTP
  error rather than a reset. Head already relayed: RST_STREAM `INTERNAL_ERROR`
  (`CANCEL` would read as a deliberate cancellation). Streams the upstream never saw
  (waiting for its concurrency limit): `REFUSED_STREAM`, retryable (RFC 9113 8.7).
- A connection error ends only the failing leg, also when the client has nothing else
  open; the keep-alive idle timeout closes an idle client connection afterwards. One
  rule, no special case.
