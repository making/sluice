Difficulty: Medium

# netty h2 demux, step 4: upstream concurrency waiting and its deadline

Step 4 of the netty h2 demux migration (context and user decisions: 098). Depends on 100.

## Scope

- Streams beyond an upstream's MAX_CONCURRENT_STREAMS wait in `StreamBufferingEncoder`;
  one stream until the upstream's SETTINGS, applied through `encoder.remoteSettings(...)`
  (the encoder assumes 100 otherwise; see the spike).
- 091 on the netty engine: its decision (`sluice.limits.h2-stream-wait-timeout`, 30s
  default, 0 disables, metric reason `wait-timeout`) is implemented here by resetting the
  buffered stream, then REFUSED_STREAM to the client. Close 091 with this step.

## Tests

- The concurrency-waiting subset passes on netty.
- 091's tests on the netty engine (excluded for legacy, which never gets the deadline).

## Notes from 098

- `StreamBufferingEncoder`: `Hop.Builder#codec(c -> c.encoderEnforceMaxConcurrentStreams(true))`.
- The 091 deadline: schedule on `hop.ctx().executor()`; the loop's ticker runs it under the
  lock when due (no I/O needed, `LoopTest`).

## Notes from 099 (engine switch and routing, 2026-10-13)

- Today a head past the upstream's `MAX_CONCURRENT_STREAMS` fails the leg encoder's
  `writeHeaders` promise and the client gets `REFUSED_STREAM`
  (`NettyH2DemuxRelayE2ETest#aHeadTheLegCannotTakeIsRefused`); this step replaces it with
  waiting, so rewrite that test.
- `H2DemuxUpstreamStreamLimitE2ETest` is excluded for netty at class level naming this
  step.

## Notes from 100 (upstream error scope, 2026-10-13)

- A stream the leg never takes reaches `loseLocked` through the leg `writeHeaders`
  promise failure with `exchange.refused` set: `REFUSED_STREAM`. `StreamBufferingEncoder`
  fails a buffered stream's promise on the upstream's GOAWAY and on close, so waiting
  streams should be refused that way; check that a buffered stream never exists on the
  leg's connection (else `LegStreams#onStreamClosed` answers it `502` first).
- The upstream's GOAWAY marks the exchanges above its last stream id `refused` in
  `LegListener#onGoAwayRead` (netty closes them right after) and closes the leg
  gracefully as a task.
- Waiting for the upstream's concurrency limit, to put on netty here:
  `H2DemuxUpstreamErrorE2ETest#upstreamFrameOnAWaitingStreamEndsItsLegAndRefusesTheStream`
  (`@LegacyOnly`), the `waits` branch of `#upstreamClosingBeforeTheResponseHeadAnswers502`
  and the stream 5 assertion of `#upstreamGoAwayRefusesTheStreamsItDidNotProcess`
  (legacy only: today netty sends stream 5 before the upstream's SETTINGS arrive).
