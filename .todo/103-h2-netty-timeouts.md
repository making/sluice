Difficulty: Medium

# netty h2 demux, step 6: stall, idle and keep-alive timeouts

## Plan change (2026-10-13, the user)

Production is a test deployment; breaking changes are fine, and no code is written just to
keep two engines side by side. New order: 102 -> 107 (remove the legacy relay now, netty
only) -> 103, 104, 105 (in parallel, each re-enables its disabled tests on netty) -> 106.

Depends on 107 (no engine switch; re-enable the tests 107 disabled for this behaviour). Can run in parallel with 104 and 105.

Step 6 of the netty h2 demux migration (context and user decisions: 098). Depends on 102.

## Scope

- The stall timeout (`sluice.limits.stall-timeout`, `stall-min-bytes`), the idle and
  keep-alive timeouts and the PING probe after the client's EOF on the netty engine; the
  timers read netty state (active streams, windows, `isWritable`) and close the transport
  under a blocked write.
- Netty acks PING as it reads; tests that encode PING ordering are rewritten.

## Tests

- The stall / idle / keep-alive subsets pass on netty, with margins that hold on Linux CI.
- Write progress towards a client taking bytes slowly: no test pins it for the legacy h2
  relay's write pieces. `StallTimeoutE2ETest#aClientTakingOneWritePieceASecondIsNotCut`
  pins it for HTTP/1.1 through `DataProxyServer#clientPipeSeam` (a client pipe accepting
  16KiB per second); netty writes do not block, so the netty engine needs its own test of
  what counts as progress while its outbound buffer drains slowly.

## Notes from 098

- Timers: `hop.ctx().executor().schedule(...)`; the loop's ticker sleeps until the earliest
  task of its hops and runs it under the lock. Never call `EmbeddedChannel#close` (it
  cancels every scheduled task of the channel); use `Hop#closeLocked` / `abortLocked`.
- Nothing in the hop cuts a writer blocked on a peer that does not read while the channel
  is open; only after the channel closed does the close grace (1s default) close the pipe.
  The stall timeout has to abort the hop. Progress: `Hop#bytesWritten()`, `queuedBytes()`,
  `writable()`.

## Notes from 099 (engine switch and routing, 2026-10-13)

- The netty relay only disarms the `KeepAliveDeadline` when it finishes: no idle, stall or
  keep-alive timeout yet, so a client may hold an idle connection open for good (why the
  engine is documented as not for production).
- The client's EOF aborts the client hop and with it every leg at once
  (`clientClosedLocked`); there is no half-close drain and PING probe as in the legacy
  relay.
- Excluded for netty naming this step: `H2DemuxStallE2ETest`, `H2DemuxStallTimeoutE2ETest`
  (classes), the idle and stall tests of `H2DemuxConnectE2ETest`. Not parameterized at all
  (mixed h1 / h2 classes): `KeepAliveIdleTimeoutE2ETest`, `StallTimeoutE2ETest` -- put
  their h2 tests on both engines here (`@H2EngineTest`, `@LegacyOnly` for the h1 ones).

## Notes from 100 (upstream error scope, 2026-10-13)

- `H2DemuxUpstreamErrorE2ETest#aConnectionErrorWithNothingElseOpenLeavesTheConnectionToTheIdleTimeout`
  is `@LegacyOnly` naming the idle timeout.
- A leg that received a GOAWAY closes through netty's graceful close
  (`Hop#closeLocked`): it waits for its other streams up to the handler's graceful
  shutdown timeout, netty's default 30s (not configured). A stream the upstream never
  finishes holds the leg until then; the stall timeout should bound it.
- A relay-answered page (`502`, `503`, `403`) waiting for a client window of 0 keeps the
  client stream and its exchange with no deadline.

## Notes from 101 (concurrency waiting, 2026-10-13)

- `H2DemuxUpstreamStreamLimitE2ETest#halfClosedClientGetsTheResponseOfItsWaitingStream` is
  `@LegacyOnly` naming the half-close drain (the client's EOF ends the netty relay at once).
- The legacy stall timeout counts a waiting stream only while its client holds window to
  send its request (`requestDueLocked`); `Exchange#waiting` gives the netty engine the same
  test. The wait deadline (`sluice.limits.h2-stream-wait-timeout`) is a client-hop timer
  (`waitLocked`), cancelled when the stream opens, leaves the buffer or is reset.

## Notes from 102 (flow control and budget, 2026-10-13)

- `@LegacyOnly` naming the half-close drain: `H2DemuxUpstreamFlowControlE2ETest`
  `#largeResponseReachesAHalfClosedClient`, `#nettyUpstreamAnswersAHalfClosedClient`,
  `#queuedRequestDataIsWrittenBeforeTheHalfClose`,
  `#clientGoneAfterTheUpstreamsHalfCloseReleasesTheUpstreamConnection`. Naming PING
  ordering: `H2DemuxLegWriterE2ETest#upstreamPingIsAckedAheadOfQueuedData` (it also needs
  the tunnel byte counter, 104).
- The stall timer must not count a stream whose window the relay withholds (README:
  "an h2 request whose window the relay withholds" does not count): the relay's
  `withheld` flag tells it. With per-hop windows a request waits on the client only while
  the client holds window on the client hop; what the relay holds for the upstream is not
  the client's stall.
- The relay reviews its accounts after each action of its loop (`Loop#afterSettle`),
  and its hops run a loop action after each writer batch while it queues anything
  (`Hop#watchWrites`): a timer that reads `Hop#queuedBytes()` sees fresh numbers there.

## Notes from 107 (legacy relay removed, 2026-10-13)

Every h2 connection runs on netty now; no engine switch. Disabled (`@Disabled`, reason
naming the missing behaviour) until this step; re-enable each, then drop the "none yet" /
"not yet" wording below:

- Idle timeout: `KeepAliveIdleTimeoutE2ETest#aTlsTerminatedH2ConnectionWithNoOpenStreamIsClosedAtTheIdleTimeout`,
  `#anH2ConnectionWithNoOpenStreamIsClosedWithAGoAwayAtTheIdleTimeout`,
  `#anH2ConnectionClosedByItsClientLeavesNoDeadlineBehind`, `#pingsDoNotKeepAnIdleH2ConnectionOpen`,
  `#anH2StreamOpenLongerThanTheIdleTimeoutIsRelayed`, `#aStreamTheRelayAnswersItselfStartsTheIdleTimeoutOver`,
  `#anIdleH2ConnectionWhoseClientStoppedReadingIsClosedAfterTheTeardownGrace`,
  `#anH2ConnectionOpeningAStreamEverySecondStaysOpen` (passes on netty, vacuously: nothing
  closes an idle h2 connection); `H2DemuxConnectE2ETest#aConnectionOpeningWithAConnectIdlesOutAtTheIdleTimeout`;
  `H2DemuxUpstreamErrorE2ETest#aConnectionErrorWithNothingElseOpenLeavesTheConnectionToTheIdleTimeout`.
- Stall timeout: `H2DemuxStallTimeoutE2ETest` (class; its "not cut" tests pass on netty
  vacuously, the "closed at the stall timeout" ones fail);
  `H2DemuxConnectE2ETest#aRequestStalledOnAConnectionOpeningWithAConnectIsCutAtTheStallTimeout`.
- Half-close drain: `H2DemuxUpstreamFlowControlE2ETest#largeResponseReachesAHalfClosedClient`,
  `#nettyUpstreamAnswersAHalfClosedClient`, `#queuedRequestDataIsWrittenBeforeTheHalfClose`,
  `#clientGoneAfterTheUpstreamsHalfCloseReleasesTheUpstreamConnection`;
  `H2DemuxUpstreamStreamLimitE2ETest#halfClosedClientGetsTheResponseOfItsWaitingStream`;
  `H2DemuxStallE2ETest#halfCloseAfterAFinishedRequestStillGetsTheResponse`.
- PING ordering: `H2DemuxLegWriterE2ETest#upstreamPingIsAckedAheadOfQueuedData` (also waits
  on the tunnel byte counter, 104).
- Docs saying "none yet" for h2 that this step corrects: README (`idle-timeout` /
  `stall-timeout` rows, "Connection limits" idle / stall / half-close probe paragraphs, the
  "h2 demux ..., not yet there" and "h2 flow control" bullets, the h2 upstream errors
  bullet's idle sentence), `SluiceServerProperties.Limits` javadoc (`idleTimeout`,
  `stallTimeout`), `application.properties` (`idle-timeout`, `stall-timeout`).
- `ConnectionLimits.KeepAliveDeadline#watchStalls` (the sampled stall clock, unit-tested in
  `ConnectionLimitsTest`) was kept for this step although nothing calls it now.
