Difficulty: Medium

# netty h2 demux, step 6: stall, idle and keep-alive timeouts

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
