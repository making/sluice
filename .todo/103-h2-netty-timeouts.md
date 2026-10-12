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
