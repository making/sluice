Difficulty: Low

# Tunnel clients reconnect on a backoff without jitter

Found while bounding the pending tunnel streams (119).

## Problem

The Java client reconnects with exponential backoff 1s..30s (`NodeConnection`, reset once
the stream is accepted), with no jitter; the wasmlet's reconnect loop is the same shape
(read from the code, untested). When a server restarts, or refuses streams past
`sluice.limits.max-pending-tunnel-streams` (`RESOURCE_EXHAUSTED`), every client retries on
the same schedule, so the refusals repeat in waves.

## Scope

- Add jitter to both clients' reconnect delay (e.g. full or equal jitter on the capped
  exponential), keeping the bounds documented in README.

## Tests

- Java: the delays of consecutive failures stay within the documented bounds and are not
  all equal across clients / attempts (seeded randomness or an injected source).
- wasmlet: a unit test of the delay function.
