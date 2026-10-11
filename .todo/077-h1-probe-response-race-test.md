Difficulty: Medium

# No test pins that an h1 probe never lands inside a response head

Found while working on 064. `ReroutingRelay` decides on a probe (`Leg#probeable`: the
tracker at a response start) and writes it under the sink lock, and the inbound side
writes response bytes and feeds the tracker under the same lock. The E2E tests cover a
probe after a response, before one, and none within a stalled head or body, but not the
race: response bytes arriving between the decision and the write. Checking `probeable()`
outside the lock passes every test; the window is microseconds, so a stress test would
catch it rarely.

## Direction

A deterministic test needs a pause between the decision and the write: a package-private
hook in the probe path, or a unit harness for `ReroutingRelay` (fake `DuplexPipe`, a
`TunnelSession` over a recording sender) driving the inbound side while the probe is held.

## Test (watch it fail first)

With the decision moved outside the sink lock: the upstream's response head arrives while
the probe is held after its decision; the client must read the probe before the whole
head, never inside it.
