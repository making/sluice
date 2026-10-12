Difficulty: Low

# TunnelStallWatchdogE2ETest: "the public peer is still held back" failed once

Seen while working on 081: one full `./mvnw verify` on macOS (2026-10-12) failed
`TunnelStallWatchdogE2ETest#anUpstreamThatDoesNotReadIsNoStall` at the precondition
`writer.isAlive()` ("the public peer is still held back"); the class passed twice in a
row alone afterwards, and 081 touched nothing on that path (raw TCP gateway, no h2).

The writer thread ends either because its 4MiB write completed or because the write
failed -- the test swallows the `IOException` -- and the failure message cannot tell
which, nor whether the server closed the public socket (a deadline or limit of the data
plane) or the kernel buffers absorbed what the window did not.

## Direction

Diagnose before fixing: record the writer's outcome (bytes written, the exception) and
assert on it in the precondition's message; check which server-side timeout or limit can
close a gateway connection whose upstream reads nothing within the test's 3x
`TIMEOUT`. Then pin the precondition on what actually holds the peer back.
