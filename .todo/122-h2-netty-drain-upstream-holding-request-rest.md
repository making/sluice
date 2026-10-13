Difficulty: Low

# netty h2 demux: a half-close drain held by an upstream that does not take the request rest is untested

Found while adding the half-close drain (103).

## Problem

After the client half-closes, the drain waits for every stream and leg to end. A stream
whose response ended while the relay still holds request DATA the upstream does not read
keeps its leg open, so the drain does not finish. The client hop has no open stream then,
so the connection should idle and end at `idle-timeout`; read from the code, not shown by
a test.

## Tests

- First, a test: a client sends a request body larger than the upstream's window, ends
  its side, and the upstream answers the whole response without reading the body. The
  client connection ends at the (short) idle timeout, the leg is released and
  `sluice_data_connections_idle_closed_total` counts it.
- Without the idle bound in that state (mutation) the test fails.
