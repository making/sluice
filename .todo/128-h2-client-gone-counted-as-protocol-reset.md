Difficulty: Medium

# h2 demux: a client vanishing mid-request is counted as a protocol error

Found by the measurements of 106 (`.todo/artefacts/106/RESULTS.md`, finding 4).

## Problem

In the 106 harness runs, an h2 client that closed its connection with uploads in flight
added counts that should mean misbehaving peers:

- `sluice_h2demux_streams_reset_total{reason="protocol"}`: one per client stream in flight
  (1 with 1 stream, 10 with 10 connections or 10 streams);
- with 10 streams on one connection, or with a 20 ms tunnel RTT: also one
  `reason="upstream"` per stream and one `sluice_h2demux_legs_failed_total`. That last
  counter means a leg sent its upstream a GOAWAY with an error code.

Downloads ended the same way counted nothing. With the server at DEBUG, netty logs this on the
client hop's reader thread as the connection ends (one 2 s upload, `bench.py resets`):

```
i.n.h.c.http2.Http2ConnectionHandler : [id: 0xembedded] Sending GOAWAY failed: lastStreamId
'2147483647', errorCode '2', debugData ''. Forcing shutdown of the connection.
io.netty.channel.StacklessClosedChannelException
```

So something drives netty's `onConnectionError(INTERNAL_ERROR)` on the client hop at the
client's EOF mid-request. The streams it resets then look reset by the codec
(`resetByCodecLocked`). Their legs then see stream errors, and one of them a connection error.
The Grafana dashboard (123) would show a client going away as protocol errors and failed legs.

## Scope

- Find what raises the INTERNAL_ERROR: the reader's EOF path, the close, or an exception in
  the pipeline. Reproduce it in a sluice-it test (client sends a partial request body on two
  streams, then closes the socket), assert the counters stay 0, and watch it fail first.
- Make a client's EOF or reset count as what it is, and keep a leg closed for that reason off
  `legs_failed`. Check the GOAWAY each leg sends its upstream in that case: `NO_ERROR` or
  `CANCEL`, not `INTERNAL_ERROR`.
