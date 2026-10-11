Difficulty: High

# wasmlet: an h2c connection half-closed mid-stream drops its responses

Found while working on 055 (verified by a throwaway test, not kept). http/1.1 now
answers a request the server's CLOSE follows (hyper `half_close`). h2c does not: hyper's
h2 reads the EOF and h2 0.4.20 `recv_eof` fails every open stream, so a request with the
CLOSE right behind it gets only SETTINGS / SETTINGS ACK, then CLOSE.

The current server never does this: the h2 demux half-closes a leg only once its
responses ended, and ends the relay (ERROR) when a request is unfinished. A server
predating the demux relays raw h2 and forwards a client's FIN as CLOSE at once. Cancel
this if such servers need no support.

## Why not the obvious fixes

- `graceful_shutdown` on CLOSE (with the EOF held back): h2 sends GOAWAY(2^31-1) plus a
  PING and closes only after the PING ACK (`go_away.should_close_on_idle`), which a
  half-closed peer can never send. The connection would stay open.
- Hold the EOF while requests are in flight (counted in the service, the guard living in
  the response body): h2 may read the EOF in the same poll as the HEADERS before it,
  and releasing the EOF while response DATA is still queued in h2's send buffer may drop
  it. Read `h2::proto::connection` / `streams::recv_eof` before choosing.

## Test (`wasm_host.rs`; watch it fail first)

An h2c request (preface, empty SETTINGS, HEADERS stream 1 END_STREAM|END_HEADERS with
the HPACK block `82 86 84 41 01 74`) followed by the server's CLOSE: the wasmlet sends a
HEADERS frame and the DATA with END_STREAM on stream 1, then CLOSE alone. The probe that
found this parsed the frames out of `data_bytes(&frames, 1)`.
