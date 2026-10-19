Difficulty: High

# h2 demux: asynchronous credential check per stream

`NettyHttp2DemuxRelay#routeLocked` calls `RouteAuthenticator#verify` synchronously under the
relay's loop lock. A slow check (bcrypt, pbkdf2) holds every other stream of the connection and its
ticker meanwhile; a check needing network I/O (OIDC code exchange in 159, token introspection) is
impossible there. HTTP/1.1 is unaffected: `ReroutingRelay` verifies on its own virtual thread.

## Shape

- `verify` may return a pending verdict (a future). The stream is parked: its head held, its DATA
  buffered within the stream window it was announced (no window granted back while parked), its
  budget charged like a stream waiting for `SETTINGS_MAX_CONCURRENT_STREAMS`.
- The verification runs off the loop (virtual thread); its completion re-enters the loop and
  resumes `routeLocked` from the verdict on (strip, force-http1, admit, leg).
- Bound: parked streams count against the same verification bound; a deadline (reuse
  `h2-stream-wait-timeout` or a new one) refuses a check that never completes.
- Failure modes: client resets the parked stream; client goes away / GOAWAY; connection teardown
  while a verification is in flight (the completion must find the stream gone and do nothing);
  verification throws; budget exhausted while parked.
- Once in place, move the slow encoders (bcrypt, pbkdf2) off the lock too, so no check holds the
  connection.

## Acceptance

- A test where a check blocks: the connection's other streams keep flowing meanwhile (assert on
  frames, not timing), the parked stream resumes and is relayed once the check completes.
- Each failure mode above has a test; leak check (`LeakCheck`) stays clean.
- README: drop any "a check holds the connection" caveat it gains in 157.
