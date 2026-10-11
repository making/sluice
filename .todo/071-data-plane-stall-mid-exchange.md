Difficulty: High

# Data plane: a peer stalling mid-exchange holds its slot

Found while working on 060. The keep-alive idle timeout stops while an exchange is in
flight: an HTTP/1.1 response is owed until the relay wrote its last byte to the client, a
request body is in flight until its last byte arrived, an h2 stream is open until both
sides ended it. Nothing bounds an exchange that stops moving, so a peer can hold its slot
of `sluice.limits.max-connections` (and, for h2, the leg and the queue behind it) for good:

- Not reading: a client whose response outgrows the socket buffers and that never reads
  blocks the relay's write to it (`ReroutingRelay#writeToClient`,
  `Http2DemuxRelay#writeToClient`, `StreamRelay`). A response that fits the buffers
  completes and is then cut by the idle timeout; only the large ones escape.
- Not granting (h2, since 062): a client that reads but never grants its connection
  window keeps the response data waiting in the relay's response queue
  (`Http2DemuxRelay#responseQueue`) and the stream open, so the idle timeout never
  starts; after the upstream left, its leg waits in `awaitResponsesWritten`. A client gone
  after its TCP half-close is noticed by the probe PINGs; one that keeps the connection
  open is not.
- Not sending: a request head announcing a body (`Content-Length`, chunked) followed by
  nothing, or by a byte now and then; on h2, a stream opened without END_STREAM and never
  continued. The upstream waits for the body, so no response ends the exchange either.

nginx bounds these with `send_timeout` / `client_body_timeout` (60s between two
successful operations), Tomcat with the socket write timeout and
`connectionUploadTimeout`.

An idle h2 connection whose client stops reading is covered since 060: the GOAWAY at the
idle timeout runs on a teardown thread and the transport is closed under the blocked
write after `KeepAliveDeadline.TEARDOWN_GRACE`.

## Direction

Per client connection, a deadline on progress rather than on idleness: armed while the
relay waits to write to the client, or waits for request bytes it knows are due, and
pushed back by every successful write / read; its expiry closes the transport. Blocking
socket writes take no timeout (`SO_TIMEOUT` covers reads only), so it has to be a
scheduled close like the head deadline (`ConnectionLimits.KeepAliveDeadline` can host a
further phase). Mind that the relay writes from several threads under one sink lock: the
deadline is the oldest write waiting, not the one holding the lock. An upstream that is
slow to answer is not the client's stall: waiting for a response must stay unbounded
(long polling).

## Tests (`sluice-it`; watch each fail first)

- h1: a client requesting a 16MiB response and never reading is closed within the timeout
  plus a margin, and its slot released; the same for one h2 stream, and for an h2 client
  that reads but never grants its connection window.
- h1: a request head with `Content-Length: 10` and no body is closed at the timeout; one
  dripping a byte every second past it is closed too (progress per byte must not keep it
  alive forever -- decide between a minimum rate and an overall body deadline); h2: a
  stream opened without END_STREAM and never continued.
- A client reading slowly but steadily (one 16KiB read per second), and an upstream taking
  longer than the timeout to answer, are not cut.
