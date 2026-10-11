Difficulty: Medium

# A public client gone after its FIN holds a stalled wasm request until the budget

Found while working on 055. The wasmlet now treats the server's CLOSE (the public
client's FIN) as a half-close and answers the request; only the server's ERROR counts as
the client leaving. TCP cannot tell a FIN from `shutdownOutput` from one from `close()`:
the server learns that the client is gone only when a write to it fails (the second write
after the client's RST). A client that closes entirely while its guest stalls is
therefore held until the request's budget (default 10s), where before 055 the CLOSE
freed it at once. Manual check (jar of HEAD, hello.wasm): `/stall`, head and first
chunk read, then `close()`: freed by the budget after 10s; with `SO_LINGER 0` (RST):
freed at once.

## Direction

Only a write can probe the client. The h2 demux does this with PING; http/1.1 has no
such frame. Before the response head, the server's h1 relay could write a 1xx interim
response after the client's FIN (RFC 9110 15.2: a client must parse 1xx responses; not
for HTTP/1.0 clients). Once the head is out, nothing can be injected into a body, so
mid-body the budget stays the bound. Weigh it against the cost: 1xx handling bugs in
clients, and the relay has to keep the response stream framed.

## Tests (`sluice-it`; watch each fail first)

- An HTTP/1.1 client sends a request to a route whose upstream never answers, half-closes
  and then closes: the server fails the tunnel connection (ERROR) within the probe
  interval, which it does not do today.
- A client that only half-closes still gets the response, with the probe's 1xx before it.
