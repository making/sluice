Difficulty: Medium

# Data plane: verbatim relays keep no idle deadline

Found while working on 060. The keep-alive idle timeout (`sluice.limits.idle-timeout`)
covers only the HTTP the data plane parses: plaintext and TLS-terminated HTTP/1.1 and h2.
A connection relayed verbatim -- TLS passthrough, a hostless TLS-terminated connection
(RESP over TLS, say), a tcp route -- has no request boundaries to find and keeps no
deadline once its head phase is over, so a peer can hold a slot of
`sluice.limits.max-connections` for good by going quiet (`StreamRelay` in
`DataProxyServer#relay`, `TcpPortGateway`).

The stall timeout (071, `sluice.limits.stall-timeout`) covers parsed HTTP only, so a
verbatim relay whose client stops reading also holds its slot: the relay's write blocks
and nothing moves either way. A byte-level idle timeout covers that case as well, provided
its expiry closes the transport under the blocked write (a blocking socket write takes no
timeout) -- as `ConnectionLimits.KeepAliveDeadline` does with its teardown grace. The same
holds for an upgraded HTTP/1.1 connection (WebSocket) and for HTTP/1.1 input the parser
cannot follow (072), both relayed verbatim with no deadline.

## Needs a decision

A byte-level idle timeout (nothing relayed in either direction for N) would bound them,
but legitimate long-idle connections are common here: a Redis or database pool keeps
connections idle for minutes, and a TLS-passthrough HTTP/2 client may idle between
streams. Options: off by default and per route (a client-side upstream setting carried in
the route), one server-wide default with a per-route override, or none and document the
exposure. The user decides before implementation.

## Tests (`sluice-it`; watch each fail first)

- With the timeout set, a tcp route connection with no bytes either way for the timeout
  is closed and its slot released (gauge); one echoing every second stays open.
- The same for a TLS passthrough connection.
- With the timeout unset (or off for the route), an idle connection stays open.

## Decision (2026-10-12)

- A byte-level idle timeout for verbatim relays (nothing relayed in either direction for
  N): off by default, so pools that idle legitimately are not cut.
- Per route, set by the client on the upstream it advertises (the client operator knows
  the protocol), carried in the advertise (proto field; 0 / absent = server default).
- A server-wide default (`sluice.limits.*`, default off) applies to routes that set
  none, so a server operator can bound anonymous exposure for every route.
- README states the exposure while both are off.
