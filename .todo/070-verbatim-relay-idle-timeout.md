Difficulty: Medium

# Data plane: verbatim relays keep no idle deadline

Found while working on 060. The keep-alive idle timeout (`sluice.limits.idle-timeout`)
covers only the HTTP the data plane parses: plaintext and TLS-terminated HTTP/1.1 and h2.
A connection relayed verbatim -- TLS passthrough, a hostless TLS-terminated connection
(RESP over TLS, say), a tcp route -- has no request boundaries to find and keeps no
deadline once its head phase is over, so a peer can hold a slot of
`sluice.limits.max-connections` for good by going quiet (`StreamRelay` in
`DataProxyServer#relay`, `TcpPortGateway`).

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
