# PROXY protocol

Full behavior of the two server properties; README "Configuration (server)" has the summary.

## `sluice.data-proxy-protocol`

Parses the PROXY protocol (v1 / v2) header an SNAT front end prepends on the data plane (data port and tcp route ports): the header is consumed before routing / relay (never forwarded) and its source address becomes the connection peer for access control and the access log; a malformed header fails the connection. Headerless connections are served, but detecting the header takes a connection's first byte and a tcp route dials its upstream only after it: a protocol whose server speaks first needs the header (a silent connection is closed at `head-timeout`). The header is taken from any peer: expose the ports only to the front end.

## `sluice.control-proxy-protocol`

The same on the gRPC control plane (`spring.grpc.server.port`), ahead of its TLS: the header's source becomes the tunnel session's remote address (console, logs) and the source address of the per-address tunnel limits; headerless connections are served; a malformed header, or a head undecided within `head-timeout`, closes the connection. The control plane then runs on NIO event loops of its own instead of gRPC's.
