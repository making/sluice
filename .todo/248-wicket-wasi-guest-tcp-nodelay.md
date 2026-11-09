Difficulty: Medium

# sluice-wicket-runtime-wasi: a guest's TCP connections wait on delayed ACKs (no `TCP_NODELAY`)

A guest's `wasi:sockets` TCP socket is made by wasmtime-wasi (50), which sets `TCP_NODELAY` nowhere, and
wasi:sockets has no option a guest could set it with. A guest writing a message in more than one write
(rontolisp's cl-postgres does) has its second segment held by Nagle until the first is ACKed, and the peer
delays that ACK (40ms on Linux).

Seen with rontolisp's Shiori against PostgreSQL 18 in the cf-api's Docker Compose (`.kb/shiori-on-wasi-wicket.md`):
the server's statement log shows 84ms from each `parse` to its `bind` and 43ms to the next `parse`. A page of
six statements takes 0.48s, the first start (schema and sample data) 15.6s. With `quickack 1` on the
PostgreSQL container's routes (delayed ACK off on the peer's side) the same page takes 0.025s and the first
start 0.24s.

Set `TCP_NODELAY` on every TCP socket the wicket makes for a guest (connected and accepted), as Go's `net`
does by default: wasmtime-wasi has no hook for it (`socket_addr_check` sees the address alone), so find the
least intrusive way (an upstream change and a pinned patch meanwhile, or a wrapper of the p3 sockets host).
Pin it with a test: a guest writing a message in two writes to a peer that delays its ACKs answers within
a bound far below 40ms per round trip (a hang guard, not a benchmark: assert the segments or the socket
option if it can be read). Then remove the `budget-ms` annotation and the latency section from the .kb
file above if they no longer apply, and measure again.
