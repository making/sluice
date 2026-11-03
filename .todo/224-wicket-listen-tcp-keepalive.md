Difficulty: Low

# Wickets' `--listen`: a peer vanished without a FIN or reset holds its connection slot for good

Seen while working on a client gone from `--listen`. `sluice_wicket_tunnel::listener::run` only sets `TCP_NODELAY`.
A peer gone silently (a host removed, a NAT entry dropped) never closes the connection: a request in flight ends at
its budget, but an idle keep-alive connection, or a verbatim relay on sluice-wicket-runtime-firecracker, keeps its
`--max-connections` slot until the wicket restarts. The server's data plane has `sluice.limits.tcp-keep-alive` for
this reason (60s idle, then 3 probes 10s apart; "Vanished peers" in `.kb/connection-limits.md`).

- the same keepalive on every socket `listener::run` accepts, both wickets at once. `socket2` is in the lock file
  already, through tokio
- check that the verbatim relay's idle timeout, if the firecracker wicket has one, does not cover this already
- the test: the socket's options as accepted (`SO_KEEPALIVE`, the idle, interval and count) in the listener's unit
  tests; a vanishing peer cannot be simulated cheaply
