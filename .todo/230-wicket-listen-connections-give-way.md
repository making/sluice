Difficulty: Medium

# The wickets' `--listen`: connections left open still take every slot

The consoles and sluice-wicket-cf-api give way when all are taken (`sluice_wicket_server::accept`,
`.kb/connection-limits.md`). `--listen` keeps its slots to the end: whoever reaches the port can open
`--max-connections` (256) again every minute, the next waiting in the backlog. `--listen` has no gate, so every
request counts as admitted.

- wasi: `Claim::yields` while a connection has no request in progress (the sniff, http/1.1 between requests, h2
  with no stream open, as `listener::h2_quiet` knows); an h2 client told by a GOAWAY rather than cut
- firecracker: `Claim::yields` while `exchange::Exchanges::waits_on_client` (the head before `route.open` too), a slot
  asked ending the connection as `listener::relay` does at its minute. Verbatim connections cannot tell, and hold. A
  request whose body its client has not finished sending should yield as well: no budget bounds it here as on wasi, so
  it holds a slot and a VM for as long as a patient guest waits, dripped or not (`.kb/connection-limits.md`)
