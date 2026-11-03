Difficulty: Medium

# The wickets' `--listen`: connections left open still take every slot

The consoles and sluice-wicket-cf-api give way when all are taken (`sluice_wicket_server::accept`,
`.kb/connection-limits.md`). `--listen` keeps its slots to the end: whoever reaches the port can open
`--max-connections` (256) again every minute on sluice-wicket-runtime-wasi, and for as long as a guest keeps
them on sluice-wicket-runtime-firecracker (`.todo/229-*`), the next waiting in the backlog. `--listen` has no gate,
so every request counts as admitted.

- wasi: `Claim::yields` while a connection has no request in progress (the sniff, http/1.1 between requests, h2
  with no stream open, as `listener::h2_quiet` knows); an h2 client told by a GOAWAY rather than cut
- firecracker: a relay is verbatim, so only a connection still in its head (`HEAD_TIMEOUT`) knows it waits on its
  client. Past that, decide with todo 229 whether anything may yield
