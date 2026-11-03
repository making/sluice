Difficulty: Medium

# Wickets' consoles and sluice-wicket-cf-api: one address keeps taking all 64 connections

A quiet connection now closes after 60s (`sluice_wicket_server::http`), so 64 of them no longer stop a console or the
cf-api for good, but whoever can open TCP connections can open 64 again at once, for as long as it likes, signed in or
not, and hold a browser's or the CLI's connection in the backlog. The server's data plane has
`sluice.limits.max-connections-per-address` for this.

- a limit per source address in `sluice_wicket_server::accept::run`, one beyond it closed rather than waiting: what a
  browser opens at once (6, and its long poll) and a cf-api's wicket client must fit
- behind a proxy, or a cf-api in a wicket's VM, every client is one address (`/init`'s relay on the loopback): decide
  whether the limit is off there, or takes the forwarded address, or an idle connection gives way when all are taken
