Difficulty: Low

# sluice-wicket-runtime-firecracker: rethink a long upload on `--listen` giving way when all slots are taken

Left as it is for now. The user asks only to look again for a better idea; nothing to change unless one is found.

With `--max-connections` all taken, a request whose body its client did not finish may give way, answer begun or not
(`.kb/connection-limits.md`, `Standing::Sending`): a long upload or a stream both ways is lost when it is its source's
turn. It may because no budget bounds it, unlike on sluice-wicket-runtime-wasi, so it would hold a slot and a VM for as
long as a patient guest waits, and dripped bytes cannot be told from an upload in earnest.

Weigh, against that drip:

- order: those with no request in progress go first, one still sending only when none is left
- a minimum rate, as Apache's `mod_reqtimeout` `MinRate`, rather than "any bytes moved": what rate, kept per what
- a body's progress against its `Content-Length`; a chunked one has none
- a bound on the route, as `budget-ms` on wasi

Report what was found: a change, or that this stays and why.
