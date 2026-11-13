Difficulty: High

# sluice-wicket-runtime-wasi: a wicket-wide memory budget

The wicket admits by instance count only (`--max-instances`, `Instances`); its listing's budget says so
(`console.rs`: `max_memory_mib: None, memory_mib: None`), while the Firecracker wicket has
`--max-memory-mib`. The README's bound, `--max-instances` x the largest `memory-mib` (16 GiB by default),
is never compared with the host.

Outside any guest's linear memory and unbounded:

- compiled code: every live component, no limit on compiles at once (`spawn_blocking`), recompiled at
  every start (no disk cache)
- a component upload: 128 MiB read into memory, x 64 console connections, carried through the edit
  channel (`routes.rs`)
- a remote or file component fetched whole (`response.bytes()`), no size limit
- `--max-connections` applies to the tunnel and to `--listen` each, so twice over with both

To decide: `--max-memory-mib` as the Firecracker wicket's (shared arithmetic where it fits), what an
instance counts (its cap, or its use with 299 fixed and 291's numbers), a reserve for compiled code, the
upload streamed to a file under its digest as the files tar is, compiles at once bounded.

Acceptance: an instance beyond the budget waits for a slot or is refused as one beyond `--max-instances`;
the listing reports the budget; existing admission tests extended; README's sizing paragraph.
