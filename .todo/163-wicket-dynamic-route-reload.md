Difficulty: High

# sluice-wicket-wasi: reload routes and components without a restart

Changing or adding a wasm route needs a wicket restart today. Make the route table and
the components reloadable at runtime. Includes 036 (YAML config), merged into this item
(its spec: the 036 row of `.todo/history/2026-10.md`): the reload source is its
`--config` file.

## Decided

- Trigger: the `--config` YAML (036) and the wasm files its `file` locators reference,
  watched (`notify`, watch the parent directories so editor rename-saves are seen,
  debounce ~300ms), plus SIGHUP as an explicit trigger. `--wasm` flag routes stay static.
- Existing connections: the handler is resolved per request (from the connection's
  target into the current generation), not per connection. A request in flight finishes
  on the handler it started with.
- Zero routes: fix the server so an empty advertise removes the client's routes.

## Current blockers

- `Shared.components` / `Shared.advertised` are built once by `handlers()`
  (`main.rs:402`) and immutable; each stream advertises once (`main.rs:774`).
- `load()` (`wasm_host.rs:502`) caches by `(target, cli)`: a changed file at the same
  path returns the stale component.
- `serve(io, handler)` (`wasm_host.rs:1830`) pins the handler per connection: h2 /
  keep-alive connections stay on the old version.
- Pool units hold `Arc<Pool>` and respawn forever: no way to retire a handler.
  `Handler::new` takes pool slots with `try_acquire` (startup-only assumption).
- Server: `Router.register` returns 0 without removing anything for an empty list
  (`Router.java:382`). Mid-stream re-advertise is otherwise supported (it replaces the
  client's routes; the Java client already re-advertises for rejected ports): no proto
  change.

## Design

1. Reload = parse the config, load / compile / pre-instantiate every component, check
   `sum(pool) <= --max-instances` statically. Nothing of the current generation is
   touched until all of it succeeded; any failure rejects the whole reload (logged) and
   the current generation stays. A half-written wasm fails here and is retried on the
   next event.
2. Reuse handlers whose target and content digest are unchanged (pools stay warm).
3. Publish `Generation { handlers, advertised }` through `tokio::sync::watch`. A stream
   builds its opening Advertise from `borrow_and_update()` and re-advertises on
   `changed()` in its select loop (only when the `(host, target)` set changed), so a
   reload racing a reconnect is not lost. No re-advertise once the drain began.
4. A removed target stays resolvable until every live stream acked the new Advertise
   (CONNECTs for it may still be in flight); then its pool closes: idle units exit,
   busy ones exit after their job, no respawn; slots free.
5. Content-only change: the target is unchanged, so no re-advertise; key `load()`'s
   cache by `(target, sha256, cli)` and log the digest (shared with 024's pinning).
6. Pool slots: units acquire their slots with `await` instead of `try_acquire`; while
   the old units release theirs, requests take the cold path (existing fallback). A
   sandbox change (new target) may transiently hold the old and the new `instances` cap;
   accepted.
7. A request whose target no longer exists gets 404; http/1.1 closes the connection.
8. Reloads are serialized (one reload task); a reload during the drain is ignored.

## Acceptance (each with a test that fails without it)

- 036's acceptance items (YAML shape, flag > file > default, human durations / sizes,
  unknown keys fail fast, flag-only invocations unchanged).
- Adding / removing / changing a route in the YAML re-advertises the new set on every
  live stream without a restart; a changed wasm at the same path serves the new content
  on the next request of an existing keep-alive connection.
- Invalid YAML, a component that fails to compile, a truncated wasm and a failed http
  fetch keep the current generation and send no Advertise.
- A request in flight across a swap completes on the old handler.
- A retired handler's pool frees its slots (`Instances::held`).
- A reload racing a reconnect is reflected in that stream's Advertise.
- Server: an empty Advertise removes the client's routes (Java test in `sluice-server`).
- README (wicket) documents the reload behavior and its failure handling.
