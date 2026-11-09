Difficulty: Medium

# sluice-wicket-runtime-firecracker / sluice-wicket-tunnel: withdraw a route's hosts while none of its VMs serves

A wicket advertises a route whatever its VMs: with none up and starting again (a process that keeps exiting, a
snapshot gone, a boot failing), its connections fail -- a booted route's at once (`guest unreachable: no VM up:
the route is restarting`), a pool's after its `wait-ms`. Alone, there is nothing better. Once several wickets
serve the host (252), the server keeps sending the failing one its share; the gorouter routes only to instances
whose health check passes.

## To do

- A route down -- `instances.down` set with none alive, not a `warm=0` route idle -- leaves the advertisement on
  every node stream (the table re-advertised without its hosts) until a VM is up; the local listener answers as
  now
- The server: a host no client advertises any more falls through as a stopped app's does (a pattern, the
  catch-all, else 404): check it, say so
- A wasm route: decide whether one is ever down so (a component whose every instantiation fails)
- Todo 154 is the same lever by hand (a gate per client)
- READMEs: the Firecracker wicket's A VM going down, the tunnel's advertisement

## Tests

`tests/vm.rs` (where KVM is): a route whose process exits leaves the advertisement the tunnel test server
(sluice-wicket-tunnel's `session::testing`) sees, and is advertised again once its VM is up.
