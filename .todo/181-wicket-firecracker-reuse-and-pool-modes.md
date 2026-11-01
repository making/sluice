Difficulty: Medium

# sluice-wicket-firecracker: clone reuse and shared VM pool modes

The spike has two route modes (`sluice-wicket-firecracker/src/route.rs`): `shared`, one
resident VM serving every connection, and `per-connection`, a fresh clone of the route's
snapshot per connection, discarded after it. Add the modes between them:

- Clone reuse, as Lambda keeps a warm execution environment: a clone serves further
  connections and is discarded after an idle time (and / or a lifetime or connection
  count), scaling to zero when idle. It gives up isolation (a connection sees the state of
  the previous ones): state it per route
- A pool of shared VMs per route, connections spread over them: fixed size, or grown and
  shrunk with the load from the snapshot. Each VM has its own VMM thread, the suspected
  bound of one shared VM's throughput (about 8k req/s keep-alive, 2.9k new connections/s
  in the spike's measurement): measure whether N VMs give about N times that

Measure each mode as the README's Findings table does (`oha -c 16` through the data plane,
keep-alive and `--disable-keepalive`) and add the rows there.
