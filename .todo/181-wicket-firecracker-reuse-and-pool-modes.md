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
  shrunk with the load from the snapshot. The VMM thread turned out not to bound a shared
  VM (at most 57% busy; the vCPUs are at 85-97%, see the README's Findings): `vcpus=N` on
  one VM already scales it (the sample guest 10.8k req/s keep-alive on 1 vCPU, 13.1k on 2;
  the CNB Go app 8.4k on 1, 15k on 4). Measure what a pool adds over that: N VMs of 1 vCPU
  against one VM of N (throughput, memory, a VM's failure), and whether the vsock device's
  single VMM thread shows past 4 vCPUs

Measure each mode as the README's Findings table does (`oha -c 16` through the data plane,
keep-alive and `--disable-keepalive`) and add the rows there.
