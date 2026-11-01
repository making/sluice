Difficulty: Low

# sluice-wicket-runtime-firecracker: the production host setup

Firecracker's production host setup recommendations (`docs/prod-host-setup.md` of its
repository) ask of the host what the wicket cannot do itself: no SMT siblings shared between
tenants (side channels), KSM off (memory deduplicated across tenants), the host kernel's
mitigations, swap considered with the guests' memory, `/dev/kvm` to the wicket's user alone.
Work out what applies to a wicket host (one tenant's VMs per wicket, or many) and document
it in the README, with:

- per-connection isolation is per connection to the sluice server (the server opens an
  upstream connection per client connection and route, h2 too): a proxy in front of the
  server pooling its clients' connections puts several clients in one clone
