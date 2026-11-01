Difficulty: Medium

# sluice-wicket-runtime-firecracker: a runtime worker per core doubles the relay's CPU

The wicket runs on tokio's default runtime (`src/main.rs`, `Runtime::new()`): a worker per
core, 64 on the 64-core test host. Measured (the README's Findings): an egress round trip
took 55-69us of the wicket's CPU with the default workers, 25-30us with one
(`TOKIO_WORKER_THREADS=1`), at the same rate (the guest bound it); a keep-alive request on
`--listen` (sample guest, 2 vCPUs, `oha -c 16`) 75us against 27us, and the rate rose from
17k to 25k req/s. The workers wake and steal for each frame relayed.

- Measure through a server too (the tunnel's gRPC streams and flow control), per-connection
  routes (the restores, the keeper's client) and many VMs at once, before choosing
- Choose: a worker count fixed or configured to what the host serves, or a VM's relays
  kept on one worker; check what fewer workers cost the restores and the output's thread
- sluice-wicket-runtime-wasi starts the same runtime (`Runtime::new()`): measure it as well
