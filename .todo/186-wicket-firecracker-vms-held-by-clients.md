Difficulty: Medium

# sluice-wicket-firecracker: a per-connection route's VMs held by idle or flooding clients

A per-connection route's clone lives as long as its connection: a client opening many
connections, or keeping them idle, holds as many VMs -- up to the route's `vms` or
`--max-vms` -- until the server closes them (`sluice.limits.idle-timeout` between requests).
The other clients then wait `wait-ms` and fail. Nothing bounds one client's share.

- A cap per client: the server knows the client's address; whether CONNECT carries it to the
  wicket, or the server limits a client's connections per route
- An idle bound of the route's own (`idle-ms`): a clone relaying no byte for it is killed
  and its connection failed, below the server's idle timeout
- CPU: a guest spinning its vCPUs takes host cores from the others. cgroups (`cpu.max` per
  VM) are `.todo` 183's (the jailer); until then the README names the vCPUs as the bound
