Difficulty: Medium

# sluice-wasmlet: instance pooling and CPU/memory limits

Every request pays full instantiation (`Service::instantiate_async` on a fresh `Store`)
and guests can burn unlimited CPU / memory:

- reuse instances across requests where the guest is reentrant, or pre-instantiate via
  `InstancePre` + wasmtime pooling allocator; measure cold vs warm before choosing
- epoch-based CPU interruption (`Engine::increment_epoch`) with a per-route budget,
  yielding a 504/503-style response on exhaustion
- `wasmtime` memory limits (`Config::memory_reservation`, limiter via `ResourceLimiter`)
  so a guest cannot OOM the relay process

The sandbox story ("guest panic must not take the tunnel down") is demonstrated in
the PoC via `/panic`; limits are the other half and are required before calling the
isolation story complete.

Acceptance: a busy-loop guest and a memory-hungry guest degrade to clean error
responses; other routes keep serving; no per-request instantiation on the warm path
(or a measured decision why not).
