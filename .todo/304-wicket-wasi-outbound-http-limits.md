Difficulty: Medium

# sluice-wicket-runtime-wasi: bound `wasi:http` outgoing requests

`wasi:http`'s outgoing handler is linked for every route with empty hooks (`wasm_host.rs`: `NoHooks`):
any destination (loopback, the console, a metadata endpoint) with no capability, any number of requests
at once, no bandwidth bound. 250 gives sockets an allow list (`socket_addr_check`) and does not cover
this path. Not read: wasmtime-wasi-http's own buffer bounds and connect timeouts.

Do it with 250 (one allow list for both paths): the hooks refuse a destination outside it, cap a route's
requests in flight, and time a connect out. Extend 250's tests.
