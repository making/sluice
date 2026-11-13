Difficulty: Low

# sluice-wicket-runtime-wasi: bound a guest's log volume

A guest's output is cut at 16 KiB a line (`wasm_host.rs`) and at nothing a second: it reaches the
wicket's stdout and the flume without bound. The Firecracker wicket drops past a buffer and counts what
it dropped (`guest/src/log.rs`: 4096 lines / 1 MiB). The cf-api shows a wasi app's log rate limit as
"accepted and shown, no effect" (its README).

Give a route a log rate (bytes a second, a burst), drop past it and say how much was dropped, as the
Firecracker guest's line does; have the cf-api map the log rate limit onto it (305). Extend the guest
output test; READMEs.
