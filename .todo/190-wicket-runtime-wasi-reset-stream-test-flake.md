Difficulty: Medium

# sluice-wicket-runtime-wasi: a reset stream's share test failed once under load

`wasm_host::tests::a_reset_stream_keeps_its_share_until_its_instance_is_gone` failed once in
`cargo test` (2026-11-01, the failure message not kept) while a Maven build ran beside it,
the host's cores busy; it passed seven times after, alone and in the full suite. It spins a
guest past a reset stream on a multi-threaded runtime (4 workers): what it asserts depends on
the spin's progress against wall-clock budgets. Reproduce under load (e.g. `stress-ng --cpu
64` beside it), find the timing it assumes, and have it hold on a busy machine (wait on an
event, not a duration).
