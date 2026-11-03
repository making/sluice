Difficulty: Medium

# sluice-wicket-runtime-wasi: `a_reset_stream_keeps_its_share_until_its_instance_is_gone` fails intermittently

Seen on main at `f918e08`: 2 failures in 4 runs of the whole crate, 0 in 10 runs of the test alone (each 2.4s).

```
pooled false: answered only once the spin's budget ended it: 22.078648ms
pooled false: answered only once the spin's budget ended it: 38.872532ms
```

The second stream was answered 20 to 40ms after the first started, against a 1s budget: too early to be
the lateness `c7c11bb` fixed, where the same test asserted inside a wall-clock window. So either the spinning
guest ended early, or the reset gave back the share while its instance was alive. The second is a real bug
in the limit this test guards. Not known: whether it came with `708dda0`, which moved the instance index out of
`wasm_host.rs` to `sluice-wicket-tunnel`'s `flume::Indices`, or was there before. Run the commit before it too.

- find which it is, from the order of events rather than from the timing: reproduce under the crate's whole
  run (`cargo test --locked --lib`) or a pinned load
- a bug in the share: the fix, this test seen failing without it. A test that does not hold: make it check
  the order of events, as `c7c11bb` meant to
