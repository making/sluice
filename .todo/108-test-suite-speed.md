Difficulty: Medium

# Test suite: sluice-it takes 11 minutes, serially

Measured on CI run 38224125594 (5a15a4e, ubuntu-latest): the `test` job takes 12 min, of
which `sluice-it` takes 10:57 (107 classes, 673 s summed class time, about 120 s of it app
startups). The other modules take under a minute together. Every class runs one after
another; most of the time is spent waiting on wall-clock timeouts (stall, idle,
keep-alive, tunnel watchdog), not on CPU. The netty h2 migration (099) runs the h2 classes
on two engines, which would add several minutes more.

Slowest classes (CI, seconds): `KeepAliveIdleTimeoutE2ETest` 73, `H2DemuxStallTimeoutE2ETest`
67, `StallTimeoutE2ETest` 35, `MalformedRequestE2ETest` 35, `H2DemuxLegWriterE2ETest` 31,
`StallTimeoutSlowReaderE2ETest` 29 (one test), `ConsoleE2ETest` 29,
`H2DemuxClientFrameValidationE2ETest` 26, `HalfCloseProbeE2ETest` 25,
`TunnelStallWatchdogE2ETest` 22.

## Decisions (2026-10-12)

1. **Run `sluice-it` classes concurrently.** This is the main lever: the time is waiting, so
   3 or 4 classes at once should take the job to about 4 min. JUnit's class-level parallel
   execution (or surefire forks); a test that needs the machine quiet (throughput, a
   timing bound measured against CPU) gets `@Isolated` or a shared `@ResourceLock` rather
   than wider margins. Check first that tests share no global state (static fields,
   system properties, a global meter registry, fixed ports; `TestPorts.freePort()` is the
   rule). It must hold on Linux CI: rerun the suite there several times before merging.
2. **Replace `StallTimeoutSlowReaderE2ETest`** (25 s for one test, and imprecise, see 086)
   by the relay-level test of 086 with a sink accepting bytes at a set rate (no kernel
   socket buffers). Do it together with 086 and close 086 with this todo if it is
   covered; keep at most a short E2E smoke.
3. **Keep the "is not cut" tests** (about 4.2 s each, two dozen). They must outlast the
   timeout to prove anything; shortening the configured timeouts only buys Linux flakes.
4. **Remove fixed waits that prove nothing**: find sleeps and awaits that wait out a delay
   the scenario does not need (for example, check whether
   `MalformedRequestE2ETest#aResponseTheRelayCannotFrameIsRelayedUntilTheUpstreamCloses`,
   4 x 4.3 s, waits for an upstream that could close at once). Each shortened test still
   fails without its behaviour (mutation, watched).
5. **Delete only true duplicates**: a test is deleted only when another one exercises the
   same code path with the same assertion; h1 and h2 variants are different paths and stay.
   Do not spend effort on the h2 tests the migration rewrites or drops (`WriteSeam` races,
   settings replay; list in `.todo/artefacts/096/FINDINGS.md`). List every deletion in the
   final report.

## Acceptance

- CI `test` job time before and after, from the run logs; local `./mvnw verify` time too.
- Several green CI runs in a row with the parallel setup (no new flakes).
