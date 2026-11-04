Difficulty: Medium

# sluice-server: `H2DemuxBudgetE2ETest` sees a new stream admitted with the budget exhausted, about 1 run in 5

`newStreamIsRefusedOnlyOnAConnectionPastItsAllowance` fails at line 269: no `REFUSED_STREAM` for `/extra` within 5s. It
failed in the full test at `279c09c`, and 2 of 10 runs of that one test alone, so load is not needed. Earlier full tests
passed on the same Java code.

```
./mvnw -B -q --no-transfer-progress -pl sluice-it -am test -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='H2DemuxBudgetE2ETest#newStreamIsRefusedOnlyOnAConnectionPastItsAllowance'
```

What is known: in the failing runs the access log has a `type=req` line for `/extra` on the first connection, which the
passing run has not, so the stream was admitted, not lost. The test's assertions just before it held: `budgetUsed()` at
`BUDGET` or above, the client's connection window at `credit` or above.

Not known: which of these it is. Find out by measuring, before changing anything.

- the head fits what is charged already: `admitLocked` admits when `share(queued + bytes, chargedCredit) <= pooled`, so
  a share that fell since it was charged has room, and the test is what is wrong
- the budget had room at that moment: `tryCharge` succeeded just after `budgetUsed()` read it exhausted
- a real gap in admission: a connection past its allowance can start streams with the budget exhausted, and the relay
  is what is wrong. The class's Javadoc promises a refusal there

`c266a81` (2026-11-01) is the last change to `NettyHttp2DemuxRelay`; check whether the test fails before it.

If the relay is wrong, the fix starts from this test failing every run. If the test is, make it hold the condition it
means rather than only retrying.
