Difficulty: Medium

# H2DemuxBudgetE2ETest: withheld-window test failed once under the full build

`H2DemuxBudgetE2ETest.withheldWindowIsGrantedOnceAnotherConnectionFreesTheBudget` failed
once in a full `./mvnw verify` on 2026-10-12 (the 084 run, on top of cf13a7a):

```
[the withheld window was granted]
Expecting value to be true but was false
	at ...H2DemuxBudgetE2ETest.java:408
```

Rerun alone it passed 3 times out of 3. 084 does not touch the h2 demux or its budget
(it adds build fields to Advertise / AdvertiseAck and one log line per stream). The test
log of the failing run: `.todo/artefacts/087/verify-H2DemuxBudgetE2ETest.log`.

## To do

- Dump the timeline first: when `leaving.close()` freed the budget, whether the relay of
  the waiting connection was woken (`H2DemuxBudget` waiters), and when it sent the
  WINDOW_UPDATE versus the 5s deadline of `awaitTrue`.
- Decide between a real race in the wake-up (a waiter registered after the release, or
  a release that does not reach the waiter) and a deadline too short under load; fix the
  race with a test that forces the interleaving, or justify and widen the deadline.
