Difficulty: Low

# h2 demux: a connection withheld at its share must not wait on the budget -- untested

Found in 130's report (5ed09fd).

## Problem

A connection past its share (`CONNECTION_SHARE`, 2MiB) has its connection windows withheld
while the shared budget has room. It deliberately does not register as a budget waiter:
registered, every budget release would wake it, find it still past its share, and review
again -- a busy loop of reviews while the budget has room. It is re-reviewed when its own
hops drain instead. No test shows that it neither registers nor spins; read from the code.

## Tests

- A connection held at its share while other connections release budget repeatedly: count
  its reviews (a counter or a seam on the loop) and assert they follow its own hops' drains,
  not the budget releases. Registering it as a waiter (mutation) must fail the test.
