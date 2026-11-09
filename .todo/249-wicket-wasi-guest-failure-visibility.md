Difficulty: Medium

# sluice-wicket-runtime-wasi / sluice-wicket-cf-api: a guest failing every request shows as running, and its trap goes to the visitor

Seen with rontolisp's Shiori (`.kb/shiori-on-wasi-wicket.md`): its top level opens a database connection at
an instance's first request. With an unreachable database, or a first start longer than `budget-ms`, every
request ends in a trap or an overrun and the unit is retired and replaced, the next request starting over.
Meanwhile:

- `cf app` shows every instance `running`, `cf push` and `cf start` succeed. The Firecracker wicket lists
  what is down (`crashed` in its listing, `CRASHED` with why in `cf app`); the wasi wicket lists nothing of
  the kind. Weigh listing a unit whose requests keep ending in a trap or an overrun (how many, since when,
  the last why) and showing it as the Firecracker wicket's are shown, without flapping on one bad request.
- the visitor gets `500` with `wasm handler failed: wasm trap: error while executing at wasm backtrace:` and
  the backtrace as the body. Answer a generic body instead (as a gorouter's `502` tells nothing of the app)
  and keep the detail in the wicket's log and `cf logs`, where it already is.

Pin both in the existing tests of the trap path and of the cf-api's instance states; update both READMEs.
