Difficulty: Medium

# sluice-it: a browser test fails when another fork's container starts or stops

`./mvnw verify` failed twice in a row on c550a6a, one error of 688 both times, the same test at the
same line:

```text
ForceHttp1MisdirectedE2ETest.aBrowserReachesAForceHttp1RouteOnceItsGateOpened:395 » Playwright Error
  net::ERR_NETWORK_CHANGED at https://h1only.local:<port>/
```

The class alone passes (7 of 7). Chromium drops a navigation in flight when the host's interfaces
change, and a Testcontainers container starting or stopping adds or removes a veth on `docker0`:

- first run: the failure at 15:24:51 UTC, a Redis container's `network disconnect` in the same second
  (`docker events`)
- second run: the failure at 15:31:09 UTC, veths coming and going from 15:31:07 to 15:31:10
  (`ip monitor link address route`)

`RedisE2ETest`, `SniRoutingRedisE2ETest` and `TlsPassthroughRedisE2ETest` run containers;
`ConsoleE2ETest`, `ConsolePeerBuildE2ETest`, `ForceHttp1MisdirectedE2ETest` and `NoRouteE2ETest` run
Chromium. `sluice-it` runs 6 forks at once, and its rule is that a test does not depend on a quiet
machine. Anything else on the host changing an interface (another Docker user, a Firecracker tap) does
the same, so keeping those classes apart in the forks would not settle it.

- settle what makes Chromium stop reacting to the host's interfaces, or a navigation that survives
  them, in one place the four classes share
- the failing test first: an interface changing during a navigation, on demand (a container started
  and stopped in a loop, say), if that reproduces it at will
- then `./mvnw verify` again
