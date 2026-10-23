Difficulty: Medium

# TunnelStallWatchdogE2ETest: the data plane never took up the h1 connection (once)

One full `./mvnw verify` on macOS (2026-10-23, during the Spring Boot 4.1.2 upgrade) failed
`TunnelStallWatchdogE2ETest#anH1RouteSwitchWaitingOnAStalledLegEndsWithTheSession` at its first
read (`in.readNBytes(RESPONSE.length)`, line 338): `SocketTimeoutException` after the socket's
10s timeout. The next full verify and three runs of the class alone passed.

## Evidence

- The server JVM (one of the 6 surefire forks; earlier classes' contexts still cached in it:
  `H2DemuxFrameLimitsE2ETest`, `QueuedFramesHoldSlotsE2ETest`, `ConsoleAuthE2ETest`) logged
  the stalling client's session at 21:33:17.96, then nothing at all until the test's teardown
  at 21:33:28.08: no `type=conn ... event=accept listener=data`, no `type=req`, no reject.
- `access.accept()` runs in `DataProxyServer#transport` before the first byte is read, right
  after `acceptLoop` hands the socket to a virtual thread. Nothing on that path blocks
  (`ConnectionReserve#tryReserve`, `Admission#claim`, socket options). So either
  `ServerSocket#accept` did not return, or the handler's virtual thread did not run, or the
  whole JVM stood still for 10s.
- This was the data port's first connection in that context; the other three tests of the
  class use only the tcp route ports.

## Direction

Make the next occurrence diagnosable before guessing: on this read's timeout, dump the
test JVM's threads (the server runs in it), including virtual threads
(`jcmd <pid> Thread.dump_to_file -format=json`, or `HotSpotDiagnosticMXBean#dumpThreads`),
and record GC pauses (`-Xlog:gc`) for the forks. Then look for what parks `acceptLoop` or
starves the carriers, e.g. virtual threads of the cached contexts.
