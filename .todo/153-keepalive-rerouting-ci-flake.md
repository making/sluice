Difficulty: Medium

# KeepAliveReroutingE2ETest: a rerouted request got no response on CI

CI run 39184784789 (commit ca951ab, 2026-10-19), `rewritesTheHostOfEveryReroutedRequest`: the
second request of the keep-alive connection (`rw.a.test` -> `rw.b.test`, both `rewrite-host=true`)
read EOF instead of a response (`expected "HTTP/1.1 200 OK|beta:..." but was "null|"`). First
occurrence; not reproduced locally (3300 iterations of the scenario, 40 runs of the class with every
core busy).

Timeline (server access log, the fork with pid 3000):

- 01:24:14.316 `type=conn id=6 event=accept`
- 01:24:14.317 `type=req id=6 route=rw.a.test` -- the first request; the test read its response
- 01:24:14.324 `type=conn id=6 event=close route=rw.a.test connId=1 bytesIn=42 bytesOut=123`,
  logged by the `sluice-reroute-out` thread
- no `type=req` line for `rw.b.test`; no WARN line around it

`bytesIn=42` is the first request as rewritten, so nothing of the second reached a leg. The close
line comes from the thread whose `settle()` ran last: the outbound pump. In a normal end the
inbound pump of the last leg settles last. So some leg ended on its own before the outbound pump
did. Two hypotheses:

1. The alpha leg's remote ended it (CLOSE or ERROR) right after the first response, before the
   second head was parsed. `pumpInbound` then aborts the relay, since the leg is in charge and not
   replaced.
2. The beta leg's CONNECT failed on the client (the client logs a failed dial only at debug), its
   inbound ended first, and `toUpstream` of the second request threw.

Neither JDK HttpServer (`sun.net.httpserver.ServerImpl`) nor `LocalConnector` shows a path that
closes an idle keep-alive GET connection within milliseconds.

Diagnostics added: `ReroutingRelay` logs at debug why it ends (the leg in charge ended by its
upstream, a failed response relay, a failed request relay, with the connection id, route and
cause). `KeepAliveReroutingE2ETest` turns that logger and the client's `NodeConnection` (dial
failures) to debug. On the next occurrence, read those lines for connection 1 / 2 of the failing
relay, then fix the race they show.
