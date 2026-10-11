Difficulty: Medium

# Data plane: idle timeout for keep-alive connections between requests

Found while working on 048. The head deadline (`sluice.limits.head-timeout`) covers only
the first request head; an established relay has no deadline, by design (WebSocket,
long responses). But a plaintext HTTP/1.1 connection idle between two requests is not a
relay in use: a peer can send one valid request, read the response and keep the
connection idle forever, holding a slot of `sluice.limits.max-connections`. The
per-address limit is off by default (SNAT front ends), so one source can hold every slot
this way. HTTP servers close such connections (Tomcat `keepAliveTimeout`, nginx
`keepalive_timeout`).

## Direction

`ReroutingRelay` knows the request boundaries: after a response completes with no next
request head started, start an idle deadline (e.g. `sluice.limits.idle-timeout`,
default about 60s); a new head cancels it, and its expiry closes the connection. Upgrades
and CONNECT leave the parser, so they keep no deadline. h2: the same for a connection with
no open stream (send GOAWAY first). Decide whether the next request head then gets the
head deadline again (a dripped second head is the same slowloris as the first).

## Tests (`sluice-it`; watch each fail first)

- With a 2s idle timeout, a keep-alive connection idle after its response is closed
  within 3s, and the slot is released (gauge).
- A connection sending its next request every 1s for 5s stays open.
- A WebSocket relay idle for 5s stays open.
