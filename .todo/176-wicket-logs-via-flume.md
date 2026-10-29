Difficulty: Medium

# `cf logs` from the flume: the wicket feeds its guests' output and its answered requests

The cf-api's log-cache (173) holds only its own lines (staging, lifecycle, the wicket's
outcome). The rest comes from the flume, which already merges the lines of every node while
someone watches (`.kb/live-access-log.md`).

- the wicket feeds the flume as a node does (`--flume`, `--flume-token`, TLS as for
  `--server`; Feed: hello, start / stop, a ring of 512 for the backfill, a queue of 4096 with
  gaps), kinds of its own, the route key being the request's host:
  - `stdout` / `stderr`: the guest's output lines (fields: component, the store id the log
    prefix shows, line); today they reach only the wicket's stdout
  - `response`: one line per answered request (method, path, http, status, durationMs, bytes,
    remote from `X-Forwarded-For`, userAgent). The nodes log request heads, not answers
    (they relay bytes), and in local mode there is no node
- the proto stays as it is (`kind` is a string, the flume filters on it). The sluice console's
  live access log shows the wicket's rows: check how it renders them and how its node list
  names a wicket
- the cf-api watches the flume (`--flume`, `--flume-token`): route keys = its apps' hosts,
  kinds = stdout, stderr, response; it resumes after its last cursor (kept with its state)
  when the filter changes or the stream drops. A record goes to the app holding its host:
  `APP/PROC/WEB/0` OUT / ERR, and `RTR` for `response` in the router's format (`<host> -
  [time] "GET /x HTTP/1.1" 200 ... response_time:0.003`); a gap becomes a line saying how
  many lines were missed
- while the cf-api watches, the nodes and wickets keep feeding its apps' lines (as an open
  console page has them do); `--recent` holds what arrived since, in the ring per app
- an app's output is attributed exactly: its routes have a handler of their own (172), and a
  pooled unit serves one request at a time

## Acceptance

- an integration test with the flume as a child process (built from `sluice-flume`):
  `hello`'s `/log`, then `cf logs --recent` shows its stdout and stderr lines and an RTR line
  with status 200; `cf logs` streams a later request
- `.kb/live-access-log.md` (wickets feed too) with the README summary, and the wicket's and
  the cf-api's READMEs
