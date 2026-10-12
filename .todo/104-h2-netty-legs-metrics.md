Difficulty: Medium

# netty h2 demux, step 7: leg limits, leg retirement, metrics and access logs

Step 7 of the netty h2 demux migration (context and user decisions: 098). Depends on 103.

## Scope

- Leg limits and slots (`ConnectionLimits`) on the netty engine.
- Leg retirement: a leg is removed when its channel goes inactive (066); a stream routed to
  a leg its upstream retires meanwhile gets REFUSED_STREAM (075). Close 066 and 075 with
  this step if their tests pass on netty.
- The h2 demux metrics and access log on the netty engine, same names and tags.

## Tests

- The leg-limit, metrics and access-log subsets pass on netty; 066's and 075's tests.
