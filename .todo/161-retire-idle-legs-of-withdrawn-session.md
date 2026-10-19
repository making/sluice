Difficulty: High

# Retire idle legs of a withdrawn session

A client drain (`Advertise.withdraw`) ends once no connection is left on the session. Connections
that are idle but still bound to the withdrawn client hold it until the grace
(`sluice.client.drain-grace`, 20s) and are then cut:

- an HTTP/1.1 keep-alive connection whose last request went to the client: `ReroutingRelay`'s
  leg in charge stays open until the next request switches it, the server's `idle-timeout` (60s)
  closes the connection, or the grace cuts the leg -- and the cut ends the whole client connection
  ("the upstream of connection N in charge ended it"), possibly racing a request just sent
- an h2 connection with a leg on the client and no stream open on it: the leg stays until the
  grace; its cut is harmless (the relay lets the upstream connection go), but it holds the drain

Behind a front end that keeps connections open (most do), every drain therefore runs the full
grace and logs `drain grace ... expired; closing N open connection(s)`.

## Goal

The server closes the legs on a withdrawn session at a request boundary, without ending the client
connection, so the drain ends as soon as the in-flight requests did:

- `ReroutingRelay`: when the session of the leg in charge withdraws while no response is owed and
  nothing of a next request was read, close the leg and let the next request open a new one
  (resolved to another client). The retirement must be atomic with `onRequestHead` (a head parsed
  concurrently must not be relayed to the closed leg) and must not end the relay on the leg's EOF.
  Upgraded / verbatim connections are not at a boundary and keep their leg.
- `NettyHttp2DemuxRelay`: close the legs on a withdrawn session that have no stream open or waiting
  (on the event loop).
- A notification from `TunnelSession` to the relays holding legs on it (registered with the leg,
  removed with it).

## Acceptance

- Extend `ClientDrainE2ETest`: an idle HTTP/1.1 keep-alive connection and an idle h2 connection
  served by A before the stop; A's drain ends without the grace (log `drain ended with 0 open
  connection(s)`, grace long), and the next request on each connection is served by B.
- A request head arriving while the leg is being retired is relayed to B, not lost (pin the race
  with a seam, as `ReroutingRelay.ProbeSeam` does for probes).
- README "Client shutdown (drain)": drop idle connections from the list of what holds the drain.
