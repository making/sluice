# Load balancing

How a server node picks among the clients serving one host or listen port (a key). The root
README's Scale-out (routing) and its `sluice.http-load-balance` / `sluice.tcp-load-balance`
rows summarize it.

## Without weights

A key one of whose clients carries no weight goes by the setting: the smallest client id
(default, the same on every node), round robin per node, or random. Nothing is counted.

## With weights: the fewest in flight

When every client serving a key carries a weight (`Upstream.weight`; a wicket's route
advertises its `instances`), whatever the setting, a request goes to the client with the
fewest requests in flight for its weight (in flight / weight, compared multiplied out), and
the clients tied for it take turns by smooth weighted round robin: nginx's `least_conn` with
weights. Each pick adds every tied client's weight to its score and takes the highest score,
the first of equals, which then gives back the sum of their weights. With none in flight, as
when each request ends before the next, the rotation alone decides: weights 2 and 1 pick the
first, the second, the first, and again. A client whose requests take longer, or wait in it
for room -- its instances held by another app, cold after a restart, a request costing more --
holds more of them and passes its turn while another has room, until they end.

Each node counts its own requests, per key and client (told by its id and target address); no
state is shared between nodes. A client keeps its count over a re-registration (a weight
changed by a scale) and over leaving the key while requests are in flight, until they end; a
key left without a client starts afresh. The console's routes and lookup show a client's
count beside its weight (`Weight 2 of 3, 1 in flight`); `Router.resolve` returns them
(`RouteGroup.inFlight`).

A request counts from its pick until its relay closes the pick (`Router.Pick`), once,
whichever way it ends:

| relay | picked | closed |
|---|---|---|
| HTTP/1.1 (`ReroutingRelay`) | each request head, the connection's first with the connection | at its response's end (the response tracker's completion); those of the requests a leg still owes as the leg ends; at once for a request refused (no route, the access control, its credentials) |
| HTTP/1.1, upgraded | the request answered 101 (or a 2xx to CONNECT) | as the leg ends: for the connection's life |
| h2 (`NettyHttp2DemuxRelay`) | each stream, the connection's first with the connection | as the stream closes (both ends, or a reset either way), or as the relay answers it itself (no route, forbidden, 401, 421, refused); those of streams still open as the relay finishes |
| verbatim: TLS passthrough, a hostless head, a tcp route | the connection | as it ends |

The TLS handshake looks the route up by SNI without picking (passthrough or termination,
force-http1): a connection is picked once, as it is relayed.

An HTTP/1.1 response's end is seen as the relay writes its last bytes to the client, so a
keep-alive client's next request may in rare cases be picked before the previous one's pick
closed; an h2 stream closes as its end is queued for the client, before the client sees it.

## Measured

2026-11-09, one server node and two `sluice-wicket-runtime-wasi` serving one host on Linux
(Xeon E5-2697A v4, 64 cores, loopback). A guest holding its instance for a request's cost
(`/hold/<ms>`, then answering its route's name); `concurrency: 1`. Wicket A `instances: 2`,
wicket B `instances: 1`: the weights 2 and 1. Requests of uneven cost, 80% 20ms and 20%
400ms (mean 96ms), Poisson arrivals, a new connection each, for 60s, the first 5s left out,
two seeds. A request finding no instance within its budget (2s on A, 2.6s on B, which tells
whose a 503 is) is answered 503. "To a full wicket": the requests arriving while their wicket
had every instance busy and the other had one free.

1. As advertised: A serves with 2 instances, B with 1; 22 requests/s (70% of 3 instances).
2. One of A's instances held by another app on it (`max-instances: 2`, the other app
   `warm: 1`): A serves with 1, still weighted 2; 15 requests/s (75% of 2).

| scenario, seed | balancing | to a full wicket | 503 | p50 / p90 / p99 (ms) | A's share |
|---|---|---|---|---|---|
| 1, 1 | weights | 12.4% | 0 | 224 / 775 / 1435 | 66.7% |
| 1, 1 | in flight | 0.3% | 0 | 64 / 685 / 1204 | 63.9% |
| 1, 2 | weights | 15.2% | 0 | 61 / 577 / 1013 | 66.7% |
| 1, 2 | in flight | 0.9% | 0 | 28 / 424 / 833 | 63.9% |
| 2, 1 | weights | 30.6% | 12 (1.5%) | 431 / 1527 / 2004 | 66.7% |
| 2, 1 | in flight | 0.1% | 0 | 105 / 773 / 1186 | 53.3% |
| 2, 2 | weights | 27.2% | 1 (0.1%) | 362 / 1040 / 1696 | 66.6% |
| 2, 2 | in flight | 0.1% | 0 | 54 / 609 / 1105 | 54.9% |

In scenario 2 a request waited for an instance 738 / 460ms on A on average by the weights, 274
/ 202ms by the counts, against 70-108ms on B either way: the weight A advertises for its one
free instance still lets it hold two requests for B's one. The counts correct a share that
fills, not a weight that is wrong. A node does not see another node's requests to the same
wicket, nor its other apps': they show only as its own requests taking longer.
