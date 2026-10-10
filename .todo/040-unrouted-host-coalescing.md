Difficulty: Medium

# Coalesced requests to a host with no data-plane route answer 503

Follow-up discovered while verifying 038 in production (2026-10-11). With 037/038 deployed
(`07148de`), per-host routing works, but one UX-visible case remains, observed on the
management console host:

## The situation

- The console vhost is served by the server's own 8081 listener via the front proxy; it
  is **not** a tunnel route (the router only knows the registered clients' hosts).
- The data plane terminates TLS with a certificate whose SANs cover the console host too,
 so the browser legitimately coalesces the console onto an already-open data-plane h2
 connection (same IP:port, certificate covers both origins).
- A coalesced console request reaches the data plane, whose router has no route for that
 host -> the demux answers 503 no-route **on the coalesced connection**. Before 038 the
 same request was silently misrouted to the first host's upstream (the original bug);
 the 503 is honest but the console still fails to load whenever the browser coalesces.

Fresh connections to the console host are unaffected (the front proxy sends them to
8081 directly); only reuse of a data-plane connection hits this.

## Status (2026-10-11)

On hold: the coalescing itself was addressed on the operations side by splitting the
data-plane certificate from the server's own listeners' certificate (gitops
`sluice-apps-ik-am` / `sluice-system-ik-am`) -- with the apps-only SAN the browsers no
longer coalesce the console onto a data-plane connection, so the 503 path below should
no longer trigger. Revisit this todo only if coalesced no-route requests reappear (the
`type=req route=-` access lines make them visible).

## Decision needed (product), then implement

1. **Register the console host as a tunnel route** on some client, targeting the
   cluster-local console listener -- config only, but hairpins through the tunnel to
   reach the same pod, and needs a client to own it.
2. **Let the data plane serve the server's own vhosts** (console) locally: a small
   self-route (loopback to the console port) resolved before the router; new
   server-side concept, but no client dependency and no tunnel hop. Needs a config key
   for the console host name.
3. **Accept the 503** and document it; the console recovers on any fresh connection.

Preference leans 2 (the data plane and the console are the same process; a coalesced
request for the server's own vhost is best answered by it), but 1 is zero-code.

## Notes

- The refusal is now access-logged (`type=req route=- ...`), so these events are visible
  in the server log; count them there when deciding.
- The repro: browser walk app-A -> app-B -> console in one profile; the console
  navigation lands on app-B's h2 connection and 503s (verify via the access log and a
  fresh-connection curl, which returns the login redirect).
