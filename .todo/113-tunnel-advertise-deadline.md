Difficulty: Medium

# Deadline for a tunnel stream's first Advertise

Follow-up of 054 (legacy flow control removal).

## Problem

A tunnel stream registers its session at once, replacing the client id's previous
session, but nothing bounds how long the client may take to send its first `Advertise`.
The routes of the replaced session stay registered for the newer one
(`TunnelSession#removeRoutes`), so requests lead to a session that has not advertised.
Since 054 a connection opened there has no send credit until the advertise brings the
client's window (`TunnelSession#open`, `connectBeforeTheAdvertiseWaitsForTheClientsWindow`):
its writer waits in `VirtualConnection#awaitCredit`, which has no timeout. Real clients
send the advertise as their first frame, so the wait lasts milliseconds; a client that
connects and never advertises (broken, or hostile with a valid token) holds the session,
the replaced session's routes and every connection opened meanwhile until the stream
ends.

## Scope

- Fail a stream whose first `Advertise` does not arrive within a bound (server setting,
  e.g. `sluice.limits.advertise-timeout`), with a status the client logs; release the
  connections waiting for the window.
- Decide whether the session should replace the previous one only once it advertised.

## Tests

- A stream that never advertises is failed at the bound; the connections opened on it
  end; the client id's routes are released.
- A client advertising in time is unaffected.
