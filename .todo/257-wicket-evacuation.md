Difficulty: High

# sluice-wicket-cf-api / the wickets: evacuate a wicket before it stops

After 252. Upgrading a wicket (a new image, the host's maintenance) stops it: it drains (`--drain-grace`, 20s),
its routes withdrawn on every node, and the server sends their hosts to the other wickets serving them. An app
with an instance elsewhere (252) serves on; one whose instances are all on that wicket (`-i 1`) answers 503 from
the withdrawal until the wicket is back with its VMs up (a CNB Java app's boot, 12s, on top of the restart).
The other wickets take the whole load meanwhile.

Diego evacuates a cell first: its instances are started elsewhere, each stopped once its replacement runs.

## To do

- A wicket told to stop (SIGTERM) with an evacuation bound (an option; 0: the drain at once, as today) keeps
  serving and advertising, lists itself leaving (its revision moves: the cf-api's long poll answers at once),
  and drains once the apps it waits for are gone or the bound passed. Decide which apps it waits for (of the
  owners it is told, or until the cf-api says it is done)
- The cf-api, seeing a wicket leaving: each index placed on it gets a replacement on another wicket with room,
  the spread kept as far as it goes; once the replacement's put returned, the app's share is removed from the
  leaving wicket (drained as a removed route: the server sends the host to the replacements). Nowhere to go:
  logged, the share stays until the drain
- A wicket back, or a new one, takes indices again the same way round (started there, then removed from where
  they went), the spread restored; decide when (at once, or a bounded number per pass)
- The evacuated apps are gone from the leaving wicket's `routes.yaml`: it comes back without them
- An app with a Firecracker disk (the cf-api's own VM, `owner: ops`) is not moved: its wicket's stop is its
  downtime, as today
- `stop_grace_period` / `terminationGracePeriodSeconds` above the evacuation, the drain and a disk's stop
  (20s): both wickets' Shutdown sections, the cf-api's Operating it, and an upgrade procedure

## Tests

`tests/cli.rs` (each wicket's listener asked directly): an app of `-i 1` on the wicket given SIGTERM is put on
the other and removed from the first, which exits before its bound, the other's listener answering throughout.
Through a sluice server, no request failing meanwhile: in sluice-it (which runs no wicket yet: todo 025).
