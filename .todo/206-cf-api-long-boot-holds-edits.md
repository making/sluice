Difficulty: Medium

# sluice-wicket-cf-api: a VM booting holds every other edit

`Controller::edits` is held while the wicket boots an app's VM, and the wicket's reload is one at a
time too. With `boot-ms` from the health check's timeout (60s by default, 180s at most), one app
that never listens holds every other app's edits for that long:

- a CNB Java image listening on another port than `PORT` took 60s each try
  (`ghcr.io/categolj/blog-frontend-htmx:jvm`, 8081 by its own `server.port`)
- at the cf-api's start, and whenever the wicket's revision moves, `Controller::fix` tries a refused
  app again under the lock: `cf set-env` of another app waited for it (seen: past a 20s client
  timeout)
- a client giving up (Ctrl-C, a timeout) drops the request's future with the put in flight: the app
  is left started with no outcome, shown `starting`, until the next put

A process exiting before it listens no longer costs the wait (`LogFrame::Exited`). This is what
remains: a process that lives and does not listen.

Look at: a put in a task of its own, outliving the request, its outcome recorded; the edit lock not
held across the boot, or per app; `fix` trying a refused app only when something it depends on
changed (the capacity a refusal named) rather than at every revision. On the wicket's side, whether
a reload must wait for one route's boot.

Test: `the_cf_cli_deploys_to_a_firecracker_wicket` in `sluice-wicket-cf-api/tests/cli.rs`, with
`SAMPLE_WAIT_MS` past the timeout on one app and an edit of another timed.
