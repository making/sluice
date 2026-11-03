Difficulty: Medium

# sluice-wicket-cf-api: `cf scale` checks the annotations it does not apply

`cf scale -i` applies at once, with what the last start took: `vm_sandbox` is given `running.lifetime`
(`controller.rs`). Its check reads the app's annotations as they are now (`check_annotations(&app.annotations, ...)`
in `api.rs`), which apply only at the next start. When they differ, the check answers for another
configuration. Read from the code, not run:

- started with `warm: 3` at 3 instances, the annotation then removed, `cf scale -i 2`: the check passes, and
  `vms=2, warm=3` reach the wicket, which refuses it (`vms must be at least 1 and at least warm`)
- started with `mode: shared`, the annotation then removed, `cf scale -i 2`: the check passes, the VM stays
  one, and `cf app` says `2/2`
- the reverse: a change annotated for the next start refuses a scale the running app could take

The wasi wicket's `warm` takes the same path (`pool` from `running.lifetime.warm`).

- reproduce both cases with a failing test first: `check_annotations`'s table in `manifest.rs`, or the
  scale steps of `tests/cli.rs`
- a scale must hold both now and at the next start: it is checked against what it applies
  (`running.lifetime`) and against the annotations as they are, and refused naming which, the restart being
  the way out of a difference. An annotation is checked against the count as it is. No state then reaches
  the wicket only to be refused
- the README's "How long a VM lives" in step
