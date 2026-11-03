Difficulty: Medium

# sluice-wicket-cf-api: `cf app` at `warm: 0` shows `0/0` while every connection's restore fails

Left by todo 223, which told what keeps a route below `warm` (`Route::down`, `route.rs`) and, as it asked, nothing of
a VM beyond it.

- **no VM kept, none restoring**: at `warm: 0` (a pool kept at none, a VM per connection) every VM is beyond `warm`.
  Its snapshot gone, each connection fails with the restore's error and `cf app` shows `0/0`, what an app idle at none
  shows. Tell a restore failing for anything but room, with no VM alive, in `down`; mind that nothing retries there: the
  next connection is the only one to clear it, so a CRASHED entry may outlive its cause for as long as none comes
- **a CRASHED entry's `since`**: an instance alive has its start (`alive`'s `since`), what the wicket is short of has
  none, so `stats` (`views.rs`) gives it the app's last start. The listing's `down` could carry when it went down

The test: the steps of `below_warm` in `tests/vm.rs`, `stats`' unit test in `views.rs`.
