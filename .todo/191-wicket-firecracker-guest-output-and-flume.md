Difficulty: High

# sluice-wicket-firecracker: the guests' output to the wicket's stdout and a flume

A guest process's stdout / stderr reach only the VM's serial console, which is off by default
(it is the slowest part of a boot) and, with `SLUICE_FIRECRACKER_CONSOLE=1`, lands in each
VM's `firecracker.log` under the state dir: inside the image's tmpfs (no `docker logs`), named
`route-<n>` by start order, and gone with the VM, so a per-connection clone's output (and its
template's before the snapshot) is lost. sluice-wicket-wasi prefixes its guests' lines onto
its own stdout / stderr and feeds them to a sluice-flume (`--flume`, `src/flume.rs`, README
"Live access log"), which the consoles and the cf-api (`cf logs`) watch.

- `/init` takes the process's stdout / stderr through pipes, as whole lines, and sends them
  to the wicket over vsock (a guest-initiated stream to the host, Firecracker's
  `<uds_path>_<port>` socket, or the control channel of port 1024). It never blocks the
  process on a slow host: past a bounded buffer, lines are dropped and counted as a gap
- vsock streams are reset at a snapshot's restore: a clone reconnects after the restore
  notice, and its lines carry the clone's identity (the route, the VM's index), the
  template's the template's
- the wicket writes each line to its stdout / stderr with a prefix naming the route's host(s)
  and the VM (`stdout [app.local vm-3] :: ...`), so `docker logs` shows them
- `--flume` / `--flume-token` / `--flume-ca-cert` / `--flume-insecure` feed the lines as
  wasi's do (`stdout` / `stderr` kinds, the node `wicket:<client-id>`, the ring and queue,
  the redial). Move wasi's feed into a crate both share (sluice-tunnel's or one of its own)
  rather than copy it. Response lines: the wicket relays bytes verbatim, the server's own
  access log has the requests; decide whether the wicket adds any
- the console can stay off; `SLUICE_FIRECRACKER_CONSOLE` remains for the kernel's and
  `/init`'s own boot output
- the VM test checks a sample guest's line reaching the wicket's stdout from a shared VM, a
  clone and a template; a flume (`Flume.start()`-like child process, or wasi's test double)
  receives them. README (Run, Docker image, the image's `docker logs`) and the wasi README's
  pointer
