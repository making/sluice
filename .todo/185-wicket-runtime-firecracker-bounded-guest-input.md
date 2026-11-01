Difficulty: Low

# sluice-wicket-runtime-firecracker: bound what the wicket takes from a guest

A guest is untrusted: its image is a tenant's, and a process running as root in it controls
the VM. Every read of the wicket's from a guest must be bounded in size and time. The data
path is (frames read into a fixed buffer, the tunnel's credit), and so is the log stream
(`src/output.rs`: a frame of 16KiB at most, its syncs timed), and so is a VM's log on an
error (`vm::log_tail`: its last 4KiB); two reads are not:

- The answer to the restore notice (`src/route.rs`, `Cloner::restore`): `read_line` without a length
  bound, only `RESTORE_TIMEOUT` (2s): a guest sending a line without end has the wicket
  allocate for 2s at vsock speed
- The template's priming answers (`src/route.rs`, `prime`): collected whole, without a
  timeout. A template that never answers, or streams, stops its route's start -- and with
  it the wicket's startup (the routes start together) or every reload after it

Bound them (a short answer line, the priming answer's status line, a timeout on a route's
start) and cover them with a guest double where a test reaches them without KVM.
