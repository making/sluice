Difficulty: Low

# sluice-wicket-cf-api: try the Firecracker Docker Compose on Linux

The README's `### Docker Compose` under `## On a Firecracker wicket` has only run in a Lima VM on an
Apple silicon Mac (arm64, `cf` and `curl` on the Mac), on a VM `lima.sh` made new. Its intro says
Docker Engine on Linux. Run it there, on an amd64 host with KVM, from the page's blocks as written.

What the Mac could not show:

- **`group_add`**: Lima's `/dev/kvm` is 0666 (`lima.sh`'s udev rule), so `KVM_GID` was seen in
  `docker inspect` only. On a host whose `/dev/kvm` is 0660, check the VMs boot with it and fail
  without it (the wicket runs as 65532)
- **amd64 images**: the wicket, the cf-api, the flume and `ghcr.io/traefik/whoami:v1.11.0`, and the
  sample `guest.ext4`, of that arch
- **the ports**: `28000:8080` binds every interface of the host: the apps from another machine, with
  `APPS_DOMAIN` and a wildcard DNS; 9090, 9091 and 5556 from the host only, and through the SSH
  tunnel the page gives
- **a real browser**: `cf login --sso` and the console's sign-in through Dex ran by `curl`
  (`demo/passcode.sh`); on the host or through the tunnel, open both
- `open` in the page's commands is macOS's: `xdg-open`, or leave it to the reader

`artefacts/205-cf-api-firecracker-compose-on-linux/compose-check.sh` is the Mac's run: the page's
blocks, push, scale, logs, the console, `down` and `up`. On Linux drop `lima.sh` and `limactl`, and
run `in_vm`'s commands in the directory. It uses the ports as written: free 5556, 9090, 9091 and
28000 first.

After `down` and `up`, expect `web:0/1` and `The wicket is unreachable, the app is put when it is
back` in the cf-api's log until the wicket answers: 15s was enough on the Mac. Note the time the
page's readers would wait, if it differs much.

Fix what differs in the README; delete the artefact with this file.
