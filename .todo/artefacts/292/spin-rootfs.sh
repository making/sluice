#!/usr/bin/env bash
# A rootfs whose guest spins every vCPU (a shell loop each) beside an HTTP server on its port:
# the wicket's /init launching a static busybox. usage: spin-rootfs.sh [out.ext4] (spin.ext4)
set -euo pipefail
out=$(realpath "${1:-spin.ext4}")
cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker"
guest=$(scripts/guest.sh)
busybox=$(command -v busybox)
file -L "$busybox" | grep -q "statically linked" || { echo "$busybox: not static (busybox-static)" >&2; exit 1; }
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root"/{dev,etc,proc,sys,tmp,bin,sluice}
install -m 0755 "$guest" "$root/init"
install -m 0755 "$busybox" "$root/bin/busybox"
printf '127.0.0.1 localhost sluice\n' > "$root/etc/hosts"
cat > "$root/sluice/init.json" <<'JSON'
{
  "argv": ["/bin/busybox", "sh", "-c", "n=$(/bin/busybox nproc); i=0; while [ $i -lt $n ]; do (while :; do :; done) & i=$((i+1)); done; exec /bin/busybox httpd -f -p 8080 -h /tmp"],
  "env": ["PATH=/bin"],
  "uid": 0,
  "gid": 0,
  "workdir": "/",
  "port": 8080,
  "restore": []
}
JSON
rm -f "$out"
truncate -s 16M "$out"
mkfs.ext4 -q -F -d "$root" -L sluice-spin "$out"
echo "$out"
