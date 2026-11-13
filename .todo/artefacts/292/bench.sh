#!/usr/bin/env bash
# The rate of one route of a jailed wicket on --listen, with oha (5s a run).
# usage: bench.sh <wicket binary> <label> <route query> <oha arguments>...
#   bench.sh ../../../sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker \
#       clones 'mode=per-connection&warm=16' -c 16 --disable-keepalive
# RUNS=<n> runs of oha (3); EXTRA="<arguments>" for the wicket; ROOTFS=<rootfs> (the sample guest's).
# Needs sudo -n, oha, firecracker in ~/.local/bin and the kernel in ~/.local/firecracker.
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
bin=$(realpath "$1"); label=$2; query=$3; shift 3
rootfs=${ROOTFS:-$wicket/target/guest.ext4}
port=${PORT:-18292}
cgroup=sluice-bench-292
dir=$(mktemp -d)
log=$dir/wicket.log
pidfile=$dir/pid
cd "$dir"
sudo -n sh -c "echo \$\$ > $pidfile; exec $bin --jail-ids 1900000000-1900000127 --jail-user $(id -un) \
  --jail-cgroup $cgroup --firecracker $HOME/.local/bin/firecracker --kernel $HOME/.local/firecracker/vmlinux \
  --listen 127.0.0.1:$port --max-instances 128 ${EXTRA:-} --vm 'a.local=$rootfs?$query'" >"$log" 2>&1 &
cleanup() {
  if [ -s "$pidfile" ]; then
    sudo -n kill -TERM "$(cat "$pidfile")" 2>/dev/null || true
    for _ in $(seq 100); do
      sudo -n kill -0 "$(cat "$pidfile")" 2>/dev/null || break
      sleep 0.1
    done
  fi
  sudo -n rm -rf "$dir"
}
trap cleanup EXIT
for _ in $(seq 300); do
  grep -q "listening on" "$log" 2>/dev/null && break
  sleep 0.1
done
if ! grep -q "listening on" "$log"; then
  echo "the wicket did not listen:"; cat "$log"; exit 1
fi
sleep 1
for run in $(seq "${RUNS:-3}"); do
  out=$(oha -z 5s --no-tui "$@" -H 'Host: a.local' "http://127.0.0.1:$port/" 2>&1)
  rps=$(echo "$out" | awk '/Requests\/sec:/ {print $2}')
  p50=$(echo "$out" | awk '/50.00% in/ {print $3, $4}')
  echo "$label run $run: $rps req/s, p50 $p50"
  sleep 1
done
