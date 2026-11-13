#!/usr/bin/env bash
# The rate of one route of a wicket without the jail on --listen, with oha (5s a run).
# usage: bench.sh <wicket binary> <label> <route query> <oha arguments>...
#   bench.sh ../../../sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker \
#       clones 'mode=per-connection&warm=16' -c 16 --disable-keepalive
# SCOPE=1: in a delegated scope of the user's manager (the wicket's cgroup its own to write: its VMs
# in cgroups of their own). RUNS=<n> runs of oha (3); EXTRA="<arguments>" for the wicket;
# ROOTFS=<rootfs> (the sample guest's).
# Needs oha, firecracker in ~/.local/bin and the kernel in ~/.local/firecracker.
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
bin=$(realpath "$1"); label=$2; query=$3; shift 3
rootfs=${ROOTFS:-$wicket/target/guest.ext4}
port=${PORT:-18293}
dir=$(mktemp -d)
log=$dir/wicket.log
cd "$dir"
run=()
if [ -n "${SCOPE:-}" ]; then
  run=(systemd-run --user --scope --quiet --collect -p Delegate=yes --)
fi
"${run[@]}" "$bin" --firecracker "$HOME/.local/bin/firecracker" --kernel "$HOME/.local/firecracker/vmlinux" \
  --listen "127.0.0.1:$port" --max-instances 128 ${EXTRA:-} --vm "a.local=$rootfs?$query" >"$log" 2>&1 &
pid=$!
cleanup() {
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -rf "$dir"
}
trap cleanup EXIT
for _ in $(seq 300); do
  grep -q "listening on" "$log" 2>/dev/null && break
  sleep 0.1
done
if ! grep -q "listening on" "$log"; then
  echo "the wicket did not listen:"; cat "$log"; exit 1
fi
grep -i "cgroup" "$log" || true
sleep 1
for run in $(seq "${RUNS:-3}"); do
  out=$(oha -z 5s --no-tui "$@" -H 'Host: a.local' "http://127.0.0.1:$port/" 2>&1)
  rps=$(echo "$out" | awk '/Requests\/sec:/ {print $2}')
  p50=$(echo "$out" | awk '/50.00% in/ {print $3, $4}')
  p99=$(echo "$out" | awk '/99.00% in/ {print $3, $4}')
  ok=$(echo "$out" | awk '/\[200\]/ {print $2}')
  echo "$label run $run: $rps req/s, p50 $p50, p99 $p99, ${ok:-0} answered 200"
  sleep 1
done
