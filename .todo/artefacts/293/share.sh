#!/usr/bin/env bash
# What each VM and the wicket take of the CPUs without the jail: the wicket pinned to a few CPUs
# (its forker and VMs with it), every route loaded at once with oha for 12s, the CPU time of each
# VM's process and of the wicket's over 6s of it.
# usage: share.sh <wicket binary> <label> <taskset cpu list> <host>=<route>[@<oha concurrency>]...
#   <route>: <rootfs>?<query>, or the query alone on the sample guest; @0: not loaded
#   SCOPE=1 share.sh $bin shares 0,1 "a.local=$PWD/spin.ext4?vcpus=4@0" "b.local=$PWD/spin.ext4?vcpus=1@0"
# SCOPE=1: in a delegated scope of the user's manager (the VMs in cgroups of the wicket's own);
# else where it is started (none of their own). UNKNOWN=<c>: connections to an unknown host too,
# the wicket's own 404s (two routes at least: a route alone takes any host).
# The guest spinning every vCPU: ../292/spin-rootfs.sh.
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
bin=$(realpath "$1"); label=$2; cpus=$3; shift 3
rootfs=${ROOTFS:-$wicket/target/guest.ext4}
port=${PORT:-18294}
dir=$(mktemp -d)
log=$dir/wicket.log
cd "$dir"
vms=()
hosts=()
conc=()
for route in "$@"; do
  spec=${route%@*}
  c=16
  [[ $route == *@* ]] && c=${route##*@}
  hosts+=("${spec%%=*}")
  conc+=("$c")
  what=${spec#*=}
  [[ $what == /* ]] || what="$rootfs?$what"
  vms+=(--vm "${spec%%=*}=$what")
done
if [ -n "${UNKNOWN:-}" ]; then
  hosts+=(unknown.local)
  conc+=("$UNKNOWN")
fi
run=()
if [ -n "${SCOPE:-}" ]; then
  run=(systemd-run --user --scope --quiet --collect -p Delegate=yes --)
fi
"${run[@]}" taskset -c "$cpus" "$bin" --firecracker "$HOME/.local/bin/firecracker" \
  --kernel "$HOME/.local/firecracker/vmlinux" --listen "127.0.0.1:$port" --max-instances 128 \
  ${EXTRA:-} "${vms[@]}" >"$log" 2>&1 &
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
sleep 1
loads=()
for i in "${!hosts[@]}"; do
  [ "${conc[$i]}" = 0 ] && continue
  oha -z 12s --no-tui -c "${conc[$i]}" ${OHA:-} -H "Host: ${hosts[$i]}" "http://127.0.0.1:$port/" \
    >"$dir/oha-${hosts[$i]}" 2>&1 &
  loads+=($!)
done
sleep 3
# every firecracker child of the wicket's threads, by its cgroup; and the wicket itself
sample() {
  for child in $(cat /proc/"$pid"/task/*/children); do
    [ "$(cat "/proc/$child/comm" 2>/dev/null)" = firecracker ] || continue
    vcpus=$(cat /proc/"$child"/task/*/comm | grep -c fc_vcpu || true)
    ticks=$(awk '{print $14 + $15}' "/proc/$child/stat")
    group=$(sed -n 's/^0:://p' "/proc/$child/cgroup")
    group=${group##*.scope}
    echo "vm $child $vcpus $((ticks * 10000)) ${group:--}"
  done
  ticks=$(awk '{print $14 + $15}' "/proc/$pid/stat")
  echo "wicket $pid 0 $((ticks * 10000)) -"
}
sample >"$dir/before"
sleep 6
sample >"$dir/after"
[ ${#loads[@]} -eq 0 ] || wait "${loads[@]}" || true
echo "== $label (CPUs $cpus${SCOPE:+, in a delegated scope})"
join -j 2 <(sort -k2,2 "$dir/before") <(sort -k2,2 "$dir/after") |
  awk '{ printf "  %-7s %s vCPU(s): %.2f CPUs  %s\n", $2, $3, ($8 - $4) / 6e6, $5 }' | sort
for i in "${!hosts[@]}"; do
  [ "${conc[$i]}" = 0 ] && continue
  rps=$(awk '/Requests\/sec:/ {print $2}' "$dir/oha-${hosts[$i]}")
  p50=$(awk '/50.00% in/ {print $3, $4}' "$dir/oha-${hosts[$i]}")
  echo "  ${hosts[$i]} (c=${conc[$i]}): $rps req/s, p50 $p50"
done
