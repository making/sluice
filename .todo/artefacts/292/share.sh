#!/usr/bin/env bash
# What each VM and the wicket take of the CPUs: the wicket jailed and pinned to a few CPUs, every
# route loaded at once with oha for 12s, the CPU time of each VM's cgroup and of the wicket's
# process over 6s of it.
# usage: share.sh <wicket binary> <label> <taskset cpu list> <host>=<route>[@<oha concurrency>]...
#   <route>: <rootfs>?<query>, or the query alone on the sample guest; @0: not loaded
#   share.sh $bin shares 0,1 "a.local=$PWD/spin.ext4?vcpus=4@0" "b.local=$PWD/spin.ext4?vcpus=1@0"
# LAYOUT=own: in a delegated scope (the default --jail-cgroup: the keeper's leaf beside the VMs'
# cgroups); else with --jail-cgroup. UNKNOWN=<c>: connections to an unknown host too, the wicket's
# own 404s (two routes at least: a route alone takes any host). SHOW=<shell>: run before the end,
# with $keeper, $port and $dir (". threads.sh": the wicket's threads under such a load).
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
bin=$(realpath "$1"); label=$2; cpus=$3; shift 3
rootfs=${ROOTFS:-$wicket/target/guest.ext4}
port=${PORT:-18293}
cgroup=sluice-share-292
dir=$(mktemp -d)
log=$dir/wicket.log
pidfile=$dir/pid
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
  vms+=(--vm "'${spec%%=*}=$what'")
done
if [ -n "${UNKNOWN:-}" ]; then
  hosts+=(unknown.local)
  conc+=("$UNKNOWN")
fi
command="$bin --jail-ids 1900000000-1900000127 --jail-user $(id -un) \
  --firecracker $HOME/.local/bin/firecracker --kernel $HOME/.local/firecracker/vmlinux \
  --listen 127.0.0.1:$port --max-instances 128 ${EXTRA:-} ${vms[*]}"
if [ "${LAYOUT:-}" = own ]; then
  sudo -n systemd-run --quiet --scope --collect -p Delegate=yes --unit "$cgroup-$$" \
    sh -c "echo \$\$ > $pidfile; exec taskset -c $cpus $command" >"$log" 2>&1 &
else
  sudo -n sh -c "echo \$\$ > $pidfile; exec taskset -c $cpus $command --jail-cgroup $cgroup" >"$log" 2>&1 &
fi
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
keeper=$(cat "$pidfile")
loads=()
for i in "${!hosts[@]}"; do
  [ "${conc[$i]}" = 0 ] && continue
  oha -z 12s --no-tui -c "${conc[$i]}" ${OHA:-} -H "Host: ${hosts[$i]}" "http://127.0.0.1:$port/" \
    >"$dir/oha-${hosts[$i]}" 2>&1 &
  loads+=($!)
done
sleep 3
# every child of the keeper: a VM (an id of the jail's) by its cgroup, else the wicket
sample() {
  for pid in $(cat "/proc/$keeper/task/$keeper/children"); do
    if [ "$(stat -c %u "/proc/$pid")" -ge 1900000000 ]; then
      group=$(sed -n 's/^0:://p' "/proc/$pid/cgroup")
      vcpus=$(cat /proc/"$pid"/task/*/comm | grep -c fc_vcpu || true)
      usec=$(awk '/^usage_usec/ {print $2}' "/sys/fs/cgroup$group/cpu.stat")
      echo "vm $pid $vcpus $usec $group"
    else
      ticks=$(awk '{print $14 + $15}' "/proc/$pid/stat")
      echo "wicket $pid 0 $((ticks * 10000)) -"
    fi
  done
}
sample >"$dir/before"
sleep 6
sample >"$dir/after"
[ ${#loads[@]} -eq 0 ] || wait "${loads[@]}" || true
echo "== $label (CPUs $cpus${LAYOUT:+, layout $LAYOUT})"
join -j 2 <(sort -k2,2 "$dir/before") <(sort -k2,2 "$dir/after") |
  awk '{ printf "  %-7s %s vCPU(s): %.2f CPUs  %s\n", $2, $3, ($8 - $4) / 6e6, $5 }' | sort
for i in "${!hosts[@]}"; do
  [ "${conc[$i]}" = 0 ] && continue
  rps=$(awk '/Requests\/sec:/ {print $2}' "$dir/oha-${hosts[$i]}")
  p50=$(awk '/50.00% in/ {print $3, $4}' "$dir/oha-${hosts[$i]}")
  echo "  ${hosts[$i]} (c=${conc[$i]}): $rps req/s, p50 $p50"
done
if [ -n "${SHOW:-}" ]; then
  eval "$SHOW"
fi
