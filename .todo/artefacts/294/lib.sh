# Sourced by the scripts here: the wicket of run.sh started and stopped, what it holds shown.
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
wicket=$(cd "$here/../../../sluice-wicket-runtime-firecracker" && pwd)
port=${PORT:-18294}
url=http://127.0.0.1:$port
sample=${ROOTFS:-$wicket/target/guest.ext4}

# start <rootfs> <route query>: the wicket, once it listens; stopped as the script ends
start() {
  rm -f wicket.log
  ("$here/run.sh" "$1" "$2" &)
  for _ in $(seq 300); do grep -q "listening on" wicket.log 2>/dev/null && break; sleep 0.1; done
  if ! grep -q "listening on" wicket.log; then echo "the wicket did not listen:"; cat wicket.log; exit 1; fi
  trap stop EXIT
  pid=$(cat wicket.pid)
}

stop() {
  local pid; pid=$(cat wicket.pid 2>/dev/null) || return 0
  kill -INT "$pid" 2>/dev/null || true
  for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  rm -f wicket.pid
}

# the VMs' parent cgroup, where the wicket has them
cgroups() { sed -n "s/^the VMs' cgroups in \([^,]*\),.*/\1/p" wicket.log; }

mib() { [ "$1" = max ] && echo max || echo $(( $1 >> 20 )); }
of() { awk -v k="$2" '$1 == k {print $2}' "$1/memory.stat"; }

# what each of the wicket's cgroups is charged, its snapshots' sizes, its VMs' processes
show() {
  local cg d f p; cg=$(cgroups)
  if [ -n "$cg" ]; then
    for d in "$cg"/route-* "$cg"/vm-* "$cg"/shared/*-vm-* "$cg"/shared/vm-*; do
      [ -d "$d" ] || continue
      printf '%-13s current %4s peak %4s max %4s MiB | anon %4s shmem %4s | oom_kill %s | %s\n' "$(basename "$d")" \
        "$(mib "$(cat "$d/memory.current")")" "$(mib "$(cat "$d/memory.peak")")" "$(mib "$(cat "$d/memory.max")")" \
        "$(mib "$(of "$d" anon)")" "$(mib "$(of "$d" shmem)")" \
        "$(awk '$1 == "oom_kill" {print $2}' "$d/memory.events")" "$(tr '\n' ' ' < "$d/cgroup.procs")"
    done
  fi
  for f in "${STATE:-/run/user/$(id -u)/sluice-wicket-runtime-firecracker-$pid}"/route-*/snapshot/vm.mem; do
    [ -f "$f" ] && echo "snapshot $(du -m "$f" | cut -f1) of $(( $(stat -c %s "$f") >> 20 )) MiB"
  done
  for p in $(pgrep -x firecracker -P "$pid"); do
    echo "vm $p: $(grep -E 'VmRSS|RssAnon|RssShmem' "/proc/$p/status" | tr -s ' \t' ' ' | tr '\n' ' ')"
  done
}
