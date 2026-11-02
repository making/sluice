#!/usr/bin/env bash
# What a VM's writable drive does, measured on Firecracker directly (no wicket): making it, the
# guest's mount, a durable save per cache type, what a SIGKILL of the VM loses, one drive under
# two VMs, a lock held across firecracker's exec, RLIMIT_FSIZE, growing it, a stop's sync. Needs
# the kvm group (sg kvm -c), firecracker, ~/.local/firecracker/vmlinux, musl-gcc, e2fsprogs.
# Usage: disk-measure.sh [directory the drives go on, a file system to measure; default mktemp]
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
base=${1:-}
work=$(mktemp -d ${base:+-p "$base"})
echo "work: $work ($(df -T "$work" | awk 'NR==2 {print $1, $2}'))"
trap 'pkill -KILL -f "api-sock $work/" 2>/dev/null; rm -rf "$work"' EXIT
kernel=${KERNEL:-$HOME/.local/firecracker/vmlinux}

musl-gcc -static -O2 -o "$work/init" "$here/disk-init.c"
mkdir "$work/root" "$work/root/proc" "$work/root/sys" "$work/root/dev" "$work/root/mnt"
cp "$work/init" "$work/root/init"
truncate -s 16M "$work/rootfs.ext4"
mkfs.ext4 -q -F -O ^has_journal -d "$work/root" "$work/rootfs.ext4"

# boot <name> <drive> <cache type> <scenario> [firecracker's launcher prefix...]: a VM, its
# serial console to <name>.log, ext4's mount options $opts; returns once firecracker is started,
# its pid in $vm.
opts=
boot() {
  local name=$1 drive=$2 cache=$3 scenario=$4
  shift 4
  cat >"$work/$name.json" <<EOF
{
  "boot-source": {"kernel_image_path": "$kernel",
    "boot_args": "console=ttyS0 reboot=k panic=1 pci=off clocksource=kvm-clock init=/init disk=$scenario${opts:+ diskopts=$opts}"},
  "drives": [
    {"drive_id": "rootfs", "path_on_host": "$work/rootfs.ext4", "is_root_device": true, "is_read_only": true},
    {"drive_id": "disk", "path_on_host": "$drive", "is_root_device": false, "is_read_only": false, "cache_type": "$cache"}
  ],
  "machine-config": {"vcpu_count": 1, "mem_size_mib": 128}
}
EOF
  rm -f "$work/$name.sock"
  "$@" firecracker --api-sock "$work/$name.sock" --config-file "$work/$name.json" \
    </dev/null >"$work/$name.log" 2>&1 &
  vm=$!
}

# wait_line <name> <pattern> <seconds>
wait_line() {
  local deadline=$((SECONDS + $3))
  while ! grep -q -- "$2" "$work/$1.log"; do
    [ "$SECONDS" -gt "$deadline" ] && return 1
    kill -0 "$vm" 2>/dev/null || { grep -q -- "$2" "$work/$1.log"; return; }
    sleep 0.02
  done
}

# run <name> <drive> <cache type> <scenario>: a VM to its end; its lines of the scenario.
run() {
  boot "$1" "$2" "$3" "$4"
  wait "$vm"
  grep -aE "^(mounted|durable|the counter|written|wrote|grown|FAILED|done|[0-9]+ MiB dirty)" "$work/$1.log" | tr -d '\r'
}

echo
echo "== making it: mkfs.ext4 -F -q -m 0, a sparse file or an allocated one"
for mib in 256 1024 16384 262144; do
  for how in sparse allocated; do
    f="$work/made-$mib-$how.ext4"
    if [ "$how" = sparse ]; then truncate -s "${mib}M" "$f"; else fallocate -l "${mib}M" "$f"; fi
    start=$(date +%s%N)
    mkfs.ext4 -F -q -m 0 "$f"
    took=$(( ($(date +%s%N) - start) / 1000000 ))
    echo "$mib MiB $how: ${took}ms, $(du -m "$f" | cut -f1) MiB on the disk; journal $(dumpe2fs -h "$f" 2>/dev/null | awk -F: '/Journal size/ {gsub(/ /, "", $2); print $2}')"
    rm -f "$f"
  done
done

drive=$work/disk.ext4
new_drive() { rm -f "$drive"; truncate -s "${1}M" "$drive"; mkfs.ext4 -F -q -m 0 "$drive"; }

echo
echo "== mount: a new file system, then one cleanly unmounted"
new_drive 1024
run mount1 "$drive" Writeback read
run mount2 "$drive" Writeback read

echo
echo "== a durable save of 100 KiB (write, fsync, rename, fsync of the directory), by cache type"
for cache in Unsafe Writeback; do
  new_drive 1024
  echo "cache_type $cache:"
  run "fsync-$cache" "$drive" "$cache" fsync:500
done

echo
echo "== a SIGKILL of the VM: a counter saved every 20ms, each printed once saved, read after"
for opts in "" nodioread_nolock; do
  for durable in 0 1; do
    new_drive 1024
    boot "count-$durable" "$drive" Writeback "count:$durable"
    wait_line "count-$durable" "saved 200" 60 || echo "never got to 200"
    # past ext4's commit interval (5s) from the first saves, in the middle of the rest
    sleep 7
    kill -KILL "$vm"
    wait "$vm" 2>/dev/null
    printed=$(grep -a '^saved' "$work/count-$durable.log" | tail -1 | tr -cd '0-9')
    boot "after-$durable" "$drive" Writeback read
    wait "$vm"
    read=$(grep -a '^the counter' "$work/after-$durable.log" | tr -d '\r')
    mounted=$(grep -a '^mounted' "$work/after-$durable.log" | tr -d '\r')
    echo "options ${opts:-default}, fsync $durable: printed $printed; $read; $mounted"
  done
done
opts=

echo
echo "== one drive under two VMs: does anything refuse?"
new_drive 1024
boot two-a "$drive" Writeback count:1
first=$vm
wait_line two-a "saved 5" 30
boot two-b "$drive" Writeback count:1
second=$vm
sleep 1.5
kill -0 "$second" 2>/dev/null && echo "the second firecracker runs: nothing refused it"
grep -a "^mounted\|^saved\|FAILED\|EXT4-fs error" "$work/two-b.log" | tr -d '\r' | head -3 | sed 's/^/    b: /'
sleep 1
grep -a "FAILED\|EXT4-fs error\|Remounting\|aborted" "$work/two-a.log" "$work/two-b.log" | tr -d '\r' | head -5 | sed 's/^/    /'
kill -KILL "$first" "$second" 2>/dev/null
wait 2>/dev/null
echo "e2fsck after it: $(e2fsck -fn "$drive" 2>&1 | grep -cE 'Inode|Free blocks|Block bitmap|Directory|FILE SYSTEM WAS') lines of damage"

echo
echo "== a lock on the drive, held by firecracker through the exec of its launcher"
new_drive 256
cat >"$work/locked.sh" <<EOF
#!/usr/bin/env bash
exec 9<>"$drive"
flock -n 9 || { echo "the drive is in use"; exit 75; }
exec "\$@"
EOF
chmod +x "$work/locked.sh"
boot lock "$drive" Writeback count:1 "$work/locked.sh"
holder=$vm
wait_line lock "saved 5" 30
if flock -n "$drive" true; then echo "NOT held while it runs"; else echo "held while it runs"; fi
"$work/locked.sh" true; echo "a second launcher: exit $?"
kill -KILL "$holder"
start=$(date +%s%N)
while ! flock -n "$drive" true; do sleep 0.001; done
echo "free $(( ($(date +%s%N) - start) / 1000 ))us after the SIGKILL was sent"
wait "$holder" 2>/dev/null

echo
echo "== RLIMIT_FSIZE of 16 MiB under a 256 MiB drive"
new_drive 256
limited() { ( ulimit -f $((16 * 1024)); exec "$@" ); }
boot limit "$drive" Writeback fill:128 limited
wait "$vm"; status=$?
grep -a "^written\|^wrote\|FAILED" "$work/limit.log" | tr -d '\r' | tail -2
grep -a "SIGXFSZ\|signal" "$work/limit.log" | tr -d '\r' | head -2
echo "firecracker's exit: $status"
echo "the same with the limit at the drive's size:"
new_drive 256
limited256() { ( ulimit -f $((256 * 1024)); exec "$@" ); }
boot limit2 "$drive" Writeback fill:128 limited256
wait "$vm"; status=$?
grep -a "^wrote\|FAILED" "$work/limit2.log" | tr -d '\r'
echo "firecracker's exit: $status"

echo
echo "== grown: the file larger, the guest's ioctl after the mount"
new_drive 256
truncate -s 1024M "$drive"
run grow "$drive" Writeback grow
new_drive 1024
truncate -s 65536M "$drive"
run grow2 "$drive" Writeback grow
echo "e2fsck after it: $(e2fsck -fn "$drive" >/dev/null 2>&1; echo $?) (0 clean)"

echo
echo "== a stop: sync of what is dirty, then the unmount"
for mib in 1 16 64; do
  new_drive 1024
  run "stop-$mib" "$drive" Writeback "stop:$mib" | grep "dirty"
done
