#!/usr/bin/env bash
# The sample rootfs running the guest's sample process as an image's process, whose /oom takes
# memory until the guest kernel's OOM killer takes it: -> process.ext4 in the current directory.
# Needs debugfs (e2fsprogs), the sample rootfs (scripts/build-guest.sh) and the guest's release
# build (it makes it).
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
sample=$wicket/guest/target/$(uname -m)-unknown-linux-musl/release/sample-process
cp "${ROOTFS:-$wicket/target/guest.ext4}" process.ext4
printf '{"argv":["/serve"],"env":[],"uid":0,"gid":0,"workdir":"/","port":8080}' > init.json
printf 'mkdir sluice\nwrite init.json sluice/init.json\nwrite %s serve\nsif serve mode 0100755\n' "$sample" > process.debugfs
PATH=$PATH:/sbin:/usr/sbin debugfs -w -f process.debugfs process.ext4 >/dev/null 2>&1
rm -f init.json process.debugfs
echo "$PWD/process.ext4"
