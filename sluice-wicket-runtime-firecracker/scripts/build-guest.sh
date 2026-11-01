#!/usr/bin/env bash
# Builds the sample guest's rootfs: an ext4 image holding the static guest as /init.
# Usage: scripts/build-guest.sh [out.ext4]   (default target/guest.ext4)
# SLUICE_GUEST=<path> takes a guest built already (the image's /usr/local/share/
# sluice-wicket-runtime-firecracker/init: the wicket's own version) instead of building guest/.
# SLUICE_GUEST_ARCH=x86_64|aarch64 builds the guest for VMs of that arch (default: the host's).
# No root needed: mkfs.ext4 -d populates the image from a directory.
set -euo pipefail
# before the cd: a path relative to the caller's directory
guest=${SLUICE_GUEST:+$(realpath "$SLUICE_GUEST")}
cd "$(dirname "$0")/.."
out=${1:-target/guest.ext4}
if [ -z "$guest" ]; then
  guest=$(scripts/guest.sh)
fi
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root"/{dev,proc,sys,tmp}
install -m 0755 "$guest" "$root/init"
mkdir -p "$(dirname "$out")"
rm -f "$out"
truncate -s 16M "$out"
mkfs.ext4 -q -F -d "$root" -L sluice-guest "$out"
echo "$out"
