#!/usr/bin/env bash
# Builds the sample guest's rootfs: an ext4 image holding the static guest as /init.
# Usage: scripts/build-guest.sh [out.ext4]   (default target/guest.ext4)
# No root needed: mkfs.ext4 -d populates the image from a directory.
set -euo pipefail
cd "$(dirname "$0")/.."
out=${1:-target/guest.ext4}
(cd guest && cargo build --release --target x86_64-unknown-linux-musl)
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root"/{dev,proc,sys,tmp}
install -m 0755 guest/target/x86_64-unknown-linux-musl/release/sluice-firecracker-guest "$root/init"
mkdir -p "$(dirname "$out")"
rm -f "$out"
truncate -s 16M "$out"
mkfs.ext4 -q -F -d "$root" -L sluice-guest "$out"
echo "$out"
