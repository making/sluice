#!/usr/bin/env bash
# Builds the guest (guest/, the VMs' /init) and prints its path, relative to the wicket's
# directory. SLUICE_GUEST_ARCH=x86_64|aarch64: the VMs' arch, default the host's (a VM's
# is its host's); either is linked by rust-lld, the other arch's a cross build.
set -euo pipefail
cd "$(dirname "$0")/../guest"
arch=${SLUICE_GUEST_ARCH:-$(uname -m)}
case "$arch" in
  x86_64|aarch64) ;;
  *) echo "SLUICE_GUEST_ARCH: x86_64 or aarch64, not $arch" >&2; exit 1 ;;
esac
cargo build --release --locked --target "$arch-unknown-linux-musl" >&2
echo "guest/target/$arch-unknown-linux-musl/release/sluice-wicket-runtime-firecracker-guest"
