#!/usr/bin/env bash
# Runs a command for this directory on a Mac of Apple silicon (M3 or later, macOS 15 or later),
# in a Lima VM with nested virtualization: an aarch64 Linux whose /dev/kvm boots Firecracker.
# The VM is made and provisioned at the first run; the work tree (uncommitted changes
# included) is copied into it at each.
# Usage: scripts/lima.sh [command...]   (default: cargo test --locked --test vm)
# SLUICE_LIMA_VM=<name>: the VM (default sluice-firecracker). limactl delete -f <name> removes it.
set -euo pipefail
vm=${SLUICE_LIMA_VM:-sluice-firecracker}
here=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$here/.." && pwd)
commit=$(git -C "$root" rev-parse HEAD)
# Lima's default forwarder leaves the VM's end of a forwarded connection open when the Mac's
# closes (Lima 2.1.1): each would hold one of the wicket's --max-connections
export LIMA_SSH_PORT_FORWARDER=true

if ! limactl list -q | grep -qx "$vm"; then
  # no mount (the VM's builds stay out of the work tree), no containerd (docker below, the CI's)
  limactl start --name="$vm" --vm-type=vz --arch=aarch64 --nested-virt \
    --cpus=8 --memory=16 --disk=60 --mount-none --tty=false \
    --set '.containerd.system=false | .containerd.user=false' template:ubuntu-24.04
elif [ "$(limactl list -f '{{.Status}}' "$vm")" != Running ]; then
  limactl start --tty=false "$vm"
fi
ssh_config=$HOME/.lima/$vm/ssh.config

# The tools of the CI's VM test, the release and the kernel it pins (.github/workflows/rust.yaml),
# and the OSS cf CLI of sluice-wicket-cf-api's test
limactl shell --workdir / "$vm" bash -c '
  set -euo pipefail
  command -v cf8 >/dev/null || (
    cd "$(mktemp -d)"
    curl -fsSLO https://github.com/cloudfoundry/cli/releases/download/v8.19.0/cf8-cli_8.19.0_linux_arm64.tgz
    echo "454c29a44a51c8edc9696678403e2e40808357a397033af5a018e6ca8ee32117  cf8-cli_8.19.0_linux_arm64.tgz" | sha256sum -c
    tar xzf cf8-cli_8.19.0_linux_arm64.tgz cf8
    sudo install cf8 /usr/local/bin/cf8
  )
  [ -e ~/.sluice-provisioned ] && exit 0
  [ -e /dev/kvm ] || { echo "no /dev/kvm: nested virtualization needs an M3 or later, macOS 15 or later" >&2; exit 1; }
  sudo apt-get -o DPkg::Lock::Timeout=600 update -q
  sudo apt-get -o DPkg::Lock::Timeout=600 install -y --no-install-recommends \
    protobuf-compiler e2fsprogs python3 build-essential pkg-config curl ca-certificates rsync \
    docker.io docker-buildx
  sudo usermod -aG docker "$(id -un)"
  echo "KERNEL==\"kvm\", GROUP=\"kvm\", MODE=\"0666\", OPTIONS+=\"static_node=kvm\"" \
    | sudo tee /etc/udev/rules.d/99-kvm4all.rules
  sudo udevadm control --reload-rules
  sudo udevadm trigger --name-match=kvm
  cd "$(mktemp -d)"
  FC=v1.17.0
  curl -fsSLO https://github.com/firecracker-microvm/firecracker/releases/download/$FC/firecracker-$FC-aarch64.tgz
  echo "e351ebe4f7a16b5873bbd51005d2e6767103cff4d5ebc829df2d3f95a93e2256  firecracker-$FC-aarch64.tgz" | sha256sum -c
  tar xzf firecracker-$FC-aarch64.tgz
  sudo install -D release-$FC-aarch64/firecracker-$FC-aarch64 /usr/local/bin/firecracker
  curl -fsSL -o vmlinux https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261028-84df49062576-0/aarch64/vmlinux-6.18.56
  echo "ba2081f71b3405e7f7be606b849b5341d2272dcb79999cb67f8061e711d0883f  vmlinux" | sha256sum -c
  sudo install -D -m 0644 vmlinux /opt/firecracker/vmlinux
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
  touch ~/.sluice-provisioned
'

# target/ stays the VM's: its objects are aarch64 Linux's, and --delete leaves what is excluded
rsync -a --delete --exclude target --exclude .git --exclude node_modules \
  -e "ssh -F $ssh_config" "$root/" "lima-$vm:sluice/"

(($#)) || set -- cargo test --locked --test vm
# a connection of its own: the one Lima keeps open may predate the docker group
exec ssh -F "$ssh_config" -o ControlMaster=no -o ControlPath=none "lima-$vm" "
  source ~/.cargo/env
  cd ~/sluice/sluice-wicket-runtime-firecracker
  export SLUICE_GIT_COMMIT=$commit
  export SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER=/usr/local/bin/firecracker
  export SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL=/opt/firecracker/vmlinux
  $(printf '%q ' "$@")"
