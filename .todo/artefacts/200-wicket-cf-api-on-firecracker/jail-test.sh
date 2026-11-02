#!/usr/bin/env bash
# The jailed VM test, as the CI's "Test the jail" runs it, where the host has no `sudo -n`: the
# test binary in a container of the capabilities the README's jail mode lists, sudo inside it.
# Build it first (cargo test --locked --test vm --no-run). Usage: jail-test.sh <test binary> [its args]
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
fc=$repo/sluice-wicket-runtime-firecracker
test=$1
shift
context=$(mktemp -d)
cat >"$context/Dockerfile" <<DOCKERFILE
FROM ubuntu:24.04
RUN apt-get update -q && apt-get install -y --no-install-recommends sudo e2fsprogs util-linux procps ca-certificates
RUN (getent group $(id -g) || groupadd -g $(id -g) tester) \
    && (getent group $(stat -c %g /dev/kvm) || groupadd -g $(stat -c %g /dev/kvm) kvmhost) \
    && useradd -o -u $(id -u) -g $(id -g) -G $(stat -c %g /dev/kvm) -m tester \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD:ALL' > /etc/sudoers.d/tester && chmod 0440 /etc/sudoers.d/tester
DOCKERFILE
docker build -q -t sluice-jail-test "$context" >/dev/null
rm -rf "$context"
# build-guest.sh takes this instead of building the guest: no cargo in the container
cp "$fc/$(cd "$fc" && scripts/guest.sh 2>/dev/null)" "$fc/target/guest-init"
docker run --rm --name "sluice-jail-test-$$" --device /dev/kvm -u 0 --cap-add SYS_ADMIN \
  --security-opt apparmor=unconfined --security-opt seccomp=unconfined --cgroupns=private \
  --tmpfs /run:exec --tmpfs /tmp:exec \
  -v "$fc/target:$fc/target" -v "$fc/scripts:$fc/scripts:ro" \
  -v "$HOME/.local/firecracker/vmlinux:/opt/vmlinux:ro" \
  -v "$(command -v firecracker):/usr/local/bin/firecracker:ro" \
  -e SLUICE_GUEST="$fc/target/guest-init" \
  -e SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL=/opt/vmlinux \
  -e SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER=/usr/local/bin/firecracker \
  -e SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL=1 -e RUST_BACKTRACE=1 -w "$fc" \
  sluice-jail-test bash -c '
    set -e
    # the jail-cgroup below the root of the namespace, which then holds no process
    mount -o remount,rw /sys/fs/cgroup
    mkdir -p /sys/fs/cgroup/init
    for pid in $(cat /sys/fs/cgroup/cgroup.procs); do
      echo "$pid" >/sys/fs/cgroup/init/cgroup.procs 2>/dev/null || true
    done
    exec sudo -u "#$0" -g "#$1" --preserve-env=SLUICE_GUEST,SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL,SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER,SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL,RUST_BACKTRACE,PATH "${@:2}"
  ' "$(id -u)" "$(id -g)" "$test" "$@"
