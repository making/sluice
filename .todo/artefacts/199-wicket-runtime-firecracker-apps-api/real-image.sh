#!/usr/bin/env bash
# PUT /api/images on a real image: a Paketo run image (real users, thousands of files, setuid
# binaries) with a binary on top, from a local registry, served in a VM. Needs docker, the kvm
# group, firecracker, ~/.local/firecracker/vmlinux, registry:3 and
# paketobuildpacks/ubuntu-noble-run. Exits non-zero when what it shows changes; files stay in the
# directory it prints.
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
fc=$repo/sluice-wicket-runtime-firecracker
work=$(mktemp -d)
echo "work: $work"
cd "$work"
listen=${LISTEN_PORT:-18199} console=${CONSOLE_PORT:-19199}
registry_name=sluice-real-image-registry
registry=
wicket=
cleanup() {
  [ -n "$wicket" ] && pkill -TERM -f "console-dir $work/deployed" || true
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  [ -n "$registry" ] && docker rmi "$registry/test/cnblike:1" >/dev/null 2>&1 || true
}
trap cleanup EXIT
expect() { # <what> <text> <pattern>
  grep -q -- "$3" <<<"$2" || { echo "UNEXPECTED $1: $2" >&2; exit 1; }
  echo "ok: $1"
}

docker run -d --name "$registry_name" -p 127.0.0.1:0:5000 registry:3 >/dev/null
registry=127.0.0.1:$(docker port "$registry_name" 5000/tcp | cut -d: -f2)
guest=$fc/$("$fc/scripts/guest.sh")
mkdir ctx
cp "$(dirname "$guest")/sample-process" ctx/serve
cat > ctx/Dockerfile <<'DOCKERFILE'
FROM paketobuildpacks/ubuntu-noble-run:latest
USER root
COPY serve /workspace/serve
RUN mkdir -p /workspace/data && echo hi > /workspace/data/f && chown -R 1002:1000 /workspace/data && chmod 640 /workspace/data/f && rm -f /etc/motd
USER cnb
WORKDIR /workspace
ENV FROM_IMAGE=yes
ENTRYPOINT ["/workspace/serve", "/workspace/data/f", "/usr/bin/passwd", "/bin"]
DOCKERFILE
docker build -q -t "$registry/test/cnblike:1" ctx >/dev/null
docker push -q "$registry/test/cnblike:1" >/dev/null
cargo build --locked --manifest-path "$fc/Cargo.toml" 2>/dev/null
sg kvm -c "$fc/target/debug/sluice-wicket-runtime-firecracker --kernel $HOME/.local/firecracker/vmlinux \
  --firecracker $(command -v firecracker) --listen 127.0.0.1:$listen --console 127.0.0.1:$console \
  --console-dir $work/deployed --guest $guest --watch-interval 0" > wicket.log 2>&1 &
wicket=$!
sleep 2
api=http://127.0.0.1:$console
body="{\"image\":\"$registry/test/cnblike:1\"}"
started=$SECONDS
made=$(curl -sS -X PUT --max-time 300 -d "$body" "$api/api/images")
echo "made in $((SECONDS - started))s: $made"
sha=$(python3 -I -c 'import json,sys;print(json.loads(sys.argv[1])["sha256"])' "$made")
expect "an app" "$(curl -sS -X PUT --max-time 120 -d "{\"owner\":\"me\",\"hosts\":[\"cnb.localhost\"],\
\"component\":{\"sha256\":\"$sha\",\"name\":\"cnblike.ext4\"},\"sandbox\":{\"mem\":128,\
\"env\":{\"GREETING\":\"hello world\"}}}" "$api/api/apps/cnb")" "added cnb.localhost"
served=$(curl -sS --max-time 20 -H 'Host: cnb.localhost' "http://127.0.0.1:$listen/")
expect "the image's user" "$served" "^uid 1002 gid 1001$"
expect "a file's owner and mode" "$served" "^/workspace/data/f uid 1002 gid 1000 mode 640$"
expect "setuid kept" "$served" "^/usr/bin/passwd uid 0 gid 0 mode 4755$"
expect "the image's env" "$served" "^FROM_IMAGE=yes$"
expect "the app's env" "$served" "^GREETING=hello world$"
expect "the same digest again" "$(curl -sS -X PUT --max-time 300 -d "$body" "$api/api/images")" "\"sha256\":\"$sha\""
image=$(ls "deployed/rootfs/$sha/"*.ext4)
expect "e2fsck" "$(/usr/sbin/e2fsck -fn "$image" 2>&1 || echo failed)" "non-contiguous"
du -h --apparent-size "$image"
echo "as measured"
