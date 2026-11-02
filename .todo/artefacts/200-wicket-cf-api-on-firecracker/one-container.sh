#!/usr/bin/env bash
# The cf-api's README, "On a Firecracker wicket", run from the README: its blocks of commands in
# their order, each failing command failing the run. What differs from the page is what a run on
# a shared host needs: images built from the work tree, from a registry on the wicket's loopback
# (the registry's container holds the network and the published ports, and the wicket joins it:
# a jailed wicket's PID 1 leaves that namespace, which no container could join after it), names
# of its own for the containers, the volume and the network, free host ports.
# Then what the container goes through: a kill right after a push, 1 GiB written in a VM of
# 256 MiB. `one-container.sh jail` runs it in jail mode, as the wicket's README has it.
# Needs docker, jq, curl and cf8. Lines of the commands: <work>/doc.log, kept on a failure.
set -euo pipefail
mode=${1:-plain}
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
readme=$repo/sluice-wicket-cf-api/README.md
free() { python3 -I -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])'; }
listen=$(free) console=$(free) registry_port=$(free)
registry=127.0.0.1:$registry_port
run=$$
name=sluice-one-wicket-$run flume=sluice-one-flume-$run volume=sluice-one-$run network=sluice-one-$run
registry_name=sluice-one-registry-$run
work=$(mktemp -d)
log=$work/doc.log
echo "work: $work ($mode), console on 127.0.0.1:$console, listener on 127.0.0.1:$listen"
cd "$work"
wicket_image=sluice-wicket-runtime-firecracker:one-$run
flume_image=sluice-flume:one-$run
failed=true
cleanup() {
  if [ "$failed" = true ]; then
    echo "FAILED: the commands' lines:" >&2
    tail -40 "$log" >&2 || true
    echo "the wicket's:" >&2
    docker logs --tail 20 "$name" 2>&1 | cat >&2 || true
  fi
  docker rm -f "$name" "$flume" >/dev/null 2>&1 || true
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  docker rmi "$wicket_image" "$flume_image" "$registry/test/cf-api:1" >/dev/null 2>&1 || true
  cd /
  rm -rf "$work"
}
trap cleanup EXIT
expect() { # <what> <text> <pattern>
  grep -q -- "$3" <<<"$2" || { echo "UNEXPECTED $1: $2" >&2; exit 1; }
  echo "ok: $1"
}

docker build -q -f "$repo/sluice-wicket-runtime-firecracker/Dockerfile" -t "$wicket_image" "$repo" >/dev/null
docker build -q -f "$repo/sluice-flume/Dockerfile" -t "$flume_image" "$repo" >/dev/null
docker build -q -f "$repo/sluice-wicket-cf-api/Dockerfile" -t "$registry/test/cf-api:1" "$repo" >/dev/null

# the page's <n>th block of commands of the section
block() {
  awk -v wanted="$1" '
    /^## On a Firecracker wicket$/ { section = 1; next }
    section && /^## / { section = 0 }
    section && /^```text$/ { n++; inside = 1; next }
    inside && /^```$/ { inside = 0; next }
    inside && n == wanted { print }
  ' "$readme"
}
# what differs from the page
local_to_this_run() {
  local jail=(-e 's|^$||')
  if [ "$mode" = jail ]; then
    jail=(-e 's#--group-add \$(stat -c %g /dev/kvm)#-u 0 --cap-add SYS_ADMIN --security-opt apparmor=unconfined --security-opt seccomp=unconfined#'
      -e 's#--flume grpc://flume:8002$#--flume grpc://flume:8002 --jail-ids 900000-900127 --jail-user nonroot#')
  fi
  sed -e "s#ghcr.io/making/sluice/sluice-wicket-runtime-firecracker#$wicket_image#" \
    -e "s#ghcr.io/making/sluice/sluice-flume#$flume_image#" \
    -e "s#ghcr.io/making/sluice/sluice-wicket-cf-api#$registry/test/cf-api:1#" \
    -e "s#docker network create sluice#docker network create $network \&\& docker run -d --name $registry_name --network $network -p 127.0.0.1:$listen:8080 -p 127.0.0.1:$console:9090 -p $registry:$registry_port -e REGISTRY_HTTP_ADDR=0.0.0.0:$registry_port registry:3#" \
    -e "s#--name wicket --network sluice#--name $name --network container:$registry_name#" \
    -e "s#--network sluice #--network $network #" \
    -e "s#--name flume#--name $flume --network-alias flume#" \
    -e "s#-v wicket:/data#-v $volume:/data#" \
    -e "s#docker cp wicket:#docker cp $name:#" \
    -e "s#docker restart wicket#docker restart $name#" \
    -e "s#-p 127.0.0.1:8080:8080 -p 127.0.0.1:9090:9090##" \
    -e "s#http://127.0.0.1:9090#http://127.0.0.1:$console#g" \
    -e "s#\.localhost:8080#.localhost:$listen#g" \
    "${jail[@]}"
}
# the page's commands, a failure of any failing the run
cf() { timeout 300 cf8 "$@"; }
curl() { command curl -fsS --max-time 300 "$@"; }
export CF_HOME=$work/cfhome CF_COLOR=false
mkdir -p "$CF_HOME"
page() { # <n>: the block, in this shell: its variables stay
  echo "--- block $1" >>"$log"
  # shellcheck disable=SC1090
  source <(block "$1" | local_to_this_run) >>"$log" 2>&1
}

echo "the wicket and the flume"
page 1
deadline=$((SECONDS + 60))
until docker push -q "$registry/test/cf-api:1" >/dev/null 2>&1; do
  [ "$SECONDS" -lt "$deadline" ] || exit 1
  sleep 1
done
until docker logs "$name" 2>&1 | grep -q "console on"; do
  [ "$SECONDS" -lt "$deadline" ] || exit 1
  sleep 0.2
done

echo "the cf-api"
page 2
expect "the cf-api's app" "$(tail -1 "$log")" "added cf.localhost"
[ "$mode" = jail ] && expect "the VMs jailed" "$(docker logs "$name" 2>&1)" "its VMs in jails"

echo "the CLI"
page 3
expect "the image the page names, served" "$(grep -c "^Host: web.localhost:$listen" "$log")" "^1$"
expect "the env given" "$(grep -c '^GREETING=hi$' "$log")" "^1$"
deadline=$((SECONDS + 60))
until cf8 logs web --recent 2>&1 | grep -q 'APP/PROC/WEB/0\] ERR .*Starting up on port 80'; do
  [ "$SECONDS" -lt "$deadline" ] || { echo "the app's output never came through the flume" >&2; exit 1; }
  sleep 0.5
done
echo "ok: the process's output in cf logs, through the flume"
apps=$(cf8 apps 2>&1)
expect "web there" "$apps" "^web "
if grep -q "^hello " <<<"$apps"; then echo "UNEXPECTED hello: $apps" >&2; exit 1; fi
echo "ok: hello gone"

echo "a restart of the wicket"
started=$(date +%s%N)
page 4
echo "   back, the app served, $(( ($(date +%s%N) - started) / 1000000 ))ms after the restart began (5s of it the page's sleep)"
expect "the CLI's login, its app" "$(tail -30 "$log")" "^web "
expect "the app served" "$(tail -30 "$log")" "^Host: web.localhost:$listen"

answers() {
  local deadline=$((SECONDS + 60))
  until [ "$(command curl -s -o /dev/null -w '%{http_code}' --max-time 2 -H 'Host: cf.localhost' "http://127.0.0.1:$listen/v3/info")" = 200 ]; do
    [ "$SECONDS" -lt "$deadline" ] || exit 1
    sleep 0.1
  done
}
served() { command curl -sS --max-time 10 -H "Host: $1.localhost" "http://127.0.0.1:$listen/"; }

echo "a kill of the container right after a push"
timeout 300 cf8 push kept -p "$work/hello" >/dev/null
docker kill "$name" >/dev/null
docker start "$name" >/dev/null
answers
expect "the push kept" "$(cf8 apps 2>&1)" "kept"
expect "served" "$(served kept)" "Hello from a Firecracker microVM"

echo "the sample guest with a disk, written well past its VM's memory"
auth="Authorization: Bearer $TOKEN" api=http://127.0.0.1:$console
digest=$(sha256sum "$work/hello/guest.ext4" | cut -d' ' -f1)
command curl -sS -X PUT --max-time 120 -H "$auth" --data-binary "@$work/hello/guest.ext4" "$api/api/components/$digest" >/dev/null
expect "its app" "$(command curl -sS -X PUT --max-time 60 -H "$auth" "$api/api/apps/data" -d "{\"owner\":\"ops\",\"hosts\":[\"data.localhost\"],\"component\":{\"sha256\":\"$digest\",\"name\":\"guest.ext4\"},\"sandbox\":{\"mem\":256,\"disk\":\"/tmp/data\",\"disk-mib\":2048}}")" "added data.localhost"
head -c 48M /dev/urandom | base64 -w0 >blob
started=$(date +%s%N)
for n in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do
  wrote=$(command curl -sS --max-time 60 -X POST --data-binary @blob -H 'Host: data.localhost' "http://127.0.0.1:$listen/disk?write=big$n&sync=1")
  [ "$wrote" = "wrote big$n" ] || { echo "UNEXPECTED $n: $wrote" >&2; exit 1; }
done
echo "   1 GiB written in 64 MiB files, fsynced, in $(( ($(date +%s%N) - started) / 1000000 ))ms"
expect "read back" "$(command curl -sS --max-time 60 -H 'Host: data.localhost' "http://127.0.0.1:$listen/disk?read=big16" | sha256sum)" "$(sha256sum blob | cut -d' ' -f1)"
expect "the disks" "$(command curl -sS -H "$auth" "$api/api/disks")" '"name":"data","mib":2048,"app":true'
failed=false
echo "as written"
