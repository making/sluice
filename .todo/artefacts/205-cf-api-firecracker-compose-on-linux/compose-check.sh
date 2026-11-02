#!/usr/bin/env bash
# Runs the cf-api README's Firecracker Docker Compose as written: its files from the page, the
# stack in the Lima VM, cf and curl on the Mac.
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
readme=$repo/sluice-wicket-cf-api/README.md
lima=$repo/sluice-wicket-runtime-firecracker/scripts/lima.sh
export SLUICE_LIMA_VM=sluice-compose-check; vm=$SLUICE_LIMA_VM
scratch=$(cd "$(dirname "$0")" && pwd)
work=$scratch/compose
rm -rf "$work"
mkdir -p "$work/compose" "$work/cfhome"

# the section's <n>th code block
block() {
  awk -v wanted="$1" '
    /^## On a Firecracker wicket$/ { part = 1; next }
    part && /^### Docker Compose$/ { section = 1; next }
    section && /^### / { section = 0 }
    section && /^```(text|yaml)$/ { n++; inside = 1; next }
    inside && /^```$/ { inside = 0; next }
    inside && n == wanted { print }
  ' "$readme"
}
block 1 > "$work/compose/compose.yaml"
block 2 > "$work/compose/dex.yaml"
block 3 > "$work/compose/.env"
wc -l "$work/compose/compose.yaml" "$work/compose/dex.yaml" "$work/compose/.env"

"$lima" true
in_vm() { "$lima" bash -c "cd ~/compose && $1"; }
"$lima" bash -c 'cd ~/compose 2>/dev/null && docker compose down -v --remove-orphans; rm -rf ~/compose; docker rm -f wicket flume dex registry; docker volume rm wicket; docker network rm sluice; true' > /dev/null 2>&1 || true
(cd "$work" && limactl copy -r compose "$vm:compose")
in_vm 'ls -A; echo "KVM_GID=$(stat -c %g /dev/kvm)" >> .env; tail -1 .env; docker compose config -q && docker compose up -d --quiet-pull 2>&1 | tail -6'

export CF_HOME=$work/cfhome CF_COLOR=false
cf=${SLUICE_CF_CLI:-$(command -v cf8 || command -v cf)}
echo "cf: $("$cf" version)"
for _ in $(seq 1 120); do
  curl -fsS -o /dev/null http://127.0.0.1:9091/v3/info 2> /dev/null && break
  sleep 0.5
done
curl -fsS -o /dev/null http://127.0.0.1:9091/v3/info
"$cf" api http://127.0.0.1:9091
passcode=$("$repo/sluice-wicket-cf-api/demo/passcode.sh" http://127.0.0.1:9091 admin@example.com secret)
"$cf" login --sso-passcode "$passcode"
"$cf" push web --docker-image ghcr.io/traefik/whoami:v1.11.0
curl -fsS http://web.localhost:28000/
sleep 2
"$cf" logs web --recent | grep -c 'APP/PROC' || true
in_vm 'rm -rf hello; mkdir hello && docker compose cp wicket:/usr/local/share/sluice-wicket-runtime-firecracker/guest.ext4 hello/hello.ext4; ls -l hello'
mkdir -p "$work/apps/hello"
limactl copy -r "$vm:compose/hello" "$work/apps/hello/"
(cd "$work/apps" && "$cf" push hello -p hello/)
curl -fsS http://hello.localhost:28000/ | head -3
"$cf" scale hello -i 2
"$cf" apps; sleep 15; "$cf" apps

echo "--- the console through Dex"
jar=$(mktemp)
next() { curl -sS -o /dev/null -w '%{redirect_url}' -b "$jar" -c "$jar" "$@"; }
curl -sS -o /dev/null -w 'a browser at the console: %{http_code} %{redirect_url}\n' -H 'Accept: text/html' http://127.0.0.1:9090/
authorize=$(next http://127.0.0.1:9090/login)
connector=$(next "$authorize")
login=$(next "$connector")
approval=$(next -X POST --data-urlencode login=admin@example.com --data-urlencode password=secret "$login")
callback=$(next "$approval")
echo "callback: ${callback%%\?*}"
echo "after the callback: $(next "$callback")"
curl -sS -o /dev/null -w 'console page %{http_code}\n' -b "$jar" http://127.0.0.1:9090/
curl -sS -o /dev/null -w 'console api %{http_code}\n' -b "$jar" http://127.0.0.1:9090/api/apps
curl -sS -o /dev/null -w 'console api without a session %{http_code}\n' http://127.0.0.1:9090/api/apps
rm -f "$jar"

echo "--- down and up"
in_vm 'docker compose down 2>&1 | tail -6; docker volume ls -q | grep compose; docker compose up -d --quiet-pull 2>&1 | tail -6'
started=$SECONDS
until curl -fs -o /dev/null --max-time 2 http://web.localhost:28000/; do
  [ $((SECONDS - started)) -lt 120 ] || { echo "not served again"; exit 1; }
  sleep 0.5
done
echo "served again after $((SECONDS - started))s"
"$cf" apps; sleep 15; "$cf" apps
curl -fsS http://hello.localhost:28000/ | head -1
"$cf" delete hello -r -f
curl -s -o /dev/null -w 'hello after delete: %{http_code}\n' http://hello.localhost:28000/
in_vm 'docker compose ps -a --format "{{.Service}} {{.Status}}"; docker compose logs wicket 2>&1 | grep -iE "warn|error|fail" | tail -10; docker compose logs cf-api 2>&1 | grep -iE "warn|error|fail" | tail -10'
echo DONE
