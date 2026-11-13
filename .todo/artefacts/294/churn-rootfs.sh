#!/usr/bin/env bash
# A guest whose process takes memory and gives it back, staying up: busybox httpd, whose
# /cgi-bin/churn?<MiB> writes so many MiB into the guest's /tmp (a tmpfs: its memory) and
# removes them, and /cgi-bin/mem answers the guest's /proc/meminfo. -> churn.ext4 in the
# current directory. Needs docker and sudo (scripts/cnb-rootfs.sh).
# usage: churn-rootfs.sh
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
out=$PWD/churn.ext4
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
mkdir -p "$dir/www/cgi-bin"
cat > "$dir/www/cgi-bin/churn" <<'CGI'
#!/bin/sh
dd if=/dev/zero of=/tmp/churn bs=1M count="${QUERY_STRING:-64}" 2>/dev/null
rm -f /tmp/churn
printf 'Content-Type: text/plain\r\n\r\nchurned %s MiB\n' "${QUERY_STRING:-64}"
CGI
cat > "$dir/www/cgi-bin/mem" <<'CGI'
#!/bin/sh
printf 'Content-Type: text/plain\r\n\r\n'
grep -E 'MemTotal|MemFree|MemAvailable|^Cached|Shmem:' /proc/meminfo
CGI
printf 'it works\n' > "$dir/www/index.html"
chmod +x "$dir/www/cgi-bin/"*
cat > "$dir/Dockerfile" <<'DOCKER'
FROM busybox:1.37
COPY www /www
CMD ["httpd", "-f", "-p", "8080", "-h", "/www"]
DOCKER
docker build -q -t sluice-294-churn "$dir" >/dev/null
"$wicket/scripts/cnb-rootfs.sh" sluice-294-churn "$out" 8080
