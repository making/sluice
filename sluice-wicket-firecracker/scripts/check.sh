#!/usr/bin/env bash
# Spike E2E check through a running sluice server's data plane (default 127.0.0.1:8000):
# http/1.1, h2c, keep-alive, a 5MB POST and a client half-closing after its request.
# Usage: scripts/check.sh <host>...
set -euo pipefail
data=${SLUICE_DATA:-127.0.0.1:8000}
body=$(mktemp); trap 'rm -f "$body"' EXIT
head -c 5000000 /dev/urandom > "$body"
for host in "$@"; do
  h1=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $host" "http://$data/")
  h2=$(curl -s -o /dev/null -w '%{http_code}' --http2-prior-knowledge -H "Host: $host" "http://$data/")
  ka=$(curl -s -o /dev/null -o /dev/null -o /dev/null -w '%{http_code}/%{num_connects} ' -H "Host: $host" "http://$data/1" "http://$data/2" "http://$data/3")
  post=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $host" --data-binary "@$body" "http://$data/up")
  half=$(python3 -I - "$data" "$host" <<'PY'
import socket, sys
address, port = sys.argv[1].rsplit(":", 1)
s = socket.create_connection((address, int(port))); s.settimeout(5)
s.sendall(f"GET /half HTTP/1.1\r\nHost: {sys.argv[2]}\r\n\r\n".encode())
s.shutdown(socket.SHUT_WR)
data = b""
try:
    while (chunk := s.recv(4096)): data += chunk
    print(data.split(b" ")[1].decode() if data else "empty")
except socket.timeout:
    print("no-EOF")
PY
)
  echo "$host: h1=$h1 h2c=$h2 keep-alive(code,connects)=$ka post5MB=$post half-close=$half"
done
