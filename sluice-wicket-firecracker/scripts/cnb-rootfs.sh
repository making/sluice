#!/usr/bin/env bash
# Turns an OCI image (e.g. one built by Cloud Native Buildpacks) into a Firecracker rootfs:
# the image's filesystem (docker export), the guest as /init, and /sluice/init.json
# holding what the image's config says to run (Entrypoint + Cmd, Env, User, WorkingDir).
# Usage: scripts/cnb-rootfs.sh <image> <out.ext4> [port]   (port: the process's, default 8080)
# Needs docker and sudo (the files keep the image's owners).
set -euo pipefail
cd "$(dirname "$0")/.."
image=$1 out=$2 port=${3:-8080}
(cd guest && cargo build --release --target x86_64-unknown-linux-musl)
work=$(mktemp -d)
trap 'sudo rm -rf "$work"; [ -n "${cid:-}" ] && docker rm -f "$cid" >/dev/null' EXIT
cid=$(docker create "$image")
docker inspect --format '{{json .Config}}' "$image" > "$work/config.json"
mkdir "$work/root"
docker export "$cid" | sudo tar -xpf - -C "$work/root"
sudo install -m 0755 guest/target/x86_64-unknown-linux-musl/release/sluice-firecracker-guest "$work/root/init"
# docker export leaves the files docker bind mounts empty
printf '127.0.0.1 localhost sluice\n' | sudo tee "$work/root/etc/hosts" >/dev/null
sudo mkdir -p "$work/root/sluice"
python3 -I - "$work/config.json" "$work/root" "$port" <<'PY' | sudo tee "$work/root/sluice/init.json" >/dev/null
import json, sys
config, root, port = json.load(open(sys.argv[1])), sys.argv[2], int(sys.argv[3])

def lookup(table, name):
    # numeric, or a name in the image's /etc/passwd / /etc/group
    if name.isdigit():
        return int(name)
    for line in open(f"{root}/etc/{table}"):
        fields = line.split(":")
        if fields[0] == name:
            return int(fields[2])
    raise SystemExit(f"{name}: not in the image's /etc/{table}")

user, _, group = (config.get("User") or "0").partition(":")
uid = lookup("passwd", user)
if group:
    gid = lookup("group", group)
else:
    gid = next((int(l.split(":")[3]) for l in open(f"{root}/etc/passwd") if int(l.split(":")[2]) == uid), 0)
env = config.get("Env") or []
if not any(pair.startswith("HOME=") for pair in env):
    home = next((l.split(":")[5] for l in open(f"{root}/etc/passwd") if int(l.split(":")[2]) == uid), "/")
    env.append(f"HOME={home}")
print(json.dumps({
    "argv": (config.get("Entrypoint") or []) + (config.get("Cmd") or []),
    "env": env,
    "uid": uid,
    "gid": gid,
    "workdir": config.get("WorkingDir") or "/",
    "port": port,
}, indent=2))
PY
sudo cat "$work/root/sluice/init.json" >&2
size_mib=$(( $(sudo du -sm "$work/root" | cut -f1) * 13 / 10 + 32 ))
rm -f "$out"
truncate -s "${size_mib}M" "$out"
sudo mkfs.ext4 -q -F -d "$work/root" -L sluice-rootfs "$out"
echo "$out (${size_mib} MiB)"
