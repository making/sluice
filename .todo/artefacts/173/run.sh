#!/usr/bin/env bash
# run.sh <cf args...>: runs the OSS cf CLI ($CF, default cf8) against the stub, then prints
# the requests the command made (from stub.log)
here=$(cd "$(dirname "$0")" && pwd)
export CF_HOME=$here/.cfhome
mkdir -p "$CF_HOME"
touch "$here/stub.log"
before=$(wc -l < "$here/stub.log")
"${CF:-cf8}" "$@" 2>&1
echo "--- exit $?"
tail -n +$((before + 1)) "$here/stub.log" | grep -vE 'Traceback|File "|~~~|\^\^\^|self\.|return self|ConnectionReset|^-+$|Exception occurred|raw_requestline|recv_into'
