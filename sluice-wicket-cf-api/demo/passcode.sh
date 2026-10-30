#!/usr/bin/env bash
# The browser's part of cf login --sso, scripted against the demo's Dex: signs <email> in with
# <password> through the cf-api's /uaa/passcode and prints the one-time code the page shows.
#   passcode.sh [<api url>] [<email>] [<password>]    (default http://127.0.0.1:9091, demo@example.com, demo)
set -euo pipefail
api=${1:-http://127.0.0.1:9091}
email=${2:-demo@example.com}
password=${3:-demo}
jar=$(mktemp)
trap 'rm -f "$jar"' EXIT
next() { curl -sS -o /dev/null -w '%{redirect_url}' -b "$jar" -c "$jar" "$@"; }
authorize=$(next "$api/uaa/passcode")                      # the cf-api's authorization request
connector=$(next "$authorize")                              # Dex: its one connector, the local passwords
login=$(next "$connector")                                  # the login form
approval=$(next -X POST --data-urlencode "login=$email" --data-urlencode "password=$password" "$login")
[ -n "$approval" ] || { echo "Dex refused the login of $email" >&2; exit 1; }
callback=$(next "$approval")                                # back to the cf-api with the code
page=$(curl -sS -b "$jar" "$callback")
code=$(sed -n 's/.*class="big">\([a-z0-9]*\)<\/code>.*/\1/p' <<< "$page")
[ -n "$code" ] || { echo "no code on the page:" >&2; echo "$page" >&2; exit 1; }
echo "$code"
