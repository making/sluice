# Sourced by share.sh's SHOW: the wicket's threads over 3s of a load of its own (connections to
# an unknown host: accepted, answered 404, closed): how long each ran, and waited for a CPU
# (/proc/<pid>/task/<tid>/schedstat). C=<oha concurrency> (64), HOST=<host> (unknown.local).
set +e +o pipefail
for w in $(cat "/proc/$keeper/task/$keeper/children"); do
  [ "$(stat -c %u "/proc/$w")" -lt 1900000000 ] || continue
  oha -z 5s --no-tui -c "${C:-64}" -H "Host: ${HOST:-unknown.local}" "http://127.0.0.1:$port/" >"$dir/oha-own" 2>&1 &
  load=$!
  sleep 1
  snap() { for t in /proc/"$w"/task/*; do echo "$(basename "$t") $(tr ' ' '_' <"$t/comm") $(cat "$t/schedstat")"; done; }
  snap >"$dir/t0"
  sleep 3
  snap >"$dir/t1"
  join <(sort "$dir/t0") <(sort "$dir/t1") | awk '{ run = ($7 - $3) / 3e7; n = $9 - $5
    if (run > 0.5) printf "  %-10s on a CPU %.0f%%, waiting for one %.0f%% (%.0f us a time, %d times/s)\n", $2, run, ($8 - $4) / 3e7, n ? ($8 - $4) / n / 1000 : 0, n / 3 }'
  wait $load
  echo "  $(awk '/Requests\/sec:/ {print $2}' "$dir/oha-own") req/s, p50 $(awk '/50.00% in/ {print $3, $4}' "$dir/oha-own")"
done
