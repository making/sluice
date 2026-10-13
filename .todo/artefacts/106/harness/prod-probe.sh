#!/bin/bash
# A few requests through production (read-only, light): h1 vs h2 time for a small and a
# 100KB response from the httpbin route.
for proto in --http1.1 --http2; do
  for path in /get /bytes/102400; do
    for i in 1 2 3; do
      curl -s -o /dev/null $proto -w "$proto $path %{http_version} %{http_code} connect=%{time_connect} start=%{time_starttransfer} total=%{time_total} size=%{size_download}\n" "https://httpbin.apps.sluice.ik.am$path"
    done
  done
done
