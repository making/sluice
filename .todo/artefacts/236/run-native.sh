#!/bin/zsh
# usage: run-native.sh <client-binary> <wasm or ''> <label>
BIN=$1; WASM=$2; LABEL=$3; S=${TMPDIR:-/tmp}
A=$(dirname $0)
python3 $A/llm_upstream.py 31080 ${DELTAS:-5} ${DELAY:-0.05} & UP=$!
java -jar ./sluice-server/target/sluice-server-0.0.1-SNAPSHOT-exec.jar --sluice.token=SECRET --sluice.data-port=38000 --spring.grpc.server.port=38001 --server.port=38081 > $S/server-$LABEL.log 2>&1 & SV=$!
sleep 6
if [[ -n $WASM ]]; then if [[ $WASM == *proxy* ]]; then export SLUICE_SPIKE_PROXY_WASM_FILTER=$WASM; else export SLUICE_SPIKE_WASM_FILTER=$WASM; fi; fi
$BIN --sluice.server-url=grpc://127.0.0.1:38001 '--sluice.client.upstream[0].host=llm.local' '--sluice.client.upstream[0].target=http://127.0.0.1:31080' '--sluice.client.upstream[0].force-http1=true' --sluice.token=SECRET --server.port=39001 > $S/client-$LABEL.log 2>&1 & CL=$!
sleep 3
post() { curl -s --max-time 30 --compressed -H 'Host: llm.local' -H 'Content-Type: application/json' http://127.0.0.1:38000$1 -d "$2"; }
case ${MODE:-check} in
check)
  echo '--- openai json';           post /v1/chat/completions '{}'; echo
  echo '--- openai sse';            post /v1/chat/completions '{"stream":true,"stream_options":{"include_usage":true}}' | tail -4
  echo '--- openai sse, no option'; post /v1/chat/completions '{"stream":true}' | tail -2
  echo '--- anthropic json';        post /v1/messages '{}'; echo
  echo '--- anthropic sse';         post /v1/messages '{"stream":true}' | tail -6
  sleep 1
  curl -s http://127.0.0.1:39001/actuator/prometheus | grep -E '^sluice_client_filter' ;;
load)
  N=${N:-200}
  cpu0=$(ps -o cputime= -p $CL); rss0=$(ps -o rss= -p $CL)
  seq $N | xargs -P $N -I{} curl -s -o /dev/null --max-time 60 -H 'Host: llm.local' -H 'Content-Type: application/json' http://127.0.0.1:38000/v1/messages -d '{"stream":true}' & C=$!
  sleep ${HOLD:-4}; rss=$(ps -o rss= -p $CL); wait $C
  sleep 1; cpu1=$(ps -o cputime= -p $CL)
  echo "$LABEL N=$N rss before ${rss0}KiB during ${rss}KiB cpu ${cpu0} -> ${cpu1}"
  curl -s http://127.0.0.1:39001/actuator/prometheus | grep -E '^sluice_client_filter_llm_requests|guest_failures' ;;
esac
kill $CL $SV $UP 2>/dev/null; wait 2>/dev/null
