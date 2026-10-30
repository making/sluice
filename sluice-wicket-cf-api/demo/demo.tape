# The README's demo: the cf CLI against the cf-api and a wicket in local mode. record.sh runs it
# in ../target/demo (apps/ holds the packages, out/ takes the video) on a fresh stack, with
# {{PASSCODE}} replaced by the one-time code of cf login --sso (passcode.sh, the browser's part).
Output "out/sluice-cf-demo.mp4"

Require cf
Require curl
Require jq

Set Shell bash
Set FontSize 16
Set Width 1600
Set Height 1000
Set Padding 28
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set TypingSpeed 25ms
Set WaitTimeout 60s

Hide
Type "cd apps && clear"
Enter
Show

Type "# sluice-wicket-cf-api: the stock cf CLI deploys wasm components to a sluice wicket"
Enter
Sleep 1.5s
Type "cf version    # the OSS CLI v8"
Enter
Wait
Sleep 1s
Type "cf api http://127.0.0.1:9091"
Enter
Wait
Sleep 1.5s
Type "cf login --sso    # an OpenID Provider (Dex here) signs the user in: a one-time code from the browser"
Enter
Sleep 2s
Type "{{PASSCODE}}"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "# push: the package holds a wasi:http component"
Enter
Sleep 1s
Type "ls hello/"
Enter
Wait
Sleep 1s
Type "cf push hello -p hello/"
Enter
Wait
Sleep 3s
Type "curl http://hello.localhost:8080/    # the wicket serves the app's route"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "# routes apply at once; instances are warm instances in the wicket's pool"
Enter
Sleep 1s
Type "cf map-route hello localhost --hostname hi"
Enter
Wait
Sleep 1s
Type "curl http://hi.localhost:8080/"
Enter
Wait
Sleep 1.5s
Type "cf scale hello -i 2"
Enter
Wait
Sleep 3s
Type "curl -s -H 'Authorization: Bearer wicket-secret' http://127.0.0.1:9090/api/apps | jq '.apps[] | {hosts, component: .component.name, pool: .sandbox.pool}'"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# environment variables: the guest reads them through wasi:cli/environment"
Enter
Sleep 1s
Type `cf set-env hello GREETING "Hello from cf set-env"`
Enter
Wait
Sleep 1s
Type "cf restart hello"
Enter
Wait
Sleep 2s
Type "curl -s http://hello.localhost:8080/env/GREETING; echo"
Enter
Wait
Sleep 1.5s
Type "curl -s http://hello.localhost:8080/env/VCAP_APPLICATION | jq '{application_name, application_uris}'"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# services: counter keeps its count in wasi:keyvalue, granted by a bound keyvalue instance"
Enter
Sleep 1s
Type "cf marketplace    # the wicket's keyvalue stores and capabilities"
Enter
Wait
Sleep 4s
Type "cf create-service keyvalue local kv"
Enter
Wait
Sleep 2s

Hide
Type "clear"
Enter
Show

Type "cf push counter -p counter/ --no-start    # uploaded, not started yet"
Enter
Wait
Sleep 2.5s
Type "cf bind-service counter kv"
Enter
Wait
Sleep 2s

Hide
Type "clear"
Enter
Show

Type "cf start counter    # stages and starts with the binding"
Enter
Wait
Sleep 2s
Type "for i in 1 2 3; do curl -s http://counter.localhost:8080/; done"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "cf env counter | head -26    # the binding in VCAP_SERVICES: a namespace of the store"
Enter
Wait
Sleep 5s

Hide
Type "clear"
Enter
Show

Type "# logs: the guest's stdout / stderr and the requests (RTR), fed by the wicket through sluice-flume"
Enter
Sleep 1s
Type "curl -s http://hello.localhost:8080/log > /dev/null"
Enter
Wait
Sleep 1.5s
Type "cf logs hello --recent | tail -4"
Enter
Wait
Sleep 6s

Hide
Type "clear"
Enter
Show

Type "cf apps"
Enter
Wait
Sleep 2s
Type "cf delete counter -r -f && cf delete-service kv -f && cf delete hello -r -f"
Enter
Wait
Sleep 1.5s
Type `curl -s -o /dev/null -w '%{http_code}\n' http://hello.localhost:8080/    # gone from the wicket`
Enter
Wait
Sleep 4s
