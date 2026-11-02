# The README's demo on a Firecracker wicket: the cf CLI on the Mac, the stack in the Lima VM.
# record-firecracker.sh runs it in ../target/demo-firecracker (apps/ holds the rootfs, out/ takes
# the video) on a fresh stack, with {{PASSCODE}} replaced by the one-time code of cf login --sso
# (passcode.sh, the browser's part).
Output "out/sluice-cf-firecracker-demo.mp4"

Require cf
Require curl
Require jq
Require limactl

Set Shell bash
Set FontSize 16
Set Width 1600
Set Height 1000
Set Padding 28
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set TypingSpeed 25ms
Set WaitTimeout 120s

Hide
Type "cd apps && clear"
Enter
Show

Type "# sluice-wicket-cf-api on a Firecracker wicket: the stock cf CLI runs container images as microVMs"
Enter
Sleep 1.5s
Type "cf version    # the OSS CLI v8"
Enter
Wait
Sleep 1s
Type "cf api http://cf.localhost:8080"
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

Type "# push: any container image; the wicket pulls it and makes its rootfs"
Enter
Sleep 1s
Type "cf push web --docker-image ghcr.io/traefik/whoami:v1.11.0"
Enter
Wait
Sleep 3s
Type "curl http://web.localhost:8080/"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "# logs: the process's output, from inside its VM, through sluice-flume"
Enter
Sleep 1s
Type "cf logs web --recent | grep APP/PROC"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "# or a root filesystem of your own: an ext4 image, here of a small sample server"
Enter
Sleep 1.5s
Type "du -h hello/*"
Enter
Wait
Sleep 1s
Type "cf push hello -p hello/"
Enter
Wait
Sleep 2s
Type "curl http://hello.localhost:8080/"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# routes apply at once; instances are VMs: 2 cloned from a snapshot"
Enter
Sleep 1s
Type "cf map-route hello localhost --hostname hi"
Enter
Wait
Sleep 1s
Type "curl -s http://hi.localhost:8080/ | head -2"
Enter
Wait
Sleep 1.5s
Type "cf scale hello -i 2"
Enter
Wait
Sleep 3s
Type `curl -s -H 'Authorization: Bearer wicket-secret' http://127.0.0.1:9090/api/apps | jq '.apps[] | select(.owner == "cf") | {hosts, mode: .sandbox.mode, vms: .sandbox.vms}'`
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# environment variables: the process's environment, at the next start"
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
Type "curl -s http://hello.localhost:8080/env | grep ^GREETING"
Enter
Wait
Sleep 1.5s
Type "curl -s http://hello.localhost:8080/env | sed -n 's/^VCAP_APPLICATION=//p' | jq '{application_name, application_uris}'"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# a restart of the wicket: the apps and the login come back, the VMs boot again"
Enter
Sleep 1s
Type "limactl shell sluice-firecracker sudo docker restart wicket"
Enter
Wait
Sleep 1s
Type "until curl -fs -o /dev/null http://web.localhost:8080/; do sleep 0.5; done"
Enter
Wait
Sleep 1s
Type "cf apps"
Enter
Wait
Sleep 4s
Type "cf delete hello -r -f"
Enter
Wait
Sleep 1.5s
Type `curl -s -o /dev/null -w '%{http_code}\n' http://hello.localhost:8080/    # gone from the wicket`
Enter
Wait
Sleep 4s
