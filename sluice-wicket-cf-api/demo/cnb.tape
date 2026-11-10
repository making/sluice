# The README's demo of source staged by buildpacks on a Firecracker wicket. record-cnb.sh runs it
# in ../target/demo-cnb (apps/ holds the sources, out/ takes the video) on a fresh stack, the
# builder's rootfs made before.
Output "out/sluice-cf-cnb-demo.mp4"

Require cf
Require curl

Set Shell bash
Set FontSize 16
Set Width 1600
Set Height 1000
Set Padding 28
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set TypingSpeed 25ms
Set WaitTimeout 300s

Hide
Type "cd apps && clear"
Enter
Show

Type "# sluice-wicket-cf-api on a Firecracker wicket: cf push of source, built by Cloud Native Buildpacks in a microVM"
Enter
Sleep 1.5s
Type "cf api http://127.0.0.1:9091"
Enter
Wait
Sleep 1s
Type "cf login -u demo -p demo"
Enter
Wait
Sleep 2s
Type "cf buildpacks    # cnb: the builder the cf-api names (--cnb-builder paketobuildpacks/ubuntu-resolute-builder)"
Enter
Wait
Sleep 3s

Hide
Type "clear"
Enter
Show

Type "# source alone: no image, no Dockerfile. The builder's buildpacks detect and build it in a VM of a wicket"
Enter
Sleep 1.5s
Type "ls hello-node/"
Enter
Wait
Sleep 1s
Type "cf push node -p hello-node/ -m 512M"
Enter
Wait
Sleep 3s
Type "curl http://node.localhost:8080/    # the app, in a microVM of its own"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# the same builder, another language: Maven builds a Spring Boot app"
Enter
Sleep 1.5s
Type "find hello-java -type f"
Enter
Wait
Sleep 1s
Type "cf push java -p hello-java/ -m 1G"
Enter
Wait
Sleep 3s
Type "curl http://java.localhost:8080/"
Enter
Wait
Sleep 4s

Hide
Type "clear"
Enter
Show

Type "# the droplet is what the buildpacks made, laid over the builder's run image: built once, placed on any wicket"
Enter
Sleep 1.5s
Type "cf apps"
Enter
Wait
Sleep 2s
Type "cf logs java --recent | grep -E 'STG|Started' | tail -8"
Enter
Wait
Sleep 5s
