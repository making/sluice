# The cf-api on the Firecracker wicket (todo 200)

`./spike.sh`: what the item's "Measured" says, run again. It builds the cf-api's image and the
wicket from the working tree, and fails if a step behaves otherwise. Step 4 shows the bug: once the
item is done, invert it (the app kept, the token still taken).

Ports 18399 and 19399 (`LISTEN_PORT`, `CONSOLE_PORT`) are fixed: the cf-api's egress names the
console's. On `--listen`, a sole route answers any host, so a host check after step 4 proves nothing.
