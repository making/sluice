Difficulty: Low

# StreamRelayTest: SocketPair can hang in accept() for good

Seen while working on 081 (2026-10-12): a full `./mvnw verify` hung for 20 minutes in
`sluice-proto`'s `StreamRelayTest#abandonReleasesTheSendingSideParkedAtTheSenderCap`,
main thread in `ServerSocket.accept` from `StreamRelayTest.SocketPair#create` (jstack),
while another worktree ran its own verify on the same host.

`SocketPair#create` binds `new ServerSocket(0)` on the wildcard address and connects to
`127.0.0.1` on that port: a listener of another process bound to `127.0.0.1` on the same
port number is the more specific match, takes the connection, and the test's `accept`
waits forever. `@Timeout(10)` runs on the same thread and cannot interrupt a blocking
`accept`.

## Direction

Bind the server socket to the loopback address the client connects to
(`new ServerSocket(0, 1, InetAddress.getLoopbackAddress())`), and give `accept` an
`SO_TIMEOUT` so a stray connection fails the test instead of hanging the build. Look for
the same pattern in the other modules' tests (`grep -rn "new ServerSocket(0)"`).

## Test

Hold a listener on `127.0.0.1:<port>` in the test, then call the fixed `create()` with a
server on the same port number: it must not hang (or fail fast) -- before the fix it
hangs.
