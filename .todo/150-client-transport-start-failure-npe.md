Difficulty: Medium

# sluice-client (native, Docker): NPE in NettyClientTransport.shutdownNow on connect

Reopens the cancelled 143 (2026-10-17): the user still sees it with the latest native
image when the client starts and connects to `grpcs://control.sluice.ik.am`:

```
ERROR io.grpc.internal.ManagedChannelImpl [Channel<13>: (control.sluice.ik.am:443)]
  Uncaught exception in the SynchronizationContext. Panic!
java.lang.NullPointerException
  at io.grpc.netty.NettyClientTransport.shutdownNow(NettyClientTransport.java:377)
  ...
  at io.grpc.internal.InternalSubchannel$8.run(InternalSubchannel.java:513)
  ...
  at am.ik.sluice.client.tunnel.NodeConnection.shutdownChannel(NodeConnection.java:761)
  at am.ik.sluice.client.tunnel.NodeConnection.runLoop(NodeConnection.java:392)
```

## What is known

- grpc-netty 1.83.1 line 377 is `handler.getWriteQueue().enqueue(...)` behind
  `channel != null && channel.isOpen()`: the write queue is null while the channel exists.
- `NettyClientTransport.start()` assigns `channel` and only later calls
  `handler.startWriteQueue(channel)`; in between sits the epoll / TCP_USER_TIMEOUT block
  (`Class.forName`, catching only `ClassNotFoundException`). `InternalSubchannel` adds the
  transport to `transports` before `start()`. So a `start()` that threw in that window
  leaves a transport whose `shutdownNow` NPEs: this NPE is the second symptom; an earlier
  `Panic!` carries the cause. `Channel<13>` means twelve channels came before.
- Not reproduced locally (arm64 Docker images at 19144a0): grpc and grpcs (insecure, and
  untrusted cert) connect / fail cleanly; the f83806a notes in 143 list the rest. The amd64
  image cannot run here (AVX).

## Work

1. Get the first `ERROR` / `Panic!` of the failing run, the `docker run` command (memory
   limit?), and the host OS / arch from the user.
2. Reproduce that cause locally, fix it, and make the client survive a transport whose
   start failed (the run loop must keep reconnecting).
3. Verify with the Docker native image against a local server.
