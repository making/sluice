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

## Cause (from the user's log, 2026-10-17)

The first error, at the very first connect (Channel<1>), on the user's host (Docker
Compose, `memory: 1g`, `grpcs://control.sluice.ik.am`; the JVM image of the same commit
works):

```
java.lang.RuntimeException: ChannelOption(TCP_USER_TIMEOUT) is not available
  at io.grpc.netty.Utils.getEpollChannelOption(Utils.java:482)
  at io.grpc.netty.Utils.maybeGetTcpUserTimeoutOption(Utils.java:469)
  at io.grpc.netty.NettyClientTransport.start(NettyClientTransport.java:312)
Caused by: java.lang.NoSuchFieldException: TCP_USER_TIMEOUT
  at java.lang.Class.getField(DynamicHub.java:1285)
  at io.grpc.netty.Utils.getEpollChannelOption(Utils.java:479)
```

On that host the transport channel is an epoll channel (`Utils.isEpollAvailable()` true),
and with keep-alive on, grpc reads `EpollChannelOption.TCP_USER_TIMEOUT` reflectively; the
native image has no reflection registration for that field. Every channel fails the same
way (Channel<13> later), so the client never connects. Locally (Docker Desktop arm64)
epoll is not available, so NIO is used and the path is never taken. The native binaries
of both arches contain `EpollEventLoopGroup` although `dependency:list` of sluice-client
shows no netty epoll artifact: find where the epoll classes come from.

## Work

1. (done) The user's log.
2. Reproduce that cause locally (make epoll available to the native client), fix it, and make the client survive a transport whose
   start failed (the run loop must keep reconnecting).
3. Verify with the Docker native image against a local server.

Status (2026-10-17): epoll reflection metadata for grpc-netty (EpollChannelOption fields, Epoll, EpollEventLoopGroup, Epoll channel types, EpollTcpInfo) is pushed in the client and server `reflect-config.json`; awaits the user's verification of the image. Epoll comes from the OS-activated `linux` profile of spring-grpc-core 1.1.1 (netty-transport-native-epoll `linux-x86_64`, runtime), so Linux builds of both arches ship it and only amd64 can load the library.
