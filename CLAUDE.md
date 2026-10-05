# sluice

TCP tunnel over a single gRPC bidirectional stream; the HTTP reverse proxy is its first consumer.

## Layout

Maven multi-module. Build + test with `./mvnw verify` (JDK 25 required).

- `sluice-proto`: proto + generated stubs + shared tunnel machinery (`am.ik.sluice.tunnel`: VirtualConnection / SessionSender / SocketRelay)
- `sluice-server`: gRPC control plane (8001) + raw TCP data plane (`sluice.data-port`, 8000) + actuator (8081)
- `sluice-client`: tunnel client. grpc:// / grpcs:// (TLS verification skip via `sluice.insecure`)
- `sluice-it`: full stack E2E tests (real server + client apps in one JVM); E2E tests live here, not in the app modules

The data plane relays raw bytes (only the Host header of the first request is inspected). Honoring half-close (CLOSE frame) is essential for keep-alive / WebSocket passthrough.

## Conventions

- Coding standards: user-level skills (java-code-standards / spring-code-standards / java-package-structure / java-testing-standards). spring-javaformat validate is wired into the build.
- gRPC 1.83.x quirks: `StreamObserver` / `ClientCallStreamObserver` live in `io.grpc.stub`. The client onReady handler may only be set inside `ClientResponseObserver#beforeStart`.
- Boot 4.1: `HealthIndicator` is in `org.springframework.boot.health.contributor`.
- Docker images via buildpack (`spring-boot:build-image`); no Dockerfile.

## Manual E2E check

```
java -jar sluice-server/target/sluice-server-*-exec.jar --spring.grpc.server.port=18001 --sluice.data-port=18000 --server.port=18081
java -jar sluice-client/target/sluice-client-*-exec.jar --sluice.server-url=grpc://127.0.0.1:18001 --sluice.upstream=demo.local=http://127.0.0.1:31080
curl -H 'Host: demo.local' http://127.0.0.1:18000/
```

## Open work

See `.todo/`. On completion, append a row to `.todo/history/YYYY-MM.md` (see existing rows for the format).
