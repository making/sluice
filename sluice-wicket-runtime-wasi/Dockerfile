# sluice-wicket-runtime-wasi on distroless cc (glibc: wasmtime's many threads allocate through
# glibc's malloc, not musl's). The proto lives in sluice-proto, the tunnel client in
# sluice-wicket-tunnel, the OpenID Provider client in sluice-wicket-oidc, the connections in
# sluice-wicket-server, the console's shared parts in sluice-wicket-console and the gemfire
# keyvalue store in sluice-wicket-keyvalue-gemfire, so the build context is the repository root:
#   docker build -f sluice-wicket-runtime-wasi/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
FROM rust:1.99-slim-trixie AS build
RUN apt-get update -q && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-wicket-oidc/Cargo.toml sluice-wicket-oidc/
COPY sluice-wicket-oidc/src sluice-wicket-oidc/src
COPY sluice-wicket-server/Cargo.toml sluice-wicket-server/
COPY sluice-wicket-server/src sluice-wicket-server/src
COPY sluice-wicket-console/Cargo.toml sluice-wicket-console/
COPY sluice-wicket-console/src sluice-wicket-console/src
COPY sluice-wicket-tunnel/Cargo.toml sluice-wicket-tunnel/build.rs sluice-wicket-tunnel/
COPY sluice-wicket-tunnel/src sluice-wicket-tunnel/src
COPY sluice-wicket-keyvalue-gemfire/Cargo.toml sluice-wicket-keyvalue-gemfire/build.rs sluice-wicket-keyvalue-gemfire/
COPY sluice-wicket-keyvalue-gemfire/proto sluice-wicket-keyvalue-gemfire/proto
COPY sluice-wicket-keyvalue-gemfire/src sluice-wicket-keyvalue-gemfire/src
COPY sluice-wicket-runtime-wasi/Cargo.toml sluice-wicket-runtime-wasi/Cargo.lock sluice-wicket-runtime-wasi/build.rs sluice-wicket-runtime-wasi/
COPY sluice-wicket-runtime-wasi/src sluice-wicket-runtime-wasi/src
# the keyvalue bindings are generated from it
COPY sluice-wicket-runtime-wasi/wit sluice-wicket-runtime-wasi/wit
# the context has no .git: the commit --version and the console show comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-wicket-runtime-wasi/Cargo.toml \
    && mkdir /data

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/sluice-wicket-runtime-wasi/target/release/sluice-wicket-runtime-wasi /usr/local/bin/sluice-wicket-runtime-wasi
# the working directory, where the keyvalue store is created without a --config file:
# writable, a volume to keep it
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/sluice-wicket-runtime-wasi"]
