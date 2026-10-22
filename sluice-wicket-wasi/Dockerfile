# sluice-wicket-wasi on distroless cc (glibc: wasmtime's many threads allocate through
# glibc's malloc, not musl's). The proto lives in sluice-proto, so the build context is
# the repository root:
#   docker build -f sluice-wicket-wasi/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
FROM rust:1.99-slim-trixie AS build
RUN apt-get update -q && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-wicket-wasi/Cargo.toml sluice-wicket-wasi/Cargo.lock sluice-wicket-wasi/build.rs sluice-wicket-wasi/
COPY sluice-wicket-wasi/src sluice-wicket-wasi/src
# the keyvalue bindings are generated from it
COPY sluice-wicket-wasi/wit sluice-wicket-wasi/wit
# the context has no .git: the commit --version and the console show comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-wicket-wasi/Cargo.toml \
    && mkdir /data

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/sluice-wicket-wasi/target/release/sluice-wicket-wasi /usr/local/bin/sluice-wicket-wasi
# the working directory, where the keyvalue store is created without a --config file:
# writable, a volume to keep it
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/sluice-wicket-wasi"]
