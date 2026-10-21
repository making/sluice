# sluice-flume as a static musl binary on distroless static. The proto lives in
# sluice-proto, so the build context is the repository root:
#   docker build -f sluice-flume/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
FROM rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev protoc
WORKDIR /src
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-flume/Cargo.toml sluice-flume/Cargo.lock sluice-flume/build.rs sluice-flume/
COPY sluice-flume/src sluice-flume/src
# the context has no .git: the commit --version and the console show comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-flume/Cargo.toml

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/sluice-flume/target/release/sluice-flume /usr/local/bin/sluice-flume
EXPOSE 8002
ENTRYPOINT ["/usr/local/bin/sluice-flume"]
