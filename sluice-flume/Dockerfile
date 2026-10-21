# sluice-flume as a static musl binary on distroless static. The proto lives in
# sluice-proto, so the build context is the repository root:
#   docker build -f sluice-flume/Dockerfile .
FROM rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev protoc
WORKDIR /src
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-flume/Cargo.toml sluice-flume/Cargo.lock sluice-flume/build.rs sluice-flume/
COPY sluice-flume/src sluice-flume/src
RUN cargo build --release --locked --manifest-path sluice-flume/Cargo.toml

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/sluice-flume/target/release/sluice-flume /usr/local/bin/sluice-flume
EXPOSE 8002
ENTRYPOINT ["/usr/local/bin/sluice-flume"]
