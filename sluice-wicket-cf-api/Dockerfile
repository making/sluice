# sluice-wicket-cf-api as a static musl binary on distroless static. The build script
# dependency lives in sluice-build-info, so the build context is the repository root:
#   docker build -f sluice-wicket-cf-api/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
FROM rust:1.99-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-wicket-cf-api/Cargo.toml sluice-wicket-cf-api/Cargo.lock sluice-wicket-cf-api/build.rs sluice-wicket-cf-api/
COPY sluice-wicket-cf-api/src sluice-wicket-cf-api/src
# the context has no .git: the commit --version and /v3/info show comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-wicket-cf-api/Cargo.toml \
    && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/sluice-wicket-cf-api/target/release/sluice-wicket-cf-api /usr/local/bin/sluice-wicket-cf-api
# the working directory, where the data directory (cf-api) is made: writable, a volume to keep it
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/sluice-wicket-cf-api"]
