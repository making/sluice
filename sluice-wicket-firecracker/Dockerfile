# sluice-wicket-firecracker on distroless cc (glibc), with the pinned Firecracker release,
# a guest kernel of Firecracker's CI, the guest (/init of the wicket's own version) and the
# sample rootfs. The proto lives in sluice-proto and the tunnel client in sluice-tunnel, so
# the build context is the repository root:
#   docker build -f sluice-wicket-firecracker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
# x86_64 only (the musl guest, the release and the kernel are x86_64's).
FROM rust:1.99-slim-trixie AS build
RUN apt-get update -q && apt-get install -y --no-install-recommends protobuf-compiler e2fsprogs \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /src
# the guest first: it changes less often than the wicket
COPY sluice-wicket-firecracker/guest/Cargo.toml sluice-wicket-firecracker/guest/Cargo.lock sluice-wicket-firecracker/guest/
COPY sluice-wicket-firecracker/guest/src sluice-wicket-firecracker/guest/src
COPY sluice-wicket-firecracker/scripts/build-guest.sh sluice-wicket-firecracker/scripts/
RUN cargo build --release --locked --target x86_64-unknown-linux-musl \
        --manifest-path sluice-wicket-firecracker/guest/Cargo.toml \
    && mkdir /out \
    && cp sluice-wicket-firecracker/guest/target/x86_64-unknown-linux-musl/release/sluice-firecracker-guest /out/init \
    && SLUICE_GUEST=/out/init sluice-wicket-firecracker/scripts/build-guest.sh /out/guest.ext4
# the release and checksum the CI's VM test pins (.github/workflows/rust.yaml)
ADD --checksum=sha256:06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558 \
    https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-x86_64.tgz /tmp/
# the kernel the CI's VM test pins (a remote ADD leaves it 0600; ADD --chmod would set the
# mode of the directories it creates too)
ADD --checksum=sha256:a00c1f68304e83ae4e0b5afe88972ad23fda52ed1a40335611aa18faa95d1b10 \
    https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261028-84df49062576-0/x86_64/vmlinux-6.18.56 /out/vmlinux
RUN tar -xzf /tmp/firecracker-v1.17.0-x86_64.tgz -C /tmp \
    && install /tmp/release-v1.17.0-x86_64/firecracker-v1.17.0-x86_64 /out/firecracker \
    && chmod 0644 /out/vmlinux
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-tunnel/Cargo.toml sluice-tunnel/build.rs sluice-tunnel/
COPY sluice-tunnel/src sluice-tunnel/src
COPY sluice-wicket-firecracker/Cargo.toml sluice-wicket-firecracker/Cargo.lock sluice-wicket-firecracker/build.rs sluice-wicket-firecracker/
COPY sluice-wicket-firecracker/src sluice-wicket-firecracker/src
# the context has no .git: the commit --version shows comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-wicket-firecracker/Cargo.toml \
    && mkdir -p -m 1777 /run-dir/sluice

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/sluice-wicket-firecracker/target/release/sluice-wicket-firecracker /out/firecracker /usr/local/bin/
# the kernel; /init for a rootfs of one's own (scripts/build-guest.sh, scripts/cnb-rootfs.sh
# with SLUICE_GUEST); the sample rootfs
COPY --from=build /out/vmlinux /out/init /out/guest.ext4 /usr/local/share/sluice-wicket-firecracker/
# the VMs' files (route-<n>, 0700): a tmpfs mounted there keeps the snapshots off the disk.
# 1777 as /tmp: a tmpfs over it takes the directory's mode, not its owner
COPY --from=build /run-dir/ /run/
ENV SLUICE_WICKET_FIRECRACKER_KERNEL=/usr/local/share/sluice-wicket-firecracker/vmlinux \
    SLUICE_WICKET_FIRECRACKER_FIRECRACKER=/usr/local/bin/firecracker \
    SLUICE_WICKET_FIRECRACKER_STATE_DIR=/run/sluice
ENTRYPOINT ["/usr/local/bin/sluice-wicket-firecracker"]
