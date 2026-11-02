# sluice-wicket-runtime-firecracker on distroless cc (glibc), with the pinned Firecracker release,
# a guest kernel of Firecracker's CI, the guest (/init of the wicket's own version) and the
# sample rootfs. The proto lives in sluice-proto, the tunnel client in sluice-wicket-tunnel and
# the console's shared parts in sluice-wicket-console (its sign-in in sluice-wicket-oidc) and the
# image pulls in sluice-wicket-registry, so the build context is the repository root:
#   docker build -f sluice-wicket-runtime-firecracker/Dockerfile --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
# amd64 and arm64: the release, the kernel and the guest are the target's (a VM's arch is
# its host's).
ARG TARGETARCH

# the release and the kernel of each arch, checksums verified: the ones the CI's VM test
# pins (.github/workflows/rust.yaml)
FROM scratch AS vm-amd64
ADD --checksum=sha256:06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558 \
    https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-x86_64.tgz /firecracker.tgz
ADD --checksum=sha256:a00c1f68304e83ae4e0b5afe88972ad23fda52ed1a40335611aa18faa95d1b10 \
    https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261028-84df49062576-0/x86_64/vmlinux-6.18.56 /vmlinux

FROM scratch AS vm-arm64
ADD --checksum=sha256:e351ebe4f7a16b5873bbd51005d2e6767103cff4d5ebc829df2d3f95a93e2256 \
    https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-aarch64.tgz /firecracker.tgz
ADD --checksum=sha256:ba2081f71b3405e7f7be606b849b5341d2272dcb79999cb67f8061e711d0883f \
    https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261028-84df49062576-0/aarch64/vmlinux-6.18.56 /vmlinux

FROM vm-${TARGETARCH} AS vm

FROM rust:1.99-slim-trixie AS build
RUN apt-get update -q && apt-get install -y --no-install-recommends protobuf-compiler e2fsprogs \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add "$(uname -m)-unknown-linux-musl"
WORKDIR /src
# the guest first: it changes less often than the wicket
COPY sluice-wicket-runtime-firecracker/guest/Cargo.toml sluice-wicket-runtime-firecracker/guest/Cargo.lock sluice-wicket-runtime-firecracker/guest/
COPY sluice-wicket-runtime-firecracker/guest/.cargo sluice-wicket-runtime-firecracker/guest/.cargo
COPY sluice-wicket-runtime-firecracker/guest/src sluice-wicket-runtime-firecracker/guest/src
COPY sluice-wicket-runtime-firecracker/scripts/guest.sh sluice-wicket-runtime-firecracker/scripts/build-guest.sh sluice-wicket-runtime-firecracker/scripts/
RUN mkdir /out \
    && cp "sluice-wicket-runtime-firecracker/$(sluice-wicket-runtime-firecracker/scripts/guest.sh)" /out/init \
    && SLUICE_GUEST=/out/init sluice-wicket-runtime-firecracker/scripts/build-guest.sh /out/guest.ext4
# a remote ADD leaves the kernel 0600 (ADD --chmod would set the mode of the directories it
# creates too)
COPY --from=vm /firecracker.tgz /vmlinux /tmp/
RUN arch=$(uname -m) \
    && tar -xzf /tmp/firecracker.tgz -C /tmp \
    && install /tmp/release-v1.17.0-$arch/firecracker-v1.17.0-$arch /out/firecracker \
    && install -m 0644 /tmp/vmlinux /out/vmlinux
COPY sluice-proto/src/main/proto sluice-proto/src/main/proto
COPY sluice-build-info/Cargo.toml sluice-build-info/
COPY sluice-build-info/src sluice-build-info/src
COPY sluice-wicket-oidc/Cargo.toml sluice-wicket-oidc/
COPY sluice-wicket-oidc/src sluice-wicket-oidc/src
COPY sluice-wicket-console/Cargo.toml sluice-wicket-console/
COPY sluice-wicket-console/src sluice-wicket-console/src
COPY sluice-wicket-registry/Cargo.toml sluice-wicket-registry/
COPY sluice-wicket-registry/src sluice-wicket-registry/src
COPY sluice-wicket-tunnel/Cargo.toml sluice-wicket-tunnel/build.rs sluice-wicket-tunnel/
COPY sluice-wicket-tunnel/src sluice-wicket-tunnel/src
COPY sluice-wicket-runtime-firecracker/Cargo.toml sluice-wicket-runtime-firecracker/Cargo.lock sluice-wicket-runtime-firecracker/build.rs sluice-wicket-runtime-firecracker/
COPY sluice-wicket-runtime-firecracker/src sluice-wicket-runtime-firecracker/src
# the context has no .git: the commit --version shows comes from here
ARG GIT_COMMIT=""
RUN SLUICE_GIT_COMMIT=${GIT_COMMIT} \
    cargo build --release --locked --manifest-path sluice-wicket-runtime-firecracker/Cargo.toml \
    && mkdir -p -m 1777 /run-dir/sluice \
    && mkdir /data
# what PUT /api/images makes a rootfs with: e2fsprogs' mkfs.ext4 and debugfs, their libraries
# but the base image's libc, each at its real path (the base image's /lib is a link)
RUN mkdir -p /tools/usr/sbin /tools/etc \
    && cp -L /usr/sbin/mkfs.ext4 /usr/sbin/debugfs /tools/usr/sbin/ \
    && cp /etc/mke2fs.conf /tools/etc/ \
    && ldd /usr/sbin/mkfs.ext4 /usr/sbin/debugfs | awk '$3 ~ /^\// {print $3}' | grep -v '/libc\.so' | sort -u \
    | while read -r lib; do \
        dir=$(dirname "$(readlink -f "$lib")"); mkdir -p "/tools$dir" && cp -L "$lib" "/tools$dir/"; \
      done

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker /out/firecracker /usr/local/bin/
# the kernel; /init for a rootfs of one's own (scripts/build-guest.sh, scripts/cnb-rootfs.sh
# with SLUICE_GUEST); the sample rootfs
COPY --from=build /out/vmlinux /out/init /out/guest.ext4 /usr/local/share/sluice-wicket-runtime-firecracker/
COPY --from=build /tools/ /
# the VMs' files (route-<n>, 0700): a tmpfs mounted there keeps the snapshots off the disk.
# 1777 as /tmp: a tmpfs over it takes the directory's mode, not its owner
COPY --from=build /run-dir/ /run/
ENV SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL=/usr/local/share/sluice-wicket-runtime-firecracker/vmlinux \
    SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER=/usr/local/bin/firecracker \
    SLUICE_WICKET_RUNTIME_FIRECRACKER_GUEST=/usr/local/share/sluice-wicket-runtime-firecracker/init \
    SLUICE_WICKET_RUNTIME_FIRECRACKER_STATE_DIR=/run/sluice
# the working directory, where --console keeps its routes (deployed/) without a --config
# file: writable by the wicket's user, a volume to keep it
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/sluice-wicket-runtime-firecracker"]
