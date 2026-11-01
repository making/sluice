Difficulty: Low

# sluice-wicket-firecracker: Docker image

Package the wicket as the other Rust components are (`sluice-flume/Dockerfile`,
`sluice-wicket-wasi/Dockerfile`: built from the repository root, `SLUICE_GIT_COMMIT` as the
`GIT_COMMIT` build arg):

- The image holds the wicket, the `firecracker` binary (pinned release, checksum verified)
  and a guest kernel (pinned Firecracker CI build); rootfs images come in through a volume.
  `scripts/cnb-rootfs.sh` stays a build-time tool, not in the image
- Runs with `--device /dev/kvm --group-add <gid of /dev/kvm>`, without `--privileged` and
  without `NET_ADMIN` (vsock only): verify that, and that Firecracker's own seccomp and
  Docker's default profile get along
- The VMs' memory (clones included) counts against the container's cgroup: document the
  limit to set; `--state-dir` inside the container keeps its sockets below 108 bytes
- README: the `docker run` line, and that the host needs KVM (bare metal or nested
  virtualization); Kubernetes needs `/dev/kvm` passed to the Pod (a device plugin such as
  KubeVirt's, or privileged)
- Optional: publish it in CI as the other images (amd64 only until aarch64 is supported)
