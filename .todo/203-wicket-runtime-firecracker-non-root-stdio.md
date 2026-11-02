Difficulty: Low

# sluice-wicket-runtime-firecracker: an image's non-root process cannot open /dev/stdout or /dev/stderr

`ghcr.io/nginxinc/nginx-unprivileged:alpine` (user 101) does not start on the wicket: `PUT /api/images`
makes its rootfs, the PUT of the app is refused after 10s (the published image of 7360b20):

```text
nginx: [alert] could not open error log file: open() "/var/log/nginx/error.log" failed (13: Permission denied)
guest: /docker-entrypoint.sh exited (exit status: 1)
```

Its `error.log` links to `/dev/stderr`, as in the official images. `start` in `guest/src/main.rs` makes
the process's output pipes as root and `pre_exec` then drops to the image's user, so the process
writes to the fds it holds but cannot open them again by path: a pipe's inode is its maker's, 0600.
Seen apart from the wicket, in a container of that image: on a pipe root made, `echo > /dev/stderr` as
user 101 fails with `Permission denied`; with the pipe chowned to the user first, it works. runc chowns
a container's stdio for this (`fixStdioPermissions`), so the image runs under Docker.

So any image of a non-root user whose process opens its output by path fails, where the cf-api's README
says any image listening on `$PORT` will do.

- the failing test first: the sample process (`guest/src/bin/sample-process.rs`) opening `/dev/stderr`
  by path and saying so, under the image of `"User": "app"` in `tests/cli.rs`'s Firecracker test
- `fchown` of fds 1 and 2 to the user in `pre_exec`, before `setuid`: for the process and for the
  restore hook, the same spawn further down
- then nginx-unprivileged again: what it needs past this error is not known (its config writes to
  `/tmp`, which is writable)
