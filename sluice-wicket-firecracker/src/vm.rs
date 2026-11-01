//! One Firecracker microVM: the `firecracker` process, configured through its API
//! socket (HTTP/1.1 over a Unix socket), and the vsock streams to its guest.
//!
//! The guest is reached over vsock only (no TAP device, so no root): Firecracker exposes
//! the device as a Unix socket on the host, and a host-initiated stream is a connect to
//! it followed by `CONNECT <port>\n`, answered `OK <host port>\n`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

use crate::BoxError;

/// Guest CID of every VM: each VM has its own vsock device, so they need not differ.
const GUEST_CID: u32 = 3;

/// How long the `firecracker` process may take to open its API socket.
const API_TIMEOUT: Duration = Duration::from_secs(5);

/// What a route's VMs are booted from.
#[derive(Clone, Debug)]
pub struct Spec {
    pub firecracker: PathBuf,
    pub kernel: PathBuf,
    pub rootfs: PathBuf,
    pub vcpus: u32,
    pub mem_mib: u32,
}

/// A paused VM's state and memory, which [`Vm::restore`] clones VMs from.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub firecracker: PathBuf,
    pub state: PathBuf,
    pub memory: PathBuf,
}

/// A running VM, killed and its directory removed on drop.
pub struct Vm {
    child: Child,
    dir: PathBuf,
    api: PathBuf,
    vsock: PathBuf,
}

impl Vm {
    /// Starts `firecracker` in `dir` (created; its API socket, vsock socket and log go
    /// there) and waits for its API socket.
    async fn spawn(firecracker: &Path, dir: PathBuf) -> Result<Vm, BoxError> {
        tokio::fs::create_dir_all(&dir).await?;
        let api = dir.join("api.sock");
        // sun_path holds 108 bytes, its NUL included
        if api.as_os_str().len() > 107 {
            return Err(format!(
                "{}: the VM's sockets need a path below 108 bytes; choose a shorter --state-dir",
                dir.display()
            )
            .into());
        }
        let log = std::fs::File::create(dir.join("firecracker.log"))?;
        let mut command = Command::new(firecracker);
        command
            .arg("--api-sock")
            .arg(&api)
            .current_dir(&dir)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        // SAFETY: prctl is async-signal-safe. Killed with the wicket however it ends
        // (a drop covers only an orderly end); the signal follows the spawning thread,
        // a runtime worker living as long as the wicket
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command
            .spawn()
            .map_err(|e| format!("{}: {e}", firecracker.display()))?;
        let vm = Vm {
            child,
            vsock: dir.join("v.sock"),
            api,
            dir,
        };
        let deadline = Instant::now() + API_TIMEOUT;
        while UnixStream::connect(&vm.api).await.is_err() {
            if Instant::now() > deadline {
                return Err(format!("no API socket within {API_TIMEOUT:?}; {}", vm.log()).into());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(vm)
    }

    /// Boots a fresh VM of `spec` in `dir` (cold boot).
    pub async fn boot(spec: &Spec, dir: PathBuf) -> Result<Vm, BoxError> {
        let vm = Vm::spawn(&spec.firecracker, dir).await?;
        vm.api(
            "PUT",
            "/machine-config",
            json!({ "vcpu_count": spec.vcpus, "mem_size_mib": spec.mem_mib }),
        )
        .await?;
        vm.api(
            "PUT",
            "/boot-source",
            json!({
                "kernel_image_path": spec.kernel,
                "boot_args": boot_args(),
            }),
        )
        .await?;
        vm.api(
            "PUT",
            "/drives/rootfs",
            json!({
                "drive_id": "rootfs",
                "path_on_host": spec.rootfs,
                "is_root_device": true,
                // shared by every VM of the route, and by the clones of its snapshot
                "is_read_only": true,
            }),
        )
        .await?;
        vm.api(
            "PUT",
            "/vsock",
            json!({ "guest_cid": GUEST_CID, "uds_path": vm.vsock }),
        )
        .await?;
        vm.api("PUT", "/actions", json!({ "action_type": "InstanceStart" }))
            .await?;
        Ok(vm)
    }

    /// Clones a running VM from `snapshot` into `dir`: its memory file is mapped
    /// private (copy on write), so clones share the pages they do not write.
    pub async fn restore(snapshot: &Snapshot, dir: PathBuf) -> Result<Vm, BoxError> {
        let vm = Vm::spawn(&snapshot.firecracker, dir).await?;
        vm.api(
            "PUT",
            "/snapshot/load",
            json!({
                "snapshot_path": snapshot.state,
                "mem_backend": { "backend_type": "File", "backend_path": snapshot.memory },
                "vsock_override": { "uds_path": vm.vsock },
                "resume_vm": true,
            }),
        )
        .await?;
        Ok(vm)
    }

    /// Pauses the VM and writes its full snapshot into `dir`; the VM stays paused.
    pub async fn snapshot(&self, dir: &Path) -> Result<Snapshot, BoxError> {
        tokio::fs::create_dir_all(dir).await?;
        let snapshot = Snapshot {
            firecracker: PathBuf::new(),
            state: dir.join("vm.state"),
            memory: dir.join("vm.mem"),
        };
        self.api("PATCH", "/vm", json!({ "state": "Paused" }))
            .await?;
        self.api(
            "PUT",
            "/snapshot/create",
            json!({
                "snapshot_type": "Full",
                "snapshot_path": snapshot.state,
                "mem_file_path": snapshot.memory,
            }),
        )
        .await?;
        Ok(snapshot)
    }

    /// Opens a stream to the guest's vsock `port`.
    pub async fn dial(&self, port: u32) -> Result<UnixStream, BoxError> {
        let mut stream = UnixStream::connect(&self.vsock).await?;
        stream
            .write_all(format!("CONNECT {port}\n").as_bytes())
            .await?;
        // byte by byte: what follows the line is the guest's
        let mut line = Vec::with_capacity(16);
        loop {
            let byte = stream.read_u8().await?;
            if byte == b'\n' {
                break;
            }
            line.push(byte);
            if line.len() > 64 {
                return Err("vsock handshake: overlong answer".into());
            }
        }
        if !line.starts_with(b"OK ") {
            return Err(
                format!("vsock handshake: {}", String::from_utf8_lossy(&line).trim()).into(),
            );
        }
        Ok(stream)
    }

    /// [`Vm::dial`] retried until the guest listens on `port`, within `timeout`.
    pub async fn dial_ready(&self, port: u32, timeout: Duration) -> Result<UnixStream, BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.dial(port).await {
                Ok(stream) => return Ok(stream),
                Err(e) if Instant::now() > deadline => {
                    return Err(format!(
                        "guest not listening on vsock port {port} within {timeout:?}: {e}; {}",
                        self.log()
                    )
                    .into());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    }

    /// The tail of the VM's log (Firecracker's and the guest console's): its directory
    /// goes with the VM.
    fn log(&self) -> String {
        let log = std::fs::read_to_string(self.dir.join("firecracker.log")).unwrap_or_default();
        let lines: Vec<_> = log.lines().collect();
        format!(
            "firecracker log:\n{}",
            lines[lines.len().saturating_sub(10)..].join("\n")
        )
    }

    /// One request to the API socket; a non-2xx answer is an error naming its body.
    async fn api(&self, method: &str, path: &str, body: Value) -> Result<(), BoxError> {
        let stream = UnixStream::connect(&self.api).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(connection);
        let request = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.into_body().collect().await?.to_bytes();
        Err(format!(
            "firecracker {method} {path}: {status} {}",
            String::from_utf8_lossy(&body)
        )
        .into())
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// No serial console unless `SLUICE_FIRECRACKER_CONSOLE` is set (the guest's output then
/// goes to the VM's log): printing to it is the slowest part of a boot.
fn boot_args() -> String {
    let console = match std::env::var_os("SLUICE_FIRECRACKER_CONSOLE") {
        Some(_) => "console=ttyS0",
        None => "console=none 8250.nr_uarts=0",
    };
    format!("{console} reboot=k panic=1 pci=off quiet init=/init")
}
