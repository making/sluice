//! A route: the VMs serving one host, and how a connection gets one of them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc};

use crate::BoxError;
use crate::vm::{Snapshot, Spec, Vm};
use crate::wire;

/// How long a cold-booted guest may take to listen on its port.
const BOOT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a restored guest may take to accept a stream again (its vsock transport
/// is reset on restore).
const RESTORE_TIMEOUT: Duration = Duration::from_secs(2);

/// Most clones of a route restored at once to refill its warm pool.
const REFILL_WORKERS: usize = 8;

/// How a connection of the route gets its VM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// One VM, booted when the wicket starts, serves every connection.
    Shared,
    /// Every connection gets a VM of its own, cloned from the route's snapshot and
    /// killed when the connection ends: the isolation of the wasm wicket's
    /// instance per request, at the granularity of a connection.
    PerConnection,
}

/// One `--vm` flag.
#[derive(Clone, Debug)]
pub struct RouteSpec {
    pub host: String,
    pub vm: Spec,
    pub mode: Mode,
    /// The guest's vsock port.
    pub port: u32,
    /// Per-connection mode: clones kept restored ahead of the connections.
    pub warm: usize,
    /// Per-connection mode: `GET /` requests sent to the template before its snapshot,
    /// so the clones start from a warmed-up process (JIT, lazily loaded code).
    pub prime: u32,
}

impl RouteSpec {
    /// Parses `host=<rootfs>[?mode=shared|per-connection&vcpus=N&mem=MiB&port=N&warm=N&prime=N]`.
    pub fn parse(
        flag: &str,
        firecracker: &std::path::Path,
        kernel: &std::path::Path,
    ) -> Result<RouteSpec, BoxError> {
        let (host, locator) = flag
            .split_once('=')
            .ok_or_else(|| format!("--vm {flag}: expected host=<rootfs>"))?;
        let (rootfs, query) = locator.split_once('?').unwrap_or((locator, ""));
        let mut spec = RouteSpec {
            host: host.to_string(),
            vm: Spec {
                firecracker: firecracker.to_path_buf(),
                kernel: kernel.to_path_buf(),
                rootfs: std::path::absolute(rootfs)?,
                vcpus: 1,
                mem_mib: 128,
            },
            mode: Mode::Shared,
            port: 8080,
            warm: 0,
            prime: 0,
        };
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let number = || -> Result<u32, BoxError> {
                value
                    .parse()
                    .map_err(|_| format!("--vm {flag}: {key}={value} is not a number").into())
            };
            match key {
                "mode" => {
                    spec.mode = match value {
                        "shared" => Mode::Shared,
                        "per-connection" => Mode::PerConnection,
                        _ => return Err(format!("--vm {flag}: unknown mode {value}").into()),
                    }
                }
                "vcpus" => spec.vm.vcpus = number()?,
                "mem" => spec.vm.mem_mib = number()?,
                "port" => spec.port = number()?,
                "warm" => spec.warm = number()? as usize,
                "prime" => spec.prime = number()?,
                _ => return Err(format!("--vm {flag}: unknown key {key}").into()),
            }
        }
        if !spec.vm.rootfs.is_file() {
            return Err(format!("--vm {flag}: {} is not a file", spec.vm.rootfs.display()).into());
        }
        Ok(spec)
    }

    /// The target advertised for the route. The server dials `host[:port]` of a target
    /// URL (only `wasm:` targets go verbatim), so every CONNECT for it names
    /// [`RouteSpec::address`].
    pub fn target(&self) -> String {
        format!("firecracker://{}", self.host)
    }

    /// What a CONNECT for the route names.
    pub fn address(&self) -> String {
        self.host.clone()
    }
}

/// A stream to a guest, and the VM it holds for the connection's lifetime.
pub struct Lease {
    pub stream: UnixStream,
    /// Per-connection mode: the connection's own VM, killed with the lease.
    pub vm: Option<Vm>,
}

pub struct Route {
    pub spec: RouteSpec,
    backend: Backend,
}

enum Backend {
    Shared(Vm),
    PerConnection {
        snapshot: Snapshot,
        clones: Clones,
        /// Restored ahead of the connections, refilled as they are taken.
        warm: Mutex<mpsc::Receiver<Vm>>,
    },
}

/// Names the clone directories of a route.
#[derive(Clone)]
struct Clones {
    dir: PathBuf,
    next: Arc<AtomicU64>,
}

impl Clones {
    fn next(&self) -> PathBuf {
        self.dir
            .join(format!("vm-{}", self.next.fetch_add(1, Ordering::Relaxed)))
    }
}

impl Route {
    /// Boots the route's VM (shared), or boots a template VM, snapshots it once its
    /// guest listens and kills it (per-connection); `dir` holds the route's files.
    pub async fn start(spec: RouteSpec, dir: PathBuf) -> Result<Route, BoxError> {
        let started = Instant::now();
        let vm = Vm::boot(&spec.vm, dir.join("boot")).await?;
        drop(vm.dial_ready(spec.port, BOOT_TIMEOUT).await?);
        let booted = started.elapsed();
        let backend = match spec.mode {
            Mode::Shared => {
                println!(
                    "route {}: VM booted, guest listening after {}ms",
                    spec.host,
                    booted.as_millis()
                );
                Backend::Shared(vm)
            }
            Mode::PerConnection => {
                for _ in 0..spec.prime {
                    prime(&vm, spec.port).await?;
                }
                let mut snapshot = vm.snapshot(&dir.join("snapshot")).await?;
                snapshot.firecracker = spec.vm.firecracker.clone();
                drop(vm);
                let size = tokio::fs::metadata(&snapshot.memory).await?.len();
                println!(
                    "route {}: template booted in {}ms, primed with {} request(s), snapshot taken after {}ms ({} MiB memory file)",
                    spec.host,
                    booted.as_millis(),
                    spec.prime,
                    started.elapsed().as_millis(),
                    size >> 20
                );
                let clones = Clones {
                    dir: dir.join("clones"),
                    next: Arc::default(),
                };
                let (refill, warm) = mpsc::channel(spec.warm.max(1));
                // restores run in parallel: one at a time caps the clone rate at about
                // 1 / restore time
                for _ in 0..spec.warm.min(REFILL_WORKERS) {
                    tokio::spawn(keep_warm(
                        snapshot.clone(),
                        clones.clone(),
                        spec.port,
                        refill.clone(),
                    ));
                }
                Backend::PerConnection {
                    snapshot,
                    clones,
                    warm: Mutex::new(warm),
                }
            }
        };
        Ok(Route { spec, backend })
    }

    /// A stream to a guest for one connection.
    pub async fn open(&self) -> Result<Lease, BoxError> {
        match &self.backend {
            Backend::Shared(vm) => Ok(Lease {
                stream: vm.dial(self.spec.port).await?,
                vm: None,
            }),
            Backend::PerConnection {
                snapshot,
                clones,
                warm,
            } => {
                let started = Instant::now();
                let (vm, warm) = match warm.lock().await.try_recv() {
                    Ok(vm) => (vm, true),
                    Err(_) => (Vm::restore(snapshot, clones.next()).await?, false),
                };
                let restored = started.elapsed();
                let stream = vm.dial_ready(self.spec.port, RESTORE_TIMEOUT).await?;
                if std::env::var_os("SLUICE_FIRECRACKER_TRACE").is_some() {
                    eprintln!(
                        "route {}: {} clone, restored in {}us, guest accepting after {}us",
                        self.spec.host,
                        if warm { "warm" } else { "fresh" },
                        restored.as_micros(),
                        started.elapsed().as_micros()
                    );
                }
                Ok(Lease {
                    stream,
                    vm: Some(vm),
                })
            }
        }
    }
}

/// One `GET /` to the guest, its answer read to the guest's CLOSE.
async fn prime(vm: &Vm, port: u32) -> Result<(), BoxError> {
    let stream = vm.dial(port).await?;
    let (reader, mut writer) = stream.into_split();
    wire::write_data(
        &mut writer,
        b"GET / HTTP/1.1\r\nHost: prime\r\nConnection: close\r\n\r\n",
    )
    .await?;
    wire::write_close(&mut writer).await?;
    let mut reader = wire::Reader::new(reader);
    let mut answer = Vec::new();
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        match reader.read(&mut buffer).await? {
            wire::Read::Data(n) => answer.extend_from_slice(&buffer[..n]),
            wire::Read::Close => break,
            wire::Read::Eof => return Err("priming request: no CLOSE from the guest".into()),
        }
    }
    if !answer.starts_with(b"HTTP/1.1 2") {
        return Err(format!(
            "priming request: {}",
            String::from_utf8_lossy(&answer[..answer.len().min(80)])
        )
        .into());
    }
    Ok(())
}

/// Keeps `refill`'s buffer full of restored clones until the route is gone.
async fn keep_warm(snapshot: Snapshot, clones: Clones, port: u32, refill: mpsc::Sender<Vm>) {
    loop {
        let Ok(permit) = refill.reserve().await else {
            return;
        };
        match Vm::restore(&snapshot, clones.next()).await {
            Ok(vm) => {
                // ready before it is handed out: a warm clone answers at once
                match vm.dial_ready(port, RESTORE_TIMEOUT).await {
                    Ok(probe) => drop(probe),
                    Err(e) => {
                        eprintln!("warm clone: {e}");
                        continue;
                    }
                }
                permit.send(vm);
            }
            Err(e) => {
                eprintln!("warm clone: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// The routes by the address their CONNECTs name.
pub type Routes = HashMap<String, Arc<Route>>;
