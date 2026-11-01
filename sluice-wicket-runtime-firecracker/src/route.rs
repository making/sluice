//! A route: the VMs serving one target (its hosts), and how a connection gets one of
//! them. The VMs' output is followed from their boot (a clone's from its restore) and
//! named after the route's hosts and the VM (`output`): `vm-0` a shared VM,
//! `template` a per-connection route's template, `vm-<n>` its clones in the order
//! they were restored.

use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use crate::BoxError;
use crate::capacity::{Capacity, Need, Pool, Slot};
use crate::output::{self, Hosts, Output, Sink};
use crate::vm::{Snapshot, Spec, Vm};
use crate::wire;

/// How long a cold-booted guest may take to listen on its port.
const BOOT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a restored guest may take to accept a stream again (its vsock transport
/// is reset on restore).
const RESTORE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a template's output may take to reach the wicket before its snapshot.
const SYNC_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a connection's own VM's output may take to reach the wicket once the
/// connection ended, before the VM is killed.
const END_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

/// Most clones of a route restored at once to refill its warm pool.
const REFILL_WORKERS: usize = 8;

/// How long a route's start may wait for room for its VM (a shared one, or the
/// template): the room another route's start holds frees within it.
const START_WAIT: Duration = Duration::from_secs(60);

/// Default of `wait-ms`: how long a connection may wait for room for its VM.
const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// Names the routes for the budget, over every route the wicket ever started.
static NEXT_ROUTE: AtomicU64 = AtomicU64::new(0);

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

/// One route as configured: a `--vm` flag, or a route of the `--config` file.
#[derive(Clone, Debug, PartialEq)]
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
    /// Per-connection mode: the route's VMs alive at once (warm, in use, the
    /// template) at most; None = the global limits alone.
    pub vms: Option<usize>,
    /// Per-connection mode: how long a connection may wait for room for its VM.
    pub wait: Duration,
}

impl RouteSpec {
    /// Parses `host=<rootfs>[?mode=shared|per-connection&vcpus=N&mem=MiB&port=N&warm=N&prime=N&vms=N&wait-ms=N]`.
    pub fn parse(
        flag: &str,
        firecracker: &std::path::Path,
        kernel: &std::path::Path,
    ) -> Result<RouteSpec, String> {
        let (host, locator) = flag
            .split_once('=')
            .ok_or_else(|| format!("--vm {flag}: expected host=<rootfs>"))?;
        let (rootfs, query) = locator.split_once('?').unwrap_or((locator, ""));
        let options = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| pair.split_once('=').unwrap_or((pair, "")));
        let rootfs = std::path::absolute(rootfs).map_err(|e| format!("--vm {flag}: {e}"))?;
        RouteSpec::new(
            &format!("--vm {flag}"),
            host,
            rootfs,
            options,
            firecracker,
            kernel,
        )
    }

    /// The route of `host` on `rootfs` (absolute) with `options`, the keys of a
    /// `--vm` flag's query; `named` names it in an error.
    pub fn new<'a>(
        named: &str,
        host: &str,
        rootfs: PathBuf,
        options: impl IntoIterator<Item = (&'a str, &'a str)>,
        firecracker: &std::path::Path,
        kernel: &std::path::Path,
    ) -> Result<RouteSpec, String> {
        let mut spec = RouteSpec {
            host: host.to_string(),
            vm: Spec {
                firecracker: firecracker.to_path_buf(),
                kernel: kernel.to_path_buf(),
                rootfs,
                vcpus: 1,
                mem_mib: 128,
            },
            mode: Mode::Shared,
            port: 8080,
            warm: 0,
            prime: 0,
            vms: None,
            wait: DEFAULT_WAIT,
        };
        for (key, value) in options {
            let number = || -> Result<u32, String> {
                value
                    .parse()
                    .map_err(|_| format!("{named}: {key}={value} is not a number"))
            };
            match key {
                "mode" => {
                    spec.mode = match value {
                        "shared" => Mode::Shared,
                        "per-connection" => Mode::PerConnection,
                        _ => return Err(format!("{named}: unknown mode {value}")),
                    }
                }
                "vcpus" => spec.vm.vcpus = number()?,
                "mem" => spec.vm.mem_mib = number()?,
                "port" => spec.port = number()?,
                "warm" => spec.warm = number()? as usize,
                "prime" => spec.prime = number()?,
                "vms" => spec.vms = Some(number()? as usize),
                "wait-ms" => spec.wait = Duration::from_millis(u64::from(number()?)),
                _ => return Err(format!("{named}: unknown key {key}")),
            }
        }
        let per_connection = spec.mode == Mode::PerConnection;
        if !per_connection && (spec.warm > 0 || spec.prime > 0 || spec.vms.is_some()) {
            return Err(format!(
                "{named}: warm, prime and vms need mode=per-connection"
            ));
        }
        if let Some(vms) = spec.vms
            && (vms == 0 || vms < spec.warm)
        {
            return Err(format!("{named}: vms must be at least 1 and at least warm"));
        }
        // opened: a file the wicket's user cannot read fails here, not in the VM
        let rootfs = std::fs::File::open(&spec.vm.rootfs).and_then(|file| file.metadata());
        match rootfs {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => {
                return Err(format!(
                    "{named}: {} is not a file",
                    spec.vm.rootfs.display()
                ));
            }
            Err(e) => return Err(format!("{named}: {}: {e}", spec.vm.rootfs.display())),
        }
        Ok(spec)
    }

    /// Whether `other` asks for the same VMs, its host aside.
    pub fn same_vms(&self, other: &RouteSpec) -> bool {
        let unhosted = |spec: &RouteSpec| RouteSpec {
            host: String::new(),
            ..spec.clone()
        };
        unhosted(self) == unhosted(other)
    }

    /// The target advertised for the route, which the server passes verbatim as the
    /// dial address of its CONNECTs: `firecracker:<rootfs>`, plus the options set to
    /// other than their defaults (`?mode=per-connection&vcpus=2...`, in a fixed order).
    /// Routes of one target share its VMs.
    pub fn target(&self) -> String {
        let mut options = Vec::new();
        if self.mode == Mode::PerConnection {
            options.push("mode=per-connection".to_string());
        }
        for (key, value, default) in [
            ("vcpus", self.vm.vcpus, 1),
            ("mem", self.vm.mem_mib, 128),
            ("port", self.port, 8080),
            ("warm", self.warm as u32, 0),
            ("prime", self.prime, 0),
            ("vms", self.vms.unwrap_or(0) as u32, 0),
            (
                "wait-ms",
                self.wait.as_millis() as u32,
                DEFAULT_WAIT.as_millis() as u32,
            ),
        ] {
            if value != default {
                options.push(format!("{key}={value}"));
            }
        }
        let rootfs = self.vm.rootfs.display();
        match options.join("&") {
            query if query.is_empty() => format!("firecracker:{rootfs}"),
            query => format!("firecracker:{rootfs}?{query}"),
        }
    }
}

/// A stream to a guest, and the VM it holds for the connection's lifetime.
pub struct Lease {
    pub stream: UnixStream,
    /// Per-connection mode: the connection's own VM.
    pub vm: Option<Cloned>,
}

/// A clone, killed when dropped ([`Cloned::end`] once its output came), and its output
/// followed.
pub struct Cloned {
    vm: Vm,
    output: Option<Output>,
}

impl Cloned {
    /// Kills the VM once the output its processes wrote so far reached the wicket,
    /// within [`END_SYNC_TIMEOUT`]: a process's last lines come after its answer.
    pub async fn end(mut self) {
        if let Some(output) = &mut self.output {
            output.sync(END_SYNC_TIMEOUT).await;
        }
    }
}

pub struct Route {
    pub spec: RouteSpec,
    /// The hosts the route serves, which the log and its VMs' output name.
    hosts: Arc<Hosts>,
    /// The route's files (the template's snapshot, the clones'), removed with it.
    dir: PathBuf,
    capacity: Arc<Capacity>,
    /// What each of the route's VMs takes of the budget.
    need: Need,
    backend: Backend,
}

enum Backend {
    Shared(Box<Vm>),
    PerConnection {
        cloner: Arc<Cloner>,
        /// Restored ahead of the connections, refilled as they are taken.
        warm: Arc<Warm>,
    },
}

/// A route's warm clones, which give their room up to another route's VM.
struct Warm {
    clones: Mutex<mpsc::Receiver<Cloned>>,
}

impl Warm {
    fn take(&self) -> Option<Cloned> {
        self.clones.lock().unwrap().try_recv().ok()
    }
}

impl Pool for Warm {
    fn evict(&self) -> bool {
        // dropped: killed, its slot given back
        self.take().is_some()
    }
}

/// Restores a per-connection route's clones from its snapshot, and follows their
/// output.
struct Cloner {
    snapshot: Snapshot,
    /// Where the clones' directories go (`vm-<n>`).
    dir: PathBuf,
    /// Numbers the clones.
    next: AtomicU64,
    sink: Arc<Sink>,
    hosts: Arc<Hosts>,
    /// Whether the guest sends its output: its template's did.
    output: bool,
}

impl Cloner {
    /// A VM restored from the snapshot, told it was (`wire::CONTROL_PORT`): its random
    /// state reseeded from the host's, its restore hook run; its output followed from
    /// then on.
    async fn restore(&self, slot: Slot) -> Result<Cloned, BoxError> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let vm = Vm::restore(&self.snapshot, self.dir.join(format!("vm-{n}")), slot).await?;
        let mut seed = [0u8; 32];
        // SAFETY: getrandom into a buffer of its length
        if unsafe { libc::getrandom(seed.as_mut_ptr().cast(), seed.len(), 0) }
            != seed.len() as isize
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut stream = vm.dial_ready(wire::CONTROL_PORT, RESTORE_TIMEOUT).await?;
        // borrowed halves: an owned write half shuts the socket down when dropped,
        // which Firecracker takes as the end of the whole stream (the answer lost)
        let (reader, mut writer) = stream.split();
        let answered = async {
            writer.write_all(wire::restored(&seed).as_bytes()).await?;
            let mut answer = String::new();
            tokio::io::BufReader::new(reader)
                .read_line(&mut answer)
                .await?;
            Ok::<_, std::io::Error>(answer)
        };
        let answer = tokio::time::timeout(RESTORE_TIMEOUT, answered)
            .await
            .map_err(|_| format!("restore notice: no answer within {RESTORE_TIMEOUT:?}"))??;
        if answer.trim_end() != "OK" {
            return Err(format!("restore notice: {}", answer.trim_end()).into());
        }
        // the guest's listener outlives the restore; its lines wait for the stream
        let output = match self.output {
            true => match vm.dial(wire::LOG_PORT).await {
                Ok(stream) => Some(output::follow(
                    stream,
                    self.sink.clone(),
                    self.hosts.clone(),
                    format!("vm-{n}"),
                )),
                Err(e) => {
                    eprintln!(
                        "route {} vm-{n}: its output is not followed: {e}",
                        self.hosts.current().label
                    );
                    None
                }
            },
            false => None,
        };
        Ok(Cloned { vm, output })
    }
}

impl Route {
    /// Boots the route's VM (shared), or boots a template VM, snapshots it once its
    /// guest listens and kills it (per-connection); `dir` holds the route's files,
    /// `hosts` name it in the log. Its VMs take their room from `capacity`; their
    /// output goes to `sink`.
    pub async fn start(
        spec: RouteSpec,
        hosts: &[String],
        dir: PathBuf,
        capacity: Arc<Capacity>,
        sink: Arc<Sink>,
    ) -> Result<Route, BoxError> {
        let hosts = Hosts::new(hosts);
        let label = hosts.current().label.clone();
        let need = Need {
            memory: u64::from(spec.vm.mem_mib),
            route: NEXT_ROUTE.fetch_add(1, Ordering::Relaxed),
            cap: spec.vms,
        };
        let slot = capacity
            .take(need, START_WAIT)
            .await
            .map_err(|e| format!("route {label}: {e}"))?;
        // the snapshot is the template's whole memory: the wicket's user's alone
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .map_err(|e| format!("route {label}: {}: {e}", dir.display()))?;
        let started = Instant::now();
        let vm = Vm::boot(&spec.vm, dir.join("boot"), slot).await?;
        let name = match spec.mode {
            Mode::Shared => "vm-0",
            Mode::PerConnection => "template",
        };
        let mut output = ready(&vm, spec.port, &sink, &hosts, name).await?;
        let booted = started.elapsed();
        let backend = match spec.mode {
            Mode::Shared => {
                println!(
                    "route {label}: VM booted, guest listening after {}ms",
                    booted.as_millis()
                );
                Backend::Shared(Box::new(vm))
            }
            Mode::PerConnection => {
                // its clones are told they were restored: a guest predating the
                // notice would fail every one of them
                if let Err(e) = vm.dial(wire::CONTROL_PORT).await {
                    return Err(format!(
                        "route {label}: the guest takes no restore notice (vsock port {}): \
                         its /init predates it, build the rootfs again (scripts/build-guest.sh, \
                         scripts/cnb-rootfs.sh); {e}",
                        wire::CONTROL_PORT
                    )
                    .into());
                }
                for _ in 0..spec.prime {
                    prime(&vm, spec.port).await?;
                }
                // the template's lines are the template's: before the snapshot, they
                // are the clones' to discard
                if let Some(output) = &mut output
                    && !output.sync(SYNC_TIMEOUT).await
                {
                    eprintln!(
                        "route {label}: the template's output did not come within \
                         {SYNC_TIMEOUT:?}; snapshotting without the rest"
                    );
                }
                let mut snapshot = vm.snapshot(&dir.join("snapshot")).await?;
                snapshot.firecracker = spec.vm.firecracker.clone();
                drop(vm);
                let size = tokio::fs::metadata(&snapshot.memory).await?.len();
                println!(
                    "route {label}: template booted in {}ms, primed with {} request(s), snapshot taken after {}ms ({} MiB memory file)",
                    booted.as_millis(),
                    spec.prime,
                    started.elapsed().as_millis(),
                    size >> 20
                );
                let cloner = Arc::new(Cloner {
                    snapshot,
                    dir: dir.join("clones"),
                    next: AtomicU64::new(0),
                    sink,
                    hosts: hosts.clone(),
                    output: output.is_some(),
                });
                let (refill, warm) = mpsc::channel(spec.warm.max(1));
                let warm = Arc::new(Warm {
                    clones: Mutex::new(warm),
                });
                let pool: Arc<dyn Pool> = warm.clone();
                capacity.register(need.route, Arc::downgrade(&pool));
                // restores run in parallel: one at a time caps the clone rate at about
                // 1 / restore time
                for _ in 0..spec.warm.min(REFILL_WORKERS) {
                    tokio::spawn(keep_warm(
                        cloner.clone(),
                        spec.port,
                        refill.clone(),
                        capacity.clone(),
                        need,
                    ));
                }
                Backend::PerConnection { cloner, warm }
            }
        };
        Ok(Route {
            spec,
            hosts,
            dir,
            capacity,
            need,
            backend,
        })
    }

    /// The hosts the route serves, for the log: `a.local, b.local`.
    pub fn label(&self) -> String {
        self.hosts.current().label.clone()
    }

    /// The hosts the route serves from now on: a reload kept its VMs.
    pub fn rename(&self, hosts: &[String]) {
        self.hosts.set(hosts);
    }

    /// A stream to a guest for one connection.
    pub async fn open(&self) -> Result<Lease, BoxError> {
        match &self.backend {
            Backend::Shared(vm) => Ok(Lease {
                stream: vm.dial(self.spec.port).await?,
                vm: None,
            }),
            Backend::PerConnection { cloner, warm } => {
                let started = Instant::now();
                let warm = warm.take();
                let (clone, warm) = match warm {
                    Some(clone) => (clone, true),
                    None => {
                        let slot = self.capacity.take(self.need, self.spec.wait).await?;
                        (cloner.restore(slot).await?, false)
                    }
                };
                let restored = started.elapsed();
                let stream = clone.vm.dial_ready(self.spec.port, RESTORE_TIMEOUT).await?;
                if std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_TRACE").is_some() {
                    eprintln!(
                        "route {}: {} clone, restored in {}us, guest accepting after {}us",
                        self.label(),
                        if warm { "warm" } else { "fresh" },
                        restored.as_micros(),
                        started.elapsed().as_micros()
                    );
                }
                Ok(Lease {
                    stream,
                    vm: Some(clone),
                })
            }
        }
    }
}

impl Drop for Route {
    /// The last connection on the route ended, and the route left the table
    /// (or never made it): its files go; its VMs, killed, after them.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Waits for a guest just booted to listen on `port`, its output (the VM `name` of the
/// route of `hosts`) followed meanwhile: `/init` opens the log stream before it starts
/// the process, so the output of a process failing to start is printed too. None when
/// the guest has no log stream (its `/init` predates it).
async fn ready(
    vm: &Vm,
    port: u32,
    sink: &Arc<Sink>,
    hosts: &Arc<Hosts>,
    name: &str,
) -> Result<Option<Output>, BoxError> {
    let follow = |stream| output::follow(stream, sink.clone(), hosts.clone(), name.to_string());
    let log = async {
        loop {
            match vm.dial(wire::LOG_PORT).await {
                Ok(stream) => return stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    };
    let listening = vm.dial_ready(port, BOOT_TIMEOUT);
    tokio::pin!(listening);
    let output = tokio::select! {
        stream = log => follow(stream),
        listening = &mut listening => {
            drop(listening?);
            // listening, the log stream is open if the guest has one
            return Ok(match vm.dial(wire::LOG_PORT).await {
                Ok(stream) => Some(follow(stream)),
                Err(_) => {
                    eprintln!(
                        "route {}: the guest sends no output (vsock port {}): its /init \
                         predates it, build the rootfs again (scripts/build-guest.sh, \
                         scripts/cnb-rootfs.sh)",
                        hosts.current().label,
                        wire::LOG_PORT
                    );
                    None
                }
            });
        }
    };
    drop(listening.await?);
    Ok(Some(output))
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

/// Keeps `refill`'s buffer full of restored clones, in room no VM waits for,
/// until the route is gone.
async fn keep_warm(
    cloner: Arc<Cloner>,
    port: u32,
    refill: mpsc::Sender<Cloned>,
    capacity: Arc<Capacity>,
    need: Need,
) {
    loop {
        let Ok(permit) = refill.reserve().await else {
            return;
        };
        let slot = tokio::select! {
            slot = capacity.take_spare(need) => slot,
            () = refill.closed() => return,
        };
        match cloner.restore(slot).await {
            Ok(clone) => {
                // ready before it is handed out: a warm clone answers at once
                match clone.vm.dial_ready(port, RESTORE_TIMEOUT).await {
                    Ok(probe) => drop(probe),
                    Err(e) => {
                        eprintln!("warm clone: {e}");
                        continue;
                    }
                }
                permit.send(clone);
            }
            Err(e) => {
                eprintln!("warm clone: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
