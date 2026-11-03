//! A route: the VMs serving one target (its hosts), and how a connection gets one of
//! them. The VMs' output is followed from their boot (a clone's from its restore) and
//! named after the route's hosts and the VM (`output`): `vm-0` a shared VM,
//! `template` a cloned route's template, `vm-<n>` its clones in the order they were
//! restored. The live access log's lines name the instance too: a clone holds the lowest
//! index free among its route's VMs alive from its guest accepting on; the shared VM and the
//! template, alone as they live, are 0.
//!
//! A shared VM and a pool's are watched ([`watch`]). A pool replaces a VM going down itself; a
//! shared VM's going down is told ([`Down`]): the routes start another one.
//!
//! What says how many VMs and how long ([`Bounds`]) a running route takes as it is, its VMs and
//! their snapshot kept ([`Route::adjust`]): VMs are cloned for more, and those past a lower `vms`
//! (the highest indices, so what stays is what `cf logs` lists) take no connection any more, and
//! go as `keep-ms` has them go, once their connections ended. What serves none of a VM per
//! connection goes at once: it has no connections to finish, and is in the way of the next.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use sluice_wicket_console::env::Env;
use sluice_wicket_console::status::Shortage;
use sluice_wicket_tunnel::flume::{Index, Indices};
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinSet;
use tokio_util::task::TaskTracker;

use crate::BoxError;
use crate::capacity::{Capacity, Full, Need, Pool, Slot};
use crate::disk::{self, Disk};
use crate::egress::Egress;
use crate::keeper::Role;
use crate::output::{self, Ended, Hosts, Output, Sink};
use crate::vm::{self, Place, Places, Snapshot, Spec, Vm};
use crate::wire::{self, Rule};

/// Default of `boot-ms`: how long a cold-booted guest may take to listen on its port.
const DEFAULT_BOOT: Duration = Duration::from_secs(10);

/// How long a restored guest may take to accept a stream again (its vsock transport
/// is reset on restore).
const RESTORE_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest answer to a restore notice the wicket reads (`OK`, or the guest's
/// error).
const MAX_NOTICE_ANSWER: usize = 1024;

/// How long a template may take to answer a priming request, its answer read to the end.
const PRIME_TIMEOUT: Duration = Duration::from_secs(10);

/// The bytes of a priming answer the wicket keeps: its status line, to check.
const STATUS_LINE: usize = 80;

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
// a VM stopping holds its room until it is gone
const _: () = assert!(START_WAIT.as_secs() > disk::STOP_MOST.as_secs());

/// Default of `wait-ms`: how long a connection may wait for room for its VM.
const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// Default of `keep-ms`: how long a VM serving no connection is kept (reuse, pool).
const DEFAULT_KEEP: Duration = Duration::from_secs(60);

/// Default of `conns`: the connections every VM of a pool holds before it grows.
const DEFAULT_CONNS: usize = 16;

/// How long a pool waits before it replaces a VM it failed to start.
const POOL_RETRY: Duration = Duration::from_secs(1);

/// How long after a look that found it refusing a shared VM's process is looked at again.
const LOOK_AGAIN: Duration = Duration::from_millis(100);

/// The wait before the next VM in the place of one that went down, past the first: doubled up to
/// [`RESTART_WAIT_MAX`].
const RESTART_WAIT: Duration = Duration::from_secs(1);

const RESTART_WAIT_MAX: Duration = Duration::from_secs(60);

/// A VM up that long going down has the next one started at once, as the first one was.
const RESTART_STABLE: Duration = Duration::from_secs(60);

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
    /// Every connection gets a clone of its own, as [`Mode::PerConnection`], but a
    /// clone whose connection ended serves the next one, until it was kept unused for
    /// `keep-ms`, served `uses` connections or lived `lifetime-ms`: a warm
    /// execution environment, as Lambda keeps one. A connection sees what the previous
    /// ones left in the guest.
    Reuse,
    /// A pool of VMs cloned from the route's snapshot, each serving many connections
    /// at once, as a shared VM: `warm` of them kept, grown up to `vms` with the
    /// connections (`conns` each), a VM beyond `warm` serving none killed after
    /// `keep-ms`.
    Pool,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Shared => "shared",
            Mode::PerConnection => "per-connection",
            Mode::Reuse => "reuse",
            Mode::Pool => "pool",
        }
    }

    /// Whether the route's VMs are cloned from a snapshot of a template.
    pub fn cloned(self) -> bool {
        self != Mode::Shared
    }
}

/// The modes cloning their VMs.
const CLONED: &[Mode] = &[Mode::PerConnection, Mode::Reuse, Mode::Pool];

/// The options of some modes alone, and those modes.
const MODE_OPTIONS: &[(&str, &[Mode])] = &[
    ("warm", CLONED),
    ("prime", CLONED),
    ("vms", CLONED),
    ("wait-ms", CLONED),
    ("keep-ms", &[Mode::Reuse, Mode::Pool]),
    ("uses", &[Mode::Reuse]),
    ("lifetime-ms", &[Mode::Reuse]),
    ("conns", &[Mode::Pool]),
];

/// One route as configured: a `--vm` flag, or a route of the `--config` file.
#[derive(Clone, Debug, PartialEq)]
pub struct RouteSpec {
    pub host: String,
    pub vm: Spec,
    pub mode: Mode,
    /// The guest's vsock port.
    pub port: u32,
    /// Cloned modes: `GET /` requests sent to the template before its snapshot, so the
    /// clones start from a warmed-up process (JIT, lazily loaded code).
    pub prime: u32,
    /// The console's app it is a route of: its target names it, so its VMs are its own.
    pub app: Option<String>,
    pub bounds: Bounds,
}

/// What says how many VMs a route keeps and how long, not what they are: a running route takes a
/// change of it ([`Route::adjust`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bounds {
    /// How long a cold-booted guest may take to listen on its port.
    pub boot: Duration,
    /// Per-connection and reuse modes: clones kept restored ahead of the connections;
    /// pool mode: the pool's VMs kept serving no connection (default `vms`).
    pub warm: usize,
    /// Cloned modes: the route's VMs alive at once (warm, in use, kept, the template)
    /// at most; None = the global limits alone (pool mode needs it: its size).
    pub vms: Option<usize>,
    /// Cloned modes: how long a connection may wait for room for its VM.
    pub wait: Duration,
    /// Reuse and pool modes: how long a VM serving no connection is kept (a pool's
    /// beyond its `warm`).
    pub keep: Duration,
    /// Reuse mode: the connections a clone serves at most; None = no bound.
    pub uses: Option<u32>,
    /// Reuse mode: how long a clone serves from its restore at most; None = no bound.
    pub lifetime: Option<Duration>,
    /// Pool mode: the connections every VM of the pool holds before it grows by one.
    pub conns: usize,
    /// The route's connections of one client address open at once at most (in
    /// per-connection and reuse modes, its VMs); None = no bound of its own.
    pub per_client: Option<usize>,
    /// How long a connection may relay no byte, either way, before it is failed (and
    /// its VM killed); None = no bound of its own.
    pub idle: Option<Duration>,
}

impl Bounds {
    /// A pool's `vms`.
    fn pool_size(&self) -> usize {
        self.vms.unwrap_or(self.warm)
    }
}

/// The keys of the options [`Bounds`] holds.
const BOUNDS: &[&str] = &[
    "boot-ms",
    "warm",
    "vms",
    "wait-ms",
    "keep-ms",
    "uses",
    "lifetime-ms",
    "conns",
    "per-client",
    "idle-ms",
];

impl RouteSpec {
    /// Parses `host=<rootfs>[?mode=shared|per-connection|reuse|pool&vcpus=N&mem=MiB&cpu=N&port=N&boot-ms=N&warm=N&prime=N&vms=N&wait-ms=N&keep-ms=N&uses=N&lifetime-ms=N&conns=N&per-client=N&idle-ms=N&egress=<rule>,...&disk=<path>&disk-mib=N]`.
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
    /// `--vm` flag's query; `named` names it in an error. The rootfs is opened:
    /// a file the wicket's user cannot read fails here, not in the VM.
    pub fn new<'a>(
        named: &str,
        host: &str,
        rootfs: PathBuf,
        options: impl IntoIterator<Item = (&'a str, &'a str)>,
        firecracker: &std::path::Path,
        kernel: &std::path::Path,
    ) -> Result<RouteSpec, String> {
        let spec = RouteSpec::configure(named, host, rootfs, options, firecracker, kernel)?;
        let rootfs = std::fs::File::open(&spec.vm.rootfs).and_then(|file| file.metadata());
        match rootfs {
            Ok(metadata) if metadata.is_file() => Ok(spec),
            Ok(_) => Err(format!(
                "{named}: {} is not a file",
                spec.vm.rootfs.display()
            )),
            Err(e) => Err(format!("{named}: {}: {e}", spec.vm.rootfs.display())),
        }
    }

    /// [`RouteSpec::new`] but the rootfs, not opened: the options checked.
    pub fn configure<'a>(
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
                cpu: None,
                egress: Vec::new(),
                env: Env::default(),
                disk: None,
            },
            mode: Mode::Shared,
            port: 8080,
            prime: 0,
            app: None,
            bounds: Bounds {
                boot: DEFAULT_BOOT,
                warm: 0,
                vms: None,
                wait: DEFAULT_WAIT,
                keep: DEFAULT_KEEP,
                uses: None,
                lifetime: None,
                conns: DEFAULT_CONNS,
                per_client: None,
                idle: None,
            },
        };
        let mut given = Vec::new();
        let (mut mount, mut disk_mib) = (None, None);
        for (key, value) in options {
            given.push(key);
            let number = || -> Result<u32, String> {
                value
                    .parse()
                    .map_err(|_| format!("{named}: {key}={value} is not a number"))
            };
            let positive = || match number()? {
                0 => Err(format!("{named}: {key} must be at least 1")),
                n => Ok(n),
            };
            let millis = || Ok::<_, String>(Duration::from_millis(u64::from(positive()?)));
            match key {
                "mode" => {
                    spec.mode = [Mode::Shared, Mode::PerConnection, Mode::Reuse, Mode::Pool]
                        .into_iter()
                        .find(|mode| mode.name() == value)
                        .ok_or_else(|| format!("{named}: unknown mode {value}"))?;
                }
                "vcpus" => spec.vm.vcpus = number()?,
                "mem" => spec.vm.mem_mib = number()?,
                "port" => spec.port = number()?,
                "boot-ms" => spec.bounds.boot = millis()?,
                "warm" => spec.bounds.warm = number()? as usize,
                "prime" => spec.prime = number()?,
                "vms" => spec.bounds.vms = Some(number()? as usize),
                "wait-ms" => spec.bounds.wait = Duration::from_millis(u64::from(number()?)),
                "keep-ms" => spec.bounds.keep = millis()?,
                "uses" => spec.bounds.uses = Some(positive()?),
                "lifetime-ms" => spec.bounds.lifetime = Some(millis()?),
                "conns" => spec.bounds.conns = positive()? as usize,
                "cpu" => spec.vm.cpu = Some(positive()?),
                "per-client" => spec.bounds.per_client = Some(positive()? as usize),
                "idle-ms" => spec.bounds.idle = Some(millis()?),
                "egress" => {
                    for rule in value.split(',').filter(|rule| !rule.is_empty()) {
                        let rule: Rule =
                            rule.parse().map_err(|e| format!("{named}: egress {e}"))?;
                        spec.vm.egress.push(rule);
                    }
                }
                "disk" => {
                    Disk::check_mount(value).map_err(|e| format!("{named}: {e}"))?;
                    mount = Some(value.to_string());
                }
                "disk-mib" => match number()? {
                    mib if mib < disk::MIN_MIB => {
                        return Err(format!(
                            "{named}: disk-mib must be at least {}: a smaller file system has \
                             no journal",
                            disk::MIN_MIB
                        ));
                    }
                    mib => disk_mib = Some(mib),
                },
                _ => return Err(format!("{named}: unknown key {key}")),
            }
        }
        // one order and each rule once: the target names them
        spec.vm.egress.sort();
        spec.vm.egress.dedup();
        let egress = vm::egress_list(&spec.vm.egress).len();
        if egress > vm::MAX_EGRESS {
            return Err(format!(
                "{named}: egress of {egress} bytes: a guest's kernel command line takes {} \
                 (a range takes the place of its addresses)",
                vm::MAX_EGRESS
            ));
        }
        match (mount, disk_mib) {
            (None, Some(_)) => return Err(format!("{named}: disk-mib needs disk")),
            (None, None) => {}
            (Some(_), _) if spec.mode != Mode::Shared => {
                return Err(format!(
                    "{named}: disk needs mode=shared: one VM writes a disk, and a clone's \
                     snapshot holds its guest's page cache of it"
                ));
            }
            (Some(mount), mib) => {
                spec.vm.disk = Some(Disk {
                    file: PathBuf::new(),
                    mount,
                    mib: mib.unwrap_or(disk::DEFAULT_MIB),
                });
            }
        }
        for (key, modes) in MODE_OPTIONS {
            if given.contains(key) && !modes.contains(&spec.mode) {
                let modes: Vec<_> = modes.iter().map(|mode| mode.name()).collect();
                return Err(format!(
                    "{named}: {key} needs mode={}",
                    modes.join(", mode=")
                ));
            }
        }
        if spec.mode == Mode::Pool {
            let Some(vms) = spec.bounds.vms else {
                return Err(format!("{named}: mode=pool needs vms (its VMs at most)"));
            };
            if !given.contains(&"warm") {
                spec.bounds.warm = vms;
            }
        }
        if let Some(vms) = spec.bounds.vms
            && (vms == 0 || vms < spec.bounds.warm)
        {
            return Err(format!("{named}: vms must be at least 1 and at least warm"));
        }
        Ok(spec)
    }

    /// Whether `other` asks for the same VMs, its host and its [`Bounds`] aside.
    pub fn same_vms(&self, other: &RouteSpec) -> bool {
        self.vm == other.vm
            && self.mode == other.mode
            && self.port == other.port
            && self.prime == other.prime
            && self.app == other.app
    }

    /// The VMs the route keeps for good once started: a shared VM, a pool's `warm`.
    pub fn resident(&self) -> usize {
        match self.mode {
            Mode::Shared => 1,
            Mode::Pool => self.bounds.warm,
            Mode::PerConnection | Mode::Reuse => 0,
        }
    }

    /// The target advertised for the route, which the server passes verbatim as the
    /// dial address of its CONNECTs: `firecracker:<rootfs>`, plus the options set to
    /// other than their defaults (`?mode=per-connection&vcpus=2...`, in a fixed order;
    /// the bounds of a connection too: a target's connections are bound alike), an
    /// app's name first (unreserved chars: no escaping). Routes of one target share its
    /// VMs. An app's name tells its VMs from any route's, so its target holds no [`Bounds`]: a
    /// change of them is nothing to advertise, and a keep-alive connection stays on its VM. Its
    /// env is not in it: the server sees targets, and a change of the env starts the target's VMs
    /// anew all the same ([`RouteSpec::same_vms`]).
    pub fn target(&self) -> String {
        let query: Vec<String> = self
            .app
            .iter()
            .map(|app| format!("app={app}"))
            .chain(
                self.options()
                    .into_iter()
                    .filter(|(key, _)| self.app.is_none() || !BOUNDS.contains(key))
                    .map(|(key, value)| format!("{key}={value}")),
            )
            .collect();
        let rootfs = self.vm.rootfs.display();
        match query.join("&") {
            query if query.is_empty() => format!("firecracker:{rootfs}"),
            query => format!("firecracker:{rootfs}?{query}"),
        }
    }

    /// The options set to other than their defaults, as a `--vm` flag's query
    /// has them (the order of [`RouteSpec::target`]).
    pub fn options(&self) -> Vec<(&'static str, String)> {
        let mut options = Vec::new();
        if self.mode != Mode::Shared {
            options.push(("mode", self.mode.name().to_string()));
        }
        let bounds = &self.bounds;
        let warm = match self.mode {
            Mode::Pool => bounds.vms.unwrap_or(0),
            _ => 0,
        };
        let millis = |duration: Option<Duration>| duration.map_or(0, |d| d.as_millis() as u32);
        for (key, value, default) in [
            ("vcpus", self.vm.vcpus, 1),
            ("mem", self.vm.mem_mib, 128),
            ("cpu", self.vm.cpu.unwrap_or(0), 0),
            ("port", self.port, 8080),
            (
                "boot-ms",
                bounds.boot.as_millis() as u32,
                DEFAULT_BOOT.as_millis() as u32,
            ),
            ("warm", bounds.warm as u32, warm as u32),
            ("prime", self.prime, 0),
            ("vms", bounds.vms.unwrap_or(0) as u32, 0),
            (
                "wait-ms",
                bounds.wait.as_millis() as u32,
                DEFAULT_WAIT.as_millis() as u32,
            ),
            (
                "keep-ms",
                bounds.keep.as_millis() as u32,
                DEFAULT_KEEP.as_millis() as u32,
            ),
            ("uses", bounds.uses.unwrap_or(0), 0),
            ("lifetime-ms", millis(bounds.lifetime), 0),
            ("conns", bounds.conns as u32, DEFAULT_CONNS as u32),
            ("per-client", bounds.per_client.unwrap_or(0) as u32, 0),
            ("idle-ms", millis(bounds.idle), 0),
        ] {
            if value != default {
                options.push((key, value.to_string()));
            }
        }
        if !self.vm.egress.is_empty() {
            options.push(("egress", vm::egress_list(&self.vm.egress)));
        }
        if let Some(disk) = &self.vm.disk {
            options.push(("disk", disk.mount.clone()));
            if disk.mib != disk::DEFAULT_MIB {
                options.push(("disk-mib", disk.mib.to_string()));
            }
        }
        options
    }
}

/// A stream to a guest, and what the connection holds for its lifetime.
pub struct Lease {
    pub stream: UnixStream,
    pub hold: Hold,
}

/// What a connection holds while it is open: its VM, in the modes that give it one (its
/// seat on a pool VM in pool mode), and its place in its client's share of the route.
pub struct Hold {
    vm: Held,
    /// With `per-client`: given back once the VM was.
    _share: Option<Share>,
    /// Counts the route's connections open while held.
    _open: Open,
}

/// One of a route's connections open, counted while it lives.
struct Open(Arc<AtomicU64>);

impl Open {
    fn new(open: &Arc<AtomicU64>) -> Open {
        open.fetch_add(1, Ordering::Relaxed);
        Open(open.clone())
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What a route's VMs are, at one point (the console's listing).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Vms {
    /// Its VMs alive, as the budget counts them: in use, warm, kept, pooled,
    /// being restored or started, the template while it is.
    pub alive: usize,
    /// Shared mode: its VM is up.
    pub shared: bool,
    /// Per-connection and reuse modes: clones restored ahead of the connections.
    pub warm: usize,
    /// Reuse mode: clones kept for the next connection.
    pub kept: usize,
    /// Pool mode: the pool's VMs, and those being started.
    pub pooled: usize,
    pub starting: usize,
    /// Its connections open.
    pub connections: u64,
    /// Shared and pool modes: VMs started in the place of one that went down.
    pub restarts: u64,
}

/// A cloned route's snapshot of its template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Taken {
    /// Its memory file, in bytes.
    pub memory: u64,
    /// The template booted (its guest listening) after, and the snapshot taken
    /// after its boot began.
    pub booted: Duration,
    pub taken: Duration,
    /// When it was taken.
    pub at: SystemTime,
}

enum Held {
    Shared,
    /// Per-connection mode: the connection's own clone, killed when it ends.
    Own(Cloned),
    /// Reuse mode: the connection's clone, kept for the next connection.
    Reused(Reused, Arc<Kept>),
    /// Pool mode: the connection's seat on a VM of the pool.
    Pooled(Seat),
}

impl Hold {
    /// The connection's relay ended, `clean` when as it should (not with the guest's
    /// stream cut, or idle past `idle-ms`). A reused clone is kept for the next
    /// connection at once, when the connection ended clean and the clone has uses and
    /// life left: before the connection's end is told, the next connection of its
    /// client finds it. A pool's VM is looked at as it did not. What is left to do: a clone
    /// the connection ended with killed once its output came, then the client's share given
    /// back.
    pub fn end(self, clean: bool) -> impl Future<Output = ()> {
        let ended = match self.vm {
            Held::Own(clone) => Some(clone),
            Held::Reused(reused, kept) if clean => kept.put(reused),
            Held::Reused(reused, _) => Some(reused.clone),
            Held::Pooled(seat) => {
                if !clean {
                    seat.suspect();
                }
                None
            }
            Held::Shared => None,
        };
        let (share, open) = (self._share, self._open);
        async move {
            let _open = open;
            if let Some(clone) = ended {
                clone.end().await;
            }
            drop(share);
        }
    }
}

/// A route's connections open by client address, which `per-client` bounds.
#[derive(Default)]
struct Clients(Mutex<HashMap<String, usize>>);

impl Clients {
    /// A place for a connection of `client`, unless it holds `max` already.
    fn claim(self: &Arc<Self>, client: &str, max: usize) -> Result<Share, String> {
        let mut open = self.0.lock().unwrap();
        let held = open.entry(client.to_string()).or_insert(0);
        if *held >= max {
            return Err(format!(
                "client {client} holds {max} connection(s) of the route already (per-client)"
            ));
        }
        *held += 1;
        Ok(Share {
            clients: self.clone(),
            client: client.to_string(),
        })
    }
}

/// A connection's place in its client's share of a route, given back when dropped.
struct Share {
    clients: Arc<Clients>,
    client: String,
}

impl Drop for Share {
    fn drop(&mut self) {
        let mut open = self.clients.0.lock().unwrap();
        if let Some(held) = open.get_mut(&self.client) {
            *held -= 1;
            if *held == 0 {
                open.remove(&self.client);
            }
        }
    }
}

/// A clone, killed when dropped ([`Cloned::end`] once its output came), and its output
/// followed.
pub struct Cloned {
    /// Its instance index: given back before the VM's room, so an index stays below the
    /// route's `vms`.
    instance: Index,
    vm: Vm,
    output: Option<Output>,
    /// When it was restored.
    restored: Instant,
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

/// A route's shared VM serves no more.
#[derive(Debug, PartialEq, Eq)]
pub struct Down {
    /// [`Route::id`].
    pub route: u64,
    pub why: &'static str,
}

/// Where a route starts, and what it is given to.
pub struct Starting {
    pub places: Arc<Places>,
    /// The route is the `n`th started of `places`.
    pub n: u64,
    /// Where its VMs take their room.
    pub capacity: Arc<Capacity>,
    /// The room its shared VM comes back on ([`Capacity::hold`]), instead of taking it.
    pub held: Option<Arc<Slot>>,
    /// Where its VMs' output goes.
    pub sink: Arc<Sink>,
    pub workers: Arc<Workers>,
    /// Where the stop of its VM on a disk goes, dropped ([`Vm`]).
    pub stops: TaskTracker,
}

pub struct Route {
    /// What its VMs are, and their port.
    vm: Spec,
    mode: Mode,
    port: u32,
    prime: u32,
    app: Option<String>,
    /// As they are now.
    bounds: Current,
    /// The hosts the route serves, which the log and its VMs' output name.
    hosts: Arc<Hosts>,
    /// Where its VMs are and its files (the template's snapshot, the clones'), removed
    /// with it (and its clones' restorer).
    _place: Arc<Place>,
    capacity: Arc<Capacity>,
    /// What each of the route's VMs takes of the budget.
    need: Need,
    backend: Backend,
    /// Its connections open by client address.
    clients: Arc<Clients>,
    /// Its connections open.
    open: Arc<AtomicU64>,
    /// Cloned modes: its template's snapshot.
    taken: Option<Taken>,
    /// Shared mode: the VMs started in the place of one that went down, this one included (a
    /// pool counts its own).
    restarts: AtomicU64,
    /// Shared mode: why its VM is down.
    down: Mutex<Option<Shortage>>,
    /// Held by what stops its shared VM ([`Route::stopping`]), for [`Route::stop`] to wait for.
    stops: Arc<tokio::sync::Mutex<()>>,
}

/// A shared route's VM; none once stopped ([`Route::stop`]).
type SharedVm = Mutex<Option<Arc<Vm>>>;

enum Backend {
    Shared {
        vm: Arc<SharedVm>,
        /// Has its watch look at the VM: a connection failed.
        suspects: mpsc::Sender<()>,
        /// What its watch follows, until it starts ([`Route::watch`]).
        unwatched: Mutex<Option<(Option<Output>, mpsc::Receiver<()>)>>,
        /// When its VM came up: a route has one VM, another one is another route.
        started: SystemTime,
    },
    /// Per-connection and reuse modes: a clone per connection at a time.
    Cloned {
        cloner: Arc<Cloner>,
        /// Restored ahead of the connections, refilled as they are taken.
        warm: Arc<Warm>,
        /// Reuse mode: the clones whose connection ended, kept for the next ones.
        kept: Option<Arc<Kept>>,
    },
    Pool(Arc<SharedPool>),
}

/// A route's [`Bounds`] as they are now, which a change of them finds its VMs by.
#[derive(Clone)]
struct Current(Arc<Mutex<Bounds>>);

impl Current {
    fn get(&self) -> Bounds {
        *self.0.lock().unwrap()
    }
}

/// A route's warm clones, kept to `warm` ([`keep_warm`]), which give their room up to another
/// route's VM.
struct Warm {
    clones: Mutex<VecDeque<Cloned>>,
    /// Why the last restore failed for anything but room, until one succeeds: another one is
    /// tried until then, whatever `warm` asks ([`Warm::wanted`]).
    failed: Mutex<Option<Shortage>>,
    bounds: Current,
    capacity: Arc<Capacity>,
    route: u64,
    /// Wakes what keeps them: a clone taken, their number changed, the route gone.
    changed: Arc<Notify>,
}

impl Warm {
    fn take(&self) -> Option<Cloned> {
        let clone = self.clones.lock().unwrap().pop_front();
        if clone.is_some() {
            self.changed.notify_one();
        }
        clone
    }

    /// The clones missing to `warm`.
    fn missing(&self) -> usize {
        let held = self.clones.lock().unwrap().len();
        self.bounds.get().warm.saturating_sub(held)
    }

    /// The clones to restore: those missing to `warm`, at least one while restores fail: at
    /// `warm` 0 nothing else tells they stopped failing but the next connection.
    fn wanted(&self) -> usize {
        let failing = self.failed.lock().unwrap().is_some();
        self.missing().max(usize::from(failing))
    }

    /// A clone's restore failed with `e`, as a connection's or its own.
    fn failing(&self, e: &BoxError) {
        Shortage::keep(
            &mut self.failed.lock().unwrap(),
            format!("its restore failed: {e}"),
        );
        self.changed.notify_one();
    }

    /// One more clone, unless `warm` or `vms` went down meanwhile: then it is killed.
    fn put(&self, clone: Cloned) {
        *self.failed.lock().unwrap() = None;
        let mut clones = self.clones.lock().unwrap();
        if clones.len() < self.bounds.get().warm && !self.capacity.over(self.route) {
            clones.push_back(clone);
        }
    }

    /// Kills what is past `warm`.
    fn adjust(&self) {
        let warm = self.bounds.get().warm;
        let past = {
            let mut clones = self.clones.lock().unwrap();
            let kept = warm.min(clones.len());
            clones.split_off(kept)
        };
        drop(past);
        self.changed.notify_one();
    }
}

impl Pool for Warm {
    fn evict(&self) -> bool {
        // dropped: killed, its slot given back
        self.take().is_some()
    }
}

impl Drop for Warm {
    fn drop(&mut self) {
        self.changed.notify_one();
    }
}

/// A reuse route's clone, and the connections it served (the one open included).
struct Reused {
    clone: Cloned,
    uses: u32,
}

/// A reuse route's clones serving no connection, the one used last on top, until they
/// were kept for `keep`; they give their room up to another route's VM, the one used
/// first first.
struct Kept {
    idle: Mutex<Vec<(Reused, Instant)>>,
    bounds: Current,
    capacity: Arc<Capacity>,
    route: u64,
    /// Wakes the sweep ([`sweep`]): a clone kept, the bounds changed, or the route gone.
    changed: Arc<Notify>,
}

impl Kept {
    /// Whether `reused` served its last connection: its `uses`, or its lifetime, at
    /// `now`.
    fn spent(&self, reused: &Reused, now: Instant) -> bool {
        let bounds = self.bounds.get();
        bounds.uses.is_some_and(|uses| reused.uses >= uses)
            || bounds
                .lifetime
                .is_some_and(|lifetime| now >= reused.clone.restored + lifetime)
    }

    /// Keeps `reused` for the next connection, or gives its clone back, to be ended,
    /// when it served its last, or `vms` went down.
    fn put(&self, reused: Reused) -> Option<Cloned> {
        let now = Instant::now();
        if self.spent(&reused, now) || self.capacity.over(self.route) {
            return Some(reused.clone);
        }
        self.idle.lock().unwrap().push((reused, now));
        self.changed.notify_one();
        None
    }

    /// The clone used last and a stream to its guest's `port`, counted a use; one past
    /// its lifetime, or not answering, is killed and the next one tried.
    async fn take(&self, port: u32) -> Option<(Reused, UnixStream)> {
        loop {
            let (mut reused, _) = self.idle.lock().unwrap().pop()?;
            if self.spent(&reused, Instant::now()) {
                continue;
            }
            match reused.clone.vm.dial(port).await {
                Ok(stream) => {
                    reused.uses += 1;
                    return Some((reused, stream));
                }
                Err(e) => eprintln!("kept clone: {e}"),
            }
        }
    }

    /// Takes out the clones kept for `keep`, past their lifetime or their `uses`, at `now`; and
    /// when the next of the others is.
    fn expire(&self, now: Instant) -> (Vec<Reused>, Option<Instant>) {
        let bounds = self.bounds.get();
        let mut idle = self.idle.lock().unwrap();
        let ends = |(reused, since): &(Reused, Instant)| {
            if bounds.uses.is_some_and(|uses| reused.uses >= uses) {
                return *since;
            }
            let kept = *since + bounds.keep;
            match bounds.lifetime {
                Some(lifetime) => kept.min(reused.clone.restored + lifetime),
                None => kept,
            }
        };
        let (expired, kept): (Vec<_>, Vec<_>) =
            idle.drain(..).partition(|clone| ends(clone) <= now);
        *idle = kept;
        let next = idle.iter().map(ends).min();
        (
            expired.into_iter().map(|(reused, _)| reused).collect(),
            next,
        )
    }
}

impl Pool for Kept {
    fn evict(&self) -> bool {
        let mut idle = self.idle.lock().unwrap();
        // dropped: killed, its slot given back
        (!idle.is_empty()).then(|| idle.remove(0)).is_some()
    }
}

impl Drop for Kept {
    fn drop(&mut self) {
        self.changed.notify_one();
    }
}

/// Kills a reuse route's clones as they expire ([`Kept::expire`]), until the route
/// is gone.
async fn sweep(kept: Weak<Kept>, changed: Arc<Notify>) {
    loop {
        let next = {
            let Some(kept) = kept.upgrade() else {
                return;
            };
            // dropped here: killed
            kept.expire(Instant::now()).1
        };
        match next {
            Some(next) => {
                tokio::select! {
                    () = tokio::time::sleep_until(next.into()) => {}
                    () = changed.notified() => {}
                }
            }
            None => changed.notified().await,
        }
    }
}

/// A pool route's VMs: clones of its snapshot each serving many connections at once.
/// A connection takes the VM holding the fewest (in turn among them) while it holds
/// fewer than `conns`; past that it waits for a VM being started, one started for every
/// `conns` connections waiting, up to `vms`, and takes the VM holding the fewest once
/// none is starting (the pool at `vms`) or its wait ran out. The connection taking the
/// last room has the next VM started ahead. A VM beyond `warm` serving none for `keep`
/// is killed, as is one past `vms`, which takes no connection any more; a VM going down
/// ([`watch`]) or that does not answer is killed and, below `warm`, replaced, after a
/// wait as they keep going down ([`Backoff`]).
struct SharedPool {
    cloner: Arc<Cloner>,
    port: u32,
    bounds: Current,
    capacity: Arc<Capacity>,
    need: Need,
    members: Mutex<Members>,
    /// A VM started or failed, its last connection ended: the waiting connections and
    /// the tending ([`tend`]) look again.
    changed: Arc<Notify>,
}

#[derive(Default)]
struct Members {
    vms: Vec<Member>,
    /// VMs being started.
    starting: usize,
    /// Connections waiting for one of them.
    waiting: usize,
    next_id: u64,
    /// Where the next pick starts looking: VMs alike in their connections take turns.
    turn: usize,
    /// Counts the starts that failed, the last one's error.
    failures: u64,
    failed: String,
    /// The last one failed for room.
    full: Option<Full>,
    /// No VM is started to make up `warm` again before then, after a failure or a VM going down.
    retry: Option<Instant>,
    /// Counts the VMs that went down.
    downs: u64,
    /// Why the last VM it keeps went down or the last start for one failed, until `warm` are up
    /// ([`Route::down`]).
    down: Option<Shortage>,
    /// Why the last start failed for anything but room, until one succeeds: what a pool keeping
    /// none tells with no VM alive, and tries again with none alive.
    failing: Option<Shortage>,
    backoff: Backoff,
}

struct Member {
    id: u64,
    /// Its instance index, what tells which VMs are past `vms`.
    index: usize,
    clone: Arc<Cloned>,
    /// Its connections open.
    conns: usize,
    /// Since when it serves none.
    since: Instant,
    /// Has its watch look at it: a connection failed.
    suspects: mpsc::Sender<()>,
}

impl Members {
    /// A start failed with `e`, `warm` the VMs it keeps.
    fn failed(&mut self, e: BoxError, warm: usize) {
        self.failures += 1;
        self.failed = e.to_string();
        let why = format!("its start failed: {e}");
        self.full = e.downcast().ok().map(|full| *full);
        if self.full.is_none() {
            Shortage::keep(&mut self.failing, why.clone());
        }
        if self.vms.len() < warm {
            Shortage::keep(&mut self.down, why);
        }
        self.retry = Some(Instant::now() + POOL_RETRY);
    }

    /// [`Route::down`] of a pool keeping `warm`.
    fn short(&self, warm: usize) -> Option<Shortage> {
        if self.vms.len() < warm {
            self.down.clone()
        } else if self.vms.is_empty() {
            self.failing.clone()
        } else {
            None
        }
    }

    /// The last start's failure, which says when it was for room.
    fn failure(&self) -> BoxError {
        match &self.full {
            Some(full) => full.clone().into(),
            None => self.failed.clone().into(),
        }
    }

    /// [`first_past`] of its VMs.
    fn past(&self, max: usize) -> usize {
        if self.vms.len() <= max {
            return usize::MAX;
        }
        first_past(self.vms.iter().map(|vm| vm.index).collect(), max)
    }

    /// The VM within `max` holding the fewest connections, the one after the last seat
    /// taken first among them.
    fn least(&self, max: usize) -> Option<usize> {
        let n = self.vms.len();
        let start = self.turn % n.max(1);
        let past = self.past(max);
        (0..n)
            .map(|i| (start + i) % n)
            .filter(|&i| self.vms[i].index < past)
            .min_by_key(|&i| self.vms[i].conns)
    }

    /// A seat on the VM at `at`: its connections counted one more.
    fn seat(&mut self, at: usize) -> (u64, Arc<Cloned>) {
        self.turn = at + 1;
        let member = &mut self.vms[at];
        member.conns += 1;
        (member.id, member.clone.clone())
    }

    /// The VM serving no connection the longest, past `max` or, of those within it, beyond `min`.
    fn idlest(&self, min: usize, max: usize) -> Option<usize> {
        let past = self.past(max);
        let within = self.vms.iter().filter(|vm| vm.index < past).count();
        (0..self.vms.len())
            .filter(|&i| {
                let vm = &self.vms[i];
                vm.conns == 0 && (vm.index >= past || within > min)
            })
            .min_by_key(|&i| self.vms[i].since)
    }
}

/// The lowest of `indices` past the `max` VMs a pool may hold: those from it up take no connection,
/// the highest going first as `cf logs` lists the others. By their count, not by `max` itself: a
/// VM of a higher index within it serves as it is, its room taken by nothing else.
fn first_past(mut indices: Vec<usize>, max: usize) -> usize {
    indices.sort_unstable();
    indices.get(max).copied().unwrap_or(usize::MAX)
}

/// A connection waiting for a pool VM, counted in [`Members::waiting`] until it
/// takes a seat or gives up.
struct Waiter<'a> {
    pool: &'a SharedPool,
    counted: bool,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        if self.counted {
            self.pool.members.lock().unwrap().waiting -= 1;
        }
    }
}

/// A connection's seat on a pool VM, given back when dropped.
struct Seat {
    pool: Arc<SharedPool>,
    id: u64,
}

impl Drop for Seat {
    fn drop(&mut self) {
        let bounds = self.pool.bounds.get();
        let changed = {
            let mut members = self.pool.members.lock().unwrap();
            let (shrinks, waiting) = (members.vms.len() > bounds.warm, members.waiting > 0);
            match members.vms.iter_mut().find(|vm| vm.id == self.id) {
                Some(member) => {
                    member.conns -= 1;
                    if member.conns == 0 {
                        member.since = Instant::now();
                    }
                    // one more VM may idle out, a waiting connection take the room
                    (shrinks && member.conns == 0) || (waiting && member.conns < bounds.conns)
                }
                None => false,
            }
        };
        if changed {
            self.pool.changed.notify_waiters();
        }
    }
}

impl Seat {
    /// Has its VM looked at: its connection failed.
    fn suspect(&self) {
        let members = self.pool.members.lock().unwrap();
        if let Some(member) = members.vms.iter().find(|vm| vm.id == self.id) {
            let _ = member.suspects.try_send(());
        }
    }
}

impl SharedPool {
    /// Starts a VM in the background, unless the pool holds `vms` with those starting.
    fn grow(self: &Arc<Self>, members: &mut Members, wait: Duration) -> bool {
        if members.vms.len() + members.starting >= self.bounds.get().pool_size() {
            return false;
        }
        members.starting += 1;
        tokio::spawn(self.clone().start(wait));
        true
    }

    /// A VM restored, its guest accepting, into the pool, watched from then on: room for it
    /// waited for within `wait`.
    async fn start(self: Arc<Self>, wait: Duration) {
        let started = async {
            let slot = self.capacity.take(self.need, wait).await?;
            let (clone, _probe) = self.cloner.restore(slot, self.port).await?;
            Ok::<_, BoxError>(clone)
        }
        .await;
        {
            let mut members = self.members.lock().unwrap();
            members.starting -= 1;
            match started {
                Ok(mut clone) => {
                    let id = members.next_id;
                    members.next_id += 1;
                    members.failing = None;
                    let output = clone.output.take();
                    let (suspects, suspected) = mpsc::channel(1);
                    let clone = Arc::new(clone);
                    let (pool, vm) = (Arc::downgrade(&self), Arc::downgrade(&clone));
                    tokio::spawn(watched_clone(pool, id, vm, output, suspected));
                    members.vms.push(Member {
                        id,
                        index: clone.instance.index,
                        clone,
                        conns: 0,
                        since: Instant::now(),
                        suspects,
                    });
                    if members.vms.len() >= self.bounds.get().warm {
                        members.down = None;
                    }
                }
                Err(e) => {
                    eprintln!("route {}: pool VM: {e}", self.cloner.hosts.current().label);
                    members.failed(e, self.bounds.get().warm);
                }
            }
        }
        self.changed.notify_waiters();
    }

    /// Starts `warm` VMs, all of them up within `wait` or failed.
    async fn fill(self: &Arc<Self>, wait: Duration) -> Result<(), BoxError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let min = self.bounds.get().warm;
                let mut members = self.members.lock().unwrap();
                if members.failures > 0 {
                    return Err(members.failure());
                }
                if members.vms.len() >= min {
                    return Ok(());
                }
                while members.vms.len() + members.starting < min {
                    self.grow(&mut members, wait);
                }
            }
            notified.await;
        }
    }

    /// A stream to the guest of a VM, and the seat the connection takes on it (see
    /// [`SharedPool`]); failed when no VM came within `wait` (a pool of no `warm`
    /// serving none), or its start failed.
    async fn open(self: &Arc<Self>) -> Result<(UnixStream, Seat), BoxError> {
        let wait = self.bounds.get().wait;
        let deadline = Instant::now() + wait;
        let failures = self.members.lock().unwrap().failures;
        let mut waiter = Waiter {
            pool: self,
            counted: false,
        };
        let mut late = false;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let seat = {
                let bounds = self.bounds.get();
                let mut members = self.members.lock().unwrap();
                let least = members.least(bounds.pool_size());
                let at = match least.filter(|&at| members.vms[at].conns < bounds.conns) {
                    Some(at) => Some(at),
                    None => {
                        if least.is_none() && members.failures > failures {
                            return Err(match &members.full {
                                Some(full) => full.clone().into(),
                                None => format!("no VM of the pool: {}", members.failed).into(),
                            });
                        }
                        if !waiter.counted {
                            waiter.counted = true;
                            members.waiting += 1;
                        }
                        if members.waiting > members.starting * bounds.conns {
                            self.grow(&mut members, wait);
                        }
                        match least {
                            Some(at) if members.starting == 0 || late => Some(at),
                            None if late => {
                                return Err(format!(
                                    "no VM of the pool within {}ms",
                                    wait.as_millis()
                                )
                                .into());
                            }
                            _ => None,
                        }
                    }
                };
                at.map(|at| {
                    if waiter.counted {
                        waiter.counted = false;
                        members.waiting -= 1;
                    }
                    let seat = members.seat(at);
                    if members.starting == 0
                        && members.vms.iter().all(|vm| vm.conns >= bounds.conns)
                    {
                        self.grow(&mut members, wait);
                    }
                    seat
                })
            };
            let Some((id, clone)) = seat else {
                tokio::select! {
                    () = notified => {}
                    () = tokio::time::sleep_until(deadline.into()) => late = true,
                }
                continue;
            };
            let seat = Seat {
                pool: self.clone(),
                id,
            };
            match clone.vm.dial(self.port).await {
                Ok(stream) => return Ok((stream, seat)),
                Err(e) => {
                    drop((seat, clone));
                    self.went_down(id, &e.to_string());
                }
            }
        }
    }

    /// Kills the VM `id`, which serves no more for `why`: the next connection goes to another,
    /// and one is started in its place below `warm`, after [`Backoff`]'s wait.
    fn went_down(&self, id: u64, why: &str) {
        let warm = self.bounds.get().warm;
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.vms.iter().position(|vm| vm.id == id);
            at.map(|at| {
                let gone = members.vms.remove(at);
                members.downs += 1;
                if members.vms.len() < warm {
                    Shortage::keep(&mut members.down, why);
                }
                members
                    .backoff
                    .went_down(Some(gone.clone.restored.elapsed()));
                let wait = members.backoff.next_wait();
                members.retry = (!wait.is_zero()).then(|| Instant::now() + wait);
                gone
            })
        };
        if gone.is_some() {
            eprintln!(
                "route {}: a pool VM: {why}; killed",
                self.cloner.hosts.current().label
            );
            // killed before a VM is started in its place, which takes its index
            drop(gone);
            self.changed.notify_waiters();
        }
    }

    /// Takes out the VMs past `vms` or beyond `warm` serving none for `keep` at `now`, and
    /// starts those missing to `warm`; and when to look again.
    fn tend(self: &Arc<Self>, now: Instant) -> (Vec<Member>, Option<Instant>) {
        let bounds = self.bounds.get();
        let (min, max) = (bounds.warm, bounds.pool_size());
        let mut members = self.members.lock().unwrap();
        let mut gone = Vec::new();
        while let Some(at) = members.idlest(min, max)
            && members.vms[at].since + bounds.keep <= now
        {
            gone.push(members.vms.remove(at));
        }
        let mut next = members
            .idlest(min, max)
            .map(|at| members.vms[at].since + bounds.keep);
        // one VM to see a failure through where it keeps none, gone again after `keep`: nothing
        // else tells it is over but the next connection
        let up = min.max(usize::from(members.failing.is_some()));
        if members.vms.len() + members.starting < up {
            match members.retry {
                Some(retry) if retry > now => {
                    next = Some(next.map_or(retry, |next| next.min(retry)));
                }
                _ => {
                    while members.vms.len() + members.starting < up
                        && self.grow(&mut members, bounds.wait)
                    {}
                }
            }
        }
        (gone, next)
    }
}

impl Pool for SharedPool {
    fn evict(&self) -> bool {
        let bounds = self.bounds.get();
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.idlest(bounds.warm, bounds.pool_size());
            at.map(|at| members.vms.remove(at))
        };
        // dropped: killed, its slot given back
        gone.is_some()
    }
}

impl Drop for SharedPool {
    fn drop(&mut self) {
        self.changed.notify_waiters();
    }
}

/// Keeps a pool route's VMs ([`SharedPool::tend`]) until the route is gone.
async fn tend(pool: Weak<SharedPool>, changed: Arc<Notify>) {
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let next = {
            let Some(pool) = pool.upgrade() else {
                return;
            };
            // dropped here: killed
            pool.tend(Instant::now()).1
        };
        match next {
            Some(next) => {
                tokio::select! {
                    () = tokio::time::sleep_until(next.into()) => {}
                    () = notified => {}
                }
            }
            None => notified.await,
        }
    }
}

/// The wait before a VM in the place of one that went down: none, then [`RESTART_WAIT`] doubled
/// as they keep going down, none again past one up for [`RESTART_STABLE`].
#[derive(Default)]
pub struct Backoff {
    wait: Duration,
}

impl Backoff {
    /// A VM went down, up for `lived` (None: none started so yet).
    pub fn went_down(&mut self, lived: Option<Duration>) {
        if lived.is_none_or(|lived| lived >= RESTART_STABLE) {
            self.wait = Duration::ZERO;
        }
    }

    /// The wait before the next start; the one after it is longer.
    pub fn next_wait(&mut self) -> Duration {
        let wait = self.wait;
        self.wait = (wait * 2).clamp(RESTART_WAIT, RESTART_WAIT_MAX);
        wait
    }
}

/// Restores a cloned route's clones from its snapshot, and follows their
/// output.
struct Cloner {
    snapshot: Snapshot,
    /// Where the clones are started.
    place: Arc<Place>,
    /// Numbers the clones.
    next: AtomicU64,
    /// The instance indices its clones alive hold.
    indices: Arc<Indices>,
    sink: Arc<Sink>,
    hosts: Arc<Hosts>,
    /// Whether the guest sends its output: its template's did.
    output: bool,
    /// The route's egress, if it has any.
    egress: Option<Arc<Egress>>,
}

impl Cloner {
    /// A VM restored from the snapshot, told it was (`wire::CONTROL_PORT`): its random
    /// state reseeded from the host's, its restore hook run; and a stream to its guest on
    /// `port`, its output followed from then on. Alive only then: what fails holds no index, for
    /// [`Route::alive`] to list.
    async fn restore(&self, slot: Slot, port: u32) -> Result<(Cloned, UnixStream), BoxError> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let vm = Vm::restore(&self.snapshot, &self.place, n, slot, self.egress.as_ref()).await?;
        let mut seed = [0u8; 32];
        // SAFETY: getrandom into a buffer of its length
        if unsafe { libc::getrandom(seed.as_mut_ptr().cast(), seed.len(), 0) }
            != seed.len() as isize
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut stream = vm.dial_ready(wire::CONTROL_PORT, RESTORE_TIMEOUT).await?;
        // the time read just before the write: the guest sets its clock to it as it
        // arrives
        let notice = wire::Notice {
            seed,
            time: match crate::vm::RESTORE_ADVANCES_CLOCK {
                true => None,
                false => Some(
                    SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(std::io::Error::other)?,
                ),
            },
        };
        // the stream kept whole: Firecracker takes a write half shut down as the end
        // of the whole stream (the answer lost)
        notify(&mut stream, &notice).await?;
        let accepting = vm.dial_ready(port, RESTORE_TIMEOUT).await;
        let instance = self.indices.take(0);
        // the guest's listener outlives the restore; its lines wait for the stream
        let mut output = match self.output {
            true => match vm.dial(wire::LOG_PORT).await {
                Ok(stream) => Some(output::follow(
                    stream,
                    self.sink.clone(),
                    self.hosts.clone(),
                    format!("vm-{n}"),
                    instance.index,
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
        let accepting = match accepting {
            Ok(stream) => stream,
            Err(e) => {
                // its last lines first: what says why
                if let Some(output) = &mut output {
                    output.sync(END_SYNC_TIMEOUT).await;
                }
                return Err(e);
            }
        };
        let cloned = Cloned {
            instance,
            vm,
            output,
            restored: Instant::now(),
        };
        Ok((cloned, accepting))
    }
}

/// Follows a VM, a shared one or a pool's, until it serves no more: why, or None when it is gone
/// from its route.
///
/// - the log stream ended: the VM is gone
/// - the guest said its process ended, and its port refuses at once (`accepting`, given how long
///   to look): a launcher that left a daemon listening is no end. A process ending on purpose is
///   not told apart: a route's process is to serve, and the wicket's own stop empties the route
///   first (the routes then take no notice)
/// - a connection failed (`suspected`), and the port refuses for as long as the guest may take
///   to listen at a boot (`boot`): one process's restart in place is no end, nor is a client
///   gone. What lives, listens and does not answer is not seen
async fn watch<F>(
    mut output: Option<Output>,
    mut suspected: mpsc::Receiver<()>,
    accepting: impl Fn(Duration) -> F,
    boot: impl Fn() -> Duration,
) -> Option<&'static str>
where
    F: Future<Output = bool>,
{
    let mut told = false;
    loop {
        let ended = async {
            match &mut output {
                Some(output) => output.ended(told).await,
                None => std::future::pending().await,
            }
        };
        let ended = tokio::select! {
            biased;
            ended = ended => Some(ended),
            suspect = suspected.recv() => {
                suspect?;
                None
            }
        };
        match ended {
            Some(Ended::Vm) => return Some("its VM ended"),
            Some(Ended::Process) => {
                told = true;
                if !accepting(Duration::ZERO).await {
                    return Some("its process ended");
                }
            }
            None => {
                if !accepting(boot()).await {
                    return Some("its guest accepts no connection");
                }
            }
        }
    }
}

/// Whether a VM accepts within `within`, as `look` finds it, each look again [`LOOK_AGAIN`] after
/// the last. None of `look`: the VM is gone.
async fn accepting_within<F>(look: impl Fn() -> F, within: Duration) -> bool
where
    F: Future<Output = Option<bool>>,
{
    let deadline = Instant::now() + within;
    loop {
        match look().await {
            None => return false,
            Some(true) => return true,
            Some(false) if Instant::now() >= deadline => return false,
            Some(false) => tokio::time::sleep(LOOK_AGAIN).await,
        }
    }
}

/// [`watch`] of the route `route`'s VM `vm`, on its `port`, its going down told to `down`. A look
/// holds the VM only as long as it takes: a stop kills it.
async fn watched(
    vm: Weak<SharedVm>,
    port: u32,
    bounds: Current,
    output: Option<Output>,
    suspected: mpsc::Receiver<()>,
    down: mpsc::UnboundedSender<Down>,
    route: u64,
) {
    let look = || {
        let vm = vm.clone();
        async move {
            let held = vm.upgrade().and_then(|vm| vm.lock().unwrap().clone())?;
            Some(held.accepting(port).await)
        }
    };
    let accepting = |within| accepting_within(look, within);
    let boot = || bounds.get().boot;
    if let Some(why) = watch(output, suspected, accepting, boot).await {
        let _ = down.send(Down { route, why });
    }
}

/// [`watch`] of the pool's VM `id`, its going down told to the pool.
async fn watched_clone(
    pool: Weak<SharedPool>,
    id: u64,
    vm: Weak<Cloned>,
    output: Option<Output>,
    suspected: mpsc::Receiver<()>,
) {
    let Some((port, bounds)) = pool.upgrade().map(|pool| (pool.port, pool.bounds.clone())) else {
        return;
    };
    let look = || {
        let vm = vm.clone();
        async move {
            let held = vm.upgrade()?;
            Some(held.vm.accepting(port).await)
        }
    };
    let accepting = |within| accepting_within(look, within);
    let boot = || bounds.get().boot;
    if let Some(why) = watch(output, suspected, accepting, boot).await
        && let Some(pool) = pool.upgrade()
    {
        pool.went_down(id, why);
    }
}

/// A pool's VMs not started for `e`, named after the route `label`: for room still.
fn pool_failure(label: &str, e: BoxError) -> BoxError {
    match e.downcast::<Full>() {
        Ok(full) => full.of(label).into(),
        Err(e) => format!("route {label}: pool VM: {e}").into(),
    }
}

impl Route {
    /// Boots the route's VM (shared), or boots a template VM, snapshots it once its
    /// guest listens and kills it (the cloned modes; a pool's `warm` VMs restored then);
    /// `hosts` name it in the log. See [`Starting`].
    pub async fn start(
        spec: RouteSpec,
        hosts: &[String],
        starting: Starting,
    ) -> Result<Route, BoxError> {
        let Starting {
            places,
            n,
            capacity,
            held,
            sink,
            workers,
            stops,
        } = starting;
        let hosts = Hosts::new(hosts);
        let label = hosts.current().label.clone();
        let bounds = Current(Arc::new(Mutex::new(spec.bounds)));
        let need = Need {
            memory: u64::from(spec.vm.mem_mib),
            route: NEXT_ROUTE.fetch_add(1, Ordering::Relaxed),
        };
        capacity.cap(need.route, spec.bounds.vms);
        let slot = match &held {
            Some(held) => capacity.in_room_of(held, need),
            None => capacity
                .take(need, START_WAIT)
                .await
                .map_err(|e| e.of(&label))?,
        };
        if let Some(disk) = &spec.vm.disk {
            disk.ready()
                .await
                .map_err(|e| format!("route {label}: {e}"))?;
        }
        let place = Arc::new(
            places
                .place(n, &spec.vm)
                .await
                .map_err(|e| format!("route {label}: {e}"))?,
        );
        let started = Instant::now();
        let (name, role) = match spec.mode {
            Mode::Shared => ("vm-0", Role::Shared),
            Mode::PerConnection | Mode::Reuse | Mode::Pool => ("template", Role::Template),
        };
        let egress = (!spec.vm.egress.is_empty())
            .then(|| Egress::new(spec.vm.egress.clone(), hosts.clone(), sink.clone(), workers));
        let named = egress.as_ref().map(|egress| (egress, name.to_string()));
        let vm = Vm::boot(&spec.vm, &place, role, slot, named, &stops).await?;
        let mut output = match ready(&vm, spec.port, spec.bounds.boot, &sink, &hosts, name).await {
            Ok(output) => output,
            Err(e) => {
                // what it wrote before it failed is on its disk all the same
                if spec.vm.disk.is_some()
                    && let Err(stop) = vm.stop(disk::STOP_GRACE).await
                {
                    eprintln!("route {label}: its VM's stop: {stop}; killed");
                }
                return Err(e);
            }
        };
        let booted = started.elapsed();
        let mut taken = None;
        let backend = match spec.mode {
            Mode::Shared => {
                println!(
                    "route {label}: VM booted, guest listening after {}ms",
                    booted.as_millis()
                );
                let vm = Arc::new(Mutex::new(Some(Arc::new(vm))));
                let (suspects, suspected) = mpsc::channel(1);
                Backend::Shared {
                    vm,
                    suspects,
                    unwatched: Mutex::new(Some((output, suspected))),
                    started: SystemTime::now(),
                }
            }
            Mode::PerConnection | Mode::Reuse | Mode::Pool => {
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
                    prime(vm.dial(spec.port), PRIME_TIMEOUT)
                        .await
                        .map_err(|e| format!("route {label}: {e}"))?;
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
                let snapshot = vm.snapshot(&place, &spec.vm).await?;
                drop(vm);
                // the template gone, its snapshot the clones' alone to read (jail mode)
                place.seal().await?;
                // what its guest wrote, the rest of its size a hole
                let metadata = tokio::fs::metadata(&snapshot.memory_on_host).await?;
                let size = std::os::unix::fs::MetadataExt::blocks(&metadata) * 512;
                taken = Some(Taken {
                    memory: size,
                    booted,
                    taken: started.elapsed(),
                    at: SystemTime::now(),
                });
                println!(
                    "route {label}: template booted in {}ms, primed with {} request(s), snapshot taken after {}ms ({} MiB memory file)",
                    booted.as_millis(),
                    spec.prime,
                    started.elapsed().as_millis(),
                    size >> 20
                );
                let cloner = Arc::new(Cloner {
                    snapshot,
                    place: place.clone(),
                    next: AtomicU64::new(0),
                    indices: Arc::default(),
                    sink,
                    hosts: hosts.clone(),
                    output: output.is_some(),
                    egress,
                });
                if spec.mode == Mode::Pool {
                    let changed = Arc::new(Notify::new());
                    let pool = Arc::new(SharedPool {
                        cloner,
                        port: spec.port,
                        bounds: bounds.clone(),
                        capacity: capacity.clone(),
                        need,
                        members: Mutex::default(),
                        changed: changed.clone(),
                    });
                    pool.fill(START_WAIT)
                        .await
                        .map_err(|e| pool_failure(&label, e))?;
                    println!(
                        "route {label}: {} pool VM(s) up after {}ms",
                        spec.bounds.warm,
                        started.elapsed().as_millis()
                    );
                    let spare: Arc<dyn Pool> = pool.clone();
                    capacity.register(need.route, Arc::downgrade(&spare));
                    tokio::spawn(tend(Arc::downgrade(&pool), changed));
                    Backend::Pool(pool)
                } else {
                    let kept = (spec.mode == Mode::Reuse).then(|| {
                        let changed = Arc::new(Notify::new());
                        let kept = Arc::new(Kept {
                            idle: Mutex::default(),
                            bounds: bounds.clone(),
                            capacity: capacity.clone(),
                            route: need.route,
                            changed: changed.clone(),
                        });
                        let spare: Arc<dyn Pool> = kept.clone();
                        capacity.register(need.route, Arc::downgrade(&spare));
                        tokio::spawn(sweep(Arc::downgrade(&kept), changed));
                        kept
                    });
                    let changed = Arc::new(Notify::new());
                    let warm = Arc::new(Warm {
                        clones: Mutex::default(),
                        failed: Mutex::default(),
                        bounds: bounds.clone(),
                        capacity: capacity.clone(),
                        route: need.route,
                        changed: changed.clone(),
                    });
                    let spare: Arc<dyn Pool> = warm.clone();
                    capacity.register(need.route, Arc::downgrade(&spare));
                    tokio::spawn(keep_warm(
                        Arc::downgrade(&warm),
                        changed,
                        cloner.clone(),
                        spec.port,
                        capacity.clone(),
                        need,
                    ));
                    Backend::Cloned { cloner, warm, kept }
                }
            }
        };
        Ok(Route {
            vm: spec.vm,
            mode: spec.mode,
            port: spec.port,
            prime: spec.prime,
            app: spec.app,
            bounds,
            hosts,
            _place: place,
            capacity,
            need,
            backend,
            clients: Arc::default(),
            open: Arc::default(),
            taken,
            restarts: AtomicU64::new(0),
            down: Mutex::default(),
            stops: Arc::default(),
        })
    }

    /// Starts the watch of its shared VM, its going down told to `down`: the route is one of
    /// those the routes know of ([`Down::route`]), so it is once it serves. What ended before
    /// is told all the same.
    pub fn watch(&self, down: mpsc::UnboundedSender<Down>) {
        let Backend::Shared { vm, unwatched, .. } = &self.backend else {
            return;
        };
        if let Some((output, suspected)) = unwatched.lock().unwrap().take() {
            tokio::spawn(watched(
                Arc::downgrade(vm),
                self.port,
                self.bounds.clone(),
                output,
                suspected,
                down,
                self.need.route,
            ));
        }
    }

    /// What tells it from any other route, over every route the wicket ever started.
    pub fn id(&self) -> u64 {
        self.need.route
    }

    /// What its VMs are.
    pub fn vm(&self) -> &Spec {
        &self.vm
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// As it is now: its host is the routes' to say.
    pub fn spec(&self) -> RouteSpec {
        RouteSpec {
            host: String::new(),
            vm: self.vm.clone(),
            mode: self.mode,
            port: self.port,
            prime: self.prime,
            app: self.app.clone(),
            bounds: self.bounds.get(),
        }
    }

    /// How long a connection may relay no byte, either way, before it is failed.
    pub fn idle(&self) -> Option<Duration> {
        self.bounds.get().idle
    }

    /// Takes `bounds` from now on, its VMs and their snapshot kept: whether they changed. VMs are
    /// cloned for a higher `warm` or `vms` as there is room; past a lower `vms`, a pool's VMs take
    /// no connection any more and go as `keep-ms` has them go, once theirs ended, and of a VM per
    /// connection what serves none goes at once.
    pub fn adjust(&self, bounds: Bounds) -> bool {
        {
            let mut current = self.bounds.0.lock().unwrap();
            if *current == bounds {
                return false;
            }
            *current = bounds;
        }
        self.capacity.cap(self.need.route, bounds.vms);
        match &self.backend {
            Backend::Shared { .. } => {}
            Backend::Cloned { warm, kept, .. } => {
                warm.adjust();
                if let Some(kept) = kept {
                    kept.changed.notify_one();
                }
                while self.capacity.over(self.need.route)
                    && (warm.evict() || kept.as_ref().is_some_and(|kept| kept.evict()))
                {
                }
            }
            Backend::Pool(pool) => {
                // tried again at once: what it was short for before is not this
                let mut members = pool.members.lock().unwrap();
                members.retry = None;
                members.down = None;
                drop(members);
                pool.changed.notify_waiters();
            }
        }
        true
    }

    /// What its VMs are now.
    pub fn vms(&self) -> Vms {
        let mut vms = Vms {
            alive: self.capacity.alive(self.need.route),
            connections: self.open.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            ..Vms::default()
        };
        match &self.backend {
            Backend::Shared { vm, .. } => vms.shared = vm.lock().unwrap().is_some(),
            Backend::Cloned { warm, kept, .. } => {
                vms.warm = warm.clones.lock().unwrap().len();
                vms.kept = kept
                    .as_ref()
                    .map_or(0, |kept| kept.idle.lock().unwrap().len());
            }
            Backend::Pool(pool) => {
                let members = pool.members.lock().unwrap();
                vms.pooled = members.vms.len();
                vms.starting = members.starting;
                vms.restarts = members.downs;
            }
        }
        vms
    }

    /// The instance indices of its VMs alive and their starts: a clone's from its guest accepting
    /// on, the shared VM's 0 (see the module's).
    pub fn alive(&self) -> BTreeMap<usize, SystemTime> {
        match &self.backend {
            Backend::Shared { vm, started, .. } => match vm.lock().unwrap().is_some() {
                true => BTreeMap::from([(0, *started)]),
                false => BTreeMap::new(),
            },
            Backend::Cloned { cloner, .. } => cloner.indices.held(),
            Backend::Pool(pool) => pool.cloner.indices.held(),
        }
    }

    /// Cloned modes: its template's snapshot.
    pub fn taken(&self) -> Option<&Taken> {
        self.taken.as_ref()
    }

    /// The hosts the route serves, for the log: `a.local, b.local`.
    pub fn label(&self) -> String {
        self.hosts.current().label.clone()
    }

    /// The hosts the route serves from now on: a reload kept its VMs.
    pub fn rename(&self, hosts: &[String]) {
        self.hosts.set(hosts);
    }

    /// Whether a VM of it holds a disk: its disk's file.
    pub fn disk(&self) -> Option<&std::path::Path> {
        let disk = self.vm.disk.as_ref()?;
        (!self.stopped()).then_some(disk.file.as_path())
    }

    /// Whether its shared VM was stopped: its connections fail from then on.
    pub fn stopped(&self) -> bool {
        matches!(&self.backend, Backend::Shared { vm, .. } if vm.lock().unwrap().is_none())
    }

    /// Why it is short of VMs, since that goes on: its shared VM down, as far as told; a pool below
    /// `warm`, its last VM gone or its last start failed; clones below `warm`, the last restore
    /// failed; and with no VM alive, the last restore failed for anything but room: `warm` 0 reads
    /// as idle at none otherwise. That restore is tried again, so this ends with its cause.
    ///
    /// None for what else it does not keep, however it fails (a VM beyond `warm`: its connection is
    /// told), and for what is only under way: a start waiting for room within `wait-ms`, warm
    /// clones waiting for room, which they only take where no VM waits for it.
    pub fn down(&self) -> Option<Shortage> {
        match &self.backend {
            Backend::Pool(pool) => {
                let warm = self.bounds.get().warm;
                pool.members.lock().unwrap().short(warm)
            }
            Backend::Cloned { cloner, warm, .. } => {
                let failed = warm.failed.lock().unwrap().clone();
                failed.filter(|_| warm.missing() > 0 || cloner.indices.held().is_empty())
            }
            Backend::Shared { .. } if self.stopped() => self.down.lock().unwrap().clone(),
            Backend::Shared { .. } => None,
        }
    }

    pub fn set_down(&self, why: String) {
        Shortage::keep(&mut self.down.lock().unwrap(), why);
    }

    /// The VMs started in the place of one that went down, this one included.
    pub fn set_restarts(&self, restarts: u64) {
        self.restarts.store(restarts, Ordering::Relaxed);
    }

    /// Stops its shared VM, for good ([`Route::stopping`]); a stop under way too.
    pub async fn stop(&self) {
        self.stopping().await;
        drop(self.stops.lock().await);
    }

    /// Takes its shared VM out now, its connections failing from then on, and what stops it:
    /// what the process wrote is all on the disk, as far as it ended within
    /// [`disk::STOP_GRACE`], and its open connections end. It gives the disk up as it is gone
    /// (the lock tells, [`Disk::ready`]). A cloned route's VMs end with their last connection.
    pub fn stopping(&self) -> impl Future<Output = ()> + Send + 'static {
        let vm = match &self.backend {
            Backend::Shared { vm, .. } => vm.lock().unwrap().take(),
            Backend::Cloned { .. } | Backend::Pool(_) => None,
        };
        let label = self.label();
        // one VM, taken out once: free
        let stops = vm
            .as_ref()
            .and_then(|_| self.stops.clone().try_lock_owned().ok());
        async move {
            if let Some(vm) = vm
                && let Err(e) = vm.stop(disk::STOP_GRACE).await
            {
                eprintln!("route {label}: its VM's stop: {e}; killed");
            }
            drop(stops);
        }
    }

    /// Has its shared VM looked at: a connection of it failed.
    pub fn suspect(&self) {
        if let Backend::Shared { suspects, .. } = &self.backend {
            let _ = suspects.try_send(());
        }
    }

    /// What the client of a connection failed for `e` is told when that is room for its VM;
    /// the budget's numbers are the log's.
    pub fn no_room(&self, e: &BoxError) -> Option<String> {
        if !e.is::<Full>() {
            return None;
        }
        let bounds = self.bounds.get();
        let within = format!("no room for a VM within {}ms", bounds.wait.as_millis());
        Some(match (self.mode, bounds.vms) {
            (Mode::PerConnection | Mode::Reuse, Some(vms)) => {
                format!("{within}: a VM per connection, {vms} at most")
            }
            (Mode::PerConnection | Mode::Reuse, None) => format!("{within}: a VM per connection"),
            _ => within,
        })
    }

    /// A stream to a guest for one connection of `client` (its IP address; empty when
    /// unknown, which `per-client` does not bound): failed at once when the client
    /// holds its share of the route already.
    pub async fn open(&self, client: &str) -> Result<Lease, BoxError> {
        let share = match self.bounds.get().per_client {
            Some(max) if !client.is_empty() => Some(self.clients.claim(client, max)?),
            _ => None,
        };
        let open = Open::new(&self.open);
        let (stream, vm) = self.dial().await?;
        Ok(Lease {
            stream,
            hold: Hold {
                vm,
                _share: share,
                _open: open,
            },
        })
    }

    /// A stream to a guest, and the VM the connection holds.
    async fn dial(&self) -> Result<(UnixStream, Held), BoxError> {
        let trace = std::env::var_os("SLUICE_WICKET_RUNTIME_FIRECRACKER_TRACE").is_some();
        match &self.backend {
            Backend::Shared { vm, .. } => {
                let vm = vm.lock().unwrap().clone();
                let vm = vm.ok_or("its VM was stopped: the route is restarting")?;
                match vm.dial(self.port).await {
                    Ok(stream) => Ok((stream, Held::Shared)),
                    Err(e) => {
                        self.suspect();
                        Err(e)
                    }
                }
            }
            Backend::Pool(pool) => {
                let (stream, seat) = pool.open().await?;
                Ok((stream, Held::Pooled(seat)))
            }
            Backend::Cloned { cloner, warm, kept } => {
                if let Some(kept) = kept
                    && let Some((reused, stream)) = kept.take(self.port).await
                {
                    if trace {
                        eprintln!(
                            "route {}: clone reused ({} uses)",
                            self.label(),
                            reused.uses
                        );
                    }
                    return Ok((stream, Held::Reused(reused, kept.clone())));
                }
                let started = Instant::now();
                let taken = warm.take();
                let warmed = taken.is_some();
                let accepting = match taken {
                    Some(clone) => {
                        let stream = clone.vm.dial_ready(self.port, RESTORE_TIMEOUT).await;
                        stream.map(|stream| (clone, stream))
                    }
                    None => {
                        let wait = self.bounds.get().wait;
                        let slot = self.capacity.take(self.need, wait).await?;
                        let restored = cloner.restore(slot, self.port).await;
                        if restored.is_ok() {
                            *warm.failed.lock().unwrap() = None;
                        }
                        restored
                    }
                };
                let (clone, stream) = accepting.inspect_err(|e| warm.failing(e))?;
                if trace {
                    eprintln!(
                        "route {}: {} clone, guest accepting after {}us",
                        self.label(),
                        if warmed { "warm" } else { "fresh" },
                        started.elapsed().as_micros()
                    );
                }
                let held = match kept {
                    Some(kept) => Held::Reused(Reused { clone, uses: 1 }, kept.clone()),
                    None => Held::Own(clone),
                };
                Ok((stream, held))
            }
        }
    }
}

/// Waits for a guest just booted to listen on `port`, its output (the VM `name`, the instance
/// 0, of the route of `hosts`) followed meanwhile: `/init` opens the log stream before it starts
/// the process, so the output of a process failing to start is printed too, and a process
/// exiting before it listens ends the wait. None when the guest has no log stream (its
/// `/init` predates it).
async fn ready(
    vm: &Vm,
    port: u32,
    boot: Duration,
    sink: &Arc<Sink>,
    hosts: &Arc<Hosts>,
    name: &str,
) -> Result<Option<Output>, BoxError> {
    let output = followed(
        || vm.dial(wire::LOG_PORT),
        vm.dial_ready(port, boot),
        |stream| output::follow(stream, sink.clone(), hosts.clone(), name.to_string(), 0),
    )
    .await?;
    if output.is_none() {
        eprintln!(
            "route {}: the guest sends no output (vsock port {}): its /init predates it, \
             build the rootfs again (scripts/build-guest.sh, scripts/cnb-rootfs.sh)",
            hosts.current().label,
            wire::LOG_PORT
        );
    }
    Ok(output)
}

/// [`ready`]'s course: the log stream dialed (`dial_log`, again each millisecond while
/// refused) and followed (`follow`) while the guest comes to listen (`listening`). None
/// when the guest listens without a log stream.
async fn followed<D>(
    dial_log: impl Fn() -> D,
    listening: impl Future<Output = Result<UnixStream, BoxError>>,
    follow: impl FnOnce(UnixStream) -> Output,
) -> Result<Option<Output>, BoxError>
where
    D: Future<Output = Result<UnixStream, BoxError>>,
{
    // set once the guest listens: its log stream is open by then, if it has one
    let listened = AtomicBool::new(false);
    let log = async {
        loop {
            let late = listened.load(Ordering::Relaxed);
            match dial_log().await {
                Ok(stream) => return Some(stream),
                Err(_) if late => return None,
                Err(_) if listened.load(Ordering::Relaxed) => {}
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    };
    tokio::pin!(log, listening);
    let output = tokio::select! {
        biased;
        stream = &mut log => stream.map(follow),
        listening = &mut listening => {
            drop(listening?);
            // the dial in flight is let finish: the guest may have taken the stream, and
            // the lines it sent on it would go with it
            listened.store(true, Ordering::Relaxed);
            return Ok(log.await.map(follow));
        }
    };
    let Some(mut output) = output else {
        drop(listening.await?);
        return Ok(None);
    };
    tokio::select! {
        biased;
        listening = &mut listening => drop(listening?),
        () = output.exited() => {
            // its last lines first: what says why
            output.sync(END_SYNC_TIMEOUT).await;
            return Err("the guest's process exited before it listened (see its output)".into());
        }
    }
    Ok(Some(output))
}

/// Sends `notice` on a clone's control stream and reads its answer, a line of at most
/// [`MAX_NOTICE_ANSWER`] bytes, within [`RESTORE_TIMEOUT`]: `OK`.
async fn notify<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    notice: &wire::Notice,
) -> Result<(), BoxError> {
    let answered = async {
        stream.write_all(notice.line().as_bytes()).await?;
        wire::read_short_line(stream, MAX_NOTICE_ANSWER).await
    };
    let answer = tokio::time::timeout(RESTORE_TIMEOUT, answered)
        .await
        .map_err(|_| format!("restore notice: no answer within {RESTORE_TIMEOUT:?}"))?
        .map_err(|e| format!("restore notice: {e}"))?;
    let answer = answer.trim_ascii_end();
    if answer != b"OK" {
        return Err(format!("restore notice: {}", String::from_utf8_lossy(answer)).into());
    }
    Ok(())
}

/// One `GET /` to a guest on the stream `dial` opens, its answer read to the guest's
/// CLOSE within `timeout`: a 2xx. Of the answer, its first [`STATUS_LINE`] bytes are
/// kept.
async fn prime<S: AsyncRead + AsyncWrite + Unpin>(
    dial: impl Future<Output = Result<S, BoxError>>,
    timeout: Duration,
) -> Result<(), BoxError> {
    let answered = async {
        let mut stream = dial.await?;
        wire::write_data(
            &mut stream,
            b"GET / HTTP/1.1\r\nHost: prime\r\nConnection: close\r\n\r\n",
        )
        .await?;
        wire::write_close(&mut stream).await?;
        let mut reader = wire::Reader::new(stream);
        let mut head = Vec::with_capacity(STATUS_LINE);
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            match reader.read(&mut buffer).await? {
                wire::Read::Data(n) => {
                    let kept = n.min(STATUS_LINE - head.len());
                    head.extend_from_slice(&buffer[..kept]);
                }
                wire::Read::Close => return Ok::<_, BoxError>(head),
                wire::Read::Eof => return Err("priming request: no CLOSE from the guest".into()),
            }
        }
    };
    let head = tokio::time::timeout(timeout, answered)
        .await
        .map_err(|_| format!("priming request: no answer within {timeout:?}"))??;
    if !head.starts_with(b"HTTP/1.1 2") {
        let line = head.split(|&byte| byte == b'\r' || byte == b'\n').next();
        return Err(format!(
            "priming request: {}",
            String::from_utf8_lossy(line.unwrap_or_default())
        )
        .into());
    }
    Ok(())
}

/// Keeps `warm`'s clones restored as its `warm` asks, in room no VM waits for, until the route
/// is gone: restores run in parallel, up to [`REFILL_WORKERS`], as one at a time caps the clone
/// rate at about 1 / restore time; those no more asked for are given up.
async fn keep_warm(
    warm: Weak<Warm>,
    changed: Arc<Notify>,
    cloner: Arc<Cloner>,
    port: u32,
    capacity: Arc<Capacity>,
    need: Need,
) {
    let mut restoring = JoinSet::new();
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        {
            let Some(warm) = warm.upgrade() else {
                return;
            };
            let wanted = warm.wanted();
            if restoring.len() > wanted {
                restoring.abort_all();
            }
            while restoring.len() < wanted.min(REFILL_WORKERS) {
                let restored = warm_clone(cloner.clone(), port, capacity.clone(), need);
                restoring.spawn(restored);
            }
        }
        tokio::select! {
            () = notified => {}
            Some(restored) = restoring.join_next() => {
                if let Ok(restored) = restored
                    && let Some(warm) = warm.upgrade()
                {
                    match restored {
                        Ok(clone) => warm.put(clone),
                        Err(e) => warm.failing(&e),
                    }
                }
            }
        }
    }
}

/// A clone restored in room no VM waits for, ready: it answers at once as it is handed out. Why
/// not, once it is time to try again.
async fn warm_clone(
    cloner: Arc<Cloner>,
    port: u32,
    capacity: Arc<Capacity>,
    need: Need,
) -> Result<Cloned, BoxError> {
    let slot = capacity.take_spare(need).await;
    match cloner.restore(slot, port).await {
        Ok((clone, _probe)) => Ok(clone),
        Err(e) => {
            eprintln!("warm clone: {e}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::DuplexStream;

    use super::*;

    /// A pool's start failing for room reads so up to the route's start, and only that: what a
    /// controller tries again as the routes change, unlike VMs that do not start.
    #[tokio::test]
    async fn a_pool_failing_for_room_says_so() {
        let need = Need {
            memory: 1,
            route: 1,
        };
        let Err(full) = Capacity::new(0, None).take(need, Duration::ZERO).await else {
            panic!("no room at all");
        };
        for (failed, is_full, says) in [
            (BoxError::from(full), true, "route a.local: no room"),
            ("it died".into(), false, "route a.local: pool VM: it died"),
        ] {
            let mut members = Members::default();
            members.failed(failed, 1);
            assert_eq!(members.failures, 1);
            let failure = members.failure().to_string();
            let down = format!("its start failed: {failure}");
            let why = |warm| members.short(warm).map(|short| short.why);
            assert_eq!(why(1).as_ref(), Some(&down), "what it keeps");
            let kept_none = (!is_full).then_some(down);
            assert_eq!(why(0), kept_none, "at none kept, for anything but room");
            let failure = pool_failure("a.local", members.failure());
            assert_eq!(failure.is::<Full>(), is_full, "{failure}");
            assert!(failure.to_string().starts_with(says), "{failure}");
        }
    }

    /// The VMs past a pool's limit are the highest of those alive, by their count.
    #[test]
    fn the_highest_indices_are_past_a_pools_limit() {
        for (indices, max, past) in [
            (vec![0, 1, 2], 2, 2),
            (vec![2, 0, 1], 1, 1),
            (vec![1, 2, 3], 2, 3),
            (vec![0, 2], 2, usize::MAX),
            (vec![3], 1, usize::MAX),
            (vec![], 1, usize::MAX),
        ] {
            assert_eq!(first_past(indices.clone(), max), past, "{indices:?} {max}");
        }
    }

    /// The wicket's end of a stream whose guest double runs `answer` on the other end.
    fn guest<F>(answer: impl FnOnce(DuplexStream) -> F) -> DuplexStream
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (wicket, guest) = tokio::io::duplex(4096);
        tokio::spawn(answer(guest));
        wicket
    }

    /// What the wicket takes from a guest is bounded: a restore notice's answer is a
    /// short line; a priming answer is checked by its status line, the rest dropped, and
    /// read to its end within a timeout.
    #[tokio::test]
    async fn a_guests_answers_are_read_within_bounds() {
        let notice = wire::Notice {
            seed: [0; 32],
            time: None,
        };
        for (answer, expected) in [
            (b"OK\n".to_vec(), Ok(())),
            (
                b"ERR restore hook /hook: exit status: 1\n".to_vec(),
                Err("restore notice: ERR restore hook /hook: exit status: 1"),
            ),
            // read up to the bound alone: the rest stays with the double
            (
                vec![b'x'; 1 << 20],
                Err("restore notice: a line longer than 1024 bytes"),
            ),
        ] {
            let mut wicket = guest(async move |mut guest| {
                let _ = guest.write_all(&answer).await;
            });
            let notified = notify(&mut wicket, &notice).await;
            assert_eq!(
                notified.map_err(|e| e.to_string()),
                expected.map_err(String::from)
            );
        }

        let answer = |status: &'static [u8]| {
            async move |mut guest: DuplexStream| {
                let _ = wire::write_data(&mut guest, status).await;
                let _ = wire::write_data(&mut guest, &[b'.'; 1 << 20]).await;
                let _ = wire::write_close(&mut guest).await;
            }
        };
        let timeout = Duration::from_millis(200);
        for (wicket, expected) in [
            (guest(answer(b"HTTP/1.1 200 OK\r\n\r\n")), Ok(())),
            (
                guest(answer(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")),
                Err("priming request: HTTP/1.1 503 Service Unavailable"),
            ),
            // an answer without end
            (
                guest(async |mut guest| {
                    let _ = wire::write_data(&mut guest, b"HTTP/1.1 200 OK\r\n\r\n").await;
                    while wire::write_data(&mut guest, b".").await.is_ok() {}
                }),
                Err("priming request: no answer within 200ms"),
            ),
        ] {
            let primed = prime(async { Ok(wicket) }, timeout).await;
            assert_eq!(
                primed.map_err(|e| e.to_string()),
                expected.map_err(String::from)
            );
        }
    }

    /// What a shared VM's watch makes of what it is told: only an end it finds true, a stream
    /// given up telling nothing; the route's end ends it.
    #[tokio::test]
    async fn a_shared_vm_is_down_when_its_port_refuses_too() {
        use std::collections::VecDeque;

        use crate::output::{Printer, Written};
        use crate::wire::LogFrame;
        use sluice_wicket_tunnel::flume::Tap;

        const BOOT: Duration = Duration::from_secs(7);
        let sink = Arc::new(Sink {
            printer: Printer::new(Written::default(), Written::default()),
            tap: Tap::off(),
        });
        let hosts = Hosts::new(&["a.local".into()]);
        let log = || {
            let (wicket, guest) = UnixStream::pair().unwrap();
            let followed =
                output::follow(wicket, sink.clone(), hosts.clone(), "vm-0".to_string(), 0);
            (followed, guest)
        };
        let exited = {
            let mut frame = Vec::new();
            wire::put_log_frame(&mut frame, &LogFrame::Exited);
            frame
        };
        // the answers of the looks, in turn, which are asked with how long they may take
        let answers = |looks: &[(Duration, bool)]| {
            let looks: VecDeque<_> = looks.iter().copied().collect();
            let looks = Arc::new(Mutex::new(looks));
            move |within: Duration| {
                let look = looks.lock().unwrap().pop_front();
                async move {
                    let (asked, answer) = look.expect("no more looks");
                    assert_eq!(within, asked);
                    answer
                }
            }
        };

        // its process ended, a look finding it refusing
        let (followed, mut guest) = log();
        guest.write_all(&exited).await.unwrap();
        let (_suspects, suspected) = mpsc::channel(1);
        let accepting = answers(&[(Duration::ZERO, false)]);
        let why = watch(Some(followed), suspected, accepting, || BOOT).await;
        assert_eq!(why, Some("its process ended"));

        // a launcher's end, a daemon accepting: then nothing told of the process again; a
        // connection failing while it accepts, then not: down
        let (followed, mut guest) = log();
        guest.write_all(&exited).await.unwrap();
        let (suspects, suspected) = mpsc::channel(1);
        let looks = answers(&[(Duration::ZERO, true), (BOOT, true), (BOOT, false)]);
        let (looked, mut told) = mpsc::unbounded_channel();
        let accepting = move |within| {
            let _ = looked.send(());
            looks(within)
        };
        let watching = tokio::spawn(watch(Some(followed), suspected, accepting, || BOOT));
        told.recv().await.unwrap();
        for _ in 0..2 {
            suspects.send(()).await.unwrap();
        }
        let why = watching.await.unwrap();
        assert_eq!(why, Some("its guest accepts no connection"));

        // its VM gone: no look to take. A guest without a log stream, the route gone: no end
        let (followed, guest) = log();
        drop(guest);
        let (_suspects, suspected) = mpsc::channel(1);
        let why = watch(Some(followed), suspected, answers(&[]), || BOOT).await;
        assert_eq!(why, Some("its VM ended"));
        let (suspects, suspected) = mpsc::channel(1);
        drop(suspects);
        assert_eq!(watch(None, suspected, answers(&[]), || BOOT).await, None);

        // a frame the wicket does not take: the stream given up, the VM alive
        let (followed, mut guest) = log();
        guest.write_all(&[99, 0, 0, 0, 0]).await.unwrap();
        let (suspects, suspected) = mpsc::channel(1);
        let watching = tokio::spawn(watch(Some(followed), suspected, answers(&[]), || BOOT));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!watching.is_finished());
        drop(suspects);
        assert_eq!(watching.await.unwrap(), None);
    }

    /// The log stream in flight as the guest comes to listen is followed, not dropped:
    /// the guest took it and sent its lines on it. A guest listening without one has
    /// none followed.
    #[tokio::test]
    async fn the_log_stream_in_flight_as_the_guest_listens_is_followed() {
        use std::sync::atomic::AtomicUsize;

        use crate::output::{Printer, Written};
        use crate::wire::{LogFrame, Stream};
        use sluice_wicket_tunnel::flume::Tap;

        let stdout = Written::default();
        let sink = Arc::new(Sink {
            printer: Printer::new(stdout.clone(), Written::default()),
            tap: Tap::off(),
        });
        let hosts = Hosts::new(&["a.local".into()]);
        let follow =
            |stream| output::follow(stream, sink.clone(), hosts.clone(), "vm-0".to_string(), 0);
        // the guest takes each stream at once, its lines on the first; its answer to the
        // handshake comes once it listens
        let (dials, listened) = (AtomicUsize::new(0), AtomicBool::new(false));
        let listening = async || {
            while dials.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
            listened.store(true, Ordering::Relaxed);
            Ok(UnixStream::pair()?.0)
        };
        let guests = Mutex::new(Vec::new());
        let dial_log = || async {
            let (wicket, mut guest) = UnixStream::pair()?;
            if dials.fetch_add(1, Ordering::Relaxed) == 0 {
                let mut lines = Vec::new();
                let line = LogFrame::Line(Stream::Stdout, b"hello".to_vec());
                wire::put_log_frame(&mut lines, &line);
                guest.write_all(&lines).await?;
            }
            guests.lock().unwrap().push(guest);
            while !listened.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            tokio::task::yield_now().await;
            Ok(wicket)
        };
        let output = followed(dial_log, listening(), follow).await.unwrap();
        assert!(output.is_some());
        let deadline = Instant::now() + Duration::from_secs(10);
        while stdout.text().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(stdout.text(), "stdout [a.local vm-0] :: hello\n");

        let refused = || async { Err::<UnixStream, BoxError>("refused".into()) };
        let listening = async { Ok(UnixStream::pair()?.0) };
        let output = followed(refused, listening, follow).await.unwrap();
        assert!(output.is_none());
    }
}
