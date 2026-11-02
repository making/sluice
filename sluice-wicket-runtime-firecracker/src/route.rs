//! A route: the VMs serving one target (its hosts), and how a connection gets one of
//! them. The VMs' output is followed from their boot (a clone's from its restore) and
//! named after the route's hosts and the VM (`output`): `vm-0` a shared VM,
//! `template` a cloned route's template, `vm-<n>` its clones in the order they were
//! restored.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use sluice_wicket_console::env::Env;
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::{Notify, mpsc};

use crate::BoxError;
use crate::capacity::{Capacity, Need, Pool, Slot};
use crate::disk::{self, Disk};
use crate::egress::Egress;
use crate::keeper::Role;
use crate::output::{self, Hosts, Output, Sink};
use crate::vm::{self, Place, Places, Snapshot, Spec, Vm};
use crate::wire::{self, Rule};

/// How long a cold-booted guest may take to listen on its port.
const BOOT_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Default of `wait-ms`: how long a connection may wait for room for its VM.
const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// Default of `keep-ms`: how long a VM serving no connection is kept (reuse, pool).
const DEFAULT_KEEP: Duration = Duration::from_secs(60);

/// Default of `conns`: the connections every VM of a pool holds before it grows.
const DEFAULT_CONNS: usize = 16;

/// How long a pool waits before it replaces a VM it failed to start.
const POOL_RETRY: Duration = Duration::from_secs(1);

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
    /// Per-connection and reuse modes: clones kept restored ahead of the connections;
    /// pool mode: the pool's VMs kept serving no connection (default `vms`).
    pub warm: usize,
    /// Cloned modes: `GET /` requests sent to the template before its snapshot, so the
    /// clones start from a warmed-up process (JIT, lazily loaded code).
    pub prime: u32,
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
    /// The console's app it is a route of: its target names it, so its VMs are its own.
    pub app: Option<String>,
}

impl RouteSpec {
    /// Parses `host=<rootfs>[?mode=shared|per-connection|reuse|pool&vcpus=N&mem=MiB&cpu=N&port=N&warm=N&prime=N&vms=N&wait-ms=N&keep-ms=N&uses=N&lifetime-ms=N&conns=N&per-client=N&idle-ms=N&egress=<rule>,...&disk=<path>&disk-mib=N]`.
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
            warm: 0,
            prime: 0,
            vms: None,
            wait: DEFAULT_WAIT,
            keep: DEFAULT_KEEP,
            uses: None,
            lifetime: None,
            conns: DEFAULT_CONNS,
            per_client: None,
            idle: None,
            app: None,
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
                "warm" => spec.warm = number()? as usize,
                "prime" => spec.prime = number()?,
                "vms" => spec.vms = Some(number()? as usize),
                "wait-ms" => spec.wait = Duration::from_millis(u64::from(number()?)),
                "keep-ms" => spec.keep = millis()?,
                "uses" => spec.uses = Some(positive()?),
                "lifetime-ms" => spec.lifetime = Some(millis()?),
                "conns" => spec.conns = positive()? as usize,
                "cpu" => spec.vm.cpu = Some(positive()?),
                "per-client" => spec.per_client = Some(positive()? as usize),
                "idle-ms" => spec.idle = Some(millis()?),
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
            let Some(vms) = spec.vms else {
                return Err(format!("{named}: mode=pool needs vms (its VMs at most)"));
            };
            if !given.contains(&"warm") {
                spec.warm = vms;
            }
        }
        if let Some(vms) = spec.vms
            && (vms == 0 || vms < spec.warm)
        {
            return Err(format!("{named}: vms must be at least 1 and at least warm"));
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

    /// The VMs the route keeps for good once started: a shared VM, a pool's `warm`.
    pub fn resident(&self) -> usize {
        match self.mode {
            Mode::Shared => 1,
            Mode::Pool => self.warm,
            Mode::PerConnection | Mode::Reuse => 0,
        }
    }

    /// The target advertised for the route, which the server passes verbatim as the
    /// dial address of its CONNECTs: `firecracker:<rootfs>`, plus the options set to
    /// other than their defaults (`?mode=per-connection&vcpus=2...`, in a fixed order;
    /// the bounds of a connection too: a target's connections are bound alike), an
    /// app's name first (unreserved chars: no escaping). Routes of one target share its
    /// VMs. Its env is not in it: the server sees targets, and a change of the env starts
    /// the target's VMs anew all the same ([`RouteSpec::same_vms`]).
    pub fn target(&self) -> String {
        let query: Vec<String> = self
            .app
            .iter()
            .map(|app| format!("app={app}"))
            .chain(
                self.options()
                    .into_iter()
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
        let warm = match self.mode {
            Mode::Pool => self.vms.unwrap_or(0),
            _ => 0,
        };
        let millis = |duration: Option<Duration>| duration.map_or(0, |d| d.as_millis() as u32);
        for (key, value, default) in [
            ("vcpus", self.vm.vcpus, 1),
            ("mem", self.vm.mem_mib, 128),
            ("cpu", self.vm.cpu.unwrap_or(0), 0),
            ("port", self.port, 8080),
            ("warm", self.warm as u32, warm as u32),
            ("prime", self.prime, 0),
            ("vms", self.vms.unwrap_or(0) as u32, 0),
            (
                "wait-ms",
                self.wait.as_millis() as u32,
                DEFAULT_WAIT.as_millis() as u32,
            ),
            (
                "keep-ms",
                self.keep.as_millis() as u32,
                DEFAULT_KEEP.as_millis() as u32,
            ),
            ("uses", self.uses.unwrap_or(0), 0),
            ("lifetime-ms", millis(self.lifetime), 0),
            ("conns", self.conns as u32, DEFAULT_CONNS as u32),
            ("per-client", self.per_client.unwrap_or(0) as u32, 0),
            ("idle-ms", millis(self.idle), 0),
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
    pub at: std::time::SystemTime,
}

enum Held {
    Shared,
    /// Per-connection mode: the connection's own clone, killed when it ends.
    Own(Cloned),
    /// Reuse mode: the connection's clone, kept for the next connection.
    Reused(Reused, Arc<Kept>),
    /// Pool mode: the connection's seat on a VM of the pool.
    Pooled(#[expect(dead_code, reason = "held for its drop: the seat given back")] Seat),
}

impl Hold {
    /// The connection's relay ended, `clean` when as it should (not with the guest's
    /// stream cut, or idle past `idle-ms`). A reused clone is kept for the next
    /// connection at once, when the connection ended clean and the clone has uses and
    /// life left: before the connection's end is told, the next connection of its
    /// client finds it. What is left to do: a clone the connection ended with killed
    /// once its output came, then the client's share given back.
    pub fn end(self, clean: bool) -> impl Future<Output = ()> {
        let ended = match self.vm {
            Held::Own(clone) => Some(clone),
            Held::Reused(reused, kept) if clean => kept.put(reused),
            Held::Reused(reused, _) => Some(reused.clone),
            Held::Shared | Held::Pooled(_) => None,
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

pub struct Route {
    pub spec: RouteSpec,
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
}

enum Backend {
    /// Its VM; none once stopped ([`Route::stop`]).
    Shared(Mutex<Option<Arc<Vm>>>),
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
    keep: Duration,
    uses: Option<u32>,
    lifetime: Option<Duration>,
    /// Wakes the sweep ([`sweep`]): a clone kept, or the route gone.
    changed: Arc<Notify>,
}

impl Kept {
    /// Whether `reused` served its last connection: its `uses`, or its lifetime, at
    /// `now`.
    fn spent(&self, reused: &Reused, now: Instant) -> bool {
        self.uses.is_some_and(|uses| reused.uses >= uses)
            || self
                .lifetime
                .is_some_and(|lifetime| now >= reused.clone.restored + lifetime)
    }

    /// Keeps `reused` for the next connection, or gives its clone back, to be ended,
    /// when it served its last.
    fn put(&self, reused: Reused) -> Option<Cloned> {
        let now = Instant::now();
        if self.spent(&reused, now) {
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

    /// Takes out the clones kept for `keep`, or past their lifetime, at `now`; and when
    /// the next of the others is.
    fn expire(&self, now: Instant) -> (Vec<Reused>, Option<Instant>) {
        let mut idle = self.idle.lock().unwrap();
        let ends = |(reused, since): &(Reused, Instant)| {
            let kept = *since + self.keep;
            match self.lifetime {
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
/// is killed; a VM that does not answer is killed and, below `warm`, replaced.
struct SharedPool {
    cloner: Arc<Cloner>,
    port: u32,
    /// `warm`, `vms`, `conns`.
    min: usize,
    max: usize,
    conns: usize,
    keep: Duration,
    /// How long a connection may wait for a VM.
    wait: Duration,
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
    /// No VM is started to make up `warm` again before then, after a failure.
    retry: Option<Instant>,
}

struct Member {
    id: u64,
    clone: Arc<Cloned>,
    /// Its connections open.
    conns: usize,
    /// Since when it serves none.
    since: Instant,
}

impl Members {
    /// The VM holding the fewest connections, the one after the last seat taken first
    /// among them.
    fn least(&self) -> Option<usize> {
        let n = self.vms.len();
        let start = self.turn % n.max(1);
        (0..n)
            .map(|i| (start + i) % n)
            .min_by_key(|&i| self.vms[i].conns)
    }

    /// A seat on the VM at `at`: its connections counted one more.
    fn seat(&mut self, at: usize) -> (u64, Arc<Cloned>) {
        self.turn = at + 1;
        let member = &mut self.vms[at];
        member.conns += 1;
        (member.id, member.clone.clone())
    }

    /// The VM serving no connection the longest, beyond `min`.
    fn idlest(&self, min: usize) -> Option<usize> {
        if self.vms.len() <= min {
            return None;
        }
        (0..self.vms.len())
            .filter(|&i| self.vms[i].conns == 0)
            .min_by_key(|&i| self.vms[i].since)
    }
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
        let changed = {
            let mut members = self.pool.members.lock().unwrap();
            let (shrinks, waiting) = (members.vms.len() > self.pool.min, members.waiting > 0);
            match members.vms.iter_mut().find(|vm| vm.id == self.id) {
                Some(member) => {
                    member.conns -= 1;
                    if member.conns == 0 {
                        member.since = Instant::now();
                    }
                    // one more VM may idle out, a waiting connection take the room
                    (shrinks && member.conns == 0) || (waiting && member.conns < self.pool.conns)
                }
                None => false,
            }
        };
        if changed {
            self.pool.changed.notify_waiters();
        }
    }
}

impl SharedPool {
    /// Starts a VM in the background, unless the pool holds `vms` with those starting.
    fn grow(self: &Arc<Self>, members: &mut Members, wait: Duration) -> bool {
        if members.vms.len() + members.starting >= self.max {
            return false;
        }
        members.starting += 1;
        tokio::spawn(self.clone().start(wait));
        true
    }

    /// A VM restored, its guest accepting, into the pool: room for it waited for
    /// within `wait`.
    async fn start(self: Arc<Self>, wait: Duration) {
        let started = async {
            let slot = self.capacity.take(self.need, wait).await?;
            let clone = self.cloner.restore(slot).await?;
            drop(clone.vm.dial_ready(self.port, RESTORE_TIMEOUT).await?);
            Ok::<_, BoxError>(clone)
        }
        .await;
        {
            let mut members = self.members.lock().unwrap();
            members.starting -= 1;
            match started {
                Ok(clone) => {
                    let id = members.next_id;
                    members.next_id += 1;
                    members.vms.push(Member {
                        id,
                        clone: Arc::new(clone),
                        conns: 0,
                        since: Instant::now(),
                    });
                }
                Err(e) => {
                    eprintln!("route {}: pool VM: {e}", self.cloner.hosts.current().label);
                    members.failures += 1;
                    members.failed = e.to_string();
                    members.retry = Some(Instant::now() + POOL_RETRY);
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
                let mut members = self.members.lock().unwrap();
                if members.failures > 0 {
                    return Err(members.failed.clone().into());
                }
                if members.vms.len() >= self.min {
                    return Ok(());
                }
                while members.vms.len() + members.starting < self.min {
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
        let deadline = Instant::now() + self.wait;
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
                let mut members = self.members.lock().unwrap();
                let least = members.least();
                let at = match least.filter(|&at| members.vms[at].conns < self.conns) {
                    Some(at) => Some(at),
                    None => {
                        if least.is_none() && members.failures > failures {
                            return Err(format!("no VM of the pool: {}", members.failed).into());
                        }
                        if !waiter.counted {
                            waiter.counted = true;
                            members.waiting += 1;
                        }
                        if members.waiting > members.starting * self.conns {
                            self.grow(&mut members, self.wait);
                        }
                        match least {
                            Some(at) if members.starting == 0 || late => Some(at),
                            None if late => {
                                return Err(format!(
                                    "no VM of the pool within {}ms",
                                    self.wait.as_millis()
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
                    if members.starting == 0 && members.vms.iter().all(|vm| vm.conns >= self.conns)
                    {
                        self.grow(&mut members, self.wait);
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
                    drop(seat);
                    self.remove(id, &e);
                }
            }
        }
    }

    /// Kills the VM `id`, which did not answer: the next connection goes to another.
    fn remove(&self, id: u64, e: &BoxError) {
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.vms.iter().position(|vm| vm.id == id);
            at.map(|at| members.vms.remove(at))
        };
        if gone.is_some() {
            eprintln!(
                "route {}: a pool VM failed, killed: {e}",
                self.cloner.hosts.current().label
            );
            self.changed.notify_waiters();
        }
    }

    /// Takes out the VMs beyond `warm` serving none for `keep` at `now`, and starts
    /// those missing to `warm`; and when to look again.
    fn tend(self: &Arc<Self>, now: Instant) -> (Vec<Member>, Option<Instant>) {
        let mut members = self.members.lock().unwrap();
        let mut gone = Vec::new();
        while let Some(at) = members.idlest(self.min)
            && members.vms[at].since + self.keep <= now
        {
            gone.push(members.vms.remove(at));
        }
        let mut next = members
            .idlest(self.min)
            .map(|at| members.vms[at].since + self.keep);
        if members.vms.len() + members.starting < self.min {
            match members.retry {
                Some(retry) if retry > now => {
                    next = Some(next.map_or(retry, |next| next.min(retry)));
                }
                _ => {
                    while members.vms.len() + members.starting < self.min {
                        self.grow(&mut members, self.wait);
                    }
                }
            }
        }
        (gone, next)
    }
}

impl Pool for SharedPool {
    fn evict(&self) -> bool {
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.idlest(self.min);
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

/// Restores a cloned route's clones from its snapshot, and follows their
/// output.
struct Cloner {
    snapshot: Snapshot,
    /// Where the clones are started.
    place: Arc<Place>,
    /// Numbers the clones.
    next: AtomicU64,
    sink: Arc<Sink>,
    hosts: Arc<Hosts>,
    /// Whether the guest sends its output: its template's did.
    output: bool,
    /// The route's egress, if it has any.
    egress: Option<Arc<Egress>>,
}

impl Cloner {
    /// A VM restored from the snapshot, told it was (`wire::CONTROL_PORT`): its random
    /// state reseeded from the host's, its restore hook run; its output followed from
    /// then on.
    async fn restore(&self, slot: Slot) -> Result<Cloned, BoxError> {
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
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(std::io::Error::other)?,
                ),
            },
        };
        // the stream kept whole: Firecracker takes a write half shut down as the end
        // of the whole stream (the answer lost)
        notify(&mut stream, &notice).await?;
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
        Ok(Cloned {
            vm,
            output,
            restored: Instant::now(),
        })
    }
}

impl Route {
    /// Boots the route's VM (shared), or boots a template VM, snapshots it once its
    /// guest listens and kills it (the cloned modes; a pool's `warm` VMs restored then);
    /// the route is the `n`th started of `places`, `hosts` name it in the log. Its VMs
    /// take their room from `capacity`; their output goes to `sink`.
    pub async fn start(
        spec: RouteSpec,
        hosts: &[String],
        places: &Places,
        n: u64,
        capacity: Arc<Capacity>,
        sink: Arc<Sink>,
        workers: Arc<Workers>,
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
        let vm = Vm::boot(&spec.vm, &place, role, slot, named).await?;
        let mut output = ready(&vm, spec.port, &sink, &hosts, name).await?;
        let booted = started.elapsed();
        let mut taken = None;
        let backend = match spec.mode {
            Mode::Shared => {
                println!(
                    "route {label}: VM booted, guest listening after {}ms",
                    booted.as_millis()
                );
                Backend::Shared(Mutex::new(Some(Arc::new(vm))))
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
                let size = tokio::fs::metadata(&snapshot.memory_on_host).await?.len();
                taken = Some(Taken {
                    memory: size,
                    booted,
                    taken: started.elapsed(),
                    at: std::time::SystemTime::now(),
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
                        min: spec.warm,
                        max: spec.vms.unwrap_or(spec.warm),
                        conns: spec.conns,
                        keep: spec.keep,
                        wait: spec.wait,
                        capacity: capacity.clone(),
                        need,
                        members: Mutex::default(),
                        changed: changed.clone(),
                    });
                    pool.fill(START_WAIT)
                        .await
                        .map_err(|e| format!("route {label}: pool VM: {e}"))?;
                    println!(
                        "route {label}: {} pool VM(s) up after {}ms",
                        spec.warm,
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
                            keep: spec.keep,
                            uses: spec.uses,
                            lifetime: spec.lifetime,
                            changed: changed.clone(),
                        });
                        let spare: Arc<dyn Pool> = kept.clone();
                        capacity.register(need.route, Arc::downgrade(&spare));
                        tokio::spawn(sweep(Arc::downgrade(&kept), changed));
                        kept
                    });
                    let (refill, warm) = mpsc::channel(spec.warm.max(1));
                    let warm = Arc::new(Warm {
                        clones: Mutex::new(warm),
                    });
                    let spare: Arc<dyn Pool> = warm.clone();
                    capacity.register(need.route, Arc::downgrade(&spare));
                    // restores run in parallel: one at a time caps the clone rate at
                    // about 1 / restore time
                    for _ in 0..spec.warm.min(REFILL_WORKERS) {
                        tokio::spawn(keep_warm(
                            cloner.clone(),
                            spec.port,
                            refill.clone(),
                            capacity.clone(),
                            need,
                        ));
                    }
                    Backend::Cloned { cloner, warm, kept }
                }
            }
        };
        Ok(Route {
            spec,
            hosts,
            _place: place,
            capacity,
            need,
            backend,
            clients: Arc::default(),
            open: Arc::default(),
            taken,
        })
    }

    /// What its VMs are now.
    pub fn vms(&self) -> Vms {
        let mut vms = Vms {
            alive: self.capacity.alive(self.need.route),
            connections: self.open.load(Ordering::Relaxed),
            ..Vms::default()
        };
        match &self.backend {
            Backend::Shared(vm) => vms.shared = vm.lock().unwrap().is_some(),
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
            }
        }
        vms
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
        let disk = self.spec.vm.disk.as_ref()?;
        (!self.stopped()).then_some(disk.file.as_path())
    }

    /// Whether its shared VM was stopped: its connections fail from then on.
    pub fn stopped(&self) -> bool {
        matches!(&self.backend, Backend::Shared(vm) if vm.lock().unwrap().is_none())
    }

    /// Stops the VM of a route with a disk, for good: what the process wrote is all on the
    /// disk, as far as it ended within [`disk::STOP_GRACE`], and its open connections end.
    /// It gives the disk up as it is gone (the lock tells, [`Disk::ready`]). Any other route's
    /// VMs end with their last connection.
    pub async fn stop(&self) {
        let Backend::Shared(vm) = &self.backend else {
            return;
        };
        if self.spec.vm.disk.is_none() {
            return;
        }
        let vm = vm.lock().unwrap().take();
        if let Some(vm) = vm
            && let Err(e) = vm.stop(disk::STOP_GRACE).await
        {
            eprintln!("route {}: its VM's stop: {e}; killed", self.label());
        }
    }

    /// A stream to a guest for one connection of `client` (its IP address; empty when
    /// unknown, which `per-client` does not bound): failed at once when the client
    /// holds its share of the route already.
    pub async fn open(&self, client: &str) -> Result<Lease, BoxError> {
        let share = match self.spec.per_client {
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
            Backend::Shared(vm) => {
                let vm = vm.lock().unwrap().clone();
                let vm = vm.ok_or("its VM was stopped: the route is restarting")?;
                Ok((vm.dial(self.spec.port).await?, Held::Shared))
            }
            Backend::Pool(pool) => {
                let (stream, seat) = pool.open().await?;
                Ok((stream, Held::Pooled(seat)))
            }
            Backend::Cloned { cloner, warm, kept } => {
                if let Some(kept) = kept
                    && let Some((reused, stream)) = kept.take(self.spec.port).await
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
                if trace {
                    eprintln!(
                        "route {}: {} clone, restored in {}us, guest accepting after {}us",
                        self.label(),
                        if warm { "warm" } else { "fresh" },
                        restored.as_micros(),
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
    let output = followed(
        || vm.dial(wire::LOG_PORT),
        vm.dial_ready(port, BOOT_TIMEOUT),
        |stream| output::follow(stream, sink.clone(), hosts.clone(), name.to_string()),
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
    drop(listening.await?);
    Ok(output)
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

#[cfg(test)]
mod tests {
    use tokio::io::DuplexStream;

    use super::*;

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
            |stream| output::follow(stream, sink.clone(), hosts.clone(), "vm-0".to_string());
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
