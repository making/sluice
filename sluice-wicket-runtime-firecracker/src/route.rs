//! A route: the VMs serving one target (its hosts), and how a connection gets one of
//! them. The VMs' output is followed from their boot (a clone's from its restore) and
//! named after the route's hosts and the VM (`output`): `vm-<index>` a booted VM,
//! `template` a cloned route's template, `vm-<n>` its clones in the order they were
//! restored. The live access log's lines name the instance too: a pool's VM holds the lowest
//! index free among its route's from its start on, and the one in its place the same; another
//! clone holds it from its guest accepting on; the template, alone as it lives, is 0.
//!
//! A pool's VMs ([`VmPool`]), booted (`mode=boot`) or cloned (`mode=pool`), are watched
//! ([`watch`]) and, with a liveness check, probed ([`lived`]), each going down replaced by the
//! pool.
//!
//! What says how many VMs and how long ([`Bounds`]) a running route takes as it is, its VMs and
//! their snapshot kept ([`Route::adjust`]): VMs are started for more, and those past a lower
//! `instances` (the highest indices, so what stays is what `cf logs` lists) take no connection any
//! more, and go as `keep-ms` has them go, once their connections ended. What serves none of a VM
//! per connection goes at once: it has no connections to finish, and is in the way of the next.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use sluice_wicket_console::env::Env;
use sluice_wicket_console::status::{self, Shortage, Status};
use sluice_wicket_tunnel::flume::{Index, Indices};
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::BoxError;
use crate::capacity::{Capacity, Full, Need, Pool, Slot};
use crate::disk::{self, Disk};
use crate::egress::Egress;
use crate::health::{self, Check, Liveness, Readiness};
use crate::keeper::Role;
use crate::output::{self, Ended, Hosts, Output, Sink};
use crate::usage::Meter;
use crate::vm::{self, Place, Places, Snapshot, Spec, Vm};
use crate::wire::{self, Rule};

/// Set, the wicket logs each clone's restore time.
pub const TRACE: &str = "SLUICE_WICKET_RUNTIME_FIRECRACKER_TRACE";

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

/// How long a route's start may wait for room for its VM (a booted one, or the
/// template): the room another route's start holds frees within it.
const START_WAIT: Duration = Duration::from_secs(60);
// a VM stopping holds its room until it is gone
const _: () = assert!(START_WAIT.as_secs() > disk::STOP_MOST.as_secs());

/// Default of `wait-ms`: how long a connection may wait for room for its VM.
const DEFAULT_WAIT: Duration = Duration::from_secs(10);

/// Default of `keep-ms`: how long a VM serving no connection is kept (reuse, pool).
const DEFAULT_KEEP: Duration = Duration::from_secs(60);

/// Default of `concurrency`: the connections every VM of a pool holds before it grows.
const DEFAULT_CONCURRENCY: usize = 16;

/// How long a pool waits before it starts again a VM it found no room for: no VM failing, so no
/// [`Backoff`].
const POOL_RETRY: Duration = Duration::from_secs(1);

/// How long after a look that found it refusing a VM's process is looked at again.
const LOOK_AGAIN: Duration = Duration::from_millis(100);

/// The wait before the next VM in the place of one that went down, past the first: doubled up to
/// [`RESTART_WAIT_MAX`].
const RESTART_WAIT: Duration = Duration::from_secs(1);

const RESTART_WAIT_MAX: Duration = Duration::from_secs(60);

/// A VM up that long going down has the next one started at once, as the first one was.
const RESTART_STABLE: Duration = Duration::from_secs(60);

/// Names the routes for the budget, over every route the wicket ever started.
static NEXT_ROUTE: AtomicU64 = AtomicU64::new(0);

/// The budget's name of what is no route but takes VMs of it: a build (`stage`).
pub fn next_route() -> u64 {
    NEXT_ROUTE.fetch_add(1, Ordering::Relaxed)
}

/// How a connection of the route gets its VM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// `instances` VMs (1 by default), each booted, none restored, keep serving many connections at
    /// once: a pool but for how a VM starts, for a process that does not survive a restore. A
    /// connection takes the VM holding the fewest; none is grown past `instances`.
    Boot,
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
    /// at once: `warm` of them kept, grown up to `instances` with the
    /// connections (`concurrency` each), a VM beyond `warm` serving none killed after
    /// `keep-ms`.
    Pool,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Boot => "boot",
            Mode::PerConnection => "per-connection",
            Mode::Reuse => "reuse",
            Mode::Pool => "pool",
        }
    }

    /// Whether the route's VMs are cloned from a snapshot of a template.
    pub fn cloned(self) -> bool {
        self != Mode::Boot
    }
}

/// The modes cloning their VMs.
const CLONED: &[Mode] = &[Mode::PerConnection, Mode::Reuse, Mode::Pool];

/// The options of some modes alone, and those modes.
const MODE_OPTIONS: &[(&str, &[Mode])] = &[
    ("warm", CLONED),
    ("prime", CLONED),
    ("wait-ms", CLONED),
    ("keep-ms", &[Mode::Reuse, Mode::Pool]),
    ("uses", &[Mode::Reuse]),
    ("lifetime-ms", &[Mode::Reuse]),
    ("concurrency", &[Mode::Pool]),
    ("liveness-interval-ms", &[Mode::Boot, Mode::Pool]),
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

/// What says how many VMs a route keeps, how long and when one serves, not what they are: a running
/// route takes a change of it ([`Route::adjust`]).
#[derive(Clone, Debug, PartialEq)]
pub struct Bounds {
    /// How long a cold-booted guest may take to listen on its port.
    pub boot: Duration,
    /// Per-connection and reuse modes: clones kept restored ahead of the connections; pool mode:
    /// the pool's VMs kept serving no connection (default `instances`); boot mode: `instances`.
    pub warm: usize,
    /// The route's VMs alive at once (warm, in use, kept, the template) at most; None = the
    /// global limits alone (pool mode needs it: its size; boot mode has it, from 1).
    pub instances: Option<usize>,
    /// Cloned modes: how long a connection may wait for room for its VM.
    pub wait: Duration,
    /// Reuse and pool modes: how long a VM serving no connection is kept (a pool's
    /// beyond its `warm`); boot mode: none, past `instances` it goes with its last connection.
    pub keep: Duration,
    /// Reuse mode: the connections a clone serves at most; None = no bound.
    pub uses: Option<u32>,
    /// Reuse mode: how long a clone serves from its restore at most; None = no bound.
    pub lifetime: Option<Duration>,
    /// Pool mode: the connections every VM of the pool holds before it grows by one.
    pub concurrency: usize,
    /// The route's connections of one client address open at once at most (in
    /// per-connection and reuse modes, its VMs); None = no bound of its own.
    pub per_client: Option<usize>,
    /// How long a connection may relay no byte, either way, before it is failed (and
    /// its VM killed); None = no bound of its own.
    pub idle: Option<Duration>,
    /// Boot and pool modes: what tells a VM ready to take connections.
    pub readiness: Readiness,
    /// What tells a VM up at its start (a booted VM's, a template's) and alive after it (a booted
    /// VM's, a pool's).
    pub liveness: Liveness,
}

impl Bounds {
    /// A pool's `instances`.
    fn pool_size(&self) -> usize {
        self.instances.unwrap_or(self.warm)
    }
}

/// The keys of the options [`Bounds`] holds.
const BOUNDS: &[&str] = &[
    "boot-ms",
    "warm",
    "instances",
    "wait-ms",
    "keep-ms",
    "uses",
    "lifetime-ms",
    "concurrency",
    "per-client",
    "idle-ms",
    "readiness",
    "readiness-endpoint",
    "readiness-timeout-ms",
    "readiness-interval-ms",
    "liveness",
    "liveness-endpoint",
    "liveness-timeout-ms",
    "liveness-interval-ms",
];

impl RouteSpec {
    /// Parses `host=<rootfs>[?mode=boot|per-connection|reuse|pool&vcpus=N&memory-mib=N&cpu=N&port=N&boot-ms=N&warm=N&prime=N&instances=N&wait-ms=N&keep-ms=N&uses=N&lifetime-ms=N&concurrency=N&per-client=N&idle-ms=N&readiness=process|port|http&readiness-endpoint=<path>&readiness-timeout-ms=N&readiness-interval-ms=N&liveness=port|http&liveness-endpoint=<path>&liveness-timeout-ms=N&liveness-interval-ms=N&egress=<rule>,...&disk=<path>&disk-mib=N]`.
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
                memory_mib: 128,
                cpu: None,
                egress: Vec::new(),
                env: Env::default(),
                disk: None,
                stage: false,
            },
            mode: Mode::Boot,
            port: 8080,
            prime: 0,
            app: None,
            bounds: Bounds {
                boot: DEFAULT_BOOT,
                warm: 0,
                instances: None,
                wait: DEFAULT_WAIT,
                keep: DEFAULT_KEEP,
                uses: None,
                lifetime: None,
                concurrency: DEFAULT_CONCURRENCY,
                per_client: None,
                idle: None,
                readiness: Readiness::default(),
                liveness: Liveness::default(),
            },
        };
        let mut given = Vec::new();
        let (mut mount, mut disk_mib) = (None, None);
        let (mut endpoint, mut alive_at) = (None, None);
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
                    spec.mode = [Mode::Boot, Mode::PerConnection, Mode::Reuse, Mode::Pool]
                        .into_iter()
                        .find(|mode| mode.name() == value)
                        .ok_or_else(|| format!("{named}: unknown mode {value}"))?;
                }
                "vcpus" => spec.vm.vcpus = number()?,
                "memory-mib" => spec.vm.memory_mib = number()?,
                "port" => spec.port = number()?,
                "boot-ms" => spec.bounds.boot = millis()?,
                "warm" => spec.bounds.warm = number()? as usize,
                "prime" => spec.prime = number()?,
                "instances" => spec.bounds.instances = Some(number()? as usize),
                "wait-ms" => spec.bounds.wait = Duration::from_millis(u64::from(number()?)),
                "keep-ms" => spec.bounds.keep = millis()?,
                "uses" => spec.bounds.uses = Some(positive()?),
                "lifetime-ms" => spec.bounds.lifetime = Some(millis()?),
                "concurrency" => spec.bounds.concurrency = positive()? as usize,
                "cpu" => spec.vm.cpu = Some(positive()?),
                "per-client" => spec.bounds.per_client = Some(positive()? as usize),
                "idle-ms" => spec.bounds.idle = Some(millis()?),
                "readiness" => {
                    spec.bounds.readiness.check =
                        Check::named(value).map_err(|e| format!("{named}: {e}"))?;
                }
                "readiness-endpoint" | "liveness-endpoint" => {
                    let path = health::endpoint(key, value).map_err(|e| format!("{named}: {e}"))?;
                    match key {
                        "readiness-endpoint" => endpoint = Some(path),
                        _ => alive_at = Some(path),
                    }
                }
                "readiness-timeout-ms" => spec.bounds.readiness.timeout = millis()?,
                "readiness-interval-ms" => spec.bounds.readiness.interval = millis()?,
                "liveness" => {
                    spec.bounds.liveness.endpoint =
                        Liveness::named(value).map_err(|e| format!("{named}: {e}"))?;
                }
                "liveness-timeout-ms" => spec.bounds.liveness.timeout = millis()?,
                "liveness-interval-ms" => spec.bounds.liveness.interval = millis()?,
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
            (Some(_), _) if spec.mode != Mode::Boot => {
                return Err(format!(
                    "{named}: disk needs mode=boot: one VM writes a disk, and a clone's \
                     snapshot holds its guest's page cache of it"
                ));
            }
            (Some(_), _) if spec.bounds.instances.is_some_and(|vms| vms > 1) => {
                return Err(format!(
                    "{named}: disk needs instances=1: one VM writes a disk"
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
        spec.bounds.readiness = Self::readiness(named, &spec, &given, endpoint)?;
        Self::liveness(named, &mut spec.bounds.liveness, &given, alive_at)?;
        if spec.mode == Mode::Boot {
            let vms = spec.bounds.instances.unwrap_or(1);
            spec.bounds.instances = Some(vms);
            spec.bounds.warm = vms;
            spec.bounds.keep = Duration::ZERO;
        }
        if spec.mode == Mode::Pool {
            let Some(vms) = spec.bounds.instances else {
                return Err(format!(
                    "{named}: mode=pool needs instances (its VMs at most)"
                ));
            };
            if !given.contains(&"warm") {
                spec.bounds.warm = vms;
            }
        }
        if let Some(vms) = spec.bounds.instances
            && (vms == 0 || vms < spec.bounds.warm)
        {
            return Err(format!(
                "{named}: instances must be at least 1 and at least warm"
            ));
        }
        Ok(spec)
    }

    /// The readiness check of `spec` as the options `given` set it, `endpoint` that of
    /// `readiness-endpoint`: what does not apply refused. A VM of a connection's own takes it as it
    /// comes: only VMs serving connections ahead of them are probed, a pool's or booted ones.
    fn readiness(
        named: &str,
        spec: &RouteSpec,
        given: &[&str],
        endpoint: Option<Arc<str>>,
    ) -> Result<Readiness, String> {
        let mut readiness = spec.bounds.readiness.clone();
        let check = readiness.check.name();
        let needs = |key: &str, what: &str| -> Result<(), String> {
            match given.contains(&key) {
                true => Err(format!("{named}: {key} needs {what}")),
                false => Ok(()),
            }
        };
        match &mut readiness.check {
            Check::Process => {
                needs("readiness-endpoint", "readiness=http")?;
                needs("readiness-timeout-ms", "readiness=port or readiness=http")?;
                needs("readiness-interval-ms", "readiness=port or readiness=http")?;
                return Ok(readiness);
            }
            Check::Port => needs("readiness-endpoint", "readiness=http")?,
            Check::Http(asked) => {
                if let Some(endpoint) = endpoint {
                    *asked = endpoint;
                }
            }
        }
        if matches!(spec.mode, Mode::PerConnection | Mode::Reuse) {
            return Err(format!(
                "{named}: readiness={check} needs mode=boot or mode=pool: a VM per connection \
                 takes its connection as it comes, none serves ahead of them to be probed"
            ));
        }
        Ok(readiness)
    }

    /// The liveness check `liveness` as the options `given` set it, `endpoint` that of
    /// `liveness-endpoint`: what does not apply refused. `port` is the watch's, with no probe of
    /// its own to time or space; a VM of a connection's own is not probed past its template
    /// (`liveness-interval-ms`, [`MODE_OPTIONS`]).
    fn liveness(
        named: &str,
        liveness: &mut Liveness,
        given: &[&str],
        endpoint: Option<Arc<str>>,
    ) -> Result<(), String> {
        let Some(asked) = &mut liveness.endpoint else {
            let probing = [
                "liveness-endpoint",
                "liveness-timeout-ms",
                "liveness-interval-ms",
            ];
            return match probing.into_iter().find(|key| given.contains(key)) {
                Some(key) => Err(format!(
                    "{named}: {key} needs liveness=http: port is the watch's, a VM down once its \
                     process ended or a connection failed and its port refuses"
                )),
                None => Ok(()),
            };
        };
        if let Some(endpoint) = endpoint {
            *asked = endpoint;
        }
        Ok(())
    }

    /// What the route advertises as its weight: its `instances` (a booted route's VMs, a pool's or
    /// a cloned route's most), by which the server spreads its host over the wickets serving it;
    /// none without.
    pub fn weight(&self) -> u32 {
        let instances = self.bounds.instances;
        instances.map_or(0, |vms| u32::try_from(vms).unwrap_or(u32::MAX))
    }

    /// Whether `other` asks for the same VMs, its host and its [`Bounds`] aside.
    pub fn same_vms(&self, other: &RouteSpec) -> bool {
        self.vm == other.vm
            && self.mode == other.mode
            && self.port == other.port
            && self.prime == other.prime
            && self.app == other.app
    }

    /// The VMs the route keeps for good once started: its booted VMs, a pool's `warm`.
    pub fn resident(&self) -> usize {
        match self.mode {
            Mode::Boot | Mode::Pool => self.bounds.warm,
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
        if self.mode != Mode::Boot {
            options.push(("mode", self.mode.name().to_string()));
        }
        let bounds = &self.bounds;
        // boot mode: what `instances` makes them, nobody's to set
        let (warm, vms, keep) = match self.mode {
            Mode::Boot => (bounds.warm, 1, bounds.keep),
            Mode::Pool => (bounds.instances.unwrap_or(0), 0, DEFAULT_KEEP),
            _ => (0, 0, DEFAULT_KEEP),
        };
        let millis = |duration: Option<Duration>| duration.map_or(0, |d| d.as_millis() as u32);
        for (key, value, default) in [
            ("vcpus", self.vm.vcpus, 1),
            ("memory-mib", self.vm.memory_mib, 128),
            ("cpu", self.vm.cpu.unwrap_or(0), 0),
            ("port", self.port, 8080),
            (
                "boot-ms",
                bounds.boot.as_millis() as u32,
                DEFAULT_BOOT.as_millis() as u32,
            ),
            ("warm", bounds.warm as u32, warm as u32),
            ("prime", self.prime, 0),
            (
                "instances",
                bounds.instances.unwrap_or(0) as u32,
                vms as u32,
            ),
            (
                "wait-ms",
                bounds.wait.as_millis() as u32,
                DEFAULT_WAIT.as_millis() as u32,
            ),
            (
                "keep-ms",
                bounds.keep.as_millis() as u32,
                keep.as_millis() as u32,
            ),
            ("uses", bounds.uses.unwrap_or(0), 0),
            ("lifetime-ms", millis(bounds.lifetime), 0),
            (
                "concurrency",
                bounds.concurrency as u32,
                DEFAULT_CONCURRENCY as u32,
            ),
            ("per-client", bounds.per_client.unwrap_or(0) as u32, 0),
            ("idle-ms", millis(bounds.idle), 0),
        ] {
            if value != default {
                options.push((key, value.to_string()));
            }
        }
        let (readiness, liveness) = (&bounds.readiness, &bounds.liveness);
        let checks = [
            (
                ["readiness", "readiness-endpoint"],
                ["readiness-timeout-ms", "readiness-interval-ms"],
                Some(readiness.check.clone()).filter(|check| *check != Check::Process),
                (readiness.timeout, readiness.interval),
            ),
            (
                ["liveness", "liveness-endpoint"],
                ["liveness-timeout-ms", "liveness-interval-ms"],
                liveness.check(),
                (liveness.timeout, liveness.interval),
            ),
        ];
        for ([name, path], [timeout, interval], check, times) in checks {
            if let Some(check) = check {
                options.push((name, check.name().to_string()));
                if let Check::Http(endpoint) = check
                    && &*endpoint != health::DEFAULT_ENDPOINT
                {
                    options.push((path, endpoint.to_string()));
                }
            }
            for (key, value, default) in [
                (timeout, times.0, health::DEFAULT_TIMEOUT),
                (interval, times.1, health::DEFAULT_INTERVAL),
            ] {
                if value != default {
                    options.push((key, value.as_millis().to_string()));
                }
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
    /// Per-connection and reuse modes: clones restored ahead of the connections.
    pub warm: usize,
    /// Reuse mode: clones kept for the next connection.
    pub kept: usize,
    /// Boot and pool modes: the pool's VMs serving, and those being started.
    pub pooled: usize,
    pub starting: usize,
    /// Its connections open.
    pub connections: u64,
    /// Boot and pool modes: VMs up in the place of one that went down.
    pub restarts: u64,
    /// Boot and pool modes: the VMs taking connections, their readiness check passed.
    pub ready: Option<usize>,
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
    /// Per-connection mode: the connection's own clone, killed when it ends.
    Own(Instance),
    /// Reuse mode: the connection's clone, kept for the next connection.
    Reused(Reused, Arc<Kept>),
    /// Boot and pool modes: the connection's seat on a VM of the pool.
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

/// A VM started for its route, killed when dropped (on a disk, stopped first; a clone's
/// [`Instance::end`] once its output came), and its output followed.
pub struct Instance {
    /// Its instance index, held by what comes in its place too: given back before the VM's room,
    /// so an index stays below the route's `instances`.
    index: Arc<Index>,
    vm: Vm,
    output: Option<Output>,
    /// When its guest came to accept.
    up: Instant,
    /// The same, for a listing.
    since: SystemTime,
}

impl Instance {
    /// Kills the VM once the output its processes wrote so far reached the wicket,
    /// within [`END_SYNC_TIMEOUT`]: a process's last lines come after its answer.
    pub async fn end(mut self) {
        if let Some(output) = &mut self.output {
            output.sync(END_SYNC_TIMEOUT).await;
        }
    }
}

/// Tells the advertisement that a route's readiness ([`Route::ready`]) may have moved: its streams
/// look again.
pub type ReadyChanged = Arc<dyn Fn() + Send + Sync>;

/// Where a route starts, and what it is given to.
pub struct Starting {
    pub places: Arc<Places>,
    /// The route is the `n`th started of `places`.
    pub n: u64,
    /// Where its VMs take their room.
    pub capacity: Arc<Capacity>,
    /// Where its VMs' output goes.
    pub sink: Arc<Sink>,
    pub workers: Arc<Workers>,
    /// Where the stop of its VM on a disk goes, dropped ([`Vm`]).
    pub stops: TaskTracker,
    /// What the console shows: its VMs coming and going change it.
    pub status: watch::Sender<Status>,
    /// Where its readiness moving is told.
    pub ready_changed: ReadyChanged,
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
    /// Held by what stops its VMs ([`Route::stopping`]), for [`Route::stop`] to wait for.
    stops: Arc<tokio::sync::Mutex<()>>,
    /// Where its readiness moving is told.
    ready_changed: ReadyChanged,
}

enum Backend {
    /// Per-connection and reuse modes: a clone per connection at a time.
    Cloned {
        cloner: Arc<Cloner>,
        /// Restored ahead of the connections, refilled as they are taken.
        warm: Arc<Warm>,
        /// Reuse mode: the clones whose connection ended, kept for the next ones.
        kept: Option<Arc<Kept>>,
    },
    /// Boot and pool modes.
    Pool(Arc<VmPool>),
}

/// A route's [`Bounds`] as they are now, which a change of them finds its VMs by.
#[derive(Clone)]
struct Current(Arc<Mutex<Bounds>>);

impl Current {
    fn get(&self) -> Bounds {
        self.0.lock().unwrap().clone()
    }
}

/// Restores failing for anything but room, until one succeeds.
#[derive(Default)]
struct Failing {
    /// Why the last one failed, since when: none while they do not fail.
    down: Option<Shortage>,
    /// The wait after each one that fails.
    backoff: Backoff,
    /// No restore is tried before then ([`Warm::wanted`]); unset, one is at once.
    retry: Option<Instant>,
}

/// A route's warm clones, kept to `warm` ([`keep_warm`]), which give their room up to another
/// route's VM.
struct Warm {
    clones: Mutex<VecDeque<Instance>>,
    failed: Mutex<Failing>,
    bounds: Current,
    capacity: Arc<Capacity>,
    route: u64,
    /// Wakes what keeps them: a clone taken, their number changed, the route gone.
    changed: Arc<Notify>,
    /// Where the route's readiness moving is told ([`Warm::ready`]).
    ready_changed: ReadyChanged,
}

impl Warm {
    fn take(&self) -> Option<Instance> {
        let clone = self.clones.lock().unwrap().pop_front();
        if clone.is_some() {
            self.changed.notify_one();
            // the last one, restores failing: none left to serve
            if self.failed.lock().unwrap().down.is_some() {
                (self.ready_changed)();
            }
        }
        clone
    }

    /// Whether a connection of the route finds a clone: one restored ahead, else one of its own,
    /// unless the restores fail.
    fn ready(&self) -> bool {
        let failing = self.failed.lock().unwrap().down.is_some();
        !failing || !self.clones.lock().unwrap().is_empty()
    }

    /// The clones missing to `warm`.
    fn missing(&self) -> usize {
        let held = self.clones.lock().unwrap().len();
        self.bounds.get().warm.saturating_sub(held)
    }

    /// The clones to restore at `now`, and when to look again: those missing to `warm`; while
    /// restores fail, none until their wait is over, then one, whatever `warm` asks: at `warm` 0
    /// nothing else tells they stopped failing but the next connection.
    fn wanted(&self, now: Instant) -> (usize, Option<Instant>) {
        let missing = self.missing();
        let failed = self.failed.lock().unwrap();
        match failed.retry {
            _ if failed.down.is_none() => (missing, None),
            Some(retry) if retry > now => (0, Some(retry)),
            _ => (1, None),
        }
    }

    /// A clone's restore failed with `e`, as a connection's or its own: both tell the same, so the
    /// next one waits, however many came. Those under way are given up ([`keep_warm`]).
    fn failing(&self, e: &BoxError) {
        let began = {
            let mut failed = self.failed.lock().unwrap();
            let began = failed.down.is_none();
            Shortage::keep(&mut failed.down, format!("its restore failed: {e}"));
            failed.retry = Some(Instant::now() + failed.backoff.failed());
            began
        };
        self.changed.notify_one();
        if began {
            (self.ready_changed)();
        }
    }

    /// A clone's restore succeeded, as a connection's or its own: the wait is over and starts
    /// anew, those missing restored at once.
    fn restored(&self) {
        let mut failed = self.failed.lock().unwrap();
        if failed.down.is_some() {
            *failed = Failing::default();
            drop(failed);
            self.changed.notify_one();
            (self.ready_changed)();
        }
    }

    /// One more clone, unless `warm` or `instances` went down meanwhile: then it is killed.
    fn put(&self, clone: Instance) {
        self.restored();
        let mut clones = self.clones.lock().unwrap();
        if clones.len() < self.bounds.get().warm && !self.capacity.over(self.route) {
            clones.push_back(clone);
        }
    }

    /// Its bounds changed: kills what is past `warm`, and a failing restore is tried at once, the
    /// wait after it going on from where it was.
    fn adjust(&self) {
        let warm = self.bounds.get().warm;
        let past = {
            let mut clones = self.clones.lock().unwrap();
            let kept = warm.min(clones.len());
            clones.split_off(kept)
        };
        drop(past);
        self.failed.lock().unwrap().retry = None;
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
    clone: Instance,
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
                .is_some_and(|lifetime| now >= reused.clone.up + lifetime)
    }

    /// Keeps `reused` for the next connection, or gives its clone back, to be ended,
    /// when it served its last, or `instances` went down.
    fn put(&self, reused: Reused) -> Option<Instance> {
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
                Some(lifetime) => kept.min(reused.clone.up + lifetime),
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

/// A pool route's VMs, each serving many connections at once: clones of its snapshot, or booted
/// ([`Starter`]).
///
/// A connection takes the VM holding the fewest (in turn among them). A clone pool's while it holds
/// fewer than `concurrency`; past that it waits for a VM being started, one started for every
/// `concurrency` connections waiting, up to `instances`, and takes the VM holding the fewest once
/// none is starting (the pool at `instances`) or its wait ran out. The connection taking the last
/// room has the next VM started ahead. A VM beyond `warm` serving none for `keep` is killed, as is
/// one past `instances`, which takes no connection any more. A booted pool keeps `instances`, none
/// grown past it nor waited for: a boot is too slow to ask for one on demand, so a connection
/// finding none up fails.
///
/// A VM going down ([`watch`]), or a clone that does not answer, is killed, its connections alone
/// cut. One it keeps is replaced ([`Fallen`]) on the room it held and under its index, after a wait
/// as that one keeps going down ([`Backoff`]). A start failing is tried again so.
struct VmPool {
    starter: Arc<Starter>,
    port: u32,
    bounds: Current,
    capacity: Arc<Capacity>,
    need: Need,
    members: Mutex<Members>,
    /// A VM started or failed, its last connection ended: the waiting connections and
    /// the tending ([`tend`]) look again.
    changed: Arc<Notify>,
    /// A health check changed: each VM's readiness probe ([`probed`]) is made again at once, its
    /// liveness probe ([`lived`]) an interval of the new check after.
    probes: Arc<Notify>,
    status: watch::Sender<Status>,
    /// Where the route's readiness moving is told ([`VmPool::ready`]).
    ready_changed: ReadyChanged,
}

/// What starts a pool's VMs, in tasks of their own that end with the pool.
struct Starter {
    kind: Kind,
    port: u32,
    capacity: Arc<Capacity>,
    need: Need,
    indices: Arc<Indices>,
}

enum Kind {
    Clones(Arc<Cloner>),
    Boots(Box<Booter>),
}

impl Starter {
    fn boots(&self) -> bool {
        matches!(self.kind, Kind::Boots(_))
    }

    /// The hosts its route serves.
    fn hosts(&self) -> &Arc<Hosts> {
        match &self.kind {
            Kind::Clones(cloner) => &cloner.hosts,
            Kind::Boots(booter) => &booter.hosts,
        }
    }

    /// A VM of the index `index` holds, its guest accepting: on the room `held` holds, else room
    /// for it waited for within `wait`.
    async fn start(
        &self,
        index: Arc<Index>,
        held: Option<Arc<Slot>>,
        wait: Duration,
    ) -> Result<Instance, BoxError> {
        let slot = match &held {
            Some(held) => self.capacity.in_room_of(held, self.need),
            None => self.capacity.take(self.need, wait).await?,
        };
        match &self.kind {
            Kind::Clones(cloner) => {
                let restored = cloner.restore(slot, self.port, Some(index));
                Ok(restored.await?.0)
            }
            Kind::Boots(booter) => booter.boot(slot, self.port, index).await,
        }
    }
}

/// Boots a booted route's VMs, and follows their output.
struct Booter {
    vm: Spec,
    bounds: Current,
    place: Arc<Place>,
    /// Numbers the VMs' directories: one going down may be there still, its stop under way.
    next: AtomicU64,
    sink: Arc<Sink>,
    hosts: Arc<Hosts>,
    egress: Option<Arc<Egress>>,
    stops: TaskTracker,
}

impl Booter {
    /// A VM booted on `slot`, its guest listening on `port` and passing its liveness check
    /// ([`alive`]) within `boot-ms`, its output named
    /// after `index` from its boot on. On a disk, once no other VM is on it; what it wrote before
    /// it failed is on its disk all the same.
    async fn boot(&self, slot: Slot, port: u32, index: Arc<Index>) -> Result<Instance, BoxError> {
        let label = self.hosts.current().label.clone();
        if let Some(disk) = &self.vm.disk {
            disk.ready()
                .await
                .map_err(|e| format!("route {label}: {e}"))?;
        }
        let started = Instant::now();
        let name = format!("vm-{}", index.index);
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let vm = Vm::boot(
            &self.vm,
            &self.place,
            Role::Boot,
            &format!("vms/vm-{n}"),
            slot,
            self.egress.as_ref().map(|egress| (egress, name.clone())),
            &self.stops,
        )
        .await?;
        let Bounds { boot, liveness, .. } = self.bounds.get();
        let deadline = Instant::now() + boot;
        let followed = ready(&vm, port, boot, &self.sink, &self.hosts, &name, index.index).await;
        let followed = match followed {
            Ok(mut output) => {
                let host = self.hosts.current().key.clone();
                let within = (deadline, boot);
                let passed = alive(&vm, port, &liveness, &host, within, output.as_mut()).await;
                passed.map(|()| output)
            }
            Err(e) => Err(e),
        };
        let output = match followed {
            Ok(output) => output,
            Err(e) => {
                if self.vm.disk.is_some()
                    && let Err(stop) = vm.stop(disk::STOP_GRACE).await
                {
                    eprintln!("route {label}: its VM's stop: {stop}; killed");
                }
                return Err(e);
            }
        };
        let alive = match liveness.endpoint {
            Some(endpoint) => format!(", GET {endpoint} answered"),
            None => String::new(),
        };
        println!(
            "route {label}: {name} booted, guest listening{alive} after {}ms",
            started.elapsed().as_millis()
        );
        Ok(Instance {
            index,
            vm,
            output,
            up: Instant::now(),
            since: SystemTime::now(),
        })
    }
}

/// A VM a pool keeps gone down or not started, and starting again at the index it holds.
struct Fallen {
    index: Arc<Index>,
    /// Why, since when.
    down: Option<Shortage>,
    /// The wait before its next start, going on with the VM started.
    backoff: Backoff,
    /// No VM is started in its place before then.
    retry: Option<Instant>,
    /// The room of the VM that went down: none for a VM not started yet.
    held: Option<Arc<Slot>>,
    /// Its start is under way.
    starting: bool,
}

impl Fallen {
    /// Its start failed with `why`; `full`: for room, which is no VM failing.
    fn failed(&mut self, why: String, full: bool) {
        Shortage::keep(&mut self.down, why);
        let wait = match full {
            true => POOL_RETRY,
            false => self.backoff.failed(),
        };
        self.retry = Some(Instant::now() + wait);
        self.starting = false;
    }
}

#[derive(Default)]
struct Members {
    vms: Vec<Member>,
    /// VMs being started, those of [`Members::fallen`] among them.
    starting: usize,
    /// Those it keeps gone down or not started, by index.
    fallen: BTreeMap<usize, Fallen>,
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
    /// Counts the VMs up in the place of one that went down.
    restarts: u64,
    /// Why the last start failed for anything but room, until one succeeds: what a pool keeping
    /// none tells with no VM alive, and tries again with none alive.
    failing: Option<Shortage>,
    /// It was stopped for good ([`VmPool::halt`]), until told otherwise: it starts no VM.
    halted: bool,
    /// Tells the starts of before a halt, which it cancels.
    epoch: u64,
    cancel: CancellationToken,
}

struct Member {
    id: u64,
    /// Its instance index, what tells which VMs are past `instances`.
    index: usize,
    instance: Arc<Instance>,
    /// It takes connections: its readiness check passed last, or it was started for connections
    /// waiting, which take it unprobed.
    ready: bool,
    /// Why its last probe failed, as the log told it.
    unready: Option<String>,
    /// Its connections open.
    conns: usize,
    /// Since when it serves none.
    since: Instant,
    /// Has its watch look at it: a connection failed.
    suspects: mpsc::Sender<()>,
    /// Ends its probes ([`probed`]) as it leaves the pool.
    _probed: tokio_util::sync::DropGuard,
    /// The wait before the next VM in its place, should it go down.
    backoff: Backoff,
}

impl Members {
    /// The VMs it has, will have or waits to start again.
    fn kept(&self) -> usize {
        let waiting = self.fallen.values().filter(|fallen| !fallen.starting);
        self.vms.len() + self.starting + waiting.count()
    }

    /// The VMs it is to keep at `warm`: one to see a failure through where it keeps none, gone again
    /// after `keep`: nothing else tells it is over but the next connection.
    fn wanted(&self, warm: usize) -> usize {
        warm.max(usize::from(self.failing.is_some()))
    }

    /// The start of the VM of `index` failed with `e`, `warm` the VMs it keeps.
    fn failed(&mut self, e: BoxError, index: Arc<Index>, warm: usize) {
        self.failures += 1;
        self.failed = e.to_string();
        let why = format!("its start failed: {e}");
        self.full = e.downcast().ok().map(|full| *full);
        let full = self.full.is_some();
        if !full {
            Shortage::keep(&mut self.failing, why.clone());
        }
        let at = index.index;
        if !self.fallen.contains_key(&at) {
            if self.kept() >= self.wanted(warm) {
                return;
            }
            self.fallen.insert(
                at,
                Fallen {
                    index,
                    down: None,
                    backoff: Backoff::default(),
                    retry: None,
                    held: None,
                    starting: false,
                },
            );
        }
        let fallen = self.fallen.get_mut(&at).expect("just there");
        fallen.failed(why, full);
    }

    /// Its bounds changed: those it waits to start again are started at once, the wait after that
    /// going on from where it was; and the room it was short of is not what it was.
    fn bounds_changed(&mut self) {
        self.full = None;
        for fallen in self.fallen.values_mut() {
            fallen.retry = None;
        }
    }

    /// [`Route::down`] of a pool keeping `warm`: the lowest index's.
    fn short(&self, warm: usize) -> Option<Shortage> {
        if self.vms.len() < warm {
            let fallen = self.fallen.values().next()?;
            fallen.down.clone()
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

    /// The VM within `max` ready holding the fewest connections, the one after the last seat
    /// taken first among them.
    fn least(&self, max: usize) -> Option<usize> {
        let n = self.vms.len();
        let start = self.turn % n.max(1);
        let past = self.past(max);
        (0..n)
            .map(|i| (start + i) % n)
            .filter(|&i| self.vms[i].index < past && self.vms[i].ready)
            .min_by_key(|&i| self.vms[i].conns)
    }

    /// Why no VM within `max` takes a connection: none up, or none ready.
    fn none(&self, max: usize) -> &'static str {
        let past = self.past(max);
        match self.vms.iter().any(|vm| vm.index < past) {
            true => "none ready, its readiness check failing",
            false => "none up",
        }
    }

    /// A seat on the VM at `at`: its connections counted one more.
    fn seat(&mut self, at: usize) -> (u64, Arc<Instance>) {
        self.turn = at + 1;
        let member = &mut self.vms[at];
        member.conns += 1;
        (member.id, member.instance.clone())
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
    pool: &'a VmPool,
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
    pool: Arc<VmPool>,
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
                    (shrinks && member.conns == 0) || (waiting && member.conns < bounds.concurrency)
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

impl VmPool {
    /// How long a VM started in the background may wait for room: a booted one's is its route's
    /// to keep, not a connection's to wait for.
    fn room_wait(&self) -> Duration {
        match self.starter.boots() {
            true => START_WAIT,
            false => self.bounds.get().wait,
        }
    }

    /// Has the pool's VMs looked at, the console told, and the advertisement.
    fn told(&self) {
        self.changed.notify_waiters();
        self.status.send_modify(|status| status.revision += 1);
        (self.ready_changed)();
    }

    /// Whether a connection finds a VM: one within `instances` ready, or, keeping none (`warm`
    /// 0), one started for it, unless the starts fail or the pool is full. Stopped, none.
    fn ready(&self) -> bool {
        let bounds = self.bounds.get();
        let members = self.members.lock().unwrap();
        let max = bounds.pool_size();
        if members.halted {
            return false;
        }
        if members.least(max).is_some() {
            return true;
        }
        let made = !self.starter.boots() && bounds.warm == 0;
        made && members.failing.is_none() && members.kept() < max
    }

    /// The probe of the VM `id` found it ready, or why not: true while it is in the pool. A change
    /// is logged and told; a failure going on in other words, logged.
    fn probed(&self, id: u64, probed: Result<(), String>) -> bool {
        let label = self.starter_label();
        let moved = {
            let mut members = self.members.lock().unwrap();
            let Some(member) = members.vms.iter_mut().find(|vm| vm.id == id) else {
                return false;
            };
            let index = member.index;
            match probed {
                Ok(()) if member.ready => false,
                Ok(()) => {
                    (member.ready, member.unready) = (true, None);
                    eprintln!("route {label}: VM {index} ready");
                    true
                }
                Err(why) if member.unready.as_ref() == Some(&why) => false,
                Err(why) => {
                    eprintln!("route {label}: VM {index} not ready: {why}");
                    member.unready = Some(why);
                    std::mem::replace(&mut member.ready, false)
                }
            }
        };
        if moved {
            self.told();
        }
        true
    }

    /// Starts a VM in the background, of the index `index` holds, on the room `held` holds: into
    /// the pool, watched from then on; or as [`Members::failed`] says. `unprobed`: for connections
    /// waiting, which take it as it comes, its readiness probed after.
    fn spawn_start(
        self: &Arc<Self>,
        members: &mut Members,
        index: Arc<Index>,
        held: Option<Arc<Slot>>,
        wait: Duration,
        unprobed: bool,
    ) {
        members.starting += 1;
        let (pool, starter) = (Arc::downgrade(self), self.starter.clone());
        let (epoch, cancel) = (members.epoch, members.cancel.clone());
        let start = async move {
            let started = tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                started = starter.start(index.clone(), held, wait) => Some(started),
            };
            if let Some(pool) = pool.upgrade() {
                pool.started(epoch, index, started, unprobed);
            }
        };
        // the wicket's end waits for what stops a VM on a disk: this is where it starts from
        match &self.starter.kind {
            Kind::Boots(booter) if booter.vm.disk.is_some() => drop(booter.stops.spawn(start)),
            _ => drop(tokio::spawn(start)),
        }
    }

    /// Starts one more VM at the lowest index free, unless the pool holds `instances` with those
    /// starting and waiting to (`unprobed`: see [`VmPool::spawn_start`]).
    fn start_fresh(
        self: &Arc<Self>,
        members: &mut Members,
        wait: Duration,
        unprobed: bool,
    ) -> bool {
        if members.kept() >= self.bounds.get().pool_size() {
            return false;
        }
        let index = Arc::new(self.starter.indices.take(0));
        self.spawn_start(members, index, None, wait, unprobed);
        true
    }

    /// Starts the VM in the place of the one at `at` of [`Members::fallen`].
    fn start_fallen(
        self: &Arc<Self>,
        members: &mut Members,
        at: usize,
        wait: Duration,
        unprobed: bool,
    ) {
        let fallen = members.fallen.get_mut(&at).expect("one of them");
        fallen.starting = true;
        let (index, held) = (fallen.index.clone(), fallen.held.clone());
        self.spawn_start(members, index, held, wait, unprobed);
    }

    /// Starts a VM for a connection waiting, which takes it unprobed: one in the place of a VM down
    /// first, its wait aside.
    fn grow(self: &Arc<Self>, members: &mut Members, wait: Duration) -> bool {
        let waiting = members.fallen.iter().find(|(_, fallen)| !fallen.starting);
        if let Some(at) = waiting.map(|(at, _)| *at) {
            self.start_fallen(members, at, wait, true);
            return true;
        }
        self.start_fresh(members, wait, true)
    }

    /// A start ended (None: cancelled, as one of before a halt is). A VM into the pool from now on,
    /// connections reaching it once ready, `unprobed` at once ([`VmPool::spawn_start`]); watched
    /// and probed.
    fn started(
        self: &Arc<Self>,
        epoch: u64,
        index: Arc<Index>,
        started: Option<Result<Instance, BoxError>>,
        unprobed: bool,
    ) {
        let bounds = self.bounds.get();
        let warm = bounds.warm;
        let mut late = None;
        {
            let mut members = self.members.lock().unwrap();
            members.starting -= 1;
            match started {
                Some(started) if epoch != members.epoch => late = Some(started),
                Some(Ok(mut instance)) => {
                    let id = members.next_id;
                    members.next_id += 1;
                    members.failing = None;
                    let fallen = members.fallen.remove(&instance.index.index);
                    if fallen.is_some() {
                        members.restarts += 1;
                    }
                    let output = instance.output.take();
                    let (suspects, suspected) = mpsc::channel(1);
                    let instance = Arc::new(instance);
                    let (pool, vm) = (Arc::downgrade(self), Arc::downgrade(&instance));
                    tokio::spawn(watched(pool.clone(), id, vm.clone(), output, suspected));
                    let probing = CancellationToken::new();
                    tokio::spawn(lived(pool.clone(), id, vm.clone(), probing.clone()));
                    tokio::spawn(probed(pool, id, vm, probing.clone()));
                    members.vms.push(Member {
                        id,
                        index: instance.index.index,
                        instance,
                        ready: unprobed || bounds.readiness.check == Check::Process,
                        unready: None,
                        conns: 0,
                        since: Instant::now(),
                        suspects,
                        _probed: probing.drop_guard(),
                        backoff: fallen.map(|fallen| fallen.backoff).unwrap_or_default(),
                    });
                }
                Some(Err(e)) => {
                    let label = self.starter_label();
                    eprintln!("route {label}: pool VM: {e} (instance {})", index.index);
                    members.failed(e, index, warm);
                }
                None => {}
            }
        }
        // outside the lock: on a disk, its stop
        drop(late);
        self.told();
    }

    /// The hosts it serves, for the log.
    fn starter_label(&self) -> String {
        self.starter.hosts().current().label.clone()
    }

    /// Starts `warm` VMs: all of them up within `wait` or the first failed, of clones; of booted
    /// VMs, one up or all failed, the others coming as they do (a connection reaches each once its
    /// guest accepts, one failing tried again).
    async fn fill(self: &Arc<Self>, wait: Duration) -> Result<(), BoxError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let min = self.bounds.get().warm;
                let mut members = self.members.lock().unwrap();
                match self.starter.boots() {
                    true if !members.vms.is_empty() => return Ok(()),
                    true if members.starting == 0 && !members.fallen.is_empty() => {
                        return Err(members.failure());
                    }
                    false if members.failures > 0 => return Err(members.failure()),
                    false if members.vms.len() >= min => return Ok(()),
                    _ => {}
                }
                while members.kept() < min && self.start_fresh(&mut members, wait, false) {}
            }
            notified.await;
        }
    }

    /// A stream to the guest of a VM, and the seat the connection takes on it (see
    /// [`VmPool`]); failed when no VM came within `wait` (a pool of no `warm`
    /// serving none), or its start failed.
    async fn open(self: &Arc<Self>) -> Result<(UnixStream, Seat), BoxError> {
        if self.starter.boots() {
            return self.open_booted().await;
        }
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
                let at = match least.filter(|&at| members.vms[at].conns < bounds.concurrency) {
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
                        if members.waiting > members.starting * bounds.concurrency {
                            self.grow(&mut members, wait);
                        }
                        match least {
                            Some(at) if members.starting == 0 || late => Some(at),
                            None if late => {
                                return Err(format!(
                                    "no VM of the pool within {}ms: {}",
                                    wait.as_millis(),
                                    members.none(bounds.pool_size())
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
                    let mut ready = members.vms.iter().filter(|vm| vm.ready);
                    if members.starting == 0 && ready.all(|vm| vm.conns >= bounds.concurrency) {
                        self.grow(&mut members, wait);
                    }
                    seat
                })
            };
            let Some((id, instance)) = seat else {
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
            match instance.vm.dial(self.port).await {
                Ok(stream) => return Ok((stream, seat)),
                Err(e) => {
                    drop((seat, instance));
                    self.went_down(id, &e.to_string());
                }
            }
        }
    }

    /// [`VmPool::open`] of a booted pool: on the VM holding the fewest, none waited for. A dial
    /// failing has it looked at: the guest may be busy, and a boot costs more than a restore.
    async fn open_booted(self: &Arc<Self>) -> Result<(UnixStream, Seat), BoxError> {
        let (id, instance) = {
            let mut members = self.members.lock().unwrap();
            let max = self.bounds.get().pool_size();
            let least = members.least(max);
            let at = least.ok_or_else(|| match members.none(max) {
                "none up" => "no VM up: the route is restarting".to_string(),
                none => format!("no VM: {none}"),
            })?;
            members.seat(at)
        };
        let seat = Seat {
            pool: self.clone(),
            id,
        };
        match instance.vm.dial(self.port).await {
            Ok(stream) => Ok((stream, seat)),
            Err(e) => {
                seat.suspect();
                Err(e)
            }
        }
    }

    /// Kills the VM `id`, which serves no more for `why`: the next connection goes to another, and
    /// one it keeps is started in its place, after [`Backoff`]'s wait, its room held meanwhile.
    fn went_down(&self, id: u64, why: &str) {
        let warm = self.bounds.get().warm;
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.vms.iter().position(|vm| vm.id == id);
            at.map(|at| {
                let mut gone = members.vms.remove(at);
                let label = self.starter_label();
                eprintln!("route {label}: VM {}: {why}; killed", gone.index);
                if members.kept() < members.wanted(warm) {
                    let mut backoff = std::mem::take(&mut gone.backoff);
                    backoff.went_down(gone.instance.up.elapsed());
                    let wait = backoff.next_wait();
                    eprintln!(
                        "route {label}: VM {} starts again in {}ms",
                        gone.index,
                        wait.as_millis()
                    );
                    let mut down = None;
                    Shortage::keep(&mut down, why);
                    members.fallen.insert(
                        gone.index,
                        Fallen {
                            index: gone.instance.index.clone(),
                            down,
                            backoff,
                            retry: (!wait.is_zero()).then(|| Instant::now() + wait),
                            held: Some(Arc::new(self.capacity.hold(self.need))),
                            starting: false,
                        },
                    );
                }
                gone
            })
        };
        if gone.is_some() {
            // killed before a VM is started in its place
            drop(gone);
            self.told();
        }
    }

    /// Takes out the VMs past `instances` or beyond `warm` serving none for `keep` at `now`, and
    /// starts those missing to `warm`, the wait of those that went down over; and when to look
    /// again.
    fn tend(self: &Arc<Self>, now: Instant) -> (Vec<Member>, Option<Instant>) {
        let bounds = self.bounds.get();
        let (min, max) = (bounds.warm, bounds.pool_size());
        let mut members = self.members.lock().unwrap();
        if members.halted {
            return (Vec::new(), None);
        }
        let mut gone = Vec::new();
        while let Some(at) = members.idlest(min, max)
            && members.vms[at].since + bounds.keep <= now
        {
            gone.push(members.vms.remove(at));
        }
        let mut next = members
            .idlest(min, max)
            .map(|at| members.vms[at].since + bounds.keep);
        let up = members.wanted(min);
        // those past what it keeps, a lower `instances` come: the highest go
        while members.kept() > up {
            let waiting = members.fallen.iter().rev();
            let highest = waiting
                .filter(|(_, fallen)| !fallen.starting)
                .map(|(at, _)| *at)
                .next();
            let Some(at) = highest else {
                break;
            };
            members.fallen.remove(&at);
        }
        let wait = self.room_wait();
        let waiting: Vec<(usize, Option<Instant>)> = members
            .fallen
            .iter()
            .filter(|(_, fallen)| !fallen.starting)
            .map(|(at, fallen)| (*at, fallen.retry))
            .collect();
        for (at, retry) in waiting {
            match retry {
                Some(retry) if retry > now => {
                    next = Some(next.map_or(retry, |next| next.min(retry)));
                }
                _ => self.start_fallen(&mut members, at, wait, false),
            }
        }
        while members.kept() < up && self.start_fresh(&mut members, wait, false) {}
        drop(members);
        if !gone.is_empty() {
            (self.ready_changed)();
        }
        (gone, next)
    }

    /// Takes out its VMs for good, its connections failing from then on, and has it start none:
    /// those under way cancelled, what is held for them given back.
    fn halt(&self) -> Vec<Member> {
        let halted = {
            let mut members = self.members.lock().unwrap();
            members.halted = true;
            members.epoch += 1;
            members.cancel.cancel();
            members.cancel = CancellationToken::new();
            members.fallen.clear();
            std::mem::take(&mut members.vms)
        };
        (self.ready_changed)();
        halted
    }
}

impl Pool for VmPool {
    fn evict(&self) -> bool {
        let bounds = self.bounds.get();
        let gone = {
            let mut members = self.members.lock().unwrap();
            let at = members.idlest(bounds.warm, bounds.pool_size());
            at.map(|at| members.vms.remove(at))
        };
        if gone.is_some() {
            (self.ready_changed)();
        }
        // dropped: killed, its slot given back
        gone.is_some()
    }
}

impl Drop for VmPool {
    fn drop(&mut self) {
        self.members.get_mut().unwrap().cancel.cancel();
        self.changed.notify_waiters();
    }
}

/// Keeps a pool route's VMs ([`VmPool::tend`]) until the route is gone.
async fn tend(pool: Weak<VmPool>, changed: Arc<Notify>) {
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
/// as it keeps going down, none again past one up for [`RESTART_STABLE`]; and before the next try
/// of a start or a restore that failed: never none.
#[derive(Default)]
pub struct Backoff {
    wait: Duration,
}

impl Backoff {
    /// A VM went down, up for `lived`.
    pub fn went_down(&mut self, lived: Duration) {
        if lived >= RESTART_STABLE {
            self.wait = Duration::ZERO;
        }
    }

    /// The wait before the next start; the one after it is longer.
    pub fn next_wait(&mut self) -> Duration {
        let wait = self.wait;
        self.wait = (wait * 2).clamp(RESTART_WAIT, RESTART_WAIT_MAX);
        wait
    }

    /// A start or a restore failed: the wait before the next try; the one after it is longer.
    pub fn failed(&mut self) -> Duration {
        self.wait = self.wait.max(RESTART_WAIT);
        self.next_wait()
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
    /// What the clone at each index uses, the index's last one: indices are taken lowest
    /// first, so these are the most clones alive at once.
    meters: Mutex<HashMap<usize, Weak<Meter>>>,
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
    /// `port`, its output followed from then on. Of the index `index` holds (a pool's), else alive
    /// only then: what fails holds no index, for [`Route::alive`] to list.
    async fn restore(
        &self,
        slot: Slot,
        port: u32,
        index: Option<Arc<Index>>,
    ) -> Result<(Instance, UnixStream), BoxError> {
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
        let index = index.unwrap_or_else(|| Arc::new(self.indices.take(0)));
        let meter = Arc::downgrade(vm.meter());
        self.meters.lock().unwrap().insert(index.index, meter);
        // the guest's listener outlives the restore; its lines wait for the stream
        let mut output = match self.output {
            true => match vm.dial(wire::LOG_PORT).await {
                Ok(stream) => Some(output::follow(
                    stream,
                    self.sink.clone(),
                    self.hosts.clone(),
                    format!("vm-{n}"),
                    index.index,
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
        let instance = Instance {
            index,
            vm,
            output,
            up: Instant::now(),
            since: SystemTime::now(),
        };
        Ok((instance, accepting))
    }
}

/// Why a pool's VM serves no more, as its watch ([`watch`]) finds it.
#[derive(Debug, PartialEq, Eq)]
enum Down {
    /// Its log stream ended: the VM is gone.
    Vm,
    /// The guest said its process ended, how where it tells, and its port refuses.
    Process(Option<wire::Exit>),
    /// A connection failed, and its port refuses.
    Unanswered,
}

impl Down {
    /// What the log and a listing say of it; of a VM gone, the VM's end where it is known
    /// ([`Vm::end`]). An OOM kill of the guest's is named so.
    fn why(self, vm_end: Option<String>) -> String {
        match self {
            Down::Vm => vm_end.unwrap_or_else(|| "its VM ended".into()),
            Down::Process(None) => "its process ended".into(),
            Down::Process(Some(wire::Exit { status, oom: false })) => {
                format!("its process ended ({status})")
            }
            Down::Process(Some(wire::Exit { status, oom: true })) => format!(
                "its process ended ({status}): out of memory, the guest kernel's OOM killer took \
                 a process"
            ),
            Down::Unanswered => "its guest accepts no connection".into(),
        }
    }
}

/// Follows a pool's VM until it serves no more: why, or None when it is gone from its route.
///
/// - the log stream ended: the VM is gone
/// - the guest said its process ended, and its port refuses at once (`accepting`, given how long
///   to look): a launcher that left a daemon listening is no end. A process ending on purpose is
///   not told apart: a route's process is to serve, and the wicket's own stop empties the route
///   first (the routes then take no notice)
/// - a connection failed (`suspected`), and the port refuses for as long as the guest may take
///   to listen at a boot (`boot`): one process's restart in place is no end, nor is a client
///   gone. The log stream ending meanwhile is the VM gone, as above. What lives, listens and
///   does not answer is not seen
async fn watch<F>(
    mut output: Option<Output>,
    mut suspected: mpsc::Receiver<()>,
    accepting: impl Fn(Duration) -> F,
    boot: impl Fn() -> Duration,
) -> Option<Down>
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
            Some(Ended::Vm) => return Some(Down::Vm),
            Some(Ended::Process(exit)) => {
                told = true;
                if !accepting(Duration::ZERO).await {
                    return Some(Down::Process(exit));
                }
            }
            None => {
                // a VM gone refuses too: its end, seen while it is looked at, says more
                let gone = async {
                    match &mut output {
                        Some(output) => output.gone().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    () = gone => return Some(Down::Vm),
                    accepts = accepting(boot()) => {
                        if !accepts {
                            return Some(Down::Unanswered);
                        }
                    }
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

/// [`watch`] of the pool's VM `id`, its going down told to the pool. A look holds the VM only as
/// long as it takes: a stop kills it.
async fn watched(
    pool: Weak<VmPool>,
    id: u64,
    vm: Weak<Instance>,
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
    let Some(down) = watch(output, suspected, accepting, boot).await else {
        return;
    };
    // a VM gone: how its `firecracker` ended, an OOM kill named so. Held no longer than that
    // (a stop takes the VM once alone): it is dead already
    let held = matches!(down, Down::Vm).then(|| vm.upgrade()).flatten();
    let vm_end = match held {
        Some(held) => Some(held.vm.end().await),
        None => None,
    };
    if let Some(pool) = pool.upgrade() {
        pool.went_down(id, &down.why(vm_end));
    }
}

/// Probes the pool's VM `id` as the route's readiness check has it ([`health::probe`]), from
/// its start until it is gone from the pool (`probing` cancelled), each outcome to the pool
/// ([`VmPool::probed`]): at once, then as [`Readiness::next`] has it, at once again when the check
/// changed. A probe holds the VM only as long as it takes: a stop kills it.
async fn probed(pool: Weak<VmPool>, id: u64, vm: Weak<Instance>, probing: CancellationToken) {
    let Some((port, bounds, hosts, probes)) = pool.upgrade().map(|pool| {
        let hosts = pool.starter.hosts().clone();
        (pool.port, pool.bounds.clone(), hosts, pool.probes.clone())
    }) else {
        return;
    };
    loop {
        let changed = probes.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let readiness = bounds.get().readiness;
        let probed = match readiness.check {
            Check::Process => Ok(()),
            _ => {
                let Some(held) = vm.upgrade() else {
                    return;
                };
                let host = hosts.current().key.clone();
                let (check, within) = (&readiness.check, readiness.timeout);
                health::probe(&held.vm, port, check, within, &host).await
            }
        };
        let ready = probed.is_ok();
        match pool.upgrade() {
            Some(pool) if pool.probed(id, probed) => {}
            _ => return,
        }
        if bounds.get().readiness != readiness {
            continue;
        }
        let wait = async {
            match readiness.next(ready) {
                Some(wait) => tokio::time::sleep(wait).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = wait => {}
            () = changed => {}
            () = probing.cancelled() => return,
        }
    }
}

/// Probes the pool's VM `id` as the route's liveness check has it (`liveness=http`), from its
/// start until it is gone from the pool (`probing` cancelled): every `liveness-interval-ms`, the
/// wait starting anew as the check changes. One probe failing has it gone down, as Diego's does
/// ([`VmPool::went_down`]). `port` probes nothing: the watch sees what refuses ([`watched`]). A
/// probe holds the VM only as long as it takes: a stop kills it.
async fn lived(pool: Weak<VmPool>, id: u64, vm: Weak<Instance>, probing: CancellationToken) {
    let Some((port, bounds, hosts, probes)) = pool.upgrade().map(|pool| {
        let hosts = pool.starter.hosts().clone();
        (pool.port, pool.bounds.clone(), hosts, pool.probes.clone())
    }) else {
        return;
    };
    let mut liveness = bounds.get().liveness;
    let mut next = Instant::now() + liveness.interval;
    loop {
        let changed = probes.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let check = liveness.check();
        let wait = async {
            match check {
                Some(_) => tokio::time::sleep_until(next.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = wait => {}
            () = changed => {
                let now = bounds.get().liveness;
                if now != liveness {
                    next = Instant::now() + now.interval;
                    liveness = now;
                }
                continue;
            }
            () = probing.cancelled() => return,
        }
        let Some(check) = check else {
            continue;
        };
        let Some(held) = vm.upgrade() else {
            return;
        };
        let host = hosts.current().key.clone();
        let probed = health::probe(&held.vm, port, &check, liveness.timeout, &host).await;
        drop(held);
        if let Err(why) = probed {
            if let Some(pool) = pool.upgrade() {
                pool.went_down(id, &format!("its liveness check failed: {why}"));
            }
            return;
        }
        next = Instant::now() + liveness.interval;
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
    /// Starts the route's VMs (`hosts` name them in the log), up as [`VmPool::fill`] has them: a
    /// booted pool's, or a template booted, snapshotted once its guest listens and killed, and a
    /// clone pool's `warm` restored then. See [`Starting`].
    pub async fn start(
        spec: RouteSpec,
        hosts: &[String],
        starting: Starting,
    ) -> Result<Route, BoxError> {
        match spec.mode {
            Mode::Boot => Route::start_booted(spec, hosts, starting).await,
            Mode::PerConnection | Mode::Reuse | Mode::Pool => {
                Route::start_cloned(spec, hosts, starting).await
            }
        }
    }

    /// A pool, its first VM up.
    async fn start_booted(
        spec: RouteSpec,
        hosts: &[String],
        starting: Starting,
    ) -> Result<Route, BoxError> {
        let Starting {
            places,
            n,
            capacity,
            sink,
            workers,
            stops,
            status,
            ready_changed,
        } = starting;
        let hosts = Hosts::new(hosts);
        let label = hosts.current().label.clone();
        let bounds = Current(Arc::new(Mutex::new(spec.bounds.clone())));
        let need = Need {
            memory: u64::from(spec.vm.memory_mib),
            cpu: spec.vm.cpus(),
            route: NEXT_ROUTE.fetch_add(1, Ordering::Relaxed),
        };
        capacity.cap(need.route, spec.bounds.instances);
        // the place opens the disk's file: made by then
        if let Some(disk) = &spec.vm.disk {
            disk.ready()
                .await
                .map_err(|e| format!("route {label}: {e}"))?;
        }
        let place = Arc::new(
            places
                .place(&format!("route-{n}"), &spec.vm)
                .await
                .map_err(|e| format!("route {label}: {e}"))?,
        );
        let started = Instant::now();
        let egress = (!spec.vm.egress.is_empty())
            .then(|| Egress::new(spec.vm.egress.clone(), hosts.clone(), sink.clone(), workers));
        let starter = Starter {
            kind: Kind::Boots(Box::new(Booter {
                vm: spec.vm.clone(),
                bounds: bounds.clone(),
                place: place.clone(),
                next: AtomicU64::new(0),
                sink,
                hosts: hosts.clone(),
                egress,
                stops,
            })),
            port: spec.port,
            capacity: capacity.clone(),
            need,
            indices: Arc::default(),
        };
        let changed = Arc::new(Notify::new());
        let pool = Arc::new(VmPool {
            starter: Arc::new(starter),
            port: spec.port,
            bounds: bounds.clone(),
            capacity: capacity.clone(),
            need,
            members: Mutex::default(),
            changed: changed.clone(),
            probes: Arc::default(),
            status,
            ready_changed: ready_changed.clone(),
        });
        pool.fill(START_WAIT)
            .await
            .map_err(|e| pool_failure(&label, e))?;
        println!(
            "route {label}: first of {} VM(s) up after {}ms",
            spec.bounds.warm,
            started.elapsed().as_millis()
        );
        tokio::spawn(tend(Arc::downgrade(&pool), changed));
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
            backend: Backend::Pool(pool),
            clients: Arc::default(),
            open: Arc::default(),
            taken: None,
            stops: Arc::default(),
            ready_changed,
        })
    }

    /// A template booted, snapshotted once its guest listens and killed; a clone pool's `warm` VMs
    /// restored then.
    async fn start_cloned(
        spec: RouteSpec,
        hosts: &[String],
        starting: Starting,
    ) -> Result<Route, BoxError> {
        let Starting {
            places,
            n,
            capacity,
            sink,
            workers,
            stops,
            status,
            ready_changed,
        } = starting;
        let hosts = Hosts::new(hosts);
        let label = hosts.current().label.clone();
        let bounds = Current(Arc::new(Mutex::new(spec.bounds.clone())));
        let need = Need {
            memory: u64::from(spec.vm.memory_mib),
            cpu: spec.vm.cpus(),
            route: NEXT_ROUTE.fetch_add(1, Ordering::Relaxed),
        };
        capacity.cap(need.route, spec.bounds.instances);
        let slot = capacity
            .take(need, START_WAIT)
            .await
            .map_err(|e| e.of(&label))?;
        let place = Arc::new(
            places
                .place(&format!("route-{n}"), &spec.vm)
                .await
                .map_err(|e| format!("route {label}: {e}"))?,
        );
        let started = Instant::now();
        let name = "template";
        let egress = (!spec.vm.egress.is_empty())
            .then(|| Egress::new(spec.vm.egress.clone(), hosts.clone(), sink.clone(), workers));
        let named = egress.as_ref().map(|egress| (egress, name.to_string()));
        let vm = Vm::boot(
            &spec.vm,
            &place,
            Role::Template,
            "boot",
            slot,
            named,
            &stops,
        )
        .await?;
        let deadline = Instant::now() + spec.bounds.boot;
        let mut output = ready(&vm, spec.port, spec.bounds.boot, &sink, &hosts, name, 0).await?;
        // its clones start from a process that passed it
        let within = (deadline, spec.bounds.boot);
        let host = hosts.current().key.clone();
        let liveness = &spec.bounds.liveness;
        alive(&vm, spec.port, liveness, &host, within, output.as_mut()).await?;
        let booted = started.elapsed();
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
        let snapshot = match spec.mode {
            Mode::PerConnection => snapshot.of_connections(),
            _ => snapshot,
        };
        drop(vm);
        // the template gone, its snapshot the clones' alone to read (jail mode)
        place.seal().await?;
        // what its guest wrote, the rest of its size a hole
        let metadata = tokio::fs::metadata(&snapshot.memory_on_host).await?;
        let size = std::os::unix::fs::MetadataExt::blocks(&metadata) * 512;
        let taken = Some(Taken {
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
        let indices: Arc<Indices> = Arc::default();
        let cloner = Arc::new(Cloner {
            snapshot,
            place: place.clone(),
            next: AtomicU64::new(0),
            indices: indices.clone(),
            meters: Mutex::default(),
            sink,
            hosts: hosts.clone(),
            output: output.is_some(),
            egress,
        });
        let backend = if spec.mode == Mode::Pool {
            let changed = Arc::new(Notify::new());
            let starter = Starter {
                kind: Kind::Clones(cloner),
                port: spec.port,
                capacity: capacity.clone(),
                need,
                indices,
            };
            let pool = Arc::new(VmPool {
                starter: Arc::new(starter),
                port: spec.port,
                bounds: bounds.clone(),
                capacity: capacity.clone(),
                need,
                members: Mutex::default(),
                changed: changed.clone(),
                probes: Arc::default(),
                status,
                ready_changed: ready_changed.clone(),
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
                ready_changed: ready_changed.clone(),
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
            stops: Arc::default(),
            ready_changed,
        })
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
    /// started for a higher `warm` or `instances` as there is room, touching no other; past a lower
    /// `instances`, a pool's VMs take no connection any more and go as `keep-ms` has them go, once
    /// theirs ended, and of a VM per connection what serves none goes at once. What waits to be
    /// tried again is tried at once.
    pub fn adjust(&self, bounds: Bounds) -> bool {
        let instances = bounds.instances;
        let rechecked = {
            let mut current = self.bounds.0.lock().unwrap();
            if *current == bounds {
                return false;
            }
            let rechecked =
                current.readiness != bounds.readiness || current.liveness != bounds.liveness;
            *current = bounds;
            rechecked
        };
        self.capacity.cap(self.need.route, instances);
        match &self.backend {
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
                pool.members.lock().unwrap().bounds_changed();
                pool.changed.notify_waiters();
                if rechecked {
                    pool.probes.notify_waiters();
                }
            }
        }
        // a VM past `instances` takes no connection, one may be made for the next
        (self.ready_changed)();
        true
    }

    /// What its VMs are now.
    pub fn vms(&self) -> Vms {
        let mut vms = Vms {
            alive: self.capacity.alive(self.need.route),
            connections: self.open.load(Ordering::Relaxed),
            ..Vms::default()
        };
        match &self.backend {
            Backend::Cloned { warm, kept, .. } => {
                vms.warm = warm.clones.lock().unwrap().len();
                vms.kept = kept
                    .as_ref()
                    .map_or(0, |kept| kept.idle.lock().unwrap().len());
            }
            Backend::Pool(pool) => {
                let max = self.bounds.get().pool_size();
                let members = pool.members.lock().unwrap();
                vms.pooled = members.vms.len();
                vms.starting = members.starting;
                vms.restarts = members.restarts;
                let past = members.past(max);
                let ready = members.vms.iter().filter(|vm| vm.ready && vm.index < past);
                vms.ready = Some(ready.count());
            }
        }
        vms
    }

    /// The instance indices of its VMs alive, their starts, each from its guest accepting on,
    /// whether each takes connections -- a pool's VM ready, within `instances`; a clone as it
    /// comes -- and what each uses, as last read.
    pub fn alive(&self) -> BTreeMap<usize, status::Instance> {
        match &self.backend {
            Backend::Cloned { cloner, .. } => {
                let meters = cloner.meters.lock().unwrap();
                let held = cloner.indices.held().into_iter();
                held.map(|(index, since)| {
                    let meter = meters.get(&index).and_then(Weak::upgrade);
                    let usage = meter.and_then(|meter| meter.usage());
                    let ready = Some(true);
                    (
                        index,
                        status::Instance {
                            since,
                            ready,
                            usage,
                        },
                    )
                })
                .collect()
            }
            Backend::Pool(pool) => {
                let max = self.bounds.get().pool_size();
                let members = pool.members.lock().unwrap();
                let past = members.past(max);
                let vms = members.vms.iter();
                vms.map(|vm| {
                    let instance = status::Instance {
                        since: vm.instance.since,
                        ready: Some(vm.ready && vm.index < past),
                        usage: vm.instance.vm.usage(),
                    };
                    (vm.index, instance)
                })
                .collect()
            }
        }
    }

    /// Whether its next connection finds a VM to serve it ([`VmPool::ready`], [`Warm::ready`]):
    /// every stream advertises it only then.
    pub fn ready(&self) -> bool {
        match &self.backend {
            Backend::Pool(pool) => pool.ready(),
            Backend::Cloned { warm, .. } => warm.ready(),
        }
    }

    /// The instance indices of the VMs it keeps that are down, and why: each starting again.
    pub fn crashed(&self) -> BTreeMap<usize, Shortage> {
        let Backend::Pool(pool) = &self.backend else {
            return BTreeMap::new();
        };
        let members = pool.members.lock().unwrap();
        let fallen = members.fallen.iter();
        fallen
            .filter_map(|(index, fallen)| Some((*index, fallen.down.clone()?)))
            .collect()
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

    /// Whether its VMs were stopped: its connections fail from then on, until it is told to go on
    /// ([`Route::resume`]).
    pub fn stopped(&self) -> bool {
        matches!(&self.backend, Backend::Pool(pool) if pool.members.lock().unwrap().halted)
    }

    /// Why it is short of VMs, since that goes on: a pool below `warm`, a VM gone or a start failed
    /// (of the lowest index); clones below `warm`, the last restore
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
                let failed = warm.failed.lock().unwrap().down.clone();
                failed.filter(|_| warm.missing() > 0 || cloner.indices.held().is_empty())
            }
        }
    }

    /// Stops its VMs, for good ([`Route::stopping`]); a stop under way too.
    pub async fn stop(&self) {
        self.stopping().await;
        drop(self.stops.lock().await);
    }

    /// Takes its pool's VMs out now, its connections failing from then on, no other started in
    /// their place, and what stops them: the process is told to end (SIGTERM), what it wrote is
    /// all on the disk, as far as it ended within [`disk::STOP_GRACE`], and its open connections
    /// end. A VM gives its disk up as
    /// it is gone (the lock tells, [`Disk::ready`]); one under way goes as it is dropped ([`Vm`]).
    /// A cloned route's VMs end with their last connection.
    pub fn stopping(&self) -> impl Future<Output = ()> + Send + 'static {
        let members = match &self.backend {
            Backend::Pool(pool) => pool.halt(),
            Backend::Cloned { .. } => Vec::new(),
        };
        let label = self.label();
        // taken out once: free
        let stops = (!members.is_empty())
            .then(|| self.stops.clone().try_lock_owned().ok())
            .flatten();
        async move {
            for member in members {
                if let Err(e) = member.instance.vm.stop(disk::STOP_GRACE).await {
                    eprintln!("route {label}: its VM's stop: {e}; killed");
                }
                // what its process said as it ended, before the VM goes
                if let Ok(instance) = Arc::try_unwrap(member.instance) {
                    instance.end().await;
                }
            }
            drop(stops);
        }
    }

    /// Goes on after a stop ([`Route::stopping`]): VMs started again, as it keeps them.
    pub fn resume(&self) {
        if let Backend::Pool(pool) = &self.backend {
            pool.members.lock().unwrap().halted = false;
            pool.changed.notify_waiters();
            (self.ready_changed)();
        }
    }

    /// Once it [`Route::resume`]d: one VM up, or why none is; it goes on trying.
    pub async fn resumed(&self) -> Result<(), BoxError> {
        match &self.backend {
            Backend::Pool(pool) => pool.fill(START_WAIT).await,
            Backend::Cloned { .. } => Ok(()),
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
        Some(match (self.mode, bounds.instances) {
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
        let trace = std::env::var_os(TRACE).is_some();
        match &self.backend {
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
                        let restored = cloner.restore(slot, self.port, None).await;
                        if restored.is_ok() {
                            warm.restored();
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
/// `index`, of the route of `hosts`) followed meanwhile: `/init` opens the log stream before it starts
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
    index: usize,
) -> Result<Option<Output>, BoxError> {
    let output = followed(
        || vm.dial(wire::LOG_PORT),
        vm.dial_ready(port, boot),
        |stream| output::follow(stream, sink.clone(), hosts.clone(), name.to_string(), index),
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

/// Waits for a guest listening ([`ready`]) to pass the route's liveness check at its start, as
/// `host` names it: of `http`, its endpoint answered 200, probed every [`LOOK_AGAIN`] until the
/// deadline of `within` (`boot-ms`, that of the boot), its process exiting first failing it at
/// once; `port` asks nothing more.
async fn alive(
    vm: &Vm,
    port: u32,
    liveness: &Liveness,
    host: &str,
    within: (Instant, Duration),
    output: Option<&mut Output>,
) -> Result<(), BoxError> {
    let Some(check) = liveness.check() else {
        return Ok(());
    };
    let (deadline, boot) = within;
    let passed = async {
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let timeout = liveness.timeout.min(left);
            let why = match health::probe(vm, port, &check, timeout, host).await {
                Ok(()) => return Ok(()),
                Err(why) => why,
            };
            if Instant::now() + LOOK_AGAIN >= deadline {
                return Err::<(), BoxError>(
                    format!(
                        "its liveness check did not pass within {}ms: {why}",
                        boot.as_millis()
                    )
                    .into(),
                );
            }
            tokio::time::sleep(LOOK_AGAIN).await;
        }
    };
    let Some(output) = output else {
        return passed.await;
    };
    tokio::select! {
        passed = passed => passed,
        () = output.exited() => {
            // its last lines first: what says why
            output.sync(END_SYNC_TIMEOUT).await;
            Err("the guest's process exited before its liveness check passed (see its output)"
                .into())
        }
    }
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
/// rate at about 1 / restore time; those no more asked for are given up, so restores failing cost
/// one VM and one line per try ([`Warm::wanted`]).
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
        let retry = {
            let Some(warm) = warm.upgrade() else {
                return;
            };
            let (wanted, retry) = warm.wanted(Instant::now());
            if restoring.len() > wanted {
                restoring.abort_all();
            }
            while restoring.len() < wanted.min(REFILL_WORKERS) {
                let restored = warm_clone(cloner.clone(), port, capacity.clone(), need);
                restoring.spawn(restored);
            }
            retry
        };
        let waited = async {
            match retry {
                Some(retry) => tokio::time::sleep_until(retry.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = notified => {}
            () = waited => {}
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

/// A clone restored in room no VM waits for, ready: it answers at once as it is handed out; or why
/// not.
async fn warm_clone(
    cloner: Arc<Cloner>,
    port: u32,
    capacity: Arc<Capacity>,
    need: Need,
) -> Result<Instance, BoxError> {
    let slot = capacity.take_spare(need).await;
    match cloner.restore(slot, port, None).await {
        Ok((clone, _probe)) => Ok(clone),
        Err(e) => {
            eprintln!("warm clone: {e}");
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
            cpu: 100,
            route: 1,
        };
        let capacity = Capacity::new(0, None, None);
        let Err(full) = capacity.take(need, Duration::ZERO).await else {
            panic!("no room at all");
        };
        for (failed, is_full, says) in [
            (BoxError::from(full), true, "route a.local: no room"),
            ("it died".into(), false, "route a.local: pool VM: it died"),
        ] {
            let mut members = Members::default();
            let index = Arc::new(Arc::<Indices>::default().take(0));
            members.failed(failed, index, 1);
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

    /// The wait doubles as a VM keeps going down, up to its bound, and starts over once one
    /// stayed up long enough.
    #[test]
    fn a_restarts_wait_starts_over_after_a_vm_stayed_up() {
        let mut backoff = Backoff::default();
        backoff.went_down(Duration::from_secs(5));
        assert_eq!(backoff.next_wait(), Duration::ZERO, "none started so yet");
        for (up, wait) in [
            (0, 1),
            (0, 2),
            (0, 4),
            (30, 8),
            (59, 16),
            (60, 0),
            (0, 1),
            (65, 0),
            (0, 1),
        ] {
            backoff.went_down(Duration::from_secs(up));
            let next = backoff.next_wait();
            assert_eq!(next, Duration::from_secs(wait), "{up}s up");
        }
        // starts failing: no VM up, no going down in between
        assert_eq!(backoff.failed(), Duration::from_secs(2));
        let mut failing = Backoff::default();
        let waits: Vec<u64> = (0..8).map(|_| failing.failed().as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60], "never none");
    }

    /// Restores failing are tried one at a time after a longer and longer wait: a change of the
    /// bounds tries at once, the wait going on; a success goes back to `warm` at once, the wait
    /// anew.
    #[tokio::test]
    async fn warm_clones_failing_wait_longer_and_longer() {
        let spec = RouteSpec::configure(
            "r",
            "a.local",
            PathBuf::from("/a.ext4"),
            [("mode", "per-connection"), ("warm", "2")].into_iter(),
            std::path::Path::new("firecracker"),
            std::path::Path::new("vmlinux"),
        )
        .unwrap();
        let changed = Arc::new(Notify::new());
        let warm = Warm {
            clones: Mutex::default(),
            failed: Mutex::default(),
            bounds: Current(Arc::new(Mutex::new(spec.bounds))),
            capacity: Capacity::new(1, None, None),
            route: 1,
            changed: changed.clone(),
            ready_changed: Arc::new(|| {}),
        };
        assert_eq!(warm.wanted(Instant::now()), (2, None));
        let failure: BoxError = "gone".into();
        let tried_after = || {
            let before = Instant::now();
            warm.failing(&failure);
            let (none, retry) = warm.wanted(Instant::now());
            assert_eq!(none, 0, "none during the wait");
            let retry = retry.expect("a wait");
            assert_eq!(warm.wanted(retry), (1, None), "one, whatever `warm` asks");
            (retry - before).as_secs()
        };
        for wait in [1, 2, 4, 8, 16, 32, 60, 60] {
            assert_eq!(tried_after(), wait);
        }
        let since = warm.failed.lock().unwrap().down.clone().unwrap().since;

        warm.adjust();
        assert_eq!(warm.wanted(Instant::now()), (1, None), "at once");
        assert_eq!(tried_after(), 60, "going on");
        let down = warm.failed.lock().unwrap().down.clone().unwrap();
        assert_eq!(down.since, since);

        changed.notified().await;
        warm.restored();
        tokio::time::timeout(Duration::ZERO, changed.notified())
            .await
            .expect("what keeps them told");
        assert_eq!(warm.wanted(Instant::now()), (2, None));
        assert_eq!(tried_after(), 1, "anew");
    }

    /// What a VM's watch makes of what it is told: only an end it finds true, a stream
    /// given up telling nothing; the route's end ends it. A process's end says how, an OOM
    /// kill named so.
    #[tokio::test]
    async fn a_vm_is_down_when_its_port_refuses_too() {
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
        let exit = wire::Exit {
            status: "signal: 9 (SIGKILL)".into(),
            oom: true,
        };
        let exited = {
            let mut frame = Vec::new();
            wire::put_log_frame(&mut frame, &LogFrame::Exited(Some(exit.clone())));
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
        assert_eq!(why, Some(Down::Process(Some(exit))));
        assert_eq!(
            why.unwrap().why(None),
            "its process ended (signal: 9 (SIGKILL)): out of memory, the guest kernel's OOM \
             killer took a process"
        );
        assert_eq!(Down::Process(None).why(None), "its process ended");

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
        assert_eq!(why, Some(Down::Unanswered));

        // its VM gone: no look to take. A guest without a log stream, the route gone: no end
        let (followed, guest) = log();
        drop(guest);
        let (_suspects, suspected) = mpsc::channel(1);
        let why = watch(Some(followed), suspected, answers(&[]), || BOOT).await;
        assert_eq!(why, Some(Down::Vm));
        // gone while a failed connection has it looked at: its end, not its silence
        let (followed, guest) = log();
        let (suspects, suspected) = mpsc::channel(1);
        let (looked, mut told) = mpsc::unbounded_channel();
        let accepting = move |_| {
            let _ = looked.send(());
            std::future::pending::<bool>()
        };
        let watching = tokio::spawn(watch(Some(followed), suspected, accepting, || BOOT));
        suspects.send(()).await.unwrap();
        told.recv().await.unwrap();
        drop(guest);
        let why = tokio::time::timeout(Duration::from_secs(5), watching).await;
        assert_eq!(why.expect("the VM's end seen").unwrap(), Some(Down::Vm));
        assert_eq!(Down::Vm.why(None), "its VM ended");
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

    /// A VM of a pool going down holds its index and its wait, per VM: another index's are its own;
    /// past what the pool keeps, none is held; a route told why by the lowest.
    #[test]
    fn a_vm_going_down_is_its_index_alone() {
        let indices = Arc::<Indices>::default();
        let mut members = Members::default();
        let (zero, one, two) = (
            Arc::new(indices.take(0)),
            Arc::new(indices.take(0)),
            Arc::new(indices.take(0)),
        );
        members.failed("it died".into(), one.clone(), 3);
        members.failed("it died too".into(), two.clone(), 3);
        assert_eq!(members.kept(), 2);
        assert_eq!(members.short(3).unwrap().why, "its start failed: it died");
        assert_eq!(indices.take(0).index, 3, "the indices stay theirs");
        // once more of the one, its own wait: the other's untouched
        members.failed("it died".into(), one, 3);
        assert_eq!(members.fallen[&1].backoff.wait, Duration::from_secs(4));
        assert_eq!(members.fallen[&2].backoff.wait, Duration::from_secs(2));
        // a change of the bounds: all at once, their waits going on
        assert!(members.fallen[&2].retry.is_some());
        members.bounds_changed();
        assert!(members.fallen.values().all(|fallen| fallen.retry.is_none()));
        assert_eq!(members.fallen[&1].backoff.wait, Duration::from_secs(4));
        // below what it keeps no more: nothing started in its place
        members.failed("it died".into(), zero, 2);
        assert!(!members.fallen.contains_key(&0));
    }

    /// A mode's options as the docs have them: `boot` takes `instances` and none that grows,
    /// shrinks or waits for a VM; its `warm` is `instances`, its options what was asked.
    #[test]
    fn a_booted_route_keeps_its_vms() {
        let spec = |options: &[(&'static str, &'static str)]| {
            RouteSpec::configure(
                "r",
                "a.local",
                PathBuf::from("/a.ext4"),
                options.iter().copied(),
                std::path::Path::new("firecracker"),
                std::path::Path::new("vmlinux"),
            )
        };
        let one = spec(&[]).unwrap();
        assert_eq!((one.mode, one.resident()), (Mode::Boot, 1));
        assert_eq!(one.target(), "firecracker:/a.ext4");
        let three = spec(&[("mode", "boot"), ("instances", "3")]).unwrap();
        assert_eq!(three.resident(), 3);
        assert_eq!(three.target(), "firecracker:/a.ext4?instances=3");
        assert_eq!(three.bounds.keep, Duration::ZERO);
        for key in ["warm", "keep-ms", "concurrency", "prime", "wait-ms"] {
            let refused = spec(&[("instances", "2"), (key, "1")]).unwrap_err();
            assert!(refused.contains(&format!("{key} needs mode=")), "{refused}");
        }
        let refused = spec(&[("mode", "shared")]).unwrap_err();
        assert!(refused.contains("unknown mode shared"), "{refused}");
        let refused = spec(&[("instances", "2"), ("disk", "/data")]).unwrap_err();
        assert!(refused.contains("disk needs instances=1"), "{refused}");
        assert!(spec(&[("instances", "1"), ("disk", "/data")]).is_ok());
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
