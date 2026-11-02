//! The settings: the command line over the environment
//! (`SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME>`, e.g. `SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN`)
//! over a `--config` YAML file over the defaults. The routes are the file's and
//! the `--vm` flags'; they are read again on a reload ([`RouteSource`]), the
//! other settings only at startup.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use sluice_wicket_console::settings::{ConsoleOptions, ConsoleSettings, Rules};
use sluice_wicket_tunnel::flume::FlumeSettings;
use sluice_wicket_tunnel::{DEFAULT_DRAIN_GRACE, DEFAULT_MAX_CONNECTIONS, DEFAULT_STALL_TIMEOUT};

use crate::deployed::Deployed;
use crate::jail::Ids;
use crate::route::RouteSpec;

pub const USAGE: &str = "\
usage: sluice-wicket-runtime-firecracker --kernel <vmlinux> --vm <host>=<rootfs.ext4>[?<options>]...
                                 (--server <url> | --listen <host:port>) [options]
       sluice-wicket-runtime-firecracker --config <file> [options]

  --server <url>                  the sluice server, grpc://host:port or grpcs://host:port;
                                  asked for its nodes, one tunnel stream to each
  --listen <host:port>            serve the routes over plain TCP too (no server needed),
                                  relayed as the tunnel's connections are: each by its
                                  first request's host (Host, h2c :authority), a sole
                                  route whatever the host, an unknown host answered 404
  --vm <host>=<rootfs>[?<opts>]   a route served by Firecracker microVMs booted from the
                                  rootfs; repeatable. mode=shared (default: one VM serves
                                  every connection), mode=per-connection (a VM cloned from
                                  a snapshot per connection; warm=N kept restored ahead,
                                  prime=N GET / sent before the snapshot, vms=N the
                                  route's VMs at most, wait-ms=N a connection may wait for
                                  room for its VM, 10000), mode=reuse (as per-connection, a
                                  clone serving the next connection until kept unused for
                                  keep-ms=N (60000), uses=N connections, lifetime-ms=N) or
                                  mode=pool (vms=N clones each serving many connections,
                                  warm=N kept (vms), grown by one while each holds conns=N
                                  (16), one beyond warm idle keep-ms killed), vcpus=N (1),
                                  mem=MiB (128), port=N (the guest's vsock port, 8080),
                                  per-client=N a client address's connections at once,
                                  idle-ms=N a connection may relay no byte, cpu=N (jail
                                  mode) a VM's host CPU in percent of one,
                                  egress=<host>:<port>,... the destinations its guests may
                                  connect to through the wicket: names, addresses, ranges
                                  (10.0.0.0/24:5432, [2001:db8::/32]:443)
  --config <file>                 YAML settings and routes, the routes reloaded on change
  --kernel <path>                 the guest kernel (vmlinux)
  --firecracker <path>            the firecracker binary (default firecracker on the PATH)
  --state-dir <path>              the VMs' files: their sockets need paths below 108 bytes
                                  (default a directory of the process's own under
                                  $XDG_RUNTIME_DIR, else the system's temp dir; jail mode:
                                  the keeper's own under /run; a tmpfs keeps the snapshots
                                  off the disk)
  --token <token>                 bearer token of the tunnel streams (on the command line
                                  every user of the host sees it: prefer the environment,
                                  the config file or --token-file)
  --token-file <path>             read the token from a file instead
  --client-id <id>                client identity (default a random UUID)
  --ca-cert <path>                PEM CA verifying a grpcs:// server (default the webPKI roots)
  --insecure                      skip the verification of a grpcs:// server
  --max-connections <n>           connections open at once (default 256): the tunnel's,
                                  and --listen's apart
  --max-vms <n>                   VMs alive at once over every route: shared, warm, in use,
                                  templates (default 64)
  --max-memory <MiB>              their guest memory (mem) at most (default none: --max-vms
                                  x the largest mem bounds it)
  --workers <n>                   threads relaying the connections, a node's tunnel stream
                                  and its connections on one (default 4, at most the CPUs)
  --tunnel-stall-timeout <secs>   give a stream up once its frames wait that long untaken
                                  (default 60, 0 = off)
  --drain-grace <secs>            the SIGTERM drain's bound (default 20, 0 = no drain)
  --watch-interval <secs>         poll the routes' files: the config file, the rootfs
                                  images, the kernel (default 1, 0 = SIGHUP only)
  --flume <url>                   feed the VMs' output to a sluice-flume while someone
                                  watches (the live access log), grpc://host:port or
                                  grpcs://host:port
  --flume-token <token>           the flume's bearer token (required with --flume)
  --flume-ca-cert <path>          PEM CA verifying a grpcs:// flume (default the webPKI roots)
  --flume-insecure                skip the verification of a grpcs:// flume
  --jail-ids <first>-<last>       jail mode: each VM in a jail of its own (namespaces, root,
                                  cgroup) as an id (uid and gid) of the range, one per VM
                                  alive; the wicket starts as root, which stays to spawn
                                  the VMs, and runs as --jail-user
  --jail-user <name|uid:gid>      jail mode: the user the wicket runs as (not root)
  --jail-cgroup <path>            jail mode: the VMs' parent cgroup under /sys/fs/cgroup
                                  (default the wicket's own, delegated to it)
  --console <host:port>           serve the console: a page and an API deploying uploaded
                                  rootfs images as routes, kept in --console-dir
  --console-token <token>         the console's credentials (the HTTP Basic password, or a
                                  Bearer token); required off loopback, unless with
                                  --oidc-issuer
  --console-dir <path>            where the console keeps its routes and images (default
                                  deployed, next to the config file)
  --oidc-issuer <url>             the OpenID Provider the console's browsers sign in
                                  through instead of the token
  --oidc-client-id <id>           the client registered with it, whose redirect URI is
                                  <console url>/oauth2/callback (required with --oidc-issuer)
  --oidc-client-secret <secret>   its secret; none = a public client (PKCE only)
  --oidc-claim <claim>=<pattern>  who may sign in: the ID token's claim must hold (a list:
                                  contain) the pattern, * any run of characters;
                                  repeatable, distinct claims all hold; at least one
                                  rule is required with --oidc-issuer, e.g. email=*
  --oidc-scopes <list>            the scopes asked for, comma-separated (default
                                  openid,email,profile)
  --oidc-ca-cert <path>           PEM CA verifying the provider (default the webPKI roots)
  --version                      print the build (version, commit, build time) and exit

Each option but --vm also reads SLUICE_WICKET_RUNTIME_FIRECRACKER_<NAME> from the environment, e.g.
SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN.";

/// Default of `--max-vms`.
pub const DEFAULT_MAX_VMS: usize = 64;

/// Default of `--workers`, at most the CPUs: a worker per node stream of a small
/// cluster, and a few for `--listen`'s and the egress's connections (more cost more
/// wakeups than they relay in parallel, see the README's Findings).
const DEFAULT_WORKERS: usize = 4;

/// Default of `--watch-interval`.
const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// What the command line asks for.
pub enum Command {
    Help,
    Version,
    Run(Box<Settings>),
}

/// The settings of a run.
pub struct Settings {
    pub server: Option<String>,
    pub listen: Option<String>,
    pub token: String,
    pub client_id: String,
    pub insecure: bool,
    pub ca_cert: Option<PathBuf>,
    pub max_connections: usize,
    pub stall_timeout: Duration,
    pub drain_grace: Duration,
    pub max_vms: usize,
    /// MiB; None = no memory budget.
    pub max_memory: Option<u64>,
    /// The relay workers (`sluice_wicket_tunnel::workers`).
    pub workers: usize,
    /// How often the routes' files are polled; zero = SIGHUP only.
    pub watch_interval: Duration,
    /// Where the VMs' files go; None = a directory of the process's own.
    pub state_dir: Option<PathBuf>,
    /// The flume the VMs' output is fed to.
    pub flume: Option<FlumeSettings>,
    /// Jail mode: the VMs in jails, spawned by the keeper.
    pub jail: Option<JailSettings>,
    /// The console (`--console`).
    pub console: Option<ConsoleSettings>,
    /// Where the routes come from, read again on a reload (the console's
    /// added once its directory is open).
    pub routes: RouteSource,
    /// The routes of the config file and the flags, read as they were
    /// parsed: checked before anything starts.
    pub specs: Vec<RouteSpec>,
}

/// Jail mode's settings (`keeper`, `jail`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JailSettings {
    /// The user the wicket runs as: a name, or `uid:gid`.
    pub user: String,
    /// The VMs' uids and gids.
    pub ids: Ids,
    /// The VMs' parent cgroup under the hierarchy's root; None: the keeper's own.
    pub cgroup: Option<String>,
}

const VALUED: &[&str] = &[
    "config",
    "server",
    "listen",
    "kernel",
    "firecracker",
    "state-dir",
    "token",
    "token-file",
    "client-id",
    "ca-cert",
    "max-connections",
    "max-vms",
    "max-memory",
    "workers",
    "tunnel-stall-timeout",
    "drain-grace",
    "watch-interval",
    "flume",
    "flume-token",
    "flume-ca-cert",
    "jail-ids",
    "jail-user",
    "jail-cgroup",
    "console",
    "console-token",
    "console-dir",
    "oidc-issuer",
    "oidc-client-id",
    "oidc-client-secret",
    "oidc-claim",
    "oidc-scopes",
    "oidc-ca-cert",
    "vm",
];

/// Parses the arguments (without the program name) over the environment, and
/// the `--config` file they name.
pub fn parse(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Command, String> {
    let mut given: HashMap<&str, String> = HashMap::new();
    let mut insecure = false;
    let mut flume_insecure = false;
    let mut vms = Vec::new();
    let mut claims = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--insecure" => insecure = true,
            "--flume-insecure" => flume_insecure = true,
            _ => {
                let (name, inline) = match arg.strip_prefix("--").map(|rest| rest.split_once('=')) {
                    Some(Some((name, value))) => (name, Some(value.to_string())),
                    Some(None) => (&arg[2..], None),
                    None => return Err(format!("unexpected argument: {arg}")),
                };
                let Some(name) = VALUED.iter().find(|valued| **valued == name) else {
                    return Err(format!("unknown option: --{name}"));
                };
                let value = match inline {
                    Some(value) => value,
                    None => args
                        .next()
                        .ok_or_else(|| format!("--{name} needs a value"))?,
                };
                match *name {
                    "vm" => vms.push(value),
                    // repeatable: the rules add up
                    "oidc-claim" => claims.push(value),
                    name => {
                        given.insert(name, value);
                    }
                }
            }
        }
    }
    let flag = |name: &str| {
        given.get(name).cloned().or_else(|| {
            env(&format!(
                "SLUICE_WICKET_RUNTIME_FIRECRACKER_{}",
                name.to_uppercase().replace('-', "_")
            ))
            .filter(|value| !value.is_empty())
        })
    };
    let config = match flag("config") {
        Some(path) => {
            let path = std::path::absolute(path).map_err(|e| format!("--config: {e}"))?;
            Some((File::read(&path)?, path))
        }
        None => None,
    };
    let file = config.as_ref().map(|(file, _)| file);
    let dir = config
        .as_ref()
        .and_then(|(_, path)| path.parent())
        .unwrap_or(Path::new("."));
    // a path of the file is relative to it
    let in_file = |path: &Option<PathBuf>| path.as_ref().map(|path| dir.join(path));

    let positive = |name: &str, file: Option<u64>| match flag(name) {
        Some(value) => match value.parse::<u64>() {
            Ok(value) if value > 0 => Ok(Some(value)),
            _ => Err(format!("--{name} {value}: not a positive number")),
        },
        None => match file {
            Some(0) => Err(format!("{name}: must be positive")),
            value => Ok(value),
        },
    };
    let seconds = |name: &str, file: Option<&Scalar>, default: Duration| match flag(name) {
        Some(seconds) => seconds
            .parse()
            .map(Duration::from_secs)
            .map_err(|_| format!("--{name} {seconds}: not a number of seconds")),
        None => match file {
            Some(value) => value.duration(name),
            None => Ok(default),
        },
    };
    let max_connections = positive(
        "max-connections",
        file.and_then(|file| file.max_connections),
    )?
    .map_or(DEFAULT_MAX_CONNECTIONS, |max| max as usize);
    let max_vms = positive("max-vms", file.and_then(|file| file.max_vms))?
        .map_or(DEFAULT_MAX_VMS, |max| max as usize);
    let max_memory = positive("max-memory", file.and_then(|file| file.max_memory))?;
    let workers = positive("workers", file.and_then(|file| file.workers))?.map_or_else(
        || {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(DEFAULT_WORKERS)
        },
        |workers| workers as usize,
    );
    let server = flag("server").or_else(|| file.and_then(|file| file.server.clone()));
    let listen = flag("listen").or_else(|| file.and_then(|file| file.listen.clone()));
    if server.is_none() && listen.is_none() {
        return Err("--server or --listen is required".into());
    }
    let jail = match (
        flag("jail-ids").or_else(|| file.and_then(|file| file.jail_ids.clone())),
        flag("jail-user").or_else(|| file.and_then(|file| file.jail_user.clone())),
    ) {
        (Some(ids), Some(user)) => Some(JailSettings {
            user,
            ids: Ids::parse(&ids)?,
            cgroup: flag("jail-cgroup").or_else(|| file.and_then(|file| file.jail_cgroup.clone())),
        }),
        (None, None) => None,
        _ => return Err("--jail-ids and --jail-user go together (jail mode)".into()),
    };
    let routes = RouteSource {
        config: config.as_ref().map(|(_, path)| path.clone()),
        flags: vms,
        kernel: flag("kernel").map(PathBuf::from),
        firecracker: flag("firecracker").map(PathBuf::from),
        jailed: jail.is_some(),
        console: None,
    };
    let key = |key: &str| match &config {
        Some((_, path)) => format!("{}: {key}", path.display()),
        None => key.to_string(),
    };
    if claims.is_empty()
        && let Some(rules) = flag("oidc-claim")
    {
        claims.push(rules);
    }
    let console = ConsoleOptions::resolve(
        ConsoleOptions {
            console: flag("console"),
            console_token: flag("console-token"),
            console_dir: flag("console-dir").map(PathBuf::from),
            oidc_issuer: flag("oidc-issuer"),
            oidc_client_id: flag("oidc-client-id"),
            oidc_client_secret: flag("oidc-client-secret"),
            oidc_claim: (!claims.is_empty()).then_some(claims),
            oidc_scopes: flag("oidc-scopes"),
            oidc_ca_cert: flag("oidc-ca-cert").map(PathBuf::from),
        },
        match file {
            Some(file) => ConsoleOptions {
                console: file.console.clone(),
                console_token: file.console_token.clone(),
                console_dir: file.console_dir.clone(),
                oidc_issuer: file.oidc_issuer.clone(),
                oidc_client_id: file.oidc_client_id.clone(),
                oidc_client_secret: file.oidc_client_secret.clone(),
                oidc_claim: file.oidc_claim.clone().map(|rules| rules.0),
                oidc_scopes: file.oidc_scopes.clone(),
                oidc_ca_cert: file.oidc_ca_cert.clone(),
            },
            None => ConsoleOptions::default(),
        },
        dir,
        key,
    )?;
    let specs: Vec<RouteSpec> = routes.read()?.into_iter().map(|(spec, _)| spec).collect();
    if config.is_none() && specs.is_empty() && console.is_none() {
        return Err("--vm is required (or a --config file, or --console)".into());
    }
    // a token given, else the file holding it: the flag's (or environment's)
    // over the config file's
    let read = |path: PathBuf| {
        std::fs::read_to_string(&path)
            .map(|token| token.trim().to_string())
            .map_err(|e| format!("--token-file {}: {e}", path.display()))
    };
    let switch = |name: &str| {
        env(&format!("SLUICE_WICKET_RUNTIME_FIRECRACKER_{name}"))
            .is_some_and(|value| value == "true")
    };
    let flume = FlumeSettings::of(
        flag("flume").or_else(|| file.and_then(|file| file.flume.clone())),
        flag("flume-token").or_else(|| file.and_then(|file| file.flume_token.clone())),
        flag("flume-ca-cert")
            .map(PathBuf::from)
            .or_else(|| file.and_then(|file| in_file(&file.flume_ca_cert))),
        flume_insecure
            || switch("FLUME_INSECURE")
            || file.and_then(|file| file.flume_insecure).unwrap_or(false),
    )?;
    let token = match (flag("token"), flag("token-file"), file) {
        (Some(token), _, _) => token,
        (None, Some(path), _) => read(PathBuf::from(path))?,
        (None, None, Some(file)) => match (&file.token, in_file(&file.token_file)) {
            (Some(token), _) => token.clone(),
            (None, Some(path)) => read(path)?,
            (None, None) => String::new(),
        },
        (None, None, None) => String::new(),
    };
    Ok(Command::Run(Box::new(Settings {
        server,
        listen,
        token,
        client_id: flag("client-id")
            .or_else(|| file.and_then(|file| file.client_id.clone()))
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        insecure: insecure
            || switch("INSECURE")
            || file.and_then(|file| file.insecure).unwrap_or(false),
        ca_cert: flag("ca-cert")
            .map(PathBuf::from)
            .or_else(|| file.and_then(|file| in_file(&file.ca_cert))),
        max_connections,
        stall_timeout: seconds(
            "tunnel-stall-timeout",
            file.and_then(|file| file.tunnel_stall_timeout.as_ref()),
            DEFAULT_STALL_TIMEOUT,
        )?,
        drain_grace: seconds(
            "drain-grace",
            file.and_then(|file| file.drain_grace.as_ref()),
            DEFAULT_DRAIN_GRACE,
        )?,
        max_vms,
        max_memory,
        workers,
        watch_interval: seconds(
            "watch-interval",
            file.and_then(|file| file.watch_interval.as_ref()),
            DEFAULT_WATCH_INTERVAL,
        )?,
        state_dir: flag("state-dir")
            .map(PathBuf::from)
            .or_else(|| file.and_then(|file| in_file(&file.state_dir))),
        flume,
        jail,
        console,
        routes,
        specs,
    })))
}

/// Where a route comes from.
#[derive(Clone, PartialEq, Debug)]
pub enum Origin {
    /// The `--config` file.
    Config,
    /// A `--vm` flag.
    Flag,
    /// The console, which deployed it at `deployed` (RFC 3339).
    Console { deployed: String },
}

impl Origin {
    /// How the console's listing names it.
    pub fn name(&self) -> &'static str {
        match self {
            Origin::Config => "config",
            Origin::Flag => "flag",
            Origin::Console { .. } => "console",
        }
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Origin::Config => "the config file",
            Origin::Flag => "a --vm flag",
            Origin::Console { .. } => "the console",
        })
    }
}

/// Where the routes come from: the `--config` file's, then the `--vm` flags'.
pub struct RouteSource {
    config: Option<PathBuf>,
    flags: Vec<String>,
    /// `--kernel` / `--firecracker` (flag or environment), over the file's.
    kernel: Option<PathBuf>,
    firecracker: Option<PathBuf>,
    /// Jail mode: a route's `cpu` is its VMs' cgroups'.
    jailed: bool,
    /// The console's routes, with `--console`.
    console: Option<Deployed>,
}

impl RouteSource {
    /// The config file, if any.
    pub fn config(&self) -> Option<&Path> {
        self.config.as_deref()
    }

    /// The console's routes, with `--console`.
    pub fn deployed(&self) -> Option<&Deployed> {
        self.console.as_ref()
    }

    /// Takes `deployed` as the console's routes: the previous ones.
    pub fn deploy(&mut self, deployed: Deployed) -> Option<Deployed> {
        self.console.replace(deployed)
    }

    /// Whether a route's `cpu` is its VMs' cgroups' (jail mode).
    pub fn jailed(&self) -> bool {
        self.jailed
    }

    /// The firecracker binary as the routes name it now (a bare name is looked up on
    /// the PATH).
    pub fn firecracker(&self) -> Result<PathBuf, String> {
        let file = match &self.config {
            Some(path) => Some(File::read(path)?),
            None => None,
        };
        let dir = self
            .config
            .as_ref()
            .and_then(|path| path.parent())
            .unwrap_or(Path::new("."));
        self.firecracker_of(file.as_ref(), dir)
    }

    /// `--firecracker` (flag or environment), over the config `file`'s (relative to
    /// its `dir`), over `firecracker` on the PATH.
    fn firecracker_of(&self, file: Option<&File>, dir: &Path) -> Result<PathBuf, String> {
        let named = self.firecracker.clone().or_else(|| {
            file.and_then(|file| file.firecracker.as_ref()).map(|path| {
                match path.components().count() {
                    // a bare name is looked up on the PATH
                    1 => path.clone(),
                    _ => dir.join(path),
                }
            })
        });
        Ok(match named {
            Some(path) if path.components().count() > 1 => {
                std::path::absolute(path).map_err(|e| format!("--firecracker: {e}"))?
            }
            Some(name) => name,
            None => PathBuf::from("firecracker"),
        })
    }

    /// Reads the routes, the config file's again, then the flags', then the
    /// console's: a host may appear once.
    pub fn read(&self) -> Result<Vec<(RouteSpec, Origin)>, String> {
        let file = match &self.config {
            Some(path) => Some(File::read(path)?),
            None => None,
        };
        let dir = self
            .config
            .as_ref()
            .and_then(|path| path.parent())
            .unwrap_or(Path::new("."));
        let kernel = self
            .kernel
            .clone()
            .or_else(|| {
                file.as_ref()
                    .and_then(|file| file.kernel.as_ref())
                    .map(|kernel| dir.join(kernel))
            })
            .ok_or("--kernel is required")?;
        // absolute: each firecracker runs in its VM's directory
        let kernel = std::path::absolute(kernel).map_err(|e| format!("--kernel: {e}"))?;
        let firecracker = self.firecracker_of(file.as_ref(), dir)?;
        let mut specs = Vec::new();
        if let (Some(file), Some(path)) = (file, &self.config) {
            for (n, route) in file.routes.iter().enumerate() {
                let named = format!("{}: routes[{n}]", path.display());
                if route.deployed.is_some() {
                    return Err(format!("{named}: deployed is the console's (routes.yaml)"));
                }
                let rootfs = std::path::absolute(dir.join(&route.rootfs))
                    .map_err(|e| format!("{named}: {e}"))?;
                let options = route.options();
                let options = options.iter().map(|(key, value)| (*key, value.as_str()));
                specs.push((
                    RouteSpec::new(&named, &route.host, rootfs, options, &firecracker, &kernel)?,
                    Origin::Config,
                ));
            }
        }
        for flag in &self.flags {
            specs.push((RouteSpec::parse(flag, &firecracker, &kernel)?, Origin::Flag));
        }
        if let Some(console) = &self.console {
            specs.extend(console.specs(&firecracker, &kernel)?);
        }
        for (n, (spec, origin)) in specs.iter().enumerate() {
            if let Some((_, other)) = specs[..n].iter().find(|(other, _)| other.host == spec.host) {
                return Err(match other == origin {
                    true => format!("{}: routed twice", spec.host),
                    false => format!("{}: routed twice: by {other} and by {origin}", spec.host),
                });
            }
            if spec.vm.cpu.is_some() && !self.jailed {
                return Err(format!(
                    "{}: cpu needs jail mode (--jail-ids): its VMs' cgroups bound it",
                    spec.host
                ));
            }
        }
        Ok(specs)
    }
}

/// The `--config` file.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct File {
    server: Option<String>,
    listen: Option<String>,
    token: Option<String>,
    token_file: Option<PathBuf>,
    client_id: Option<String>,
    insecure: Option<bool>,
    ca_cert: Option<PathBuf>,
    kernel: Option<PathBuf>,
    firecracker: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    max_connections: Option<u64>,
    max_vms: Option<u64>,
    max_memory: Option<u64>,
    workers: Option<u64>,
    tunnel_stall_timeout: Option<Scalar>,
    drain_grace: Option<Scalar>,
    watch_interval: Option<Scalar>,
    flume: Option<String>,
    flume_token: Option<String>,
    flume_ca_cert: Option<PathBuf>,
    flume_insecure: Option<bool>,
    jail_ids: Option<String>,
    jail_user: Option<String>,
    jail_cgroup: Option<String>,
    console: Option<String>,
    console_token: Option<String>,
    /// Relative to the file.
    console_dir: Option<PathBuf>,
    oidc_issuer: Option<String>,
    oidc_client_id: Option<String>,
    oidc_client_secret: Option<String>,
    /// `<claim>=<pattern>` rules: one (comma-separated) or a list.
    oidc_claim: Option<Rules>,
    oidc_scopes: Option<String>,
    /// Relative to the file.
    oidc_ca_cert: Option<PathBuf>,
    #[serde(default)]
    routes: Vec<FileRoute>,
}

impl File {
    /// The file at `path`, `${NAME}` (or `${NAME:-default}`) taken from the
    /// environment.
    fn read(path: &Path) -> Result<File, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("--config {}: {e}", path.display()))?;
        let mut options =
            serde_saphyr::Options::default().with_properties(std::env::vars().collect());
        // an error names the line, never the file's text around it: the
        // tokens are no log's business
        options.with_snippet = false;
        serde_saphyr::from_str_with_options(&text, options).map_err(|e| {
            let e = e.to_string();
            let e = e.strip_prefix("error: ").unwrap_or(&e);
            format!("--config {}: {e}", path.display())
        })
    }
}

/// A route of the `--config` file: the keys of a `--vm` flag's query; one of
/// the console's (`deployed`) has the time it was deployed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct FileRoute {
    pub(crate) host: String,
    /// Relative to the file.
    pub(crate) rootfs: PathBuf,
    /// The console's routes only (`routes.yaml`).
    pub(crate) deployed: Option<String>,
    mode: Option<String>,
    vcpus: Option<u32>,
    mem: Option<u32>,
    port: Option<u32>,
    warm: Option<u32>,
    prime: Option<u32>,
    vms: Option<u32>,
    wait_ms: Option<u32>,
    keep_ms: Option<u32>,
    uses: Option<u32>,
    lifetime_ms: Option<u32>,
    conns: Option<u32>,
    cpu: Option<u32>,
    per_client: Option<u32>,
    idle_ms: Option<u32>,
    egress: Option<Vec<String>>,
}

impl FileRoute {
    /// The keys of [`FileRoute::options`], in their order.
    pub(crate) const KEYS: [&'static str; 16] = [
        "mode",
        "vcpus",
        "mem",
        "cpu",
        "port",
        "warm",
        "prime",
        "vms",
        "wait-ms",
        "keep-ms",
        "uses",
        "lifetime-ms",
        "conns",
        "per-client",
        "idle-ms",
        "egress",
    ];

    /// The options set, as a `--vm` flag's query has them, in the order of
    /// [`FileRoute::KEYS`].
    pub(crate) fn options(&self) -> Vec<(&'static str, String)> {
        let numbers = [
            ("vcpus", self.vcpus),
            ("mem", self.mem),
            ("cpu", self.cpu),
            ("port", self.port),
            ("warm", self.warm),
            ("prime", self.prime),
            ("vms", self.vms),
            ("wait-ms", self.wait_ms),
            ("keep-ms", self.keep_ms),
            ("uses", self.uses),
            ("lifetime-ms", self.lifetime_ms),
            ("conns", self.conns),
            ("per-client", self.per_client),
            ("idle-ms", self.idle_ms),
        ];
        self.mode
            .iter()
            .map(|mode| ("mode", mode.clone()))
            .chain(
                numbers
                    .into_iter()
                    .filter_map(|(key, value)| Some((key, value?.to_string()))),
            )
            .chain(self.egress.iter().map(|rules| ("egress", rules.join(","))))
            .collect()
    }
}

/// A YAML scalar taken as a duration.
#[derive(Deserialize)]
#[serde(untagged)]
enum Scalar {
    Number(u64),
    Text(String),
}

impl Scalar {
    /// A whole number with a unit (`ms`, `s`, `m`, `h`); a bare `0` is zero
    /// (off).
    fn duration(&self, key: &str) -> Result<Duration, String> {
        let text = match self {
            Scalar::Number(n) => n.to_string(),
            Scalar::Text(text) => text.trim().to_string(),
        };
        if text == "0" {
            return Ok(Duration::ZERO);
        }
        let expected = || {
            format!("{key}: expected a duration like 250ms, 20s, 5m or 1h (0 = off), got '{text}'")
        };
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(expected)?;
        let number: u64 = text[..split].parse().map_err(|_| expected())?;
        Ok(match &text[split..] {
            "ms" => Duration::from_millis(number),
            "s" => Duration::from_secs(number),
            "m" => Duration::from_secs(number.saturating_mul(60)),
            "h" => Duration::from_secs(number.saturating_mul(3600)),
            _ => return Err(expected()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(args: &[&str], env: &[(&str, &str)]) -> Result<Settings, String> {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        match parse(args.iter().map(|arg| arg.to_string()), |name| {
            env.get(name).cloned()
        })? {
            Command::Run(settings) => Ok(*settings),
            _ => Err("not a run".into()),
        }
    }

    /// A flag wins over the environment, the environment over the file, the
    /// file over the defaults; the file's paths are relative to it, its routes
    /// come before the flags', a host is routed once, a route's `cpu` needs jail
    /// mode.
    #[test]
    fn flags_win_over_the_environment_over_the_file_over_the_defaults() {
        let dir = std::env::temp_dir().join(format!("sfc-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rootfs = dir.join("app.ext4");
        std::fs::write(&rootfs, b"").unwrap();
        let config = dir.join("wicket.yaml");
        std::fs::write(
            &config,
            "server: grpc://file:8001\ntoken: from-file\nkernel: vmlinux\nmax-vms: 8\nworkers: 2\n\
             drain-grace: 5s\nflume: grpc://file:8002\nflume-token: from-file\n\
             flume-ca-cert: flume-ca.pem\njail-ids: 900000-900063\njail-user: from-file\n\
             console: 127.0.0.1:9090\nconsole-dir: kept\noidc-claim: [email=*]\n\
             routes:\n  - {host: a.local, rootfs: app.ext4, mode: per-connection, warm: 2, cpu: 50,\n     \
             per-client: 4, idle-ms: 30000, egress: [DB.internal:5432, '[2001:db8::/32]:443',\n     \
             10.0.0.0/24:6379]}\n",
        )
        .unwrap();
        let vm = format!(
            "b.local={}?mode=pool&vms=4&warm=1&conns=8&keep-ms=60000",
            rootfs.display()
        );
        let config_flag = config.display().to_string();

        let settings = run(
            &["--config", &config_flag, "--vm", &vm],
            &[
                ("SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN", "from-env"),
                ("SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_TOKEN", "flume-env"),
                ("SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_INSECURE", "true"),
                ("SLUICE_WICKET_RUNTIME_FIRECRACKER_JAIL_USER", "from-env"),
            ],
        )
        .unwrap();
        assert_eq!(settings.server.as_deref(), Some("grpc://file:8001"));
        assert_eq!(settings.token, "from-env");
        assert_eq!(
            settings.jail,
            Some(JailSettings {
                user: "from-env".into(),
                ids: Ids {
                    first: 900000,
                    last: 900063
                },
                cgroup: None,
            })
        );
        assert_eq!(
            settings.flume,
            Some(FlumeSettings {
                url: "grpc://file:8002".into(),
                token: "flume-env".into(),
                ca_cert: Some(dir.join("flume-ca.pem")),
                insecure: true,
            })
        );
        let console = settings.console.as_ref().expect("the file's console");
        assert_eq!(
            (
                console.address.as_str(),
                &console.dir,
                console.token.as_deref()
            ),
            ("127.0.0.1:9090", &dir.join("kept"), None),
            "its directory relative to the file"
        );
        assert_eq!(settings.max_vms, 8);
        assert_eq!(settings.workers, 2);
        assert_eq!(settings.drain_grace, Duration::from_secs(5));
        assert_eq!(settings.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(settings.stall_timeout, DEFAULT_STALL_TIMEOUT);
        let hosts: Vec<_> = settings.specs.iter().map(|spec| &spec.host).collect();
        assert_eq!(hosts, ["a.local", "b.local"]);
        assert_eq!(settings.specs[0].vm.kernel, dir.join("vmlinux"));
        assert_eq!(
            settings.specs[0].target(),
            format!(
                "firecracker:{}?mode=per-connection&cpu=50&warm=2&per-client=4&idle-ms=30000\
                 &egress=db.internal:5432,10.0.0.0/24:6379,[2001:db8::/32]:443",
                rootfs.display()
            ),
            "the options set to other than their defaults, the egress in one order"
        );
        assert_eq!(
            settings.specs[1].target(),
            format!(
                "firecracker:{}?mode=pool&warm=1&vms=4&conns=8",
                rootfs.display()
            ),
            "a pool's warm by default its vms"
        );

        let settings = run(
            &[
                "--config",
                &config_flag,
                "--token",
                "flag",
                "--max-vms=4",
                "--workers=3",
                "--flume=grpcs://flag:8002",
                "--flume-ca-cert",
                "ca.pem",
                "--flume-insecure",
                "--jail-ids",
                "1-2",
                "--jail-cgroup",
                "/sluice",
                "--console-token",
                "s3cret",
                "--oidc-issuer",
                "https://idp.example",
                "--oidc-client-id",
                "wicket",
                "--oidc-claim",
                "email=*@example.com",
                "--oidc-claim",
                "groups=devs",
            ],
            &[("SLUICE_WICKET_RUNTIME_FIRECRACKER_TOKEN", "from-env")],
        )
        .unwrap();
        let console = settings.console.as_ref().expect("the file's console");
        assert_eq!(console.token.as_deref(), Some("s3cret"));
        let oidc = console.oidc.as_ref().expect("a provider");
        assert_eq!(
            oidc.claims.len(),
            2,
            "the flags' rules add up, over the file's"
        );
        assert_eq!(settings.token, "flag");
        assert_eq!(settings.max_vms, 4);
        assert_eq!(settings.workers, 3);
        assert_eq!(
            settings.jail,
            Some(JailSettings {
                user: "from-file".into(),
                ids: Ids { first: 1, last: 2 },
                cgroup: Some("/sluice".into()),
            })
        );
        assert_eq!(
            settings.flume,
            Some(FlumeSettings {
                url: "grpcs://flag:8002".into(),
                token: "from-file".into(),
                ca_cert: Some("ca.pem".into()),
                insecure: true,
            })
        );

        // a token file: the flag's over the file's, relative to the config file
        std::fs::write(dir.join("token"), "from-token-file\n").unwrap();
        std::fs::write(
            dir.join("by-file.yaml"),
            "server: s\nkernel: k\ntoken-file: token\nroutes: []\n",
        )
        .unwrap();
        let by_file = dir.join("by-file.yaml").display().to_string();
        assert_eq!(
            run(&["--config", &by_file], &[]).unwrap().token,
            "from-token-file"
        );
        let token_file = dir.join("token").display().to_string();
        let settings = run(
            &["--config", &config_flag, "--token-file", &token_file],
            &[],
        )
        .unwrap();
        assert_eq!(settings.token, "from-token-file");

        let twice = format!("a.local={}", rootfs.display());
        let cpu = format!("c.local={}?cpu=50", rootfs.display());
        let no_share = format!("c.local={}?per-client=0", rootfs.display());
        let no_port = format!("c.local={}?egress=db.internal", rootfs.display());
        let egress: Vec<String> = (0..64).map(|n| format!("db-{n}.internal:5432")).collect();
        let too_long = format!("c.local={}?egress={}", rootfs.display(), egress.join(","));
        let shared_warm = format!("c.local={}?warm=1", rootfs.display());
        let cloned_uses = format!("c.local={}?mode=per-connection&uses=2", rootfs.display());
        let unsized_pool = format!("c.local={}?mode=pool&warm=1", rootfs.display());
        for (args, error) in [
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &shared_warm],
                "warm needs mode=per-connection, mode=reuse, mode=pool",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &cloned_uses],
                "uses needs mode=reuse",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &unsized_pool],
                "mode=pool needs vms",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &cpu],
                "c.local: cpu needs jail mode",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &no_share],
                "per-client must be at least 1",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &no_port],
                "egress db.internal: expected <host>:<port>",
            ),
            (
                vec!["--server", "s", "--kernel", "k", "--vm", &too_long],
                "egress of 1269 bytes: a guest's kernel command line takes 1024",
            ),
            (vec!["--kernel", "k", "--vm", &vm], "--server or --listen"),
            (vec!["--server", "s", "--vm", &vm], "--kernel is required"),
            (vec!["--server", "s", "--kernel", "k"], "--vm is required"),
            (
                vec![
                    "--server",
                    "s",
                    "--kernel",
                    "k",
                    "--vm",
                    &vm,
                    "--oidc-issuer",
                    "https://idp.example",
                    "--oidc-client-id",
                    "wicket",
                    "--oidc-claim",
                    "email=*",
                ],
                "oidc-issuer needs --console",
            ),
            (
                vec!["--config", &config_flag, "--vm", "c.local=missing"],
                "missing: No such file or directory",
            ),
            (
                vec!["--config", &config_flag, "--vm", &twice],
                "a.local: routed twice",
            ),
            (
                vec!["--config", &config_flag, "--token-file", "missing"],
                "--token-file missing:",
            ),
            (
                vec![
                    "--server", "s", "--kernel", "k", "--vm", &vm, "--flume", "f:1",
                ],
                "--flume: expected grpc://host:port or grpcs://host:port, got 'f:1'",
            ),
            (
                vec![
                    "--server",
                    "s",
                    "--kernel",
                    "k",
                    "--vm",
                    &vm,
                    "--flume",
                    "grpc://f:1",
                ],
                "--flume-token (or `flume-token` in the --config file) is required",
            ),
            (
                vec![
                    "--server",
                    "s",
                    "--kernel",
                    "k",
                    "--vm",
                    &vm,
                    "--jail-ids",
                    "1-2",
                ],
                "--jail-ids and --jail-user go together",
            ),
            // ids are neither root nor -1, first to last
            (
                vec!["--config", &config_flag, "--jail-ids", "0-10"],
                "jail-ids 0-10: expected <first>-<last>",
            ),
            (
                vec!["--config", &config_flag, "--jail-ids", "10-9"],
                "jail-ids 10-9: expected",
            ),
            (
                vec!["--config", &config_flag, "--jail-ids", "1-4294967295"],
                "jail-ids 1-4294967295: expected",
            ),
        ] {
            // no Debug for the settings: the token is no output's business
            let Err(failed) = run(&args, &[]) else {
                panic!("{args:?}: no error");
            };
            assert!(failed.contains(error), "{args:?}: {failed}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
