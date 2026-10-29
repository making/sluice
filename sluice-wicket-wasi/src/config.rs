//! The settings: the command line, the environment and the `--config` YAML
//! file, merged -- an option `--name <value>` overrides `SLUICE_WICKET_WASI_<NAME>`
//! (upper case, `-` as `_`), which overrides the file, the file a default. The
//! file's routes are read again by every reload (`routes::Reloader`); its other
//! settings apply at startup only, and a reload reports the ones changed since.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::deployed::Deployed;
use crate::env::Env;
use crate::keyvalue::{self, StoreConfig, StoreLimits, Stores};
use crate::routes::{Origin, Spec};
use crate::wasm_host::{Caps, Instances, Limits, Locator, Sandbox};

pub const USAGE: &str = "\
usage: sluice-wicket-wasi --server <url> --wasm <host>=<locator>[?<options>] ... [options]
       sluice-wicket-wasi --listen <host:port> --wasm <host>=<locator>[?<options>] ... [options]
       sluice-wicket-wasi --config <file> [options]

  --server <url>                  bootstrap endpoint, grpc://host:port or grpcs://host:port
  --listen <host:port>            also (or only, without --server) serve the routes over plain
                                  TCP, http/1.1 and h2c, routed by the request's host
  --wasm <host>=<locator>         a route: a component path, file:// or http(s):// url;
                                  repeatable. ?budget-ms=N&instances=N&memory-mib=N&pool=N
                                  &wasi=<cap>,... sets its sandbox (wasi=keyvalue: the
                                  default store, namespaced by the host)
  --config <file>                 YAML settings and routes, the routes reloaded on change
  --console <host:port>           serve the console: a page and an API deploying uploaded
                                  components as routes, kept in --console-dir
  --console-token <token>         the console's credentials (the HTTP Basic password, or a
                                  Bearer token); required on a non-loopback address
  --console-dir <path>            where the console keeps its routes and components
                                  (default deployed, next to the --config file)
  --keyvalue-file <path>          the SQLite file of the keyvalue store without a keyvalue
                                  section in the --config file (default keyvalue.db)
  --token <token>                 bearer token of the tunnel streams
  --client-id <id>                client identity (default a random UUID)
  --ca-cert <path>                PEM CA verifying a grpcs:// server (default the webPKI roots)
  --insecure                      skip the verification of a grpcs:// server
  --flume <url>                   feed the guests' output and the requests answered to a
                                  sluice-flume while someone watches (the live access log),
                                  grpc://host:port or grpcs://host:port
  --flume-token <token>           the flume's bearer token (required with --flume)
  --flume-ca-cert <path>          PEM CA verifying a grpcs:// flume (default the webPKI roots)
  --flume-insecure                skip the verification of a grpcs:// flume
  --max-connections <n>           connections open at once (default 256)
  --max-instances <n>             guest instances alive at once (default 64)
  --tunnel-stall-timeout <secs>   give a stream up once its frames wait that long untaken
                                  (default 60, 0 = off)
  --drain-grace <secs>            the SIGTERM drain's bound (default 20, 0 = no drain)
  --watch-interval <secs>         poll the routes' files (default 1, 0 = SIGHUP only)
  --version                       print the build (version, commit, build time) and exit

Each option but --wasm also reads SLUICE_WICKET_WASI_<NAME> from the environment, e.g.
SLUICE_WICKET_WASI_TOKEN.";

/// Default of `--max-connections`: 256 connections x one 1MiB receive window
/// (plus at most two partly used 16KiB segments and 80 segments' overhead
/// each) = about 265MiB of the server's payload buffered at most.
pub const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Default of `--tunnel-stall-timeout`, the Java side's
/// `sluice.client.tunnel-stall-timeout`.
const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(60);

/// Default of `--drain-grace`, the Java side's `sluice.client.drain-grace`.
const DEFAULT_DRAIN_GRACE: Duration = Duration::from_secs(20);

/// Default of `--max-instances`: 64 guest instances alive at once, i.e. at
/// most 64 x the largest `memory-mib` of guest linear memory (16GiB at the
/// default 256MiB).
const DEFAULT_MAX_INSTANCES: usize = 64;

/// Default of `--watch-interval`: how often the files the routes come from
/// are polled for a change.
const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// The file of the keyvalue store without a `keyvalue` section (and without
/// `--keyvalue-file`): next to the config file, else in the working
/// directory.
const DEFAULT_KEYVALUE_FILE: &str = "keyvalue.db";

/// The console's directory without `--console-dir`: next to the config file,
/// else in the working directory.
const DEFAULT_CONSOLE_DIR: &str = "deployed";

pub struct Settings {
    /// None: local mode alone (`listen`).
    pub server: Option<String>,
    pub listen: Option<String>,
    pub token: String,
    pub client_id: String,
    pub insecure: bool,
    pub ca_cert: Option<PathBuf>,
    pub max_connections: usize,
    pub max_instances: usize,
    pub stall_timeout: Duration,
    pub drain_grace: Duration,
    /// Zero: the files are not polled; only SIGHUP reloads.
    pub watch_interval: Duration,
    pub console: Option<ConsoleSettings>,
    pub flume: Option<FlumeSettings>,
    pub routes: RouteSource,
}

/// The flume the wicket feeds its live access log to (`--flume`).
#[derive(Clone, Debug, PartialEq)]
pub struct FlumeSettings {
    /// `grpc://host:port` or `grpcs://host:port`.
    pub url: String,
    /// Its bearer token.
    pub token: String,
    /// The PEM CA verifying a `grpcs://` flume; None: the webPKI roots.
    pub ca_cert: Option<PathBuf>,
    /// Skip the verification of a `grpcs://` flume.
    pub insecure: bool,
}

/// The console (`--console`).
pub struct ConsoleSettings {
    /// Where it listens, `host:port`.
    pub address: String,
    /// The credentials it asks for; None: none, on a loopback address only.
    pub token: Option<String>,
    /// Where it keeps the routes it deployed.
    pub dir: PathBuf,
}

/// What to run: print something and exit, or run.
pub enum Command {
    Help,
    Version,
    Run(Box<Settings>),
}

/// The command line over the environment, each setting None / empty when
/// given by neither.
#[derive(Default)]
struct Flags {
    config: Option<PathBuf>,
    server: Option<String>,
    listen: Option<String>,
    token: Option<String>,
    client_id: Option<String>,
    insecure: bool,
    ca_cert: Option<PathBuf>,
    max_connections: Option<usize>,
    max_instances: Option<usize>,
    stall_timeout: Option<Duration>,
    drain_grace: Option<Duration>,
    watch_interval: Option<Duration>,
    keyvalue_file: Option<PathBuf>,
    console: Option<String>,
    console_token: Option<String>,
    console_dir: Option<PathBuf>,
    flume: Option<String>,
    flume_token: Option<String>,
    flume_ca_cert: Option<PathBuf>,
    flume_insecure: bool,
    routes: Vec<Spec>,
}

const VALUED: &[&str] = &[
    "config",
    "server",
    "listen",
    "token",
    "client-id",
    "ca-cert",
    "max-connections",
    "max-instances",
    "tunnel-stall-timeout",
    "drain-grace",
    "watch-interval",
    "keyvalue-file",
    "console",
    "console-token",
    "console-dir",
    "flume",
    "flume-token",
    "flume-ca-cert",
    "wasm",
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
    let mut routes = Vec::new();
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
                    "wasm" => routes.push(parse_wasm_flag(&value)?),
                    name => {
                        given.insert(name, value);
                    }
                }
            }
        }
    }
    let value = |name: &str| {
        given.get(name).cloned().or_else(|| {
            env(&format!(
                "SLUICE_WICKET_WASI_{}",
                name.to_uppercase().replace('-', "_")
            ))
            .filter(|value| !value.is_empty())
        })
    };
    let limit = |name: &str, max: usize| {
        value(name)
            .map(|limit| parse_limit(name, &limit, max))
            .transpose()
    };
    let seconds = |name: &str| {
        value(name)
            .map(|seconds| parse_seconds(name, &seconds))
            .transpose()
    };
    let flags = Flags {
        config: value("config").map(PathBuf::from),
        server: value("server"),
        listen: value("listen"),
        token: value("token"),
        client_id: value("client-id"),
        insecure: insecure
            || env("SLUICE_WICKET_WASI_INSECURE").is_some_and(|value| value == "true"),
        ca_cert: value("ca-cert").map(PathBuf::from),
        max_connections: limit("max-connections", usize::MAX)?,
        max_instances: limit("max-instances", Instances::MAX)?,
        stall_timeout: seconds("tunnel-stall-timeout")?,
        drain_grace: seconds("drain-grace")?,
        watch_interval: seconds("watch-interval")?,
        keyvalue_file: value("keyvalue-file").map(PathBuf::from),
        console: value("console"),
        console_token: value("console-token"),
        console_dir: value("console-dir").map(PathBuf::from),
        flume: value("flume"),
        flume_token: value("flume-token"),
        flume_ca_cert: value("flume-ca-cert").map(PathBuf::from),
        flume_insecure: flume_insecure
            || env("SLUICE_WICKET_WASI_FLUME_INSECURE").is_some_and(|value| value == "true"),
        routes,
    };
    merge(flags).map(|settings| Command::Run(Box::new(settings)))
}

/// `host=locator[?query]` of a `--wasm` flag.
fn parse_wasm_flag(spec: &str) -> Result<Spec, String> {
    let (host, target) = spec
        .split_once('=')
        .ok_or_else(|| format!("--wasm must be host=component-locator, got '{spec}'"))?;
    let (target, sandbox) =
        parse_route_target(target).map_err(|e| format!("--wasm {spec}: {e}"))?;
    let component = Locator::parse(&target).map_err(|e| format!("--wasm {spec}: {e}"))?;
    Ok(Spec {
        host: host.to_string(),
        component,
        shown: None,
        sandbox,
        origin: Origin::Flag,
    })
}

/// Settles every setting: flag, else file, else default.
fn merge(flags: Flags) -> Result<Settings, String> {
    let file = match &flags.config {
        Some(path) => Some(ConfigFile::read(path)?),
        None => None,
    };
    let none = File::default();
    let (settings, dir) = match &file {
        Some(file) => (&file.settings, file.dir()),
        None => (&none, PathBuf::new()),
    };
    let key = |key: &str| match &flags.config {
        Some(path) => format!("{}: {key}", path.display()),
        None => key.to_string(),
    };
    let server = flags
        .server
        .or_else(|| settings.server.clone())
        .filter(|server| !server.is_empty());
    let listen = flags
        .listen
        .or_else(|| settings.listen.clone())
        .filter(|listen| !listen.is_empty());
    if server.is_none() && listen.is_none() {
        return Err(
            "--server or --listen (or `server` / `listen` in the --config file) is required".into(),
        );
    }
    let console = flags
        .console
        .or_else(|| settings.console.clone())
        .filter(|console| !console.is_empty())
        .map(|address| ConsoleSettings {
            address,
            token: flags
                .console_token
                .or_else(|| settings.console_token.clone())
                .filter(|token| !token.is_empty()),
            dir: flags
                .console_dir
                .or_else(|| settings.console_dir.as_ref().map(|path| dir.join(path)))
                .unwrap_or_else(|| dir.join(DEFAULT_CONSOLE_DIR)),
        });
    if flags.config.is_none() && flags.routes.is_empty() && console.is_none() {
        return Err(
            "at least one --wasm host=locator (or a --config file, or --console) is required"
                .into(),
        );
    }
    let max_connections = match (flags.max_connections, settings.max_connections) {
        (Some(limit), _) => limit,
        (None, Some(limit)) => in_range(&key("max-connections"), limit, usize::MAX)?,
        (None, None) => DEFAULT_MAX_CONNECTIONS,
    };
    let max_instances = match (flags.max_instances, settings.max_instances) {
        (Some(limit), _) => limit,
        (None, Some(limit)) => in_range(&key("max-instances"), limit, Instances::MAX)?,
        (None, None) => DEFAULT_MAX_INSTANCES,
    };
    let duration =
        |flag: Option<Duration>, file: &Option<Scalar>, name: &str, default| match (flag, file) {
            (Some(duration), _) => Ok(duration),
            (None, Some(value)) => value.duration(&key(name), true),
            (None, None) => Ok(default),
        };
    let stall_timeout = duration(
        flags.stall_timeout,
        &settings.tunnel_stall_timeout,
        "tunnel-stall-timeout",
        DEFAULT_STALL_TIMEOUT,
    )?;
    let drain_grace = duration(
        flags.drain_grace,
        &settings.drain_grace,
        "drain-grace",
        DEFAULT_DRAIN_GRACE,
    )?;
    let watch_interval = duration(
        flags.watch_interval,
        &settings.watch_interval,
        "watch-interval",
        DEFAULT_WATCH_INTERVAL,
    )?;
    let token = flags
        .token
        .or_else(|| settings.token.clone())
        .unwrap_or_default();
    // same default policy as the Java client: random id unless set
    let client_id = flags
        .client_id
        .or_else(|| settings.client_id.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let insecure = flags.insecure || settings.insecure.unwrap_or(false);
    let ca_cert = flags
        .ca_cert
        .or_else(|| settings.ca_cert.as_ref().map(|path| dir.join(path)));
    let flume = match flags
        .flume
        .or_else(|| settings.flume.clone())
        .filter(|flume| !flume.is_empty())
    {
        Some(url) => {
            if !url.starts_with("grpc://") && !url.starts_with("grpcs://") {
                return Err(format!(
                    "--flume: expected grpc://host:port or grpcs://host:port, got '{url}'"
                ));
            }
            let token = flags
                .flume_token
                .or_else(|| settings.flume_token.clone())
                .filter(|token| !token.is_empty())
                .ok_or("--flume-token (or `flume-token` in the --config file) is required with --flume")?;
            Some(FlumeSettings {
                url,
                token,
                ca_cert: flags
                    .flume_ca_cert
                    .or_else(|| settings.flume_ca_cert.as_ref().map(|path| dir.join(path))),
                insecure: flags.flume_insecure || settings.flume_insecure.unwrap_or(false),
            })
        }
        None => None,
    };
    let stores =
        stores(settings.keyvalue.as_ref(), flags.keyvalue_file, &dir).map_err(|e| key(&e))?;
    let routes = RouteSource {
        config: file,
        flags: flags.routes,
        deployed: None,
        stores: Arc::new(stores),
    };
    Ok(Settings {
        server,
        listen,
        token,
        client_id,
        insecure,
        ca_cert,
        max_connections,
        max_instances,
        stall_timeout,
        drain_grace,
        watch_interval,
        console,
        flume,
        routes,
    })
}

/// Where the routes come from: the `--config` file's, then the `--wasm`
/// flags', then the console's; and the keyvalue stores their maps may name.
pub struct RouteSource {
    config: Option<ConfigFile>,
    flags: Vec<Spec>,
    /// The console's routes, with a console.
    deployed: Option<Deployed>,
    stores: Arc<Stores>,
}

impl RouteSource {
    /// Routes of the flags alone.
    #[cfg(test)]
    pub fn flags(flags: Vec<Spec>) -> RouteSource {
        RouteSource {
            config: None,
            flags,
            deployed: None,
            stores: Arc::new(stores(None, None, Path::new("")).expect("the default store")),
        }
    }

    /// Routes of the config file at `path` alone, on its stores.
    #[cfg(test)]
    pub fn file(path: &Path) -> Result<RouteSource, String> {
        let config = ConfigFile::read(path)?;
        let stores = stores(config.settings.keyvalue.as_ref(), None, &config.dir())?;
        Ok(RouteSource {
            config: Some(config),
            flags: Vec::new(),
            deployed: None,
            stores: Arc::new(stores),
        })
    }

    /// The console's routes, if any.
    pub fn deployed(&self) -> Option<&Deployed> {
        self.deployed.as_ref()
    }

    /// Takes `deployed` as the console's routes; the ones before, if any.
    pub fn deploy(&mut self, deployed: Deployed) -> Option<Deployed> {
        self.deployed.replace(deployed)
    }

    /// The keyvalue stores.
    pub fn stores(&self) -> &Arc<Stores> {
        &self.stores
    }

    /// The config file, if any.
    pub fn config(&self) -> Option<&Path> {
        self.config.as_ref().map(|file| file.path.as_path())
    }

    /// Reads the routes, the config file's again: a host may appear once.
    /// The warnings name the settings besides the routes that changed in the
    /// file since startup.
    pub fn read(&self) -> Result<(Vec<Spec>, Vec<String>), String> {
        let mut specs = Vec::new();
        let mut warnings = Vec::new();
        if let Some(config) = &self.config {
            let mut file = File::read(&config.path)?;
            let routes = std::mem::take(&mut file.routes);
            let changed = config.settings.changed(&file);
            if !changed.is_empty() {
                warnings.push(format!(
                    "{}: {} changed since startup; applies on a restart",
                    config.path.display(),
                    changed.join(", ")
                ));
            }
            let dir = config.dir();
            for (n, route) in routes.into_iter().enumerate() {
                specs.push(
                    route
                        .spec(&dir, &self.stores)
                        .map_err(|e| format!("{}: routes[{n}]: {e}", config.path.display()))?,
                );
            }
        }
        for flag in &self.flags {
            let mut spec = flag.clone();
            if spec.sandbox.caps.keyvalue {
                spec.sandbox.keyvalue = self.stores.default_map(&spec.host);
            }
            specs.push(spec);
        }
        if let Some(deployed) = &self.deployed {
            specs.extend(deployed.specs(&self.stores));
        }
        let mut hosts: HashMap<&str, &Origin> = HashMap::new();
        for spec in &specs {
            if let Some(first) = hosts.insert(&spec.host, &spec.origin) {
                let by = if std::mem::discriminant(first) == std::mem::discriminant(&spec.origin) {
                    format!("by {first}")
                } else {
                    format!("by {first} and by {}", spec.origin)
                };
                return Err(format!(
                    "route host '{}' is configured twice: {by}",
                    spec.host
                ));
            }
        }
        Ok((specs, warnings))
    }
}

/// The `--config` file as read at startup.
struct ConfigFile {
    path: PathBuf,
    /// Its settings besides the routes.
    settings: File,
}

impl ConfigFile {
    fn read(path: &Path) -> Result<ConfigFile, String> {
        let mut settings = File::read(path)?;
        settings.routes.clear();
        Ok(ConfigFile {
            path: path.to_path_buf(),
            settings,
        })
    }

    /// The directory relative paths in the file are resolved against.
    fn dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default()
    }
}

/// The YAML file: every key optional; `${NAME}` in a plain scalar is the
/// environment variable `NAME` (`${NAME:-default}` and the other forms of
/// docker compose; unset without a default is an error, `$${` a literal).
#[derive(Deserialize, Default, PartialEq, Debug)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct File {
    server: Option<String>,
    listen: Option<String>,
    token: Option<String>,
    client_id: Option<String>,
    insecure: Option<bool>,
    ca_cert: Option<PathBuf>,
    max_connections: Option<usize>,
    max_instances: Option<usize>,
    tunnel_stall_timeout: Option<Scalar>,
    drain_grace: Option<Scalar>,
    watch_interval: Option<Scalar>,
    keyvalue: Option<KeyValueFile>,
    console: Option<String>,
    console_token: Option<String>,
    console_dir: Option<PathBuf>,
    flume: Option<String>,
    flume_token: Option<String>,
    flume_ca_cert: Option<PathBuf>,
    flume_insecure: Option<bool>,
    #[serde(default)]
    routes: Vec<RouteFile>,
}

impl File {
    fn read(path: &Path) -> Result<File, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("--config {}: {e}", path.display()))?;
        let mut options =
            serde_saphyr::Options::default().with_properties(std::env::vars().collect());
        // an error names the line, never the file's text around it: the
        // routes' env and the tokens are no log's business
        options.with_snippet = false;
        serde_saphyr::from_str_with_options(&text, options).map_err(|e| {
            let e = e.to_string();
            let e = e.strip_prefix("error: ").unwrap_or(&e);
            format!("--config {}: {e}", path.display())
        })
    }

    /// The keys besides `routes` whose value differs in `other`.
    fn changed(&self, other: &File) -> Vec<&'static str> {
        let mut changed = Vec::new();
        macro_rules! compare {
            ($($field:ident => $key:literal),* $(,)?) => {
                $(if self.$field != other.$field {
                    changed.push($key);
                })*
            };
        }
        compare!(
            server => "server",
            listen => "listen",
            token => "token",
            client_id => "client-id",
            insecure => "insecure",
            ca_cert => "ca-cert",
            max_connections => "max-connections",
            max_instances => "max-instances",
            tunnel_stall_timeout => "tunnel-stall-timeout",
            drain_grace => "drain-grace",
            watch_interval => "watch-interval",
            keyvalue => "keyvalue",
            console => "console",
            console_token => "console-token",
            console_dir => "console-dir",
            flume => "flume",
            flume_token => "flume-token",
            flume_ca_cert => "flume-ca-cert",
            flume_insecure => "flume-insecure",
        );
        changed
    }
}

#[derive(Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct RouteFile {
    host: String,
    /// A path (relative to the config file), `file://` or `http(s)://` url.
    component: String,
    #[serde(default)]
    sandbox: SandboxFile,
}

impl RouteFile {
    fn spec(self, dir: &Path, stores: &Stores) -> Result<Spec, String> {
        let component = match Locator::parse(&self.component)? {
            Locator::File(path) if path.is_relative() && !self.component.contains("://") => {
                Locator::File(dir.join(path))
            }
            locator => locator,
        };
        let sandbox = self.sandbox.sandbox(&self.host, stores)?;
        Ok(Spec {
            host: self.host,
            component,
            shown: None,
            sandbox,
            origin: Origin::Config,
        })
    }
}

/// A route's sandbox; unset keys keep the defaults. The console's routes and
/// apps are written in this shape too ([`sandbox_yaml`]).
#[derive(Deserialize, Default, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct SandboxFile {
    /// WASI capabilities (`wasi=` of the flag).
    wasi: Option<Vec<String>>,
    /// The request budget, a duration (`250ms`).
    budget: Option<Scalar>,
    /// The per-instance memory cap, whole MiB (`64Mi`).
    memory: Option<Scalar>,
    pool: Option<usize>,
    instances: Option<usize>,
    /// Keyvalue identifier -> `store[/namespace]` (`wasi: [keyvalue]`); the
    /// namespace defaults to the host, the map to `"": <default store>`.
    keyvalue: Option<BTreeMap<String, String>>,
    /// The guest's environment variables: name -> value.
    env: Option<BTreeMap<String, String>>,
}

impl SandboxFile {
    /// The sandbox but its keyvalue map, and the map named (None: the
    /// default one).
    pub(crate) fn split(self) -> Result<(Sandbox, Option<BTreeMap<String, String>>), String> {
        let SandboxFile {
            wasi,
            budget,
            memory,
            pool,
            instances,
            keyvalue,
            env,
        } = self;
        let mut sandbox = Sandbox::default();
        if let Some(wasi) = wasi {
            sandbox.caps =
                Caps::parse(&wasi.join(",")).map_err(|e| format!("sandbox.wasi: {e}"))?;
        }
        if let Some(budget) = budget {
            let budget = budget.duration("sandbox.budget", false)?;
            if budget.subsec_nanos() % 1_000_000 != 0 {
                return Err(format!(
                    "sandbox.budget: must be whole milliseconds, got {budget:?}"
                ));
            }
            sandbox.limits.budget = budget;
        }
        if let Some(memory) = memory {
            sandbox.limits.memory = memory.mebibytes("sandbox.memory")?;
        }
        sandbox.pool = pool.unwrap_or(0);
        sandbox.instances = match instances {
            Some(0) => return Err("sandbox.instances: must be at least 1".into()),
            cap => cap,
        };
        if let Some(env) = env {
            sandbox.env = Env::new(env).map_err(|e| format!("sandbox.env: {e}"))?;
        }
        sandbox.check().map_err(|e| format!("sandbox: {e}"))?;
        Ok((sandbox, keyvalue))
    }

    fn sandbox(self, host: &str, stores: &Stores) -> Result<Sandbox, String> {
        let (mut sandbox, map) = self.split()?;
        sandbox.keyvalue = match (map, sandbox.caps.keyvalue) {
            (None, false) => keyvalue::BucketMap::new(),
            (Some(_), false) => {
                return Err("sandbox.keyvalue: needs the capability (wasi: [keyvalue])".into());
            }
            (None, true) => stores.default_map(host),
            (Some(map), true) if map.is_empty() => {
                return Err("sandbox.keyvalue: maps no identifier".into());
            }
            (Some(map), true) => map
                .into_iter()
                .map(|(identifier, place)| {
                    let place = stores
                        .place(host, &place)
                        .map_err(|e| format!("sandbox.keyvalue.\"{identifier}\": {e}"))?;
                    Ok((identifier, place))
                })
                .collect::<Result<_, String>>()?,
        };
        Ok(sandbox)
    }
}

/// `sandbox` in the shape [`SandboxFile`] reads, as a YAML flow mapping of
/// its settings besides the defaults (None when all defaults); its keyvalue
/// map with `map` (an app's, every place named in full), else none, the
/// default one applying; its env as it is (the file holding it is private).
pub(crate) fn sandbox_yaml(sandbox: &Sandbox, map: bool) -> Option<String> {
    let defaults = Limits::default();
    let mut pairs = Vec::new();
    let caps = sandbox.caps.names();
    if !caps.is_empty() {
        pairs.push(format!("wasi: [{}]", caps.join(", ")));
    }
    if sandbox.limits.budget != defaults.budget {
        pairs.push(format!("budget: {}ms", sandbox.limits.budget.as_millis()));
    }
    if sandbox.limits.memory != defaults.memory {
        pairs.push(format!("memory: {}Mi", sandbox.limits.memory >> 20));
    }
    if sandbox.pool > 0 {
        pairs.push(format!("pool: {}", sandbox.pool));
    }
    if let Some(cap) = sandbox.instances {
        pairs.push(format!("instances: {cap}"));
    }
    if map && !sandbox.keyvalue.is_empty() {
        let places: Vec<String> = sandbox
            .keyvalue
            .iter()
            .map(|(identifier, place)| {
                let place = format!("{}/{}", place.store, place.namespace);
                format!("{}: {}", quote(identifier), quote(&place))
            })
            .collect();
        pairs.push(format!("keyvalue: {{{}}}", places.join(", ")));
    }
    if !sandbox.env.is_empty() {
        let vars: Vec<String> = sandbox
            .env
            .iter()
            .map(|(name, value)| format!("{}: {}", quote(name), quote(value)))
            .collect();
        pairs.push(format!("env: {{{}}}", vars.join(", ")));
    }
    (!pairs.is_empty()).then(|| format!("{{{}}}", pairs.join(", ")))
}

/// `text` as a YAML double-quoted scalar: never expanded.
pub(crate) fn quote(text: &str) -> String {
    let mut quoted = String::from('"');
    for c in text.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            c if c.is_control() => quoted.push_str(&format!("\\u{:04x}", c as u32)),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

/// The keyvalue stores: the `keyvalue` section's, else one `sqlite` store
/// (`keyvalue::LOCAL`) at `file`, by default [`DEFAULT_KEYVALUE_FILE`] in
/// `dir`.
fn stores(
    section: Option<&KeyValueFile>,
    file: Option<PathBuf>,
    dir: &Path,
) -> Result<Stores, String> {
    match (section, file) {
        (Some(section), None) => section.stores(dir),
        (Some(_), Some(_)) => Err("keyvalue: the section names the stores; --keyvalue-file \
             (SLUICE_WICKET_WASI_KEYVALUE_FILE) applies only without one"
            .into()),
        (None, file) => Ok(Stores::new(
            BTreeMap::from([(
                keyvalue::LOCAL.to_string(),
                StoreConfig {
                    kind: keyvalue::Kind::Sqlite {
                        path: file.unwrap_or_else(|| dir.join(DEFAULT_KEYVALUE_FILE)),
                        max_namespace_bytes: keyvalue::DEFAULT_MAX_NAMESPACE_BYTES,
                    },
                    limits: StoreLimits::default(),
                },
            )]),
            keyvalue::LOCAL.to_string(),
        )),
    }
}

/// The `keyvalue` section: the stores, process-wide (changes apply on a
/// restart).
#[derive(Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct KeyValueFile {
    /// The store an identifier maps to by default; optional with one store.
    default: Option<String>,
    stores: BTreeMap<String, StoreFile>,
}

impl KeyValueFile {
    /// The stores, paths relative to `dir`; errors name the key.
    fn stores(&self, dir: &Path) -> Result<Stores, String> {
        let mut configs = BTreeMap::new();
        for (name, store) in &self.stores {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
            {
                return Err(format!(
                    "keyvalue.stores: '{name}' is no store name (letters, digits, '-', '.', '_')"
                ));
            }
            let config = store
                .config(dir)
                .map_err(|e| format!("keyvalue.stores.{name}.{e}"))?;
            configs.insert(name.clone(), config);
        }
        let default = match (&self.default, configs.len()) {
            (Some(default), _) if configs.contains_key(default) => default.clone(),
            (Some(default), _) => {
                return Err(format!(
                    "keyvalue.default: no store '{default}' in keyvalue.stores"
                ));
            }
            (None, 1) => configs.keys().next().expect("one store").clone(),
            (None, 0) => return Err("keyvalue.stores: names no store".into()),
            (None, _) => {
                return Err("keyvalue.default: required with more than one store".into());
            }
        };
        Ok(Stores::new(configs, default))
    }
}

/// One store of the `keyvalue` section.
#[derive(Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct StoreFile {
    #[serde(rename = "type")]
    kind: String,
    /// The file of a `sqlite` store, relative to the config file.
    path: Option<PathBuf>,
    max_key_bytes: Option<Scalar>,
    max_value_bytes: Option<Scalar>,
    /// A `sqlite` store's bound on each namespace.
    max_namespace_bytes: Option<Scalar>,
    /// The server of a `redis` store.
    url: Option<String>,
    /// The CA of a `rediss://` server's certificate, relative to the config
    /// file.
    ca_cert: Option<PathBuf>,
    /// What a `redis` store's keys begin with.
    prefix: Option<String>,
    /// A `redis` store's bound on each call.
    timeout: Option<Scalar>,
}

impl StoreFile {
    /// The store's config; errors name the key below the store.
    fn config(&self, dir: &Path) -> Result<StoreConfig, String> {
        let foreign = |keys: &[(&str, bool)]| match keys.iter().find(|(_, set)| *set) {
            Some((key, _)) => Err(format!("{key}: not a key of a {} store", self.kind)),
            None => Ok(()),
        };
        let kind = match self.kind.as_str() {
            "sqlite" => {
                foreign(&[
                    ("url", self.url.is_some()),
                    ("ca-cert", self.ca_cert.is_some()),
                    ("prefix", self.prefix.is_some()),
                    ("timeout", self.timeout.is_some()),
                ])?;
                keyvalue::Kind::Sqlite {
                    path: dir.join(
                        self.path
                            .as_ref()
                            .ok_or("path: required by a sqlite store")?,
                    ),
                    max_namespace_bytes: match &self.max_namespace_bytes {
                        Some(max) => max.bytes("max-namespace-bytes")? as u64,
                        None => keyvalue::DEFAULT_MAX_NAMESPACE_BYTES,
                    },
                }
            }
            #[cfg(feature = "redis")]
            "redis" => {
                foreign(&[
                    ("path", self.path.is_some()),
                    ("max-namespace-bytes", self.max_namespace_bytes.is_some()),
                ])?;
                let url = self.url.as_ref().ok_or("url: required by a redis store")?;
                let tls = keyvalue::check_redis_url(url).map_err(|e| format!("url: {e}"))?;
                if self.ca_cert.is_some() && !tls {
                    return Err("ca-cert: needs a rediss:// url".into());
                }
                keyvalue::Kind::Redis {
                    url: url.clone(),
                    ca_cert: self.ca_cert.as_ref().map(|path| dir.join(path)),
                    prefix: self.prefix.clone().unwrap_or_default(),
                    timeout: match &self.timeout {
                        Some(timeout) => timeout.duration("timeout", false)?,
                        None => keyvalue::DEFAULT_REDIS_TIMEOUT,
                    },
                }
            }
            #[cfg(not(feature = "redis"))]
            "redis" => {
                return Err("type: a build without the redis store (cargo feature redis)".into());
            }
            other => {
                return Err(format!(
                    "type: unknown store type '{other}' (sqlite, redis)"
                ));
            }
        };
        let mut limits = StoreLimits::default();
        if let Some(max) = &self.max_key_bytes {
            limits.max_key_bytes = max.bytes("max-key-bytes")?;
        }
        if let Some(max) = &self.max_value_bytes {
            limits.max_value_bytes = max.bytes("max-value-bytes")?;
        }
        Ok(StoreConfig { kind, limits })
    }
}

/// A YAML scalar read as a number or a string: `0` and `20s` alike.
#[derive(Deserialize, PartialEq, Debug)]
#[serde(untagged)]
enum Scalar {
    Number(u64),
    Text(String),
}

impl Scalar {
    /// A duration: a whole number with a unit (`ms`, `s`, `m`, `h`); a bare
    /// `0` turns an `off`-able setting off.
    fn duration(&self, key: &str, off: bool) -> Result<Duration, String> {
        let text = match self {
            Scalar::Number(0) if off => return Ok(Duration::ZERO),
            Scalar::Number(n) => n.to_string(),
            Scalar::Text(text) => text.trim().to_string(),
        };
        let expected = || {
            format!(
                "{key}: expected a duration like 250ms, 20s, 5m or 1h{}, got '{text}'",
                if off { " (0 = off)" } else { "" }
            )
        };
        if text == "0" && off {
            return Ok(Duration::ZERO);
        }
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(expected)?;
        let number: u64 = text[..split].parse().map_err(|_| expected())?;
        let duration = match &text[split..] {
            "ms" => Duration::from_millis(number),
            "s" => Duration::from_secs(number),
            "m" => Duration::from_secs(number.saturating_mul(60)),
            "h" => Duration::from_secs(number.saturating_mul(3600)),
            _ => return Err(expected()),
        };
        if duration.is_zero() && !off {
            return Err(format!("{key}: must be positive, got '{text}'"));
        }
        Ok(duration)
    }

    /// A size in bytes that is a positive whole number of MiB: a number with
    /// a suffix, binary (`Ki`, `Mi`, `Gi`) or decimal (`K`, `M`, `G`).
    fn mebibytes(&self, key: &str) -> Result<usize, String> {
        let expected =
            |text: &str| format!("{key}: expected a size like 64Mi or 1Gi, got '{text}'");
        let bytes = self.size(false).map_err(|text| expected(&text))?;
        if bytes == 0 || bytes % (1 << 20) != 0 {
            return Err(format!(
                "{key}: must be a positive whole number of MiB (e.g. 64Mi), got '{}'",
                self.text()
            ));
        }
        Ok(bytes)
    }

    /// A positive size in bytes: a bare number, or one with a suffix as
    /// [`Scalar::mebibytes`] takes.
    fn bytes(&self, key: &str) -> Result<usize, String> {
        match self.size(true) {
            Ok(0) => Err(format!("{key}: must be positive, got '{}'", self.text())),
            Ok(bytes) => Ok(bytes),
            Err(text) => Err(format!(
                "{key}: expected a size like 512, 4Ki or 1Mi, got '{text}'"
            )),
        }
    }

    fn text(&self) -> String {
        match self {
            Scalar::Number(n) => n.to_string(),
            Scalar::Text(text) => text.trim().to_string(),
        }
    }

    /// The bytes of a number with a unit suffix (`bare`: or without); Err
    /// holds the text that does not parse.
    fn size(&self, bare: bool) -> Result<usize, String> {
        let text = self.text();
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len());
        let number: u64 = text[..split].parse().map_err(|_| text.clone())?;
        let unit: u64 = match &text[split..] {
            "" if bare => 1,
            "Ki" => 1 << 10,
            "Mi" => 1 << 20,
            "Gi" => 1 << 30,
            "K" => 1_000,
            "M" => 1_000_000,
            "G" => 1_000_000_000,
            _ => return Err(text),
        };
        number
            .checked_mul(unit)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(text)
    }
}

/// The value of a `--max-*` limit: a number from 1 to `max`.
pub fn parse_limit(name: &str, value: &str, max: usize) -> Result<usize, String> {
    let flag = format!("--{name}");
    match value.parse() {
        Ok(limit) => in_range(&flag, limit, max),
        Err(_) if max == usize::MAX => {
            Err(format!("{flag} must be a positive number, got '{value}'"))
        }
        Err(_) => Err(format!(
            "{flag} must be a number from 1 to {max}, got '{value}'"
        )),
    }
}

/// `limit` of the setting named `key`, from 1 to `max`.
fn in_range(key: &str, limit: usize, max: usize) -> Result<usize, String> {
    match limit {
        limit if limit > 0 && limit <= max => Ok(limit),
        _ if max == usize::MAX => Err(format!("{key} must be a positive number, got '{limit}'")),
        _ => Err(format!(
            "{key} must be a number from 1 to {max}, got '{limit}'"
        )),
    }
}

/// The value of a duration flag in whole seconds; `0` turns it off.
pub fn parse_seconds(name: &str, value: &str) -> Result<Duration, String> {
    value
        .parse()
        .map(Duration::from_secs)
        .map_err(|_| format!("--{name} must be a number of seconds (0 = off), got '{value}'"))
}

/// Splits `locator[?budget-ms=N&instances=N&memory-mib=N&pool=N&wasi=cap,...]`
/// into the locator and the per-route sandbox. The `?` separator means literal `?` in file
/// paths needs URL-escaping (`file://...%3F...`).
pub fn parse_route_target(spec: &str) -> Result<(String, Sandbox), String> {
    match spec.split_once('?') {
        Some((target, query)) => Ok((target.to_string(), Sandbox::parse(query)?)),
        None => Ok((spec.to_string(), Sandbox::default())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::route_target;
    use crate::routes::tests::Scratch;
    use crate::wasm_host;

    fn args(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    fn settings(args: &[&str]) -> Result<Settings, String> {
        settings_over(args, |_| None)
    }

    fn settings_over(
        args: &[&str],
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Settings, String> {
        match parse(self::args(args), env)? {
            Command::Run(settings) => Ok(*settings),
            _ => panic!("not a run command"),
        }
    }

    /// Every setting has its key; a flag overrides the environment, the
    /// environment the file, the file a default; `${NAME}` reads the
    /// environment; relative paths are the config file's; the flags' routes
    /// follow the file's. A keyvalue route's map names the section's stores,
    /// its namespace by default the host; a flag's has the default map. A
    /// route's env takes scalars as text, its target their digest. A reload
    /// reads the routes again and names the other settings changed since.
    #[test]
    fn the_config_file_sets_every_setting_under_the_environment_and_the_flags() {
        let dir = Scratch::new("settings");
        let yaml = |server: &str| {
            format!(
                "server: {server}
token: ${{HOME}}$${{literal}}
client-id: ${{SLUICE_WICKET_TEST_UNSET:-file-id}}
insecure: true
ca-cert: ca.pem
max-connections: 10
max-instances: 8
tunnel-stall-timeout: 90s
drain-grace: 0
watch-interval: 250ms
listen: 127.0.0.1:8080
console: 127.0.0.1:9090
console-token: t0ken
console-dir: kept
flume: grpcs://flume.example:443
flume-token: ${{SLUICE_WICKET_TEST_UNSET:-f1ume}}
flume-ca-cert: flume-ca.pem
flume-insecure: true
keyvalue:
  default: local
  stores:
    local:
      type: sqlite
      path: kv.db
      max-key-bytes: 512
      max-value-bytes: 4Ki
      max-namespace-bytes: 1Mi
    shared:
      type: sqlite
      path: /var/shared.db
    cache:
      type: redis
      url: rediss://:${{SLUICE_WICKET_TEST_UNSET:-secret}}@cache.example:6380/2
      ca-cert: cache-ca.pem
      prefix: 'wicket:'
      timeout: 2s
      max-value-bytes: 64Ki
routes:
  - host: a.local
    component: a.wasm
    sandbox:
      wasi: [cli, inherit-network]
      budget: 250ms
      memory: 64Mi
      pool: 2
      instances: 4
  - host: b.local
    component: https://example.invalid/b.wasm
    sandbox:
      wasi: [keyvalue]
      keyvalue:
        \"\": local
        sessions: shared/demo sessions
      env:
        GREETING: hello
        PORT: 8080
        TOKEN: ${{SLUICE_WICKET_TEST_UNSET:-s3cr3t}}
"
            )
        };
        let path = dir.write("wicket.yaml", yaml("grpc://file:8001"));
        let config = path.to_str().unwrap();

        let file = settings(&["--config", config]).unwrap();
        assert_eq!(file.server.as_deref(), Some("grpc://file:8001"));
        assert_eq!(file.listen.as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(
            file.token,
            format!("{}${{literal}}", std::env::var("HOME").unwrap())
        );
        assert_eq!(file.client_id, "file-id");
        assert!(file.insecure);
        assert_eq!(file.ca_cert, Some(dir.0.join("ca.pem")));
        assert_eq!(file.max_connections, 10);
        assert_eq!(file.max_instances, 8);
        assert_eq!(file.stall_timeout, Duration::from_secs(90));
        assert_eq!(file.drain_grace, Duration::ZERO);
        assert_eq!(file.watch_interval, Duration::from_millis(250));
        let console = file.console.as_ref().expect("a console");
        assert_eq!(console.address, "127.0.0.1:9090");
        assert_eq!(console.token.as_deref(), Some("t0ken"));
        assert_eq!(console.dir, dir.0.join("kept"));
        assert_eq!(
            file.flume,
            Some(FlumeSettings {
                url: "grpcs://flume.example:443".into(),
                token: "f1ume".into(),
                ca_cert: Some(dir.0.join("flume-ca.pem")),
                insecure: true,
            })
        );
        let (routes, warnings) = file.routes.read().unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(routes[0].host, "a.local");
        assert_eq!(routes[0].component, Locator::File(dir.0.join("a.wasm")));
        assert_eq!(
            route_target("a", &routes[0].sandbox),
            "a?budget-ms=250&instances=4&memory-mib=64&pool=2&wasi=cli,inherit-network"
        );
        assert_eq!(
            routes[1].component,
            Locator::parse("https://example.invalid/b.wasm").unwrap()
        );
        let vars = &routes[1].sandbox.env;
        let text = |pairs: [(&str, &str); 3]| {
            pairs.map(|(name, value)| (name.to_string(), value.to_string()))
        };
        assert_eq!(
            vars.map(),
            &BTreeMap::from(text([
                ("GREETING", "hello"),
                ("PORT", "8080"),
                ("TOKEN", "s3cr3t")
            ]))
        );
        assert_eq!(
            route_target("b", &routes[1].sandbox),
            format!(
                "b?env={}&keyvalue=:local/b.local,sessions:shared/demo%20sessions&wasi=keyvalue",
                vars.digest()
            )
        );
        let stores = file.routes.stores();
        assert_eq!(stores.default_store(), "local");
        assert_eq!(
            stores.config("local"),
            Some(&StoreConfig {
                kind: keyvalue::Kind::Sqlite {
                    path: dir.0.join("kv.db"),
                    max_namespace_bytes: 1 << 20,
                },
                limits: StoreLimits {
                    max_key_bytes: 512,
                    max_value_bytes: 4 << 10,
                },
            })
        );
        assert_eq!(
            stores.config("shared"),
            Some(&StoreConfig {
                kind: keyvalue::Kind::Sqlite {
                    path: "/var/shared.db".into(),
                    max_namespace_bytes: 64 << 20,
                },
                limits: StoreLimits {
                    max_key_bytes: 1 << 10,
                    max_value_bytes: 1 << 20,
                },
            }),
            "the defaults"
        );
        assert_eq!(
            stores.config("cache"),
            Some(&StoreConfig {
                kind: keyvalue::Kind::Redis {
                    url: "rediss://:secret@cache.example:6380/2".into(),
                    ca_cert: Some(dir.0.join("cache-ca.pem")),
                    prefix: "wicket:".into(),
                    timeout: Duration::from_secs(2),
                },
                limits: StoreLimits {
                    max_key_bytes: 1 << 10,
                    max_value_bytes: 64 << 10,
                },
            })
        );

        let env = |name: &str| match name {
            "SLUICE_WICKET_WASI_MAX_CONNECTIONS" => Some("12".to_string()),
            "SLUICE_WICKET_WASI_MAX_INSTANCES" => Some("32".to_string()),
            "SLUICE_WICKET_WASI_TOKEN" => Some(String::new()),
            "SLUICE_WICKET_WASI_FLUME_TOKEN" => Some("env-token".to_string()),
            _ => None,
        };
        let flagged = settings_over(
            &[
                "--config",
                config,
                "--server",
                "grpc://flag:1",
                "--max-instances=16",
                "--drain-grace",
                "5",
                "--ca-cert",
                "flag.pem",
                "--listen",
                "0.0.0.0:9",
                "--console-dir",
                "/var/kept",
                "--flume",
                "grpc://flag:8002",
                "--wasm=c.local=c.wasm?pool=1&wasi=keyvalue",
            ],
            env,
        )
        .unwrap();
        assert_eq!(flagged.server.as_deref(), Some("grpc://flag:1"));
        assert_eq!(flagged.listen.as_deref(), Some("0.0.0.0:9"));
        assert_eq!(flagged.max_instances, 16, "the flag's");
        assert_eq!(flagged.max_connections, 12, "the environment's");
        assert_eq!(flagged.drain_grace, Duration::from_secs(5));
        assert_eq!(flagged.ca_cert, Some(PathBuf::from("flag.pem")));
        assert_eq!(flagged.stall_timeout, Duration::from_secs(90), "the file's");
        assert_eq!(flagged.token, file.token, "an empty variable is unset");
        let console = flagged.console.as_ref().expect("the file's console");
        assert_eq!(console.dir, PathBuf::from("/var/kept"));
        assert_eq!(console.token.as_deref(), Some("t0ken"));
        let flume = flagged.flume.as_ref().expect("a flume");
        assert_eq!(flume.url, "grpc://flag:8002");
        assert_eq!(flume.token, "env-token");
        assert_eq!(
            flume.ca_cert,
            Some(dir.0.join("flume-ca.pem")),
            "the file's"
        );
        let (routes, _) = flagged.routes.read().unwrap();
        let hosts: Vec<_> = routes.iter().map(|route| route.host.as_str()).collect();
        assert_eq!(hosts, ["a.local", "b.local", "c.local"]);
        assert_eq!(routes[2].component, Locator::File("c.wasm".into()));
        assert_eq!(
            route_target("c", &routes[2].sandbox),
            "c?keyvalue=:local/c.local&pool=1&wasi=keyvalue"
        );

        dir.write("wicket.yaml", yaml("grpc://moved:8001"));
        let (_, warnings) = file.routes.read().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].ends_with(": server changed since startup; applies on a restart"));

        dir.write("wicket.yaml", "server: \"${HOME}\"\n");
        let defaults = settings(&["--config", config]).unwrap();
        assert_eq!(
            defaults.server.as_deref(),
            Some("${HOME}"),
            "quoted: not expanded"
        );
        assert_eq!(defaults.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(defaults.max_instances, DEFAULT_MAX_INSTANCES);
        assert_eq!(defaults.stall_timeout, DEFAULT_STALL_TIMEOUT);
        assert_eq!(defaults.drain_grace, DEFAULT_DRAIN_GRACE);
        assert_eq!(defaults.watch_interval, DEFAULT_WATCH_INTERVAL);
        assert!(!defaults.insecure);
        assert_eq!(defaults.listen, None);
        assert!(defaults.console.is_none());
        assert_eq!(defaults.flume, None);
        assert_eq!(defaults.routes.read().unwrap().0, [], "no route yet");
        let sqlite = |settings: Settings| match settings.routes.stores().config(keyvalue::LOCAL) {
            Some(StoreConfig {
                kind: keyvalue::Kind::Sqlite { path, .. },
                ..
            }) => path.clone(),
            other => panic!("one sqlite store: {other:?}"),
        };
        assert_eq!(sqlite(defaults), dir.0.join("keyvalue.db"));
        let moved = settings(&["--config", config, "--keyvalue-file", "/var/kv.db"]).unwrap();
        assert_eq!(sqlite(moved), PathBuf::from("/var/kv.db"));

        let local = settings(&[
            "--listen",
            "127.0.0.1:0",
            "--wasm",
            "a=a.wasm",
            "--flume=grpcs://f:1",
            "--flume-token=t",
            "--flume-insecure",
        ])
        .unwrap();
        assert_eq!(local.server, None, "local mode alone");
        assert_eq!(
            local.flume,
            Some(FlumeSettings {
                url: "grpcs://f:1".into(),
                token: "t".into(),
                ca_cert: None,
                insecure: true,
            })
        );
        let console = settings(&["--listen", "127.0.0.1:0", "--console", "127.0.0.1:0"])
            .unwrap()
            .console
            .expect("routes deployed at runtime only");
        assert_eq!(console.token, None);
        assert_eq!(
            console.dir,
            PathBuf::from("deployed"),
            "the working directory's"
        );
        assert_eq!(
            sqlite(local),
            PathBuf::from("keyvalue.db"),
            "the working directory's"
        );
    }

    /// An unknown key or a value that does not parse fails, naming it -- and
    /// a line, never the file's text: a route's env is no log's business.
    #[test]
    fn a_bad_config_names_what_failed() {
        let dir = Scratch::new("bad");
        let route = |sandbox: &str| {
            format!(
                "server: s\nroutes:\n  - {{host: a, component: a.wasm, sandbox: {{{sandbox}}}}}\n"
            )
        };
        let cases = [
            ("serverx: s\n".to_string(), "unknown field `serverx`"),
            (
                "token: x\n".into(),
                "--server or --listen (or `server` / `listen` in the --config file) is required",
            ),
            (
                "server: s\ntoken: ${SLUICE_WICKET_TEST_UNSET}\n".into(),
                "SLUICE_WICKET_TEST_UNSET",
            ),
            (
                "server: s\nmax-instances: 0\n".into(),
                "max-instances must be a number from 1 to",
            ),
            (
                "server: s\nflume: grpc://f:1\n".into(),
                "--flume-token (or `flume-token` in the --config file) is required with --flume",
            ),
            (
                "server: s\nflume: f:1\nflume-token: t\n".into(),
                "--flume: expected grpc://host:port or grpcs://host:port, got 'f:1'",
            ),
            (
                "server: s\ndrain-grace: 20\n".into(),
                "drain-grace: expected a duration like 250ms, 20s, 5m or 1h (0 = off), got '20'",
            ),
            (route("fuel: 1"), "unknown field `fuel`"),
            (route("wasi: [fs]"), "unknown wasi capability 'fs'"),
            (route("wasi: [tcp]"), "need 'cli'"),
            (route("budget: 0ms"), "sandbox.budget: must be positive"),
            (route("budget: 1d"), "sandbox.budget: expected a duration"),
            (
                route("memory: 64M"),
                "routes[0]: sandbox.memory: must be a positive whole number of MiB",
            ),
            (route("memory: 64"), "sandbox.memory: expected a size like 64Mi or 1Gi"),
            (route("instances: 0"), "sandbox.instances: must be at least 1"),
            (route("pool: 2, instances: 1"), "instances=1 is below pool=2"),
            (
                "server: s\nkeyvalue: {stores: {local: {type: mem}}}\n".into(),
                "keyvalue.stores.local.type: unknown store type 'mem' (sqlite, redis)",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: sqlite}}}\n".into(),
                "keyvalue.stores.local.path: required by a sqlite store",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: sqlite, path: x, prefix: p}}}\n"
                    .into(),
                "keyvalue.stores.local.prefix: not a key of a sqlite store",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: redis, url: 'redis://r', path: x}}}\n"
                    .into(),
                "keyvalue.stores.local.path: not a key of a redis store",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: redis}}}\n".into(),
                "keyvalue.stores.local.url: required by a redis store",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: redis, url: 'http://r'}}}\n".into(),
                "keyvalue.stores.local.url: ",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: redis, url: 'redis://r', ca-cert: c}}}\n"
                    .into(),
                "keyvalue.stores.local.ca-cert: needs a rediss:// url",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: redis, url: 'redis://r', timeout: 0s}}}\n"
                    .into(),
                "keyvalue.stores.local.timeout: must be positive",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: sqlite, path: x, max-key-bytes: 0}}}\n"
                    .into(),
                "keyvalue.stores.local.max-key-bytes: must be positive",
            ),
            (
                "server: s\nkeyvalue: {stores: {local: {type: sqlite, path: x, max-namespace-bytes: 1Q}}}\n"
                    .into(),
                "max-namespace-bytes: expected a size like 512, 4Ki or 1Mi, got '1Q'",
            ),
            (
                "server: s\nkeyvalue: {stores: {a b: {type: sqlite, path: x}}}\n".into(),
                "'a b' is no store name",
            ),
            (
                "server: s\nkeyvalue: {stores: {a: {type: sqlite, path: a}, b: {type: sqlite, path: b}}}\n"
                    .into(),
                "keyvalue.default: required with more than one store",
            ),
            (
                "server: s\nkeyvalue: {default: c, stores: {a: {type: sqlite, path: a}}}\n".into(),
                "keyvalue.default: no store 'c' in keyvalue.stores",
            ),
            (
                route("keyvalue: {'': local}"),
                "sandbox.keyvalue: needs the capability (wasi: [keyvalue])",
            ),
            (
                route("wasi: [keyvalue], keyvalue: {'': nowhere}"),
                "routes[0]: sandbox.keyvalue.\"\": unknown keyvalue store 'nowhere' (local)",
            ),
            (
                route("wasi: [keyvalue], keyvalue: {s: local/}"),
                "'local/': empty namespace",
            ),
            (
                route("wasi: [keyvalue], keyvalue: {}"),
                "sandbox.keyvalue: maps no identifier",
            ),
            (
                route("env: {'A=B': s3cr3t}"),
                "routes[0]: sandbox.env: \"A=B\" is no variable name",
            ),
            (route("env: {K: [s3cr3t]}"), "expected string scalar at line 3"),
            (route("env: s3cr3t"), "expected mapping start at line 3"),
            (route("env: {K: s3cr3t}, fuel: 1"), "unknown field `fuel`"),
            (
                "server: s\nroutes:\n  - {host: a, component: a.wasm}\n  - {host: a, component: b.wasm}\n"
                    .into(),
                "route host 'a' is configured twice: by the config file",
            ),
        ];
        for (yaml, error) in cases {
            let path = dir.write("wicket.yaml", &yaml);
            let failed = settings(&["--config", path.to_str().unwrap()])
                .and_then(|settings| settings.routes.read().map(|_| ()));
            assert!(
                matches!(&failed, Err(e) if e.contains(error) && !e.contains("s3cr3t")),
                "{yaml}: {:?}",
                failed.err()
            );
        }
        let path = dir.write(
            "wicket.yaml",
            "server: s\nroutes:\n  - {host: a, component: a.wasm}\n",
        );
        let twice = settings(&["--config", path.to_str().unwrap(), "--wasm", "a=b.wasm"])
            .unwrap()
            .routes
            .read();
        assert_eq!(
            twice.err().as_deref(),
            Some("route host 'a' is configured twice: by the config file and by a --wasm flag"),
            "a file's host given as a flag too"
        );
        let path = dir.write(
            "wicket.yaml",
            "server: s\nkeyvalue: {stores: {local: {type: sqlite, path: x}}}\n",
        );
        let moved = settings(&["--config", path.to_str().unwrap(), "--keyvalue-file", "y"]);
        assert!(
            matches!(&moved, Err(e) if e.ends_with("--keyvalue-file \
                (SLUICE_WICKET_WASI_KEYVALUE_FILE) applies only without one")),
            "{:?}",
            moved.err()
        );
    }

    #[test]
    fn rejects_a_command_line_it_cannot_read() {
        let none = |_: &str| None;
        let error = |line: &[&str]| match parse(args(line), none) {
            Err(e) => e,
            Ok(_) => panic!("{line:?} parsed"),
        };
        assert_eq!(error(&["--colour"]), "unknown option: --colour");
        assert_eq!(error(&["--server"]), "--server needs a value");
        assert_eq!(error(&["grpc://s:1"]), "unexpected argument: grpc://s:1");
        assert!(error(&["--server", "s"]).contains("at least one --wasm"));
        assert!(error(&["--wasm", "a=a.wasm"]).contains("--server or --listen"));
        assert!(error(&["--wasm", "a.wasm"]).contains("host=component-locator"));
        assert!(matches!(
            parse(args(&["--server", "s", "--help"]), none),
            Ok(Command::Help)
        ));
        assert!(matches!(parse(args(&["-V"]), none), Ok(Command::Version)));
    }

    #[test]
    fn limits_take_a_number_from_one_to_their_max() {
        assert_eq!(parse_limit("max-instances", "8", 64), Ok(8));
        assert_eq!(parse_limit("max-instances", "64", 64), Ok(64));
        for bad in ["0", "65", "-1", "x", ""] {
            assert_eq!(
                parse_limit("max-instances", bad, 64),
                Err(format!(
                    "--max-instances must be a number from 1 to 64, got '{bad}'"
                )),
            );
        }
        assert_eq!(
            parse_limit("max-connections", "0", usize::MAX),
            Err("--max-connections must be a positive number, got '0'".to_string()),
        );
    }

    #[test]
    fn durations_take_whole_seconds_and_zero_turns_them_off() {
        assert_eq!(
            parse_seconds("tunnel-stall-timeout", "90"),
            Ok(Duration::from_secs(90))
        );
        assert_eq!(
            parse_seconds("tunnel-stall-timeout", "0"),
            Ok(Duration::ZERO)
        );
        for bad in ["-1", "1.5", "60s", ""] {
            assert_eq!(
                parse_seconds("tunnel-stall-timeout", bad),
                Err(format!(
                    "--tunnel-stall-timeout must be a number of seconds (0 = off), got '{bad}'"
                )),
            );
        }
    }

    #[test]
    fn route_target_without_options_keeps_defaults() {
        let (target, sandbox) =
            parse_route_target("target/wasm32-wasip3/release/hello.wasm").unwrap();
        assert_eq!(target, "target/wasm32-wasip3/release/hello.wasm");
        assert_eq!(sandbox, Sandbox::default());
        assert_eq!(
            route_target("wasm:file:///hello.wasm", &sandbox),
            "wasm:file:///hello.wasm"
        );
    }

    #[test]
    fn route_target_options_override_limits() {
        let (target, sandbox) = parse_route_target("./a.wasm?budget-ms=250&memory-mib=64").unwrap();
        assert_eq!(target, "./a.wasm");
        assert_eq!(sandbox.limits.budget, Duration::from_millis(250));
        assert_eq!(sandbox.limits.memory, 64 << 20);
        assert_eq!(sandbox.caps, wasm_host::Caps::default());
    }

    #[test]
    fn route_target_grants_wasi_capabilities() {
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=inherit-network,cli").unwrap();
        assert_eq!(
            sandbox.caps,
            wasm_host::Caps {
                cli: true,
                inherit_network: true,
                ..Default::default()
            }
        );
    }

    #[test]
    fn route_target_rejects_unknown_capabilities_and_options() {
        let error = parse_route_target("./a.wasm?wasi=cli,fs").unwrap_err();
        assert!(error.contains("unknown wasi capability 'fs'"), "{error}");
        let error = parse_route_target("./a.wasm?fuel=1").unwrap_err();
        assert!(error.contains("unknown option 'fuel'"), "{error}");
    }

    /// `http` is accepted for flag parity with `-S http` but changes nothing:
    /// outgoing http is always linked, as under `wasmtime serve`, so it does
    /// not split the advertised target either.
    #[test]
    fn route_target_accepts_http_as_always_granted() {
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=http").unwrap();
        assert_eq!(sandbox, Sandbox::default());
        let (_, sandbox) = parse_route_target("./a.wasm?wasi=http,cli").unwrap();
        assert_eq!(
            route_target("wasm:file:///a.wasm", &sandbox),
            "wasm:file:///a.wasm?wasi=cli"
        );
    }

    /// `instances=N` caps the route's slots and is part of the advertised
    /// target; a cap equal to the pool is accepted.
    #[test]
    fn route_target_caps_the_routes_instances() {
        let (_, sandbox) = parse_route_target("./a.wasm?pool=2&instances=4").unwrap();
        assert_eq!(sandbox.instances, Some(4));
        assert_eq!(sandbox.pool, 2);
        assert_eq!(
            route_target("wasm:file:///a.wasm", &sandbox),
            "wasm:file:///a.wasm?instances=4&pool=2"
        );
        let (_, sandbox) = parse_route_target("./a.wasm?instances=2&pool=2").unwrap();
        assert_eq!(sandbox.instances, Some(2));
        let (_, sandbox) = parse_route_target("./a.wasm").unwrap();
        assert_eq!(sandbox.instances, None, "no cap unless set");
    }

    /// A zero cap or limit, a cap below the pool (in either order) and a
    /// non-number are startup errors.
    #[test]
    fn route_target_rejects_zero_and_a_cap_below_the_pool() {
        for zero in ["instances", "budget-ms", "memory-mib"] {
            assert_eq!(
                parse_route_target(&format!("./a.wasm?{zero}=0")).unwrap_err(),
                format!("{zero}: must be at least 1")
            );
        }
        assert_eq!(
            parse_route_target("./a.wasm?memory-mib=18446744073709551615").unwrap_err(),
            "memory-mib: too large '18446744073709551615'"
        );
        let below = "instances=1 is below pool=2: every pooled unit holds one of the route's \
                     instance slots";
        assert_eq!(
            parse_route_target("./a.wasm?instances=1&pool=2").unwrap_err(),
            below
        );
        assert_eq!(
            parse_route_target("./a.wasm?pool=2&instances=1").unwrap_err(),
            below
        );
        assert_eq!(
            parse_route_target("./a.wasm?instances=many").unwrap_err(),
            "instances: not a number 'many'"
        );
    }

    #[test]
    fn network_capabilities_require_cli() {
        let error = parse_route_target("./a.wasm?wasi=inherit-network").unwrap_err();
        assert!(error.contains("need 'cli'"), "{error}");
    }

    /// The advertised target carries the canonical sandbox query, so two
    /// routes on one component with different sandboxes get distinct dial
    /// addresses, and equal sandboxes share one regardless of spelling.
    #[test]
    fn route_target_canonicalizes_the_sandbox_query() {
        let (_, a) =
            parse_route_target("a.wasm?wasi=tcp,cli,inherit-network&memory-mib=64").unwrap();
        let (_, b) =
            parse_route_target("a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp").unwrap();
        let (_, c) = parse_route_target("a.wasm?budget-ms=10000&wasi=cli").unwrap();
        assert_eq!(
            route_target("wasm:file:///a.wasm", &a),
            "wasm:file:///a.wasm?memory-mib=64&wasi=cli,inherit-network,tcp"
        );
        assert_eq!(
            route_target("wasm:file:///a.wasm", &a),
            route_target("wasm:file:///a.wasm", &b)
        );
        assert_eq!(
            route_target("wasm:file:///a.wasm", &c),
            "wasm:file:///a.wasm?wasi=cli"
        );
    }
}
