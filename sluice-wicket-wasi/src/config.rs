//! The settings: the command line, the environment and the `--config` YAML
//! file, merged -- an option `--name <value>` overrides `SLUICE_WICKET_WASI_<NAME>`
//! (upper case, `-` as `_`), which overrides the file, the file a default. The
//! file's routes are read again by every reload (`routes::Reloader`); its other
//! settings apply at startup only, and a reload reports the ones changed since.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::routes::Spec;
use crate::wasm_host::{Caps, Instances, Locator, Sandbox};

pub const USAGE: &str = "\
usage: sluice-wicket-wasi --server <url> --wasm <host>=<locator>[?<options>] ... [options]
       sluice-wicket-wasi --config <file> [options]

  --server <url>                  bootstrap endpoint, grpc://host:port or grpcs://host:port
  --wasm <host>=<locator>         a route: a component path, file:// or http(s):// url;
                                  repeatable. ?budget-ms=N&instances=N&memory-mib=N&pool=N
                                  &wasi=<cap>,... sets its sandbox
  --config <file>                 YAML settings and routes, the routes reloaded on change
  --token <token>                 bearer token of the tunnel streams
  --client-id <id>                client identity (default a random UUID)
  --ca-cert <path>                PEM CA verifying a grpcs:// server (default the webPKI roots)
  --insecure                      skip the verification of a grpcs:// server
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

pub struct Settings {
    pub server: String,
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
    pub routes: RouteSource,
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
    token: Option<String>,
    client_id: Option<String>,
    insecure: bool,
    ca_cert: Option<PathBuf>,
    max_connections: Option<usize>,
    max_instances: Option<usize>,
    stall_timeout: Option<Duration>,
    drain_grace: Option<Duration>,
    watch_interval: Option<Duration>,
    routes: Vec<Spec>,
}

const VALUED: &[&str] = &[
    "config",
    "server",
    "token",
    "client-id",
    "ca-cert",
    "max-connections",
    "max-instances",
    "tunnel-stall-timeout",
    "drain-grace",
    "watch-interval",
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
    let mut routes = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--insecure" => insecure = true,
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
        sandbox,
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
        .filter(|server| !server.is_empty())
        .ok_or("--server (or `server` in the --config file) is required")?;
    if flags.config.is_none() && flags.routes.is_empty() {
        return Err(
            "--server and at least one --wasm host=locator (or a --config file) are required"
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
    let routes = RouteSource {
        config: file,
        flags: flags.routes,
    };
    Ok(Settings {
        server,
        token,
        client_id,
        insecure,
        ca_cert,
        max_connections,
        max_instances,
        stall_timeout,
        drain_grace,
        watch_interval,
        routes,
    })
}

/// Where the routes come from: the `--config` file's, then the `--wasm`
/// flags'.
pub struct RouteSource {
    config: Option<ConfigFile>,
    flags: Vec<Spec>,
}

impl RouteSource {
    /// Routes of the flags alone.
    #[cfg(test)]
    pub fn flags(flags: Vec<Spec>) -> RouteSource {
        RouteSource {
            config: None,
            flags,
        }
    }

    /// Routes of the config file at `path` alone.
    #[cfg(test)]
    pub fn file(path: &Path) -> Result<RouteSource, String> {
        Ok(RouteSource {
            config: Some(ConfigFile::read(path)?),
            flags: Vec::new(),
        })
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
                        .spec(&dir)
                        .map_err(|e| format!("{}: routes[{n}]: {e}", config.path.display()))?,
                );
            }
        }
        specs.extend(self.flags.iter().cloned());
        let mut hosts = HashSet::new();
        for spec in &specs {
            if !hosts.insert(spec.host.as_str()) {
                return Err(format!("route host '{}' is configured twice", spec.host));
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
    token: Option<String>,
    client_id: Option<String>,
    insecure: Option<bool>,
    ca_cert: Option<PathBuf>,
    max_connections: Option<usize>,
    max_instances: Option<usize>,
    tunnel_stall_timeout: Option<Scalar>,
    drain_grace: Option<Scalar>,
    watch_interval: Option<Scalar>,
    #[serde(default)]
    routes: Vec<RouteFile>,
}

impl File {
    fn read(path: &Path) -> Result<File, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("--config {}: {e}", path.display()))?;
        let options = serde_saphyr::Options::default().with_properties(std::env::vars().collect());
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
            token => "token",
            client_id => "client-id",
            insecure => "insecure",
            ca_cert => "ca-cert",
            max_connections => "max-connections",
            max_instances => "max-instances",
            tunnel_stall_timeout => "tunnel-stall-timeout",
            drain_grace => "drain-grace",
            watch_interval => "watch-interval",
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
    fn spec(self, dir: &Path) -> Result<Spec, String> {
        let component = match Locator::parse(&self.component)? {
            Locator::File(path) if path.is_relative() && !self.component.contains("://") => {
                Locator::File(dir.join(path))
            }
            locator => locator,
        };
        Ok(Spec {
            host: self.host,
            component,
            sandbox: self.sandbox.sandbox()?,
        })
    }
}

/// A route's sandbox; unset keys keep the defaults.
#[derive(Deserialize, Default, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct SandboxFile {
    /// WASI capabilities (`wasi=` of the flag).
    wasi: Option<Vec<String>>,
    /// The request budget, a duration (`250ms`).
    budget: Option<Scalar>,
    /// The per-instance memory cap, whole MiB (`64Mi`).
    memory: Option<Scalar>,
    pool: Option<usize>,
    instances: Option<usize>,
}

impl SandboxFile {
    fn sandbox(self) -> Result<Sandbox, String> {
        let mut sandbox = Sandbox::default();
        if let Some(wasi) = self.wasi {
            sandbox.caps =
                Caps::parse(&wasi.join(",")).map_err(|e| format!("sandbox.wasi: {e}"))?;
        }
        if let Some(budget) = self.budget {
            let budget = budget.duration("sandbox.budget", false)?;
            if budget.subsec_nanos() % 1_000_000 != 0 {
                return Err(format!(
                    "sandbox.budget: must be whole milliseconds, got {budget:?}"
                ));
            }
            sandbox.limits.budget = budget;
        }
        if let Some(memory) = self.memory {
            sandbox.limits.memory = memory.mebibytes("sandbox.memory")?;
        }
        sandbox.pool = self.pool.unwrap_or(0);
        sandbox.instances = match self.instances {
            Some(0) => return Err("sandbox.instances: must be at least 1".into()),
            cap => cap,
        };
        sandbox.check().map_err(|e| format!("sandbox: {e}"))?;
        Ok(sandbox)
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
        let text = match self {
            Scalar::Number(n) => n.to_string(),
            Scalar::Text(text) => text.trim().to_string(),
        };
        let expected = || format!("{key}: expected a size like 64Mi or 1Gi, got '{text}'");
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(expected)?;
        let number: u64 = text[..split].parse().map_err(|_| expected())?;
        let unit: u64 = match &text[split..] {
            "Ki" => 1 << 10,
            "Mi" => 1 << 20,
            "Gi" => 1 << 30,
            "K" => 1_000,
            "M" => 1_000_000,
            "G" => 1_000_000_000,
            _ => return Err(expected()),
        };
        let bytes = number.checked_mul(unit).ok_or_else(expected)?;
        if bytes == 0 || bytes % (1 << 20) != 0 {
            return Err(format!(
                "{key}: must be a positive whole number of MiB (e.g. 64Mi), got '{text}'"
            ));
        }
        usize::try_from(bytes).map_err(|_| expected())
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
    /// follow the file's. A reload reads the routes again and names the other
    /// settings changed since.
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
"
            )
        };
        let path = dir.write("wicket.yaml", yaml("grpc://file:8001"));
        let config = path.to_str().unwrap();

        let file = settings(&["--config", config]).unwrap();
        assert_eq!(file.server, "grpc://file:8001");
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

        let env = |name: &str| match name {
            "SLUICE_WICKET_WASI_MAX_CONNECTIONS" => Some("12".to_string()),
            "SLUICE_WICKET_WASI_MAX_INSTANCES" => Some("32".to_string()),
            "SLUICE_WICKET_WASI_TOKEN" => Some(String::new()),
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
                "--wasm=c.local=c.wasm?pool=1",
            ],
            env,
        )
        .unwrap();
        assert_eq!(flagged.server, "grpc://flag:1");
        assert_eq!(flagged.max_instances, 16, "the flag's");
        assert_eq!(flagged.max_connections, 12, "the environment's");
        assert_eq!(flagged.drain_grace, Duration::from_secs(5));
        assert_eq!(flagged.ca_cert, Some(PathBuf::from("flag.pem")));
        assert_eq!(flagged.stall_timeout, Duration::from_secs(90), "the file's");
        assert_eq!(flagged.token, file.token, "an empty variable is unset");
        let (routes, _) = flagged.routes.read().unwrap();
        let hosts: Vec<_> = routes.iter().map(|route| route.host.as_str()).collect();
        assert_eq!(hosts, ["a.local", "b.local", "c.local"]);
        assert_eq!(routes[2].component, Locator::File("c.wasm".into()));

        dir.write("wicket.yaml", yaml("grpc://moved:8001"));
        let (_, warnings) = file.routes.read().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].ends_with(": server changed since startup; applies on a restart"));

        dir.write("wicket.yaml", "server: \"${HOME}\"\n");
        let defaults = settings(&["--config", config]).unwrap();
        assert_eq!(defaults.server, "${HOME}", "quoted: not expanded");
        assert_eq!(defaults.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(defaults.max_instances, DEFAULT_MAX_INSTANCES);
        assert_eq!(defaults.stall_timeout, DEFAULT_STALL_TIMEOUT);
        assert_eq!(defaults.drain_grace, DEFAULT_DRAIN_GRACE);
        assert_eq!(defaults.watch_interval, DEFAULT_WATCH_INTERVAL);
        assert!(!defaults.insecure);
        assert_eq!(defaults.routes.read().unwrap().0, [], "no route yet");
    }

    /// An unknown key or a value that does not parse fails, naming it.
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
            ("token: x\n".into(), "--server (or `server` in the --config file) is required"),
            (
                "server: s\ntoken: ${SLUICE_WICKET_TEST_UNSET}\n".into(),
                "SLUICE_WICKET_TEST_UNSET",
            ),
            (
                "server: s\nmax-instances: 0\n".into(),
                "max-instances must be a number from 1 to",
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
                "server: s\nroutes:\n  - {host: a, component: a.wasm}\n  - {host: a, component: b.wasm}\n"
                    .into(),
                "route host 'a' is configured twice",
            ),
        ];
        for (yaml, error) in cases {
            let path = dir.write("wicket.yaml", &yaml);
            let failed = settings(&["--config", path.to_str().unwrap()])
                .and_then(|settings| settings.routes.read().map(|_| ()));
            assert!(
                matches!(&failed, Err(e) if e.contains(error)),
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
            Some("route host 'a' is configured twice"),
            "a file's host given as a flag too"
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

    /// A zero cap, a cap below the pool (in either order) and a non-number
    /// are startup errors.
    #[test]
    fn route_target_rejects_a_cap_below_one_or_the_pool() {
        assert_eq!(
            parse_route_target("./a.wasm?instances=0").unwrap_err(),
            "instances: must be at least 1"
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
