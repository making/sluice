//! The settings: the command line over the environment
//! (`SLUICE_WICKET_FIRECRACKER_<NAME>`, e.g. `SLUICE_WICKET_FIRECRACKER_TOKEN`)
//! over a `--config` YAML file over the defaults. The routes are the file's and
//! the `--vm` flags'; they are read again on a reload ([`RouteSource`]), the
//! other settings only at startup.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use sluice_tunnel::{DEFAULT_DRAIN_GRACE, DEFAULT_MAX_CONNECTIONS, DEFAULT_STALL_TIMEOUT};

use crate::route::RouteSpec;

pub const USAGE: &str = "\
usage: sluice-wicket-firecracker --kernel <vmlinux> --vm <host>=<rootfs.ext4>[?<options>]...
                                 (--server <url> | --listen <host:port>) [options]
       sluice-wicket-firecracker --config <file> [options]

  --server <url>                  the sluice server, grpc://host:port or grpcs://host:port;
                                  asked for its nodes, one tunnel stream to each
  --listen <host:port>            serve the sole route over plain TCP too (no server
                                  needed), relayed as the tunnel's connections are
  --vm <host>=<rootfs>[?<opts>]   a route served by Firecracker microVMs booted from the
                                  rootfs; repeatable. mode=shared (default: one VM serves
                                  every connection) or mode=per-connection (a VM cloned from
                                  a snapshot per connection; warm=N kept restored ahead,
                                  prime=N GET / sent before the snapshot, vms=N the
                                  route's VMs at most, wait-ms=N a connection may wait for
                                  room for its VM, 10000), vcpus=N (1), mem=MiB (128),
                                  port=N (the guest's vsock port, 8080)
  --config <file>                 YAML settings and routes, the routes reloaded on change
  --kernel <path>                 the guest kernel (vmlinux)
  --firecracker <path>            the firecracker binary (default firecracker on the PATH)
  --state-dir <path>              the VMs' files: their sockets need paths below 108 bytes
                                  (default a directory of the process's own under
                                  $XDG_RUNTIME_DIR, else the system's temp dir; a tmpfs
                                  keeps the snapshots off the disk)
  --token <token>                 bearer token of the tunnel streams (on the command line
                                  every user of the host sees it: prefer the environment,
                                  the config file or --token-file)
  --token-file <path>             read the token from a file instead
  --client-id <id>                client identity (default a random UUID)
  --ca-cert <path>                PEM CA verifying a grpcs:// server (default the webPKI roots)
  --insecure                      skip the verification of a grpcs:// server
  --max-connections <n>           connections open at once (default 256)
  --max-vms <n>                   VMs alive at once over every route: shared, warm, in use,
                                  templates (default 64)
  --max-memory <MiB>              their guest memory (mem) at most (default none: --max-vms
                                  x the largest mem bounds it)
  --tunnel-stall-timeout <secs>   give a stream up once its frames wait that long untaken
                                  (default 60, 0 = off)
  --drain-grace <secs>            the SIGTERM drain's bound (default 20, 0 = no drain)
  --watch-interval <secs>         poll the routes' files: the config file, the rootfs
                                  images, the kernel (default 1, 0 = SIGHUP only)
  --version                       print the build (version, commit, build time) and exit

Each option but --vm also reads SLUICE_WICKET_FIRECRACKER_<NAME> from the environment, e.g.
SLUICE_WICKET_FIRECRACKER_TOKEN.";

/// Default of `--max-vms`.
pub const DEFAULT_MAX_VMS: usize = 64;

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
    /// How often the routes' files are polled; zero = SIGHUP only.
    pub watch_interval: Duration,
    /// Where the VMs' files go; None = a directory of the process's own.
    pub state_dir: Option<PathBuf>,
    /// Where the routes come from, read again on a reload.
    pub routes: RouteSource,
    /// The routes at startup.
    pub specs: Vec<RouteSpec>,
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
    "tunnel-stall-timeout",
    "drain-grace",
    "watch-interval",
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
    let mut vms = Vec::new();
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
                    "vm" => vms.push(value),
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
                "SLUICE_WICKET_FIRECRACKER_{}",
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
    let server = flag("server").or_else(|| file.and_then(|file| file.server.clone()));
    let listen = flag("listen").or_else(|| file.and_then(|file| file.listen.clone()));
    if server.is_none() && listen.is_none() {
        return Err("--server or --listen is required".into());
    }
    let routes = RouteSource {
        config: config.as_ref().map(|(_, path)| path.clone()),
        flags: vms,
        kernel: flag("kernel").map(PathBuf::from),
        firecracker: flag("firecracker").map(PathBuf::from),
    };
    let specs = routes.read()?;
    if config.is_none() && specs.is_empty() {
        return Err("--vm is required (or a --config file)".into());
    }
    if listen.is_some() && specs.len() != 1 {
        return Err(format!(
            "listen serves the sole route: {} given",
            specs.len()
        ));
    }
    // a token given, else the file holding it: the flag's (or environment's)
    // over the config file's
    let read = |path: PathBuf| {
        std::fs::read_to_string(&path)
            .map(|token| token.trim().to_string())
            .map_err(|e| format!("--token-file {}: {e}", path.display()))
    };
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
            || env("SLUICE_WICKET_FIRECRACKER_INSECURE").is_some_and(|value| value == "true")
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
        watch_interval: seconds(
            "watch-interval",
            file.and_then(|file| file.watch_interval.as_ref()),
            DEFAULT_WATCH_INTERVAL,
        )?,
        state_dir: flag("state-dir")
            .map(PathBuf::from)
            .or_else(|| file.and_then(|file| in_file(&file.state_dir))),
        routes,
        specs,
    })))
}

/// Where the routes come from: the `--config` file's, then the `--vm` flags'.
pub struct RouteSource {
    config: Option<PathBuf>,
    flags: Vec<String>,
    /// `--kernel` / `--firecracker` (flag or environment), over the file's.
    kernel: Option<PathBuf>,
    firecracker: Option<PathBuf>,
}

impl RouteSource {
    /// The config file, if any.
    pub fn config(&self) -> Option<&Path> {
        self.config.as_deref()
    }

    /// Reads the routes, the config file's again: a host may appear once.
    pub fn read(&self) -> Result<Vec<RouteSpec>, String> {
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
        let firecracker = match self.firecracker.clone().or_else(|| {
            file.as_ref()
                .and_then(|file| file.firecracker.as_ref())
                .map(|path| match path.components().count() {
                    // a bare name is looked up on the PATH
                    1 => path.clone(),
                    _ => dir.join(path),
                })
        }) {
            Some(path) if path.components().count() > 1 => {
                std::path::absolute(path).map_err(|e| format!("--firecracker: {e}"))?
            }
            Some(name) => name,
            None => PathBuf::from("firecracker"),
        };
        let mut specs = Vec::new();
        if let (Some(file), Some(path)) = (file, &self.config) {
            for (n, route) in file.routes.iter().enumerate() {
                let named = format!("{}: routes[{n}]", path.display());
                let rootfs = std::path::absolute(dir.join(&route.rootfs))
                    .map_err(|e| format!("{named}: {e}"))?;
                let options = route.options();
                let options = options.iter().map(|(key, value)| (*key, value.as_str()));
                specs.push(RouteSpec::new(
                    &named,
                    &route.host,
                    rootfs,
                    options,
                    &firecracker,
                    &kernel,
                )?);
            }
        }
        for flag in &self.flags {
            specs.push(RouteSpec::parse(flag, &firecracker, &kernel)?);
        }
        for (n, spec) in specs.iter().enumerate() {
            if specs[..n].iter().any(|other| other.host == spec.host) {
                return Err(format!("{}: routed twice", spec.host));
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
    tunnel_stall_timeout: Option<Scalar>,
    drain_grace: Option<Scalar>,
    watch_interval: Option<Scalar>,
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

/// A route of the `--config` file: the keys of a `--vm` flag's query.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct FileRoute {
    host: String,
    /// Relative to the file.
    rootfs: PathBuf,
    mode: Option<String>,
    vcpus: Option<u32>,
    mem: Option<u32>,
    port: Option<u32>,
    warm: Option<u32>,
    prime: Option<u32>,
    vms: Option<u32>,
    wait_ms: Option<u32>,
}

impl FileRoute {
    fn options(&self) -> Vec<(&'static str, String)> {
        let numbers = [
            ("vcpus", self.vcpus),
            ("mem", self.mem),
            ("port", self.port),
            ("warm", self.warm),
            ("prime", self.prime),
            ("vms", self.vms),
            ("wait-ms", self.wait_ms),
        ];
        self.mode
            .iter()
            .map(|mode| ("mode", mode.clone()))
            .chain(
                numbers
                    .into_iter()
                    .filter_map(|(key, value)| Some((key, value?.to_string()))),
            )
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
    /// come before the flags', a host is routed once.
    #[test]
    fn flags_win_over_the_environment_over_the_file_over_the_defaults() {
        let dir = std::env::temp_dir().join(format!("sfc-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rootfs = dir.join("app.ext4");
        std::fs::write(&rootfs, b"").unwrap();
        let config = dir.join("wicket.yaml");
        std::fs::write(
            &config,
            "server: grpc://file:8001\ntoken: from-file\nkernel: vmlinux\nmax-vms: 8\n\
             drain-grace: 5s\nroutes:\n  - {host: a.local, rootfs: app.ext4, mode: per-connection, warm: 2}\n",
        )
        .unwrap();
        let vm = format!("b.local={}", rootfs.display());
        let config_flag = config.display().to_string();

        let settings = run(
            &["--config", &config_flag, "--vm", &vm],
            &[("SLUICE_WICKET_FIRECRACKER_TOKEN", "from-env")],
        )
        .unwrap();
        assert_eq!(settings.server.as_deref(), Some("grpc://file:8001"));
        assert_eq!(settings.token, "from-env");
        assert_eq!(settings.max_vms, 8);
        assert_eq!(settings.drain_grace, Duration::from_secs(5));
        assert_eq!(settings.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(settings.stall_timeout, DEFAULT_STALL_TIMEOUT);
        let hosts: Vec<_> = settings.specs.iter().map(|spec| &spec.host).collect();
        assert_eq!(hosts, ["a.local", "b.local"]);
        assert_eq!(settings.specs[0].vm.kernel, dir.join("vmlinux"));
        assert_eq!(
            settings.specs[0].target(),
            format!(
                "firecracker:{}?mode=per-connection&warm=2",
                rootfs.display()
            ),
            "the options set to other than their defaults"
        );

        let settings = run(
            &["--config", &config_flag, "--token", "flag", "--max-vms=4"],
            &[("SLUICE_WICKET_FIRECRACKER_TOKEN", "from-env")],
        )
        .unwrap();
        assert_eq!(settings.token, "flag");
        assert_eq!(settings.max_vms, 4);

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
        for (args, error) in [
            (vec!["--kernel", "k", "--vm", &vm], "--server or --listen"),
            (vec!["--server", "s", "--vm", &vm], "--kernel is required"),
            (vec!["--server", "s", "--kernel", "k"], "--vm is required"),
            (
                vec!["--config", &config_flag, "--vm", "c.local=missing"],
                "missing: No such file or directory",
            ),
            (
                vec!["--config", &config_flag, "--vm", &twice],
                "a.local: routed twice",
            ),
            (
                vec!["--listen", "l", "--config", &config_flag, "--vm", &vm],
                "listen serves the sole route: 2 given",
            ),
            (
                vec!["--config", &config_flag, "--token-file", "missing"],
                "--token-file missing:",
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
